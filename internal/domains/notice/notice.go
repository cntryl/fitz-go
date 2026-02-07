package notice

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cntryl/cntryl-go/internal/core/transport"
)

// Wire operation codes for Notice domain (per CLIENT_SPEC.md semantics).

// Wire operation codes for Notice domain. Values are low-byte uint8 equivalents.
const (
	NoticePublish        uint8 = 100
	NoticeSubscribe      uint8 = 101
	NoticeUnsubscribe    uint8 = 102
	NoticeUnsubscribeAll uint8 = 103
	NoticeNotify         uint8 = 104
)

type Client interface {
	Subscribe(ctx context.Context, route string, handler NoticeHandler) (Subscription, error)
	Publish(ctx context.Context, route string, body []byte) error
}

type NoticeHandler func(context.Context, NoticeMsg) error

type Subscription interface {
	Unsubscribe()
}

type NoticeMsg struct {
	Route    string
	Metadata NoticeMetadata
	Body     []byte
}

type NoticeMetadata map[string]string

// Internal mux provider abstraction so tests can inject mocks.
type muxProvider interface {
	Send(transport.Frame) error
	In() <-chan transport.Frame
	OnReconnect(func())
}

// client is a concrete implementation of notice.Client backed by the transport mux.
// It manages local subscriptions and dispatches inbound notice frames to handlers.
type client struct {
	mux        muxProvider
	mu         sync.Mutex
	subs       map[string][]*subscription
	ackMu      sync.Mutex
	ackWaiters map[string][]chan error
	once       sync.Once
}

// NewClient creates a new Notice domain client backed by the transport mux.
func NewClient(mux muxProvider) Client {
	c := &client{mux: mux, subs: make(map[string][]*subscription), ackWaiters: make(map[string][]chan error)}
	// On reconnect, re-emit all active subscriptions (best-effort).
	// NOTE: After a reconnect the client may miss notices published while the
	// transport was disconnected. Fitz notices are best-effort, not durable.
	mux.OnReconnect(func() {
		c.resubscribeAll(context.Background())
	})
	c.startRecv()
	return c
}

// subscription represents a single local subscription and runs a goroutine that
// invokes the user's handler serially for messages delivered to this subscription.
type subscription struct {
	route   string
	handler NoticeHandler
	inbox   chan NoticeMsg
	client  *client
	wg      sync.WaitGroup
	once    sync.Once
	mu      sync.Mutex
	// done is closed when the subscription is unsubscribed. Delivery selects
	// on done to avoid send-on-closed-channel races while preserving
	// blocking backpressure semantics.
	done chan struct{}
}

// Unsubscribe removes the subscription and waits for any in-flight handler to finish.
// It is safe to call multiple times.
func (s *subscription) Unsubscribe() {
	s.once.Do(func() {
		// signal done so concurrent deliverers stop blocking and the handler
		// loop can exit when current in-flight handler completes.
		close(s.done)

		// remove from client; if last for route, notify broker
		s.client.mu.Lock()
		chs := s.client.subs[s.route]
		for i, ss := range chs {
			if ss == s {
				chs = append(chs[:i], chs[i+1:]...)
				break
			}
		}
		if len(chs) == 0 {
			delete(s.client.subs, s.route)
			s.client.mu.Unlock()
			// send unsubscribe to broker; ignore error
			_ = s.client.sendUnsubscribe(context.Background(), s.route)
		} else {
			s.client.subs[s.route] = chs
			s.client.mu.Unlock()
		}

		// wait for any in-flight handler invocation to complete
		s.wg.Wait()
	})
}

// startRecv begins a background goroutine that reads inbound frames and dispatches
// notice frames to matching subscriptions.
func (c *client) startRecv() {
	c.once.Do(func() {
		go func() {
			for f := range c.mux.In() {
				if f.Type == NoticeNotify {
					route, body, ok := decodeNotify(f.Body)
					if !ok {
						continue
					}
					msg := NoticeMsg{Route: route, Metadata: nil, Body: body}
					c.mu.Lock()
					var targets []*subscription
					for pat, subs := range c.subs {
						if noticeMatchRoute(pat, route) {
							targets = append(targets, subs...)
						}
					}
					c.mu.Unlock()
					for _, s := range targets {
						s.deliver(msg)
					}
					continue
				}
				if isNoticeResponseType(f.Type) {
					routeKey, err := decodeNoticeResponseKey(f.Type, f.Body)
					c.notifyWaiters(routeKey, err)
					continue
				}
			}
		}()
	})
}

// noticeMatchRoute duplicates the simulator matching logic (single '*' and multi '**').
func noticeMatchRoute(pattern, route string) bool {
	pat := stripNoticeScheme(pattern)
	rt := stripNoticeScheme(route)
	if pat == rt {
		return true
	}
	pSegs := strings.Split(pat, "/")
	rSegs := strings.Split(rt, "/")
	pi, ri := 0, 0
	for pi < len(pSegs) && ri < len(rSegs) {
		if pSegs[pi] == "**" {
			// Match rest
			if pi == len(pSegs)-1 {
				return true
			}
			// Try to find next segment match
			next := pSegs[pi+1]
			for ri < len(rSegs) {
				if rSegs[ri] == next {
					break
				}
				ri++
			}
			pi++
			continue
		}
		if pSegs[pi] == "*" {
			pi++
			ri++
			continue
		}
		if pSegs[pi] != rSegs[ri] {
			return false
		}
		pi++
		ri++
	}
	// trailing patterns of '**' match
	for pi < len(pSegs) && pSegs[pi] == "**" {
		pi++
	}
	return pi == len(pSegs) && ri == len(rSegs)
}

// Subscribe registers interest in notifications matching the route and starts a
// handler goroutine. Handlers are invoked serially per subscription.
// internal context key to allow tests to override per-subscription buffer size.
// This is intentionally unexported; future public options may replace it.
type ctxKeyNoticeBuf struct{}

func (c *client) Subscribe(ctx context.Context, route string, handler NoticeHandler) (Subscription, error) {
	if err := validateNoticeRoute(route, true); err != nil {
		return nil, err
	}
	// create subscription
	buf := 1 // per-subscription inbox buffer; TODO: expose as option
	if v := ctx.Value(ctxKeyNoticeBuf{}); v != nil {
		if bi, ok := v.(int); ok {
			buf = bi
		}
	}
	s := &subscription{route: route, handler: handler, inbox: make(chan NoticeMsg, buf), client: c, done: make(chan struct{})}
	// add to local subscriptions
	c.mu.Lock()
	chs := c.subs[route]
	first := len(chs) == 0
	chs = append(chs, s)
	c.subs[route] = chs
	c.mu.Unlock()
	// if first subscriber for route, send subscribe to broker
	if first {
		if err := c.sendSubscribe(ctx, route); err != nil {
			// rollback local addition
			c.mu.Lock()
			chs := c.subs[route]
			for i := len(chs) - 1; i >= 0; i-- {
				if chs[i] == s {
					chs = append(chs[:i], chs[i+1:]...)
					break
				}
			}
			if len(chs) == 0 {
				delete(c.subs, route)
			} else {
				c.subs[route] = chs
			}
			c.mu.Unlock()
			return nil, err
		}
	}
	// start handler loop
	go func() {
		for {
			select {
			case <-ctx.Done():
				// remove subscription and stop
				s.Unsubscribe()
				return
			case <-s.done:
				return
			case msg := <-s.inbox:
				s.wg.Add(1)
				func(m NoticeMsg) {
					defer s.wg.Done()
					// derive a per-message context so we can add per-delivery timeouts/tracing later
					msgCtx, cancel := context.WithCancel(ctx)
					defer cancel()
					// Errors are intentionally ignored for v1; future versions may support retry/DLQ.
					_ = handler(msgCtx, m)
				}(msg)
			}
		}
	}()
	return s, nil
}

// deliver sends msg to the subscription inbox, or drops if the subscription has
// been unsubscribed. This uses select so Unsubscribe can close done to cancel
// blocked deliveries and avoid send-on-closed-channel races.
func (s *subscription) deliver(msg NoticeMsg) {
	select {
	case <-s.done:
		return
	case s.inbox <- msg:
		return
	}
}

// sendSubscribe sends a subscribe request for a route.
func (c *client) sendSubscribe(ctx context.Context, route string) error {
	body := encodeSubscribe(route)
	frame := transport.Frame{
		Type:    NoticeSubscribe,
		Flags:   0,
		Channel: transport.ChannelSub,
		Body:    body,
	}
	if err := c.mux.Send(frame); err != nil {
		return fmt.Errorf("send subscribe: %w", err)
	}
	return c.waitForAck(ctx, NoticeSubscribe)
}

// sendUnsubscribe sends an unsubscribe request and waits (bounded) for an ack.
func (c *client) sendUnsubscribe(ctx context.Context, route string) error {
	body := encodeUnsubscribe(route)
	frame := transport.Frame{
		Type:    NoticeUnsubscribe,
		Flags:   0,
		Channel: transport.ChannelSub,
		Body:    body,
	}
	if err := c.mux.Send(frame); err != nil {
		return fmt.Errorf("send unsubscribe: %w", err)
	}
	return c.waitForAck(ctx, NoticeUnsubscribe)
}

// resubscribeAll re-sends Subscribe frames for all active routes. Best-effort.
func (c *client) resubscribeAll(ctx context.Context) {
	c.mu.Lock()
	routes := make([]string, 0, len(c.subs))
	for r := range c.subs {
		routes = append(routes, r)
	}
	c.mu.Unlock()

	for _, r := range routes {
		_ = c.sendSubscribe(ctx, r)
	}
}

// Publish sends a notification to the given route with body bytes.
func (c *client) Publish(ctx context.Context, route string, body []byte) error {
	if err := validateNoticeRoute(route, false); err != nil {
		return err
	}
	payload := encodePublish(route, body)
	frame := transport.Frame{
		Type:    NoticePublish,
		Flags:   0,
		Channel: transport.ChannelPub,
		Body:    payload,
	}
	if err := c.mux.Send(frame); err != nil {
		return fmt.Errorf("send publish: %w", err)
	}
	return c.waitForAck(ctx, NoticePublish)
}

func (c *client) waitForAck(ctx context.Context, op uint8) error {
	key := noticeWaitKey(op)
	ch := make(chan error, 1)
	c.ackMu.Lock()
	c.ackWaiters[key] = append(c.ackWaiters[key], ch)
	c.ackMu.Unlock()

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(500 * time.Millisecond):
		c.removeWaiter(key, ch)
		return nil
	}
}

func (c *client) notifyWaiters(key string, err error) {
	if key == "" {
		return
	}
	c.ackMu.Lock()
	waiters := c.ackWaiters[key]
	delete(c.ackWaiters, key)
	c.ackMu.Unlock()
	for _, w := range waiters {
		w <- err
		close(w)
	}
}

func (c *client) removeWaiter(key string, ch chan error) {
	c.ackMu.Lock()
	ws := c.ackWaiters[key]
	for i, w := range ws {
		if w == ch {
			ws = append(ws[:i], ws[i+1:]...)
			break
		}
	}
	if len(ws) == 0 {
		delete(c.ackWaiters, key)
	} else {
		c.ackWaiters[key] = ws
	}
	c.ackMu.Unlock()
}

func noticeWaitKey(op uint8) string {
	return fmt.Sprintf("%d", op)
}

func isNoticeResponseType(t uint8) bool {
	return t == NoticePublish || t == NoticeSubscribe || t == NoticeUnsubscribe || t == NoticeUnsubscribeAll
}

func decodeNoticeResponseKey(op uint8, body []byte) (string, error) {
	status, errMsg, ok := decodeStatus(body)
	if !ok {
		return "", nil
	}
	if status != 0 {
		return "", fmt.Errorf("notice error: %s", errMsg)
	}
	return noticeWaitKey(op), nil
}

func decodeStatus(body []byte) (uint8, string, bool) {
	if len(body) < 1 {
		return 0, "", false
	}
	status := body[0]
	if status == 0 {
		return 0, "", true
	}
	if len(body) < 5 {
		return status, "", true
	}
	msgLen := uint32(body[1])<<24 | uint32(body[2])<<16 | uint32(body[3])<<8 | uint32(body[4])
	if int(5+msgLen) > len(body) {
		return status, "", true
	}
	return status, string(body[5 : 5+msgLen]), true
}

func encodePublish(route string, body []byte) []byte {
	routeBytes := []byte(route)
	buf := make([]byte, 0, 8+4+len(routeBytes)+4+len(body))
	buf = appendU64(buf, 0)
	buf = appendU32(buf, uint32(len(routeBytes)))
	buf = append(buf, routeBytes...)
	buf = appendU32(buf, uint32(len(body)))
	buf = append(buf, body...)
	return buf
}

func encodeSubscribe(route string) []byte {
	pat := []byte(route)
	buf := make([]byte, 0, 8+4+len(pat)+8+4)
	buf = appendU64(buf, 0)
	buf = appendU32(buf, uint32(len(pat)))
	buf = append(buf, pat...)
	buf = appendU64(buf, 0)
	buf = appendU32(buf, 0)
	return buf
}

func encodeUnsubscribe(route string) []byte {
	return encodeSubscribe(route)
}

func decodeNotify(body []byte) (string, []byte, bool) {
	_, route, ok := decodeFirstRoute(body)
	if !ok {
		return "", nil, false
	}
	payload, ok := decodePayload(body)
	if !ok {
		return "", nil, false
	}
	return route, payload, true
}

func decodeFirstRoute(body []byte) (int, string, bool) {
	if len(body) < 12 {
		return 0, "", false
	}
	idx := 0
	idx += 8 // family_id
	routeLen := readU32(body[idx:])
	idx += 4
	if int(idx+int(routeLen)) > len(body) {
		return 0, "", false
	}
	route := string(body[idx : idx+int(routeLen)])
	idx += int(routeLen)
	return idx, route, true
}

func decodePayload(body []byte) ([]byte, bool) {
	idx, _, ok := decodeFirstRoute(body)
	if !ok || idx+4 > len(body) {
		return nil, false
	}
	plen := readU32(body[idx:])
	idx += 4
	if int(idx+int(plen)) > len(body) {
		return nil, false
	}
	payload := append([]byte(nil), body[idx:idx+int(plen)]...)
	return payload, true
}

func appendU64(buf []byte, v uint64) []byte {
	return append(buf,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func appendU32(buf []byte, v uint32) []byte {
	return append(buf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func readU32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func stripNoticeScheme(route string) string {
	const prefix = "notice://"
	if strings.HasPrefix(route, prefix) {
		return strings.TrimPrefix(route, prefix)
	}
	return route
}

func validateNoticeRoute(route string, allowWildcards bool) error {
	if !strings.HasPrefix(route, "notice://") {
		return fmt.Errorf("notice route must start with notice://")
	}
	path := stripNoticeScheme(route)
	segs := strings.Split(path, "/")
	if len(segs) != 3 {
		if allowWildcards && len(segs) == 2 && segs[1] == "**" {
			return nil
		}
		return fmt.Errorf("notice route must have realm/area/resource")
	}
	if segs[0] == "" || segs[1] == "" || segs[2] == "" {
		return fmt.Errorf("notice route segments must be non-empty")
	}
	if !allowWildcards {
		if strings.Contains(segs[1], "*") || strings.Contains(segs[2], "*") {
			return fmt.Errorf("notice publish route cannot contain wildcards")
		}
		return nil
	}
	if segs[0] == "*" || segs[0] == "**" {
		return fmt.Errorf("notice realm cannot be wildcard")
	}
	return nil
}
