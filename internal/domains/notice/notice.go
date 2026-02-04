package notice

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cntryl/cntryl-go/internal/core/transport"
)

// Channel and wire codes for Notice domain (per CLIENT_SPEC.md semantics).
// Channels: Pub (publish) and Sub (subscribe) are reserved 1/2 in the protocol.
const (
	ChannelPub uint32 = 1
	ChannelSub uint32 = 2
)

// Wire operation codes for Notice domain. Values are low-byte uint8 equivalents.
const (
	NoticeSubscribe      uint8 = 200 % 256
	NoticeUnsubscribe    uint8 = 201 % 256
	NoticePublish        uint8 = 202 % 256
	NoticeUnsubscribeAll uint8 = 203 % 256
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
	ackWaiters map[string][]chan struct{}
	once       sync.Once
}

// NewClient creates a new Notice domain client backed by the transport mux.
func NewClient(mux muxProvider) Client {
	c := &client{mux: mux, subs: make(map[string][]*subscription), ackWaiters: make(map[string][]chan struct{})}
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
				if f.Channel != ChannelSub {
					continue
				}
				if f.Type != transport.FrameTypeResp {
					continue
				}
				dec, err := transport.NewTLVDecoder(f.Body)
				if err != nil {
					continue
				}
				// Check for operation ack (Unsubscribe / UnsubscribeAll)
				op := dec.GetBytes(transport.TagOp)
				if len(op) > 0 {
					switch op[0] {
					case 1:
						// Unsubscribe ack for a route
						r := dec.GetString(transport.TagRoute)
						c.ackMu.Lock()
						if waiters, ok := c.ackWaiters[r]; ok {
							for _, w := range waiters {
								close(w)
							}
							delete(c.ackWaiters, r)
						}
						c.ackMu.Unlock()
					case 2:
						// UnsubscribeAll ack: notify all waiters
						c.ackMu.Lock()
						for r, waiters := range c.ackWaiters {
							for _, w := range waiters {
								close(w)
							}
							delete(c.ackWaiters, r)
						}
						c.ackMu.Unlock()
					}
					continue
				}
				r := dec.GetString(transport.TagRoute)
				b := dec.GetBytes(transport.TagBody)
				md := make(NoticeMetadata)
				// Minimal metadata decoding: support single TagKey/TagValue pair.
				if dec.Has(transport.TagKey) && dec.Has(transport.TagValue) {
					k := dec.GetString(transport.TagKey)
					v := dec.GetString(transport.TagValue)
					md[k] = v
				}
				msg := NoticeMsg{Route: r, Metadata: md, Body: append([]byte(nil), b...)}
				c.mu.Lock()
				var targets []*subscription
				for pat, subs := range c.subs {
					if noticeMatchRoute(pat, r) {
						targets = append(targets, subs...)
					}
				}
				c.mu.Unlock()
				// deliver without holding client lock. Use subscription.deliver which
				// selects on the subscription's done channel and the inbox so that
				// Unsubscribe can cancel blocked deliveries.
				for _, s := range targets {
					s.deliver(msg)
				}
			}
		}()
	})
}

// noticeMatchRoute duplicates the simulator matching logic (single '*' and multi '**').
func noticeMatchRoute(pattern, route string) bool {
	if pattern == route {
		return true
	}
	pSegs := strings.Split(pattern, "/")
	rSegs := strings.Split(route, "/")
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
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	frame := transport.Frame{
		Type:    NoticeSubscribe,
		Flags:   0,
		Channel: ChannelSub,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return fmt.Errorf("send subscribe: %w", err)
	}
	return nil
}

// sendUnsubscribe sends an unsubscribe request and waits (bounded) for an ack.
func (c *client) sendUnsubscribe(ctx context.Context, route string) error {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	frame := transport.Frame{
		Type:    NoticeUnsubscribe,
		Flags:   0,
		Channel: ChannelSub,
		Body:    enc.Encode(),
	}
	// register ack waiter
	ack := make(chan struct{})
	c.ackMu.Lock()
	c.ackWaiters[route] = append(c.ackWaiters[route], ack)
	c.ackMu.Unlock()
	if err := c.mux.Send(frame); err != nil {
		// cleanup waiter
		c.ackMu.Lock()
		ws := c.ackWaiters[route]
		for i, w := range ws {
			if w == ack {
				c.ackWaiters[route] = append(ws[:i], ws[i+1:]...)
				break
			}
		}
		if len(c.ackWaiters[route]) == 0 {
			delete(c.ackWaiters, route)
		}
		c.ackMu.Unlock()
		return fmt.Errorf("send unsubscribe: %w", err)
	}
	// wait for ack (bounded)
	select {
	case <-ack:
		// got ack
	case <-time.After(500 * time.Millisecond):
		// timeout waiting for ack; cleanup our waiter to prevent leaks
		c.ackMu.Lock()
		ws := c.ackWaiters[route]
		for i, w := range ws {
			if w == ack {
				c.ackWaiters[route] = append(ws[:i], ws[i+1:]...)
				break
			}
		}
		if len(c.ackWaiters[route]) == 0 {
			delete(c.ackWaiters, route)
		}
		c.ackMu.Unlock()
	}
	return nil
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
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	enc.AddBytes(transport.TagBody, body)
	frame := transport.Frame{
		Type:    NoticePublish,
		Flags:   0,
		Channel: ChannelPub,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return fmt.Errorf("send publish: %w", err)
	}
	return nil
}
