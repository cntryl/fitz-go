package notice

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cntryl/cntryl-go/internal/transport"
)

// Client is the API for the Notice domain.
// Subscribe remains for simple use; SubscribeChan returns a channel that receives
// notifications for the subscribed route (supports wildcards '*' and '**').
type Client interface {
	Subscribe(ctx context.Context, route string) error
	SubscribeChan(ctx context.Context, route string) (<-chan []byte, error)
	Unsubscribe(ctx context.Context, route string) error
	UnsubscribeAll(ctx context.Context) error
	Publish(ctx context.Context, route string, body []byte) error
}

// client is a concrete implementation of notice.Client backed by the transport mux.
type client struct {
	mux        *transport.Mux
	mu         sync.Mutex
	subs       map[string][]chan []byte
	ackMu      sync.Mutex
	ackWaiters map[string][]chan struct{}
	once       sync.Once
}

// NewClient creates a new Notice domain client backed by the transport mux.
func NewClient(mux *transport.Mux) Client {
	c := &client{mux: mux, subs: make(map[string][]chan []byte), ackWaiters: make(map[string][]chan struct{})}
	c.startRecv()
	return c
}

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
				c.mu.Lock()
				for pat, chs := range c.subs {
					if noticeMatchRoute(pat, r) {
						for _, ch := range chs {
							select {
							case ch <- append([]byte(nil), b...):
							default:
								// drop if subscriber not ready
							}
						}
					}
				}
				c.mu.Unlock()
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

// Subscribe registers interest in notifications matching the route. Best-effort send.
func (c *client) Subscribe(ctx context.Context, route string) error {
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

// SubscribeChan registers interest and returns a channel that receives notification bodies.
func (c *client) SubscribeChan(ctx context.Context, route string) (<-chan []byte, error) {
	ch := make(chan []byte, 8)
	// Add local subscription
	c.mu.Lock()
	chs := c.subs[route]
	first := len(chs) == 0
	chs = append(chs, ch)
	c.subs[route] = chs
	c.mu.Unlock()

	// If this is the first local subscriber for the route, send SUBSCRIBE to broker
	if first {
		if err := c.Subscribe(ctx, route); err != nil {
			// Remove the subscription we just added
			c.mu.Lock()
			chs := c.subs[route]
			// drop last occurrence
			toremove := -1
			for i := len(chs) - 1; i >= 0; i-- {
				if chs[i] == ch {
					toremove = i
					break
				}
			}
			if toremove >= 0 {
				chs = append(chs[:toremove], chs[toremove+1:]...)
				if len(chs) == 0 {
					delete(c.subs, route)
				} else {
					c.subs[route] = chs
				}
			}
			c.mu.Unlock()
			close(ch)
			return nil, err
		}
	}
	// Remove subscription when context cancelled
	go func() {
		<-ctx.Done()
		// Remove this local subscription; if this was the last one, notify broker
		c.mu.Lock()
		chs := c.subs[route]
		removed := false
		for i, cch := range chs {
			if cch == ch {
				chs = append(chs[:i], chs[i+1:]...)
				removed = true
				break
			}
		}
		if !removed {
			c.mu.Unlock()
			// Channel already removed/closed by external Unsubscribe; nothing to do
			return
		}
		if len(chs) == 0 {
			delete(c.subs, route)
			c.mu.Unlock()
			// notify broker that there are no more subscribers for this route
			_ = c.Unsubscribe(context.Background(), route)
			// close our channel
			close(ch)
			return
		}
		c.subs[route] = chs
		c.mu.Unlock()
		close(ch)
	}()
	return ch, nil
}

// Unsubscribe removes a subscription for the route.
func (c *client) Unsubscribe(ctx context.Context, route string) error {
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
	case <-time.After(500 * time.Millisecond):
		// timeout waiting for ack; proceed anyway
	}
	// remove local subscriptions and close channels
	c.mu.Lock()
	if chs, ok := c.subs[route]; ok {
		delete(c.subs, route)
		for _, ch := range chs {
			close(ch)
		}
	}
	c.mu.Unlock()
	return nil
}

// UnsubscribeAll removes all subscriptions for this client and notifies the broker.
func (c *client) UnsubscribeAll(ctx context.Context) error {
	frame := transport.Frame{
		Type:    NoticeUnsubscribeAll,
		Flags:   0,
		Channel: ChannelSub,
		Body:    nil,
	}
	// register ack waiter (use empty key so op==2 will flush it)
	ack := make(chan struct{})
	c.ackMu.Lock()
	c.ackWaiters[""] = append(c.ackWaiters[""], ack)
	c.ackMu.Unlock()
	if err := c.mux.Send(frame); err != nil {
		// cleanup
		c.ackMu.Lock()
		n := c.ackWaiters[""]
		for i, w := range n {
			if w == ack {
				c.ackWaiters[""] = append(n[:i], n[i+1:]...)
				break
			}
		}
		if len(c.ackWaiters[""]) == 0 {
			delete(c.ackWaiters, "")
		}
		c.ackMu.Unlock()
		return fmt.Errorf("send unsubscribe all: %w", err)
	}
	// wait for ack (bounded)
	select {
	case <-ack:
	case <-time.After(500 * time.Millisecond):
		// timeout waiting for ack; proceed anyway
	}
	c.mu.Lock()
	for k, chs := range c.subs {
		delete(c.subs, k)
		for _, ch := range chs {
			close(ch)
		}
	}
	c.mu.Unlock()
	return nil
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
