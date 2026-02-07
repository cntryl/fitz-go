package rpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cntryl/cntryl-go/internal/core/transport"
)

// Client is the API for the RPC domain.
type Client interface {
	Call(ctx context.Context, route string, body []byte, timeout time.Duration) ([]byte, error)
	SubscribeWorker(ctx context.Context, route string, handler RPCHandler) (Subscription, error)
	UnsubscribeWorker(ctx context.Context, route string) error
}

// RPCHandler processes inbound requests; return body and optional error.
type RPCHandler func(context.Context, InboundRequest) (response []byte, err error)

// Subscription allows unsubscribing a worker.
type Subscription interface {
	Unsubscribe()
}

// InboundRequest represents an inbound RPC request to a worker.
type InboundRequest struct {
	ID         uint64
	Route      string
	Body       []byte
	ReplyRoute string
}

type client struct {
	mux        transport.MuxProvider
	handlersMu sync.Mutex
	handlers   map[string][]*subscription
	nextReqID  uint64
	pendingMu  sync.Mutex
	pending    map[uint64]chan transport.Frame
}

// NewClient creates a new RPC domain client backed by the transport mux.
func NewClient(mux transport.MuxProvider) Client {
	c := &client{mux: mux, handlers: make(map[string][]*subscription), pending: make(map[uint64]chan transport.Frame)}
	c.startRecv()
	mux.OnReconnect(func() {
		c.resubscribeAll(context.Background())
	})
	return c
}

type subscription struct {
	route   string
	handler RPCHandler
	c       *client
	done    chan struct{}
	once    sync.Once
	wg      sync.WaitGroup
}

func (s *subscription) Unsubscribe() {
	s.once.Do(func() {
		close(s.done)
		s.c.handlersMu.Lock()
		chs := s.c.handlers[s.route]
		for i, ss := range chs {
			if ss == s {
				chs = append(chs[:i], chs[i+1:]...)
				break
			}
		}
		if len(chs) == 0 {
			delete(s.c.handlers, s.route)
		}
		s.c.handlersMu.Unlock()
		s.wg.Wait()
	})
}

// startRecv dispatches inbound RPC frames to workers or pending Call correlations.
func (c *client) startRecv() {
	go func() {
		for f := range c.mux.In() {
			if f.Channel != transport.ChannelRPC {
				continue
			}
			switch f.Type {
			case RPCRequest:
				c.dispatchRequest(f)
			case transport.FrameTypeResp, transport.FrameTypeErr:
				c.deliverPending(f)
			}
		}
	}()
}

func (c *client) dispatchRequest(f transport.Frame) {
	dec, err := transport.NewTLVDecoder(f.Body)
	if err != nil {
		return
	}
	id, _ := dec.GetUint64(transport.TagID)
	route := dec.GetString(transport.TagRoute)
	body := dec.GetBytes(transport.TagBody)
	reply := dec.GetString(transport.TagReplyRoute)
	req := InboundRequest{ID: id, Route: route, Body: body, ReplyRoute: reply}

	c.handlersMu.Lock()
	handlers := append([]*subscription(nil), c.handlers[route]...)
	c.handlersMu.Unlock()

	for _, sub := range handlers {
		sub.wg.Add(1)
		go func(s *subscription) {
			defer s.wg.Done()
			select {
			case <-s.done:
				return
			default:
			}
			respBody, herr := s.handler(context.Background(), req)
			enc := transport.NewTLVEncoder()
			enc.AddUint64(transport.TagID, id)
			if herr == nil {
				enc.AddBytes(transport.TagBody, respBody)
				_ = c.mux.Send(transport.Frame{Type: transport.FrameTypeResp, Channel: transport.ChannelRPC, Body: enc.Encode()})
			} else {
				enc.AddString(transport.TagErr, herr.Error())
				_ = c.mux.Send(transport.Frame{Type: transport.FrameTypeErr, Channel: transport.ChannelRPC, Body: enc.Encode()})
			}
		}(sub)
	}
}

func (c *client) deliverPending(f transport.Frame) {
	dec, err := transport.NewTLVDecoder(f.Body)
	if err != nil {
		return
	}
	id, err := dec.GetUint64(transport.TagID)
	if err != nil {
		return
	}
	c.pendingMu.Lock()
	ch, found := c.pending[id]
	if found {
		delete(c.pending, id)
	}
	c.pendingMu.Unlock()
	if found {
		select {
		case ch <- f:
		default:
		}
	}
}

// Call sends a request and waits for a correlated response.
func (c *client) Call(ctx context.Context, route string, body []byte, timeout time.Duration) ([]byte, error) {
	reqID := atomic.AddUint64(&c.nextReqID, 1)
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	enc.AddUint64(transport.TagID, reqID)
	enc.AddBytes(transport.TagBody, body)
	frame := transport.Frame{Type: RPCRequest, Channel: transport.ChannelRPC, Body: enc.Encode()}

	ch := make(chan transport.Frame, 1)
	c.pendingMu.Lock()
	c.pending[reqID] = ch
	c.pendingMu.Unlock()

	if err := c.mux.Send(frame); err != nil {
		c.pendingMu.Lock()
		delete(c.pending, reqID)
		c.pendingMu.Unlock()
		return nil, fmt.Errorf("send request: %w", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case <-waitCtx.Done():
		c.pendingMu.Lock()
		delete(c.pending, reqID)
		c.pendingMu.Unlock()
		return nil, ErrRPCTimeout
	case f := <-ch:
		if f.Type == transport.FrameTypeResp {
			dec, err := transport.NewTLVDecoder(f.Body)
			if err != nil {
				return nil, err
			}
			return dec.GetBytes(transport.TagBody), nil
		}
		if f.Type == transport.FrameTypeErr {
			dec, _ := transport.NewTLVDecoder(f.Body)
			return nil, errors.New(dec.GetString(transport.TagErr))
		}
		return nil, errors.New("unexpected frame type")
	}
}

// SubscribeWorker registers a worker handler and notifies the broker.
func (c *client) SubscribeWorker(ctx context.Context, route string, handler RPCHandler) (Subscription, error) {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	frame := transport.Frame{Type: RPCSubscribeWorker, Channel: transport.ChannelRPC, Body: enc.Encode()}
	if err := c.mux.Send(frame); err != nil {
		return nil, fmt.Errorf("send subscribe_worker: %w", err)
	}
	s := &subscription{route: route, handler: handler, c: c, done: make(chan struct{})}
	c.handlersMu.Lock()
	c.handlers[route] = append(c.handlers[route], s)
	c.handlersMu.Unlock()
	return s, nil
}

// UnsubscribeWorker removes all workers for a route and notifies the broker.
func (c *client) UnsubscribeWorker(ctx context.Context, route string) error {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	frame := transport.Frame{Type: RPCUnsubscribeWorker, Channel: transport.ChannelRPC, Body: enc.Encode()}
	if err := c.mux.Send(frame); err != nil {
		return fmt.Errorf("send unsubscribe_worker: %w", err)
	}
	c.handlersMu.Lock()
	delete(c.handlers, route)
	c.handlersMu.Unlock()
	return nil
}

func (c *client) resubscribeAll(ctx context.Context) {
	c.handlersMu.Lock()
	routes := make([]string, 0, len(c.handlers))
	for r := range c.handlers {
		routes = append(routes, r)
	}
	c.handlersMu.Unlock()
	for _, r := range routes {
		enc := transport.NewTLVEncoder()
		enc.AddString(transport.TagRoute, r)
		_ = c.mux.Send(transport.Frame{Type: RPCSubscribeWorker, Channel: transport.ChannelRPC, Body: enc.Encode()})
	}
}
