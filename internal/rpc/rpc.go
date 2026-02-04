package rpc

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cntryl/cntryl-go/internal/transport"
)

// Client provides request/response and streaming RPC primitives.
type Client interface {
	Request(ctx context.Context, route string, body []byte, timeout time.Duration) ([]byte, error)
	RequestStream(ctx context.Context, route string, body []byte) (<-chan []byte, error)
}

// client is a concrete implementation of rpc.Client backed by the transport mux.
type client struct {
	mux *transport.Mux
}

// NewClient creates a new RPC domain client backed by the transport mux.
func NewClient(mux *transport.Mux) Client {
	return &client{mux: mux}
}

var nextReqID atomic.Uint64

func init() {
	nextReqID.Store(1)
}

// Request sends a request and waits for a single response or timeout.
func (c *client) Request(ctx context.Context, route string, body []byte, timeout time.Duration) ([]byte, error) {
	reqID := nextReqID.Add(1)
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	enc.AddBytes(transport.TagBody, body)
	enc.AddUint64(transport.TagID, reqID)

	frame := transport.Frame{
		Type:    transport.FrameTypeReq,
		Flags:   0,
		Channel: ChannelRPC,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}

	// Wait for response with matching ID.
	respCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		select {
		case <-respCtx.Done():
			return nil, respCtx.Err()
		case f, ok := <-c.mux.In():
			if !ok {
				return nil, fmt.Errorf("mux closed")
			}
			if f.Channel != ChannelRPC {
				continue
			}
			dec, err := transport.NewTLVDecoder(f.Body)
			if err != nil {
				continue
			}
			if dec.Has(transport.TagErr) {
				return nil, fmt.Errorf("broker error: %s", dec.GetString(transport.TagErr))
			}
			id, _ := dec.GetUint64(transport.TagID)
			if id != reqID {
				continue
			}
			return dec.GetBytes(transport.TagBody), nil
		}
	}
}

// RequestStream sends a request and returns a channel streaming response bodies.
func (c *client) RequestStream(ctx context.Context, route string, body []byte) (<-chan []byte, error) {
	reqID := nextReqID.Add(1)
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	enc.AddBytes(transport.TagBody, body)
	enc.AddUint64(transport.TagID, reqID)

	frame := transport.Frame{
		Type:    transport.FrameTypeReq,
		Flags:   0,
		Channel: ChannelRPC,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}

	out := make(chan []byte)
	// Start goroutine to forward responses matching reqID into channel until ctx.Done or StreamEnd tag observed.
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case f, ok := <-c.mux.In():
				if !ok {
					return
				}
				if f.Channel != ChannelRPC {
					continue
				}
				dec, err := transport.NewTLVDecoder(f.Body)
				if err != nil {
					continue
				}
				id, _ := dec.GetUint64(transport.TagID)
				if id != reqID {
					continue
				}
				if dec.Has(transport.TagStreamEnd) {
					return
				}
				if dec.Has(transport.TagBody) {
					out <- dec.GetBytes(transport.TagBody)
				}
			}
		}
	}()
	return out, nil
}
