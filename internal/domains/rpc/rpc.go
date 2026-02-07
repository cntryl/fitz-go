package rpc

import (
	"context"

	"github.com/cntryl/cntryl-go/internal/core/transport"
)

type Client interface {
	Subscribe(ctx context.Context, route string, handler RPCHandler) (Subscription, error)
	Publish(ctx context.Context, route string, body []byte) error
}

type RPCHandler func(context.Context, RPCMsg) error

type Subscription interface {
	Unsubscribe()
}

type RPCMsg struct {
	Route    string
	Metadata RPCMetadata
	Body     []byte
}

func (msg *RPCMsg) Response(ctx context.Context, body []byte) {
	// TODO respond don't forget we need to sequence responses correctly
}

type RPCMetadata map[string]string

// client is the concrete implementation of the RPC Client interface.
type muxProvider interface {
	Send(transport.Frame) error
	In() <-chan transport.Frame
	Ctx() context.Context
	OnReconnect(func())
}

type client struct {
	mux muxProvider
}

// NewClient creates a new RPC domain client backed by the transport mux.
func NewClient(mux muxProvider) Client {
	return &client{mux: mux}
}

// Subscribe registers an RPC handler for the given route.
// TODO: implement wire protocol for RPC subscribe.
func (c *client) Subscribe(ctx context.Context, route string, handler RPCHandler) (Subscription, error) {
	return nil, nil // stub
}

// Publish sends an RPC message to the given route.
// TODO: implement wire protocol for RPC publish.
func (c *client) Publish(ctx context.Context, route string, body []byte) error {
	return nil // stub
}
