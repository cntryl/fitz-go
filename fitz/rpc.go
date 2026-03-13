package fitz

import (
	"context"
	"time"

	internalrpc "github.com/cntryl/fitz-go/internal/domains/rpc"
)

type RPCInboundRequest = internalrpc.InboundRequest
type RPCResponseFrame = internalrpc.ResponseFrame
type RPCWorkerRegistration = internalrpc.Subscription

type RPCResponseWriter interface {
	Send(body []byte) error
}

type RPCHandler func(ctx context.Context, req RPCInboundRequest, writer RPCResponseWriter) error

type RPCClient interface {
	RegisterWorker(ctx context.Context, route string, handler RPCHandler) (*RPCWorkerRegistration, error)
	Call(ctx context.Context, route string, body []byte, timeout time.Duration) (Iterator[RPCResponseFrame], error)
}

type rpcClient struct {
	inner internalrpc.Client
}

type rpcResponseWriter struct {
	inner internalrpc.ResponseWriter
}

func (w *rpcResponseWriter) Send(body []byte) error {
	return w.inner.Response(body)
}

func (c *rpcClient) RegisterWorker(ctx context.Context, route string, handler RPCHandler) (*RPCWorkerRegistration, error) {
	return c.inner.Subscribe(ctx, route, func(ctx context.Context, req internalrpc.InboundRequest, writer internalrpc.ResponseWriter) error {
		return handler(ctx, req, &rpcResponseWriter{inner: writer})
	})
}

func (c *rpcClient) Call(ctx context.Context, route string, body []byte, timeout time.Duration) (Iterator[RPCResponseFrame], error) {
	return c.inner.Request(ctx, route, body, timeout)
}
