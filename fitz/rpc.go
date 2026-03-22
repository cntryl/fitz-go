package fitz

import (
	"context"

	internalrpc "github.com/cntryl/fitz-go/internal/domains/rpc"
)

type RPCInboundRequest struct {
	CorrelationID [16]byte
	Route         string
	ReplyRoute    string
	Body          []byte
}

type RPCResponseFrame struct {
	Body     []byte
	Sequence uint64
}

type RPCWorkerRegistration struct {
	inner *internalrpc.Subscription
}

func (r *RPCWorkerRegistration) Unsubscribe() {
	if r != nil && r.inner != nil {
		r.inner.Unsubscribe()
	}
}

type RPCResponseWriter interface {
	Send(body []byte) error
}

type RPCHandler func(ctx context.Context, req RPCInboundRequest, writer RPCResponseWriter) error

type RPCClient interface {
	RegisterWorker(ctx context.Context, route string, handler RPCHandler) (*RPCWorkerRegistration, error)
	Call(ctx context.Context, route string, body []byte) (Iterator[RPCResponseFrame], error)
}

type rpcClient struct {
	inner internalrpc.Client
}

type rpcResponseWriter struct {
	inner internalrpc.ResponseWriter
}

type rpcResponseIterator struct {
	inner   Iterator[internalrpc.ResponseFrame]
	current RPCResponseFrame
}

func (w *rpcResponseWriter) Send(body []byte) error {
	return w.inner.Send(body)
}

func (c *rpcClient) RegisterWorker(ctx context.Context, route string, handler RPCHandler) (*RPCWorkerRegistration, error) {
	registration, err := c.inner.RegisterWorker(ctx, route, func(ctx context.Context, req internalrpc.InboundRequest, writer internalrpc.ResponseWriter) error {
		return handler(ctx, RPCInboundRequest{
			CorrelationID: req.CorrelationID,
			Route:         req.Route,
			ReplyRoute:    req.ReplyRoute,
			Body:          append([]byte(nil), req.Body...),
		}, &rpcResponseWriter{inner: writer})
	})
	if err != nil {
		return nil, err
	}
	return &RPCWorkerRegistration{inner: registration}, nil
}

func (c *rpcClient) Call(ctx context.Context, route string, body []byte) (Iterator[RPCResponseFrame], error) {
	iter, err := c.inner.Call(ctx, route, body)
	if err != nil {
		return nil, err
	}
	return &rpcResponseIterator{inner: iter}, nil
}

func (it *rpcResponseIterator) Next() bool {
	if !it.inner.Next() {
		return false
	}
	value := it.inner.Value()
	it.current = RPCResponseFrame{
		Body:     append([]byte(nil), value.Body...),
		Sequence: value.Sequence,
	}
	return true
}

func (it *rpcResponseIterator) Value() RPCResponseFrame {
	return it.current
}

func (it *rpcResponseIterator) Err() error {
	return it.inner.Err()
}

func (it *rpcResponseIterator) Close() error {
	return it.inner.Close()
}
