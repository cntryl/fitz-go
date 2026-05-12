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

// RPCWorkerRegistration represents an active worker registration returned by
// [RPCClient.RegisterWorker]. Call [RPCWorkerRegistration.Deregister] to stop
// receiving requests and release the registration.
type RPCWorkerRegistration struct {
	inner *internalrpc.Subscription
}

// Deregister removes this worker registration from the broker and stops
// routing new requests to it. The deregistration is best-effort on the
// broker side (the local handler map is cleared immediately).
func (r *RPCWorkerRegistration) Deregister() {
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

// Send writes a response frame body for the current inbound RPC request.
func (w *rpcResponseWriter) Send(body []byte) error {
	return w.inner.Send(body)
}

// RegisterWorker registers a handler for a route and returns a deregistration handle.
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

// Call invokes an RPC route and returns an iterator over response frames.
func (c *rpcClient) Call(ctx context.Context, route string, body []byte) (Iterator[RPCResponseFrame], error) {
	iter, err := c.inner.Call(ctx, route, body)
	if err != nil {
		return nil, err
	}
	return &rpcResponseIterator{inner: iter}, nil
}

// Next advances to the next response frame.
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

// Value returns the current response frame.
func (it *rpcResponseIterator) Value() RPCResponseFrame {
	return it.current
}

// Err returns the terminal iterator error, if any.
func (it *rpcResponseIterator) Err() error {
	return it.inner.Err()
}

// Close releases iterator resources.
func (it *rpcResponseIterator) Close() error {
	return it.inner.Close()
}
