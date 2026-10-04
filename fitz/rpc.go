package fitz

import (
	"context"

	internalrpc "github.com/cntryl/fitz-go/internal/domains/rpc"
)

var (
	ErrNoWorkers       = internalrpc.ErrNoWorkers
	ErrRPCTimeout      = internalrpc.ErrRPCTimeout
	ErrRPCBackpressure = internalrpc.ErrRPCBackpressure
)

// CapabilityRPCCancellation is set when the broker supports RPC cancellation and budgets.
const CapabilityRPCCancellation uint32 = 1 << 3

type RPCInboundRequest struct {
	Route         string
	ReplyRoute    string
	Body          []byte
	correlationID [16]byte
}

type RPCResponseFrame struct {
	Body     []byte
	Sequence uint64
}

// RPCCancellationOutcome reports the result of best-effort broker cancellation.
type RPCCancellationOutcome = internalrpc.CancellationOutcome

const (
	RPCCancellationNotRequested      = internalrpc.CancellationNotRequested
	RPCCancellationRequestNotSent    = internalrpc.CancellationRequestNotSent
	RPCCancellationUnsupported       = internalrpc.CancellationUnsupported
	RPCCancellationQueuedRemoved     = internalrpc.CancellationQueuedRemoved
	RPCCancellationForwarded         = internalrpc.CancellationForwarded
	RPCCancellationWorkerUnsupported = internalrpc.CancellationWorkerUnsupported
	RPCCancellationAlreadyTerminal   = internalrpc.CancellationAlreadyTerminal
	RPCCancellationUnknown           = internalrpc.CancellationUnknown
	RPCCancellationForwardingFailed  = internalrpc.CancellationForwardingFailed
	RPCCancellationUnconfirmed       = internalrpc.CancellationUnconfirmed
	RPCCancellationConnectionClosed  = internalrpc.CancellationConnectionClosed
)

// RPCCallIterator streams frames and exposes the final cancellation outcome.
type RPCCallIterator interface {
	Iterator[RPCResponseFrame]
	CancellationResult() <-chan RPCCancellationOutcome
}

// RPCWorkerRegistration represents an active worker registration returned by
// [RPCClient.RegisterWorker]. Call [RPCWorkerRegistration.Deregister] to stop
// receiving requests and release the registration.
type RPCWorkerRegistration struct {
	inner *internalrpc.Subscription
}

// Deregister removes this worker registration from the broker and stops
// routing new requests to it. The local handler is removed immediately; any
// broker or transport failure is returned to the caller.
func (r *RPCWorkerRegistration) Deregister() error {
	if r != nil && r.inner != nil {
		return r.inner.Unsubscribe()
	}
	return nil
}

type RPCResponseWriter interface {
	Send(body []byte) error
}

type RPCHandler func(ctx context.Context, req RPCInboundRequest, writer RPCResponseWriter) error

type RPCClient interface {
	RegisterWorker(ctx context.Context, route string, maxConcurrent uint32, handler RPCHandler) (*RPCWorkerRegistration, error)
	Call(ctx context.Context, route string, body []byte) (RPCCallIterator, error)
}

type rpcClient struct {
	inner internalrpc.Client
}

type rpcResponseWriter struct {
	inner internalrpc.ResponseWriter
}

type rpcResponseIterator struct {
	inner   internalrpc.CallIterator
	current RPCResponseFrame
}

// Send writes a response frame body for the current inbound RPC request.
func (w *rpcResponseWriter) Send(body []byte) error {
	return w.inner.Send(body)
}

// RegisterWorker registers a handler for a route and returns a deregistration handle.
func (c *rpcClient) RegisterWorker(ctx context.Context, route string, maxConcurrent uint32, handler RPCHandler) (*RPCWorkerRegistration, error) {
	registration, err := c.inner.RegisterWorker(ctx, route, maxConcurrent, func(ctx context.Context, req internalrpc.InboundRequest, writer internalrpc.ResponseWriter) error {
		return handler(ctx, RPCInboundRequest{
			Route:         req.Route,
			ReplyRoute:    req.ReplyRoute,
			Body:          req.Body,
			correlationID: req.CorrelationID,
		}, &rpcResponseWriter{inner: writer})
	})
	if err != nil {
		return nil, err
	}
	return &RPCWorkerRegistration{inner: registration}, nil
}

// Call invokes an RPC route and returns an iterator over response frames.
func (c *rpcClient) Call(ctx context.Context, route string, body []byte) (RPCCallIterator, error) {
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
	it.current = RPCResponseFrame{Body: value.Body, Sequence: value.Sequence}
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

// CancellationResult resolves when the broker reports cancellation handling.
func (it *rpcResponseIterator) CancellationResult() <-chan RPCCancellationOutcome {
	return it.inner.CancellationResult()
}
