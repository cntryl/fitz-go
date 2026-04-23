// Package rpc implements the Fitz RPC domain client.
// Per CLIENT_SPEC.md: Bidirectional RPC with streaming responses.
package rpc

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/core/iter"
	"github.com/cntryl/fitz-go/internal/core/reconnect"
	coretracing "github.com/cntryl/fitz-go/internal/core/tracing"
	"github.com/cntryl/fitz-go/internal/core/types"
	"github.com/cntryl/fitz-go/internal/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// InboundRequest represents a request received by a worker.
type InboundRequest struct {
	CorrelationID [16]byte
	Route         string
	ReplyRoute    string
	Body          []byte
}

// ResponseWriter allows a worker to send responses.
type ResponseWriter interface {
	Send(body []byte) error
}

// RPCHandler handles incoming RPC requests.
type RPCHandler func(ctx context.Context, req InboundRequest, w ResponseWriter) error

// ResponseFrame represents a single response frame from a streaming RPC call.
type ResponseFrame struct {
	Body     []byte
	Sequence uint64
}

var readRandom = crand.Read

func generateCorrelationID() ([16]byte, error) {
	var correlationID [16]byte
	if _, err := readRandom(correlationID[:]); err != nil {
		return [16]byte{}, fmt.Errorf("generate correlation id: %w", err)
	}
	return correlationID, nil
}

// Subscription represents an active worker registration.
// Call Unsubscribe to stop receiving requests and release the registration.
type Subscription struct {
	route  string
	client *client
}

// Unsubscribe removes this worker registration.
func (s *Subscription) Unsubscribe() {
	if s.client != nil {
		s.client.unsubscribeWorker(s.route)
	}
}

// Client is the RPC domain client interface.
type Client interface {
	// RegisterWorker registers a worker handler for the given route.
	RegisterWorker(ctx context.Context, route string, handler RPCHandler) (*Subscription, error)

	// Call sends an RPC request and returns an iterator over response frames.
	// Callers must call Close on the returned iterator when done to release resources.
	Call(ctx context.Context, route string, body []byte) (iter.Iterator[ResponseFrame], error)
}

type client struct {
	conn *connection.Connection

	mu          sync.Mutex
	workers     map[string]RPCHandler // route -> handler
	pendingRPCs map[[16]byte]chan ResponseFrame
	initialized bool
}

// NewClient creates a new RPC domain client.
func NewClient(conn *connection.Connection) Client {
	c := &client{
		conn:        conn,
		workers:     make(map[string]RPCHandler),
		pendingRPCs: make(map[[16]byte]chan ResponseFrame),
	}
	return c
}

var _ reconnect.DomainRestorer = (*client)(nil)

// initRPCHandler registers the RPC response handler on first use.
func (c *client) initRPCHandler() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialized {
		return
	}
	c.initialized = true
	c.conn.RegisterRPCResponseHandler(c.handleRPCResponse)
	c.conn.RegisterRPCRequestHandler(c.handleRPCRequest)
}

// handleRPCRequest handles incoming RPC REQUEST frames (302) forwarded to this worker.
// Payload: [u32 BE corrLen=16][16 bytes correlation_id][route][reply_route][body] (TLV).
func (c *client) handleRPCRequest(payload []byte) {
	if len(payload) < 20 {
		return
	}
	corrLen := binary.BigEndian.Uint32(payload[0:4])
	if corrLen != 16 || len(payload) < 20 {
		return
	}
	var correlationID [16]byte
	copy(correlationID[:], payload[4:20])
	c.handleWorkerRequest(correlationID, payload[20:])
}

// handleRPCResponse handles incoming RPC RESPONSE frames (303).
// Per server rpc_codec.rs: [bytes correlation_id][u64 seq][bytes body][u8 stream_end]
// where "bytes" = [u32 BE len][data] (TLV bytes format)
func (c *client) handleRPCResponse(correlationID [16]byte, payload []byte) {
	if len(payload) < 1 {
		return
	}

	// Check if this is a worker request (status byte for worker dispatch)
	// or a call response. We use the pending RPC map to differentiate.
	c.mu.Lock()
	ch, isCall := c.pendingRPCs[correlationID]
	c.mu.Unlock()

	if isCall {
		// This is a response to our Call
		// Parse: [u64 sequence][bytes body][u8 stream_end]
		offset := 0
		if offset+8 > len(payload) {
			return
		}
		seq := binary.BigEndian.Uint64(payload[offset : offset+8])
		offset += 8

		// body is TLV bytes: [u32 len][data]
		if offset+4 > len(payload) {
			return
		}
		bodyLen := binary.BigEndian.Uint32(payload[offset : offset+4])
		offset += 4

		if offset+int(bodyLen) > len(payload) {
			return
		}
		body := make([]byte, bodyLen)
		copy(body, payload[offset:offset+int(bodyLen)])
		offset += int(bodyLen)

		streamEnd := false
		if offset < len(payload) {
			streamEnd = payload[offset] == 1
		}

		if !streamEnd || len(body) > 0 {
			select {
			case ch <- ResponseFrame{Body: body, Sequence: seq}:
			case <-c.conn.LifecycleContext().Done():
			}
		}
		if streamEnd {
			c.mu.Lock()
			delete(c.pendingRPCs, correlationID)
			c.mu.Unlock()
			close(ch)
		}
		return
	}

	// This is a request dispatched to us as a worker
	c.handleWorkerRequest(correlationID, payload)
}

// handleWorkerRequest processes an incoming request for a registered worker.
// Server forwards REQUEST payload: [bytes correlation_id][string route][string reply_route][bytes body]
// where bytes/string = [u32 BE len][data] (TLV format)
// Note: correlationID was already parsed by the mux, but the remaining payload
// contains [string route][string reply_route][bytes body] after the correlation_id.
func (c *client) handleWorkerRequest(correlationID [16]byte, payload []byte) {
	offset := 0

	// Parse route (TLV string: [u32 len][string])
	if offset+4 > len(payload) {
		return
	}
	routeLen := binary.BigEndian.Uint32(payload[offset : offset+4])
	offset += 4
	if offset+int(routeLen) > len(payload) {
		return
	}
	route := string(payload[offset : offset+int(routeLen)])
	offset += int(routeLen)

	// Parse reply_route (TLV string: [u32 len][string])
	if offset+4 > len(payload) {
		return
	}
	replyRouteLen := binary.BigEndian.Uint32(payload[offset : offset+4])
	offset += 4
	if offset+int(replyRouteLen) > len(payload) {
		return
	}
	replyRoute := string(payload[offset : offset+int(replyRouteLen)])
	offset += int(replyRouteLen)

	// Parse body (TLV bytes: [u32 len][data])
	if offset+4 > len(payload) {
		return
	}
	bodyLen := binary.BigEndian.Uint32(payload[offset : offset+4])
	offset += 4
	if offset+int(bodyLen) > len(payload) {
		return
	}
	body := make([]byte, bodyLen)
	copy(body, payload[offset:offset+int(bodyLen)])

	c.mu.Lock()
	handler, ok := c.workers[route]
	c.mu.Unlock()

	if !ok {
		return // No worker for this route
	}

	req := InboundRequest{
		CorrelationID: correlationID,
		Route:         route,
		ReplyRoute:    replyRoute,
		Body:          body,
	}

	w := &responseWriter{
		conn:          c.conn,
		correlationID: correlationID,
		seq:           0,
	}
	lifecycleCtx := c.conn.LifecycleContext()

	go func() {
		handlerCtx, cancel, span := coretracing.StartDetachedSpan(
			lifecycleCtx,
			c.conn.Tracer(),
			"fitz.rpc.worker_handler",
			c.conn.AsyncHandlerTimeout(),
			trace.WithAttributes(
				attribute.String("fitz.route", route),
				attribute.String("fitz.reply_route", replyRoute),
			),
		)
		defer cancel()
		defer span.End()

		release, ok := c.conn.AcquireAsyncHandlerSlot(handlerCtx)
		if !ok {
			if err := handlerCtx.Err(); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			return
		}
		defer release()

		if err := handler(handlerCtx, req, w); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			if log := c.conn.Logger(); log != nil {
				log.Warn("rpc worker handler failed", "route", route, "error", err)
			}
		}
		// Send stream_end
		w.sendEnd()
	}()
}

// RegisterWorker per CLIENT_SPEC.md:
// Request: [worker_route_len][worker_route]
// Response: [status]
func (c *client) RegisterWorker(ctx context.Context, route string, handler RPCHandler) (*Subscription, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.rpc.RegisterWorker", trace.WithAttributes(attribute.String("fitz.route", route)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("rpc.RegisterWorker", "route", route)
	}

	// Validate route format
	if err := types.ValidateConcreteRoute(route, "rpc"); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("invalid route: %w", err)
	}

	c.initRPCHandler()

	sub, err := c.subscribeWorker(ctx, route, handler)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return sub, nil
}

// unsubscribeWorker removes a worker registration.
func (c *client) unsubscribeWorker(route string) {
	c.mu.Lock()
	delete(c.workers, route)
	c.mu.Unlock()

	ctx := c.conn.LifecycleContext()
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeRpcUnsubscribeWorker, rpcUnsubscribeWorkerPayloadWriter(route))
	// Unsubscribe is a best-effort teardown: the local worker map entry has
	// already been removed above, so the client will stop routing new calls
	// regardless of whether the broker ACKs the deregistration. Errors here
	// do not constitute data loss — they only affect broker-side routing
	// cleanup, which the broker recovers from via session expiry.
	_ = resp
	_ = err
}

// Call per CLIENT_SPEC.md:
// Request: [correlation_id(16)][route_len][route][reply_route_len][reply_route][body_len][body]
// Response: [status] (ack that request was dispatched)
// Actual responses come via RPC RESPONSE (303) messages.
func (c *client) Call(ctx context.Context, route string, body []byte) (iter.Iterator[ResponseFrame], error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.rpc.Call", trace.WithAttributes(attribute.String("fitz.route", route)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("rpc.Call", "route", route)
	}

	// Check if context is already canceled
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Validate route format
	if err := types.ValidateConcreteRoute(route, "rpc"); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("invalid route: %w", err)
	}

	c.initRPCHandler()

	correlationID, err := generateCorrelationID()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	// Create response channel
	ch := make(chan ResponseFrame, 32)

	c.mu.Lock()
	c.pendingRPCs[correlationID] = ch
	c.mu.Unlock()

	// Build request with writer path
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeRpcRequest, rpcRequestPayloadWriter(correlationID, route, "", body))
	if err != nil {
		c.mu.Lock()
		delete(c.pendingRPCs, correlationID)
		c.mu.Unlock()
		close(ch)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("REQUEST failed: %w", err)
	}

	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		c.mu.Lock()
		delete(c.pendingRPCs, correlationID)
		c.mu.Unlock()
		close(ch)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("REQUEST failed: %w", mapRPCError(err))
	}
	if !success {
		c.mu.Lock()
		delete(c.pendingRPCs, correlationID)
		c.mu.Unlock()
		close(ch)
		recordErr := fmt.Errorf("REQUEST failed: unexpected status")
		span.RecordError(recordErr)
		span.SetStatus(codes.Error, recordErr.Error())
		return nil, recordErr
	}

	iterator := &rpcIterator{
		ch:            ch,
		ctx:           ctx,
		correlationID: correlationID,
		client:        c,
	}
	return iterator, nil
}

// responseWriter implements ResponseWriter for workers.
type responseWriter struct {
	conn          *connection.Connection
	correlationID [16]byte
	seq           uint64
	mu            sync.Mutex
}

// Send emits one response frame. RPC RESPONSE (303) is one-way: server forwards to caller and does not ack the worker.
func (w *responseWriter) Send(body []byte) error {
	w.mu.Lock()
	seq := w.seq
	w.seq++
	w.mu.Unlock()

	return w.conn.SendFireAndForgetWithWriter(w.conn.LifecycleContext(), protocol.MessageTypeRpcResponse, rpcResponsePayloadWriter(w.correlationID, seq, body, false))
}

func (w *responseWriter) sendEnd() {
	w.mu.Lock()
	seq := w.seq
	w.mu.Unlock()

	// sendEnd is called from finalisation paths (worker return, iterator close).
	// Errors here are intentionally dropped: the correlation ID has already
	// been removed from the in-flight map, so there is no state to roll back.
	// The caller observes the cancellation/end via iterator.Err() or context.
	_ = w.conn.SendFireAndForgetWithWriter(w.conn.LifecycleContext(), protocol.MessageTypeRpcResponse, rpcResponsePayloadWriter(w.correlationID, seq, nil, true))
}

// rpcIterator iterates over response frames from a Call.
type rpcIterator struct {
	ch            chan ResponseFrame
	ctx           context.Context
	correlationID [16]byte
	client        *client
	current       ResponseFrame
	err           error
	done          bool
	mu            sync.Mutex // Protects done and err
}

func (it *rpcIterator) Next() bool {
	it.mu.Lock()
	if it.done {
		it.mu.Unlock()
		return false
	}
	it.mu.Unlock()

	// Eagerly check context cancellation before entering the select.  This
	// prevents the race where both it.ch has a buffered frame AND ctx.Done()
	// is already closed simultaneously — without this guard, Go's scheduler
	// would non-deterministically choose between the two cases, causing
	// callers to receive a stale frame even after cancellation (CS-008).
	if err := it.ctx.Err(); err != nil {
		it.mu.Lock()
		it.err = err
		it.done = true
		it.mu.Unlock()
		it.client.mu.Lock()
		delete(it.client.pendingRPCs, it.correlationID)
		it.client.mu.Unlock()
		return false
	}

	select {
	case frame, ok := <-it.ch:
		if !ok {
			it.mu.Lock()
			it.done = true
			it.mu.Unlock()
			return false
		}
		it.current = frame
		return true
	case <-it.ctx.Done():
		it.mu.Lock()
		it.err = it.ctx.Err()
		it.done = true
		it.mu.Unlock()
		// Clean up pending RPC immediately when context is canceled
		it.client.mu.Lock()
		delete(it.client.pendingRPCs, it.correlationID)
		it.client.mu.Unlock()
		return false
	}
}

func (it *rpcIterator) Value() ResponseFrame {
	return it.current
}

func (it *rpcIterator) Err() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.err
}

func (it *rpcIterator) Close() error {
	it.mu.Lock()
	it.done = true
	it.mu.Unlock()
	// Clean up pending RPC
	it.client.mu.Lock()
	delete(it.client.pendingRPCs, it.correlationID)
	it.client.mu.Unlock()
	return nil
}

func (c *client) ReplaceConnection(conn *connection.Connection) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn = conn
	if c.initialized {
		c.conn.RegisterRPCResponseHandler(c.handleRPCResponse)
		c.conn.RegisterRPCRequestHandler(c.handleRPCRequest)
	}
}

func (c *client) RestoreSubscriptions(ctx context.Context) error {
	c.mu.Lock()
	snapshot := make(map[string]RPCHandler, len(c.workers))
	for route, handler := range c.workers {
		snapshot[route] = handler
	}
	c.mu.Unlock()

	for route, handler := range snapshot {
		if _, err := c.subscribeWorker(ctx, route, handler); err != nil {
			return err
		}
	}
	return nil
}

func (c *client) subscribeWorker(ctx context.Context, route string, handler RPCHandler) (*Subscription, error) {
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeRpcSubscribeWorker, rpcSubscribeWorkerPayloadWriter(route))
	if err != nil {
		return nil, fmt.Errorf("SUBSCRIBE_WORKER request failed: %w", err)
	}

	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("SUBSCRIBE_WORKER failed: %w", mapRPCError(err))
	}
	if !success {
		return nil, fmt.Errorf("SUBSCRIBE_WORKER failed: unexpected status")
	}

	c.mu.Lock()
	c.workers[route] = handler
	c.mu.Unlock()
	return &Subscription{route: route, client: c}, nil
}
