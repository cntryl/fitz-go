// Package rpc implements the Fitz RPC domain client.
// Per CLIENT_SPEC.md: Bidirectional RPC with streaming responses.
package rpc

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/core/iter"
	"github.com/cntryl/fitz-go/internal/core/reconnect"
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

type responseStream struct {
	mu     sync.Mutex
	notify chan struct{}
	frames []ResponseFrame
	head   int
	closed bool
	err    error
}

func newResponseStream() *responseStream {
	return &responseStream{
		notify: make(chan struct{}, 1),
		frames: make([]ResponseFrame, 0, 1),
	}
}

func (s *responseStream) enqueue(frame ResponseFrame) bool {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return false
	}
	s.frames = append(s.frames, frame)
	s.mu.Unlock()
	s.signal()
	return true
}

func (s *responseStream) close() {
	s.closeWithError(nil)
}

func (s *responseStream) fail(err error) {
	s.closeWithError(err)
}

func (s *responseStream) closeWithError(err error) {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.err = err
	}
	s.mu.Unlock()
	s.signal()
}

func (s *responseStream) next(ctx context.Context) (ResponseFrame, bool, error) {
	for {
		s.mu.Lock()
		if s.head < len(s.frames) {
			frame := s.frames[s.head]
			s.frames[s.head] = ResponseFrame{}
			s.head++
			s.compactLocked()
			s.mu.Unlock()
			return frame, true, nil
		}
		if s.closed {
			err := s.err
			s.mu.Unlock()
			return ResponseFrame{}, false, err
		}
		notify := s.notify
		s.mu.Unlock()

		select {
		case <-notify:
		case <-ctx.Done():
			return ResponseFrame{}, false, ctx.Err()
		}
	}
}

func (s *responseStream) compactLocked() {
	if s.head == 0 {
		return
	}
	if s.head >= len(s.frames) {
		s.frames = s.frames[:0]
		s.head = 0
		return
	}
	if s.head < 32 || s.head*2 < len(s.frames) {
		return
	}
	copy(s.frames, s.frames[s.head:])
	for idx := len(s.frames) - s.head; idx < len(s.frames); idx++ {
		s.frames[idx] = ResponseFrame{}
	}
	s.frames = s.frames[:len(s.frames)-s.head]
	s.head = 0
}

func (s *responseStream) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
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
	pendingRPCs map[[16]byte]*responseStream
	initialized bool
}

// NewClient creates a new RPC domain client.
func NewClient(conn *connection.Connection) Client {
	c := &client{
		conn:        conn,
		workers:     make(map[string]RPCHandler),
		pendingRPCs: make(map[[16]byte]*responseStream),
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
// Payload: [uuid16 correlation_id][route][body].
func (c *client) handleRPCRequest(payload []byte) {
	if len(payload) < 16 {
		return
	}
	var correlationID [16]byte
	copy(correlationID[:], payload[:16])
	c.handleWorkerRequest(correlationID, payload[16:])
}

// handleRPCResponse handles incoming RPC RESPONSE frames (303).
// Per server rpc_codec.rs: [bytes correlation_id][u64 seq][bytes body][u8 stream_end]
// where "bytes" = [u32 BE len][data] (TLV bytes format)
func (c *client) handleRPCResponse(correlationID [16]byte, payload []byte) {
	if len(payload) < 1 {
		return
	}

	// RPC REQUEST worker deliveries are routed through message type 302.
	// A 303 without a pending correlation is stale or unexpected and must be dropped.
	c.mu.Lock()
	stream, isCall := c.pendingRPCs[correlationID]
	c.mu.Unlock()

	if isCall {
		// This is a response to our Call
		// Parse: [u64 sequence][u8 flags][bytes body]
		offset := 0
		if offset+8 > len(payload) {
			return
		}
		seq := binary.BigEndian.Uint64(payload[offset : offset+8])
		offset += 8

		if offset >= len(payload) {
			return
		}
		streamEnd := payload[offset]&1 != 0
		offset++

		// body is TLV bytes: [u32 len][data]
		if offset+4 > len(payload) {
			return
		}
		bodyLen := binary.BigEndian.Uint32(payload[offset : offset+4])
		offset += 4

		if offset+int(bodyLen) > len(payload) {
			return
		}
		body := payload[offset : offset+int(bodyLen)]

		if streamEnd && len(body) > 0 && body[0] == 1 {
			if _, _, terminalErr := connection.ParseStandardResponse(body); terminalErr != nil {
				c.mu.Lock()
				delete(c.pendingRPCs, correlationID)
				c.mu.Unlock()
				stream.fail(mapRPCError(terminalErr))
				return
			}
		}

		if !streamEnd || len(body) > 0 {
			_ = stream.enqueue(ResponseFrame{Body: body, Sequence: seq})
		}
		if streamEnd {
			c.mu.Lock()
			delete(c.pendingRPCs, correlationID)
			c.mu.Unlock()
			stream.close()
		}
		return
	}
}

// handleWorkerRequest processes an incoming request for a registered worker.
// Server forwards REQUEST payload: [uuid16 correlation_id][string route][bytes body].
// Note: correlationID was already parsed by the mux, but the remaining payload
// contains [string route][bytes body] after the correlation_id.
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
	if !ok {
		patterns := matchingWorkerPatterns(route, c.workers)
		if len(patterns) > 0 {
			handler, ok = c.workers[patterns[0]], true
		}
	}
	c.mu.Unlock()

	if !ok {
		return // No worker for this route
	}

	req := InboundRequest{
		CorrelationID: correlationID,
		Route:         route,
		ReplyRoute:    "",
		Body:          body,
	}

	w := &responseWriter{
		conn:          c.conn,
		correlationID: correlationID,
		seq:           0,
	}
	lifecycleCtx := c.conn.LifecycleContext()

	if !c.conn.LaunchAsyncHandler(lifecycleCtx, "fitz.rpc.worker_handler", c.conn.AsyncHandlerTimeout(), func(handlerCtx context.Context, span trace.Span) {
		if err := handler(handlerCtx, req, w); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			if log := c.conn.Logger(); log != nil {
				log.Warn("rpc worker handler failed", "route", route, "error", err)
			}
		}
		// Send stream_end
		w.sendEnd()
	}, trace.WithAttributes(
		attribute.String("fitz.route", route),
	)) {
		if log := c.conn.Logger(); log != nil {
			log.Warn("rpc worker handler dropped", "route", route, "reason", "async handler queue full")
		}
	}
}

type workerPatternSpecificity struct {
	literals   int
	singleStar int
	doubleStar int
	depth      int
}

func matchingWorkerPatterns(route string, workers map[string]RPCHandler) []string {
	patterns := make([]string, 0, len(workers))
	for pattern := range workers {
		if types.RouteMatchesPattern(route, pattern) {
			patterns = append(patterns, pattern)
		}
	}
	sort.Slice(patterns, func(i, j int) bool {
		left, right := scoreWorkerPattern(patterns[i]), scoreWorkerPattern(patterns[j])
		if left.literals != right.literals {
			return left.literals > right.literals
		}
		if left.singleStar != right.singleStar {
			return left.singleStar > right.singleStar
		}
		if left.doubleStar != right.doubleStar {
			return left.doubleStar < right.doubleStar
		}
		if left.depth != right.depth {
			return left.depth > right.depth
		}
		return patterns[i] < patterns[j]
	})
	return patterns
}

func scoreWorkerPattern(pattern string) workerPatternSpecificity {
	_, path, _ := strings.Cut(pattern, "://")
	segments := strings.Split(path, "/")
	score := workerPatternSpecificity{depth: len(segments)}
	for _, segment := range segments {
		switch segment {
		case "*":
			score.singleStar++
		case "**":
			score.doubleStar++
		default:
			score.literals++
		}
	}
	return score
}

// RegisterWorker per CLIENT_SPEC.md:
// Request: [worker_route_len][worker_route]
// Response: [status]
func (c *client) RegisterWorker(ctx context.Context, route string, handler RPCHandler) (*Subscription, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.rpc.RegisterWorker", trace.WithAttributes(attribute.String("fitz.route", route)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.DebugContext(ctx, "rpc.RegisterWorker", "route", route)
	}

	// Validate route format
	if err := types.ValidateRegistrationPattern(route, "rpc", 0); err != nil {
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
		log.DebugContext(ctx, "rpc.Call", "route", route)
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

	// Create response stream.
	stream := newResponseStream()

	c.mu.Lock()
	c.pendingRPCs[correlationID] = stream
	c.mu.Unlock()

	// RPC requests are one-way submissions; responses arrive asynchronously as
	// message type 303 frames correlated by the UUID above.
	err = c.conn.SendFireAndForgetWithWriter(ctx, protocol.MessageTypeRpcRequest, rpcRequestPayloadWriter(correlationID, route, "", body))
	if err != nil {
		c.mu.Lock()
		delete(c.pendingRPCs, correlationID)
		c.mu.Unlock()
		stream.close()
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("REQUEST failed: %w", err)
	}

	iterator := &rpcIterator{
		stream:        stream,
		ctx:           ctx,
		correlationID: correlationID,
		client:        c,
	}
	return iterator, nil
}

// ClosePendingRPCs fails all in-flight RPC call iterators with connection.ErrConnectionClosed.
func (c *client) ClosePendingRPCs() {
	c.mu.Lock()
	if len(c.pendingRPCs) == 0 {
		c.mu.Unlock()
		return
	}
	pending := c.pendingRPCs
	c.pendingRPCs = make(map[[16]byte]*responseStream, len(pending))
	c.mu.Unlock()

	for _, stream := range pending {
		stream.fail(connection.ErrConnectionClosed)
	}
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

	// sendEnd is called from finalization paths (worker return, iterator close).
	// Errors here are intentionally dropped: the correlation ID has already
	// been removed from the in-flight map, so there is no state to roll back.
	// The caller observes the cancellation/end via iterator.Err() or context.
	_ = w.conn.SendFireAndForgetWithWriter(w.conn.LifecycleContext(), protocol.MessageTypeRpcResponse, rpcResponsePayloadWriter(w.correlationID, seq, nil, true))
}

// rpcIterator iterates over response frames from a Call.
type rpcIterator struct {
	stream        *responseStream
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

	// Eagerly check context cancellation before waiting on the response stream.
	// This preserves deadline/cancel semantics even if the stream still has
	// queued frames from the server.
	if err := it.ctx.Err(); err != nil {
		it.mu.Lock()
		it.err = err
		it.done = true
		it.mu.Unlock()
		it.client.removePendingRPC(it.correlationID)
		return false
	}

	frame, ok, err := it.stream.next(it.ctx)
	if err != nil {
		it.mu.Lock()
		it.err = err
		it.done = true
		it.mu.Unlock()
		it.client.removePendingRPC(it.correlationID)
		return false
	}
	if !ok {
		it.mu.Lock()
		it.done = true
		it.mu.Unlock()
		return false
	}
	it.current = frame
	return true
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
	it.stream.close()
	// Clean up pending RPC
	it.client.removePendingRPC(it.correlationID)
	return nil
}

func (c *client) removePendingRPC(correlationID [16]byte) {
	c.mu.Lock()
	delete(c.pendingRPCs, correlationID)
	c.mu.Unlock()
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
	maps.Copy(snapshot, c.workers)
	c.mu.Unlock()

	restoredRoutes := make([]string, 0, len(snapshot))

	for route := range snapshot {
		if err := c.restoreSubscribeWorker(ctx, route); err != nil {
			for _, v := range slices.Backward(restoredRoutes) {
				c.rollbackRestoredWorker(v)
			}
			return err
		}
		restoredRoutes = append(restoredRoutes, route)
	}

	c.mu.Lock()
	maps.Copy(c.workers, snapshot)
	c.mu.Unlock()
	return nil
}

func (c *client) restoreSubscribeWorker(ctx context.Context, route string) error {
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeRpcSubscribeWorker, rpcSubscribeWorkerPayloadWriter(route))
	if err != nil {
		return fmt.Errorf("SUBSCRIBE_WORKER request failed: %w", err)
	}

	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return fmt.Errorf("SUBSCRIBE_WORKER failed: %w", mapRPCError(err))
	}
	if !success {
		return errors.New("SUBSCRIBE_WORKER failed: unexpected status")
	}
	return nil
}

func (c *client) rollbackRestoredWorker(route string) {
	ctx := c.conn.LifecycleContext()
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeRpcUnsubscribeWorker, rpcUnsubscribeWorkerPayloadWriter(route))
	_ = resp
	_ = err
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
		return nil, errors.New("SUBSCRIBE_WORKER failed: unexpected status")
	}

	c.mu.Lock()
	c.workers[route] = handler
	c.mu.Unlock()
	return &Subscription{route: route, client: c}, nil
}
