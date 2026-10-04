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
	"time"

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

// CancellationOutcome reports how the broker handled a best-effort cancellation.
type CancellationOutcome string

const (
	CancellationNotRequested      CancellationOutcome = "not_requested"
	CancellationRequestNotSent    CancellationOutcome = "request_not_sent"
	CancellationUnsupported       CancellationOutcome = "unsupported"
	CancellationQueuedRemoved     CancellationOutcome = "queued_removed"
	CancellationForwarded         CancellationOutcome = "forwarded"
	CancellationWorkerUnsupported CancellationOutcome = "worker_unsupported"
	CancellationAlreadyTerminal   CancellationOutcome = "already_terminal"
	CancellationUnknown           CancellationOutcome = "unknown_or_unauthorized"
	CancellationForwardingFailed  CancellationOutcome = "forwarding_failed"
	CancellationUnconfirmed       CancellationOutcome = "unconfirmed"
	CancellationConnectionClosed  CancellationOutcome = "connection_closed"
)

// CallIterator streams RPC response frames and exposes the broker cancellation result.
type CallIterator interface {
	iter.Iterator[ResponseFrame]
	CancellationResult() <-chan CancellationOutcome
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
	route   string
	version uint64
	client  *client
}

// Unsubscribe removes this worker registration.
func (s *Subscription) Unsubscribe() error {
	if s.client != nil {
		return s.client.unsubscribeWorker(s.route, s.version)
	}
	return nil
}

// Client is the RPC domain client interface.
type Client interface {
	// RegisterWorker registers a worker handler for the given route.
	RegisterWorker(ctx context.Context, route string, maxConcurrent uint32, handler RPCHandler) (*Subscription, error)

	// Call sends an RPC request and returns an iterator over response frames.
	// Callers must call Close on the returned iterator when done to release resources.
	Call(ctx context.Context, route string, body []byte) (CallIterator, error)
}

type activeRPCInvocation struct {
	cancel                context.CancelFunc
	cancellationRequested bool
}

type client struct {
	conn *connection.Connection

	mu                sync.Mutex
	workers           map[string]RPCHandler // route -> handler
	workerVersions    map[string]uint64
	workerConcurrency map[string]uint32
	nextWorkerVersion uint64
	pendingRPCs       map[[16]byte]*responseStream
	calls             map[[16]byte]*rpcIterator
	cancelRequested   map[[16]byte]bool
	cancelTimers      map[[16]byte]*time.Timer
	activeInvocations map[[16]byte]*activeRPCInvocation
	initialized       bool
}

// NewClient creates a new RPC domain client.
func NewClient(conn *connection.Connection) Client {
	c := &client{
		conn:              conn,
		workers:           make(map[string]RPCHandler),
		workerConcurrency: make(map[string]uint32),
		workerVersions:    make(map[string]uint64),
		pendingRPCs:       make(map[[16]byte]*responseStream),
		calls:             make(map[[16]byte]*rpcIterator),
		cancelRequested:   make(map[[16]byte]bool),
		cancelTimers:      make(map[[16]byte]*time.Timer),
		activeInvocations: make(map[[16]byte]*activeRPCInvocation),
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
	c.conn.RegisterRawPushHandler(protocol.MessageTypeRpcLifecycle, c.handleRPCLifecycle)
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
		// This is a response to our Call. Validate the entire frame before
		// exposing its body so malformed terminal errors cannot look successful.
		response, err := decodeRPCResponsePayload(payload)
		if err != nil {
			c.finishCall(correlationID, CancellationNotRequested)
			stream.fail(fmt.Errorf("malformed RPC response: %w", err))
			return
		}

		if response.streamEnd && len(response.body) > 0 && response.body[0] == 1 {
			if _, _, terminalErr := connection.ParseStandardResponse(response.body); terminalErr != nil {
				c.finishCall(correlationID, CancellationNotRequested)
				stream.fail(mapRPCError(terminalErr))
				return
			}
		}

		if !response.streamEnd || len(response.body) > 0 {
			_ = stream.enqueue(ResponseFrame{Body: response.body, Sequence: response.sequence})
		}
		if response.streamEnd {
			c.finishCall(correlationID, CancellationNotRequested)
			stream.close()
		}
		return
	}
}

type decodedRPCResponse struct {
	sequence  uint64
	streamEnd bool
	body      []byte
}

func decodeRPCResponsePayload(payload []byte) (decodedRPCResponse, error) {
	const fixedSize = 8 + 1 + 4
	if len(payload) < fixedSize {
		return decodedRPCResponse{}, fmt.Errorf("response is too short: %d bytes", len(payload))
	}
	sequence := binary.BigEndian.Uint64(payload[:8])
	flags := payload[8]
	if flags&^byte(1) != 0 {
		return decodedRPCResponse{}, fmt.Errorf("unsupported response flags: %#x", flags)
	}
	bodyLen := uint64(binary.BigEndian.Uint32(payload[9:13]))
	bodyStart := uint64(fixedSize)
	if bodyLen != uint64(len(payload))-bodyStart {
		return decodedRPCResponse{}, fmt.Errorf("response body length %d does not match remaining bytes %d", bodyLen, uint64(len(payload))-bodyStart)
	}
	return decodedRPCResponse{
		sequence:  sequence,
		streamEnd: flags&1 != 0,
		body:      payload[fixedSize:],
	}, nil
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
	offset += int(bodyLen)
	remainingBudgetMS, err := decodeRPCRequestBudget(payload[offset:])
	if err != nil {
		return
	}

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
	active := &activeRPCInvocation{}
	c.mu.Lock()
	if c.activeInvocations == nil {
		c.activeInvocations = make(map[[16]byte]*activeRPCInvocation)
	}
	c.activeInvocations[correlationID] = active
	c.mu.Unlock()
	receivedAt := time.Now()

	if !c.conn.LaunchAsyncHandler(lifecycleCtx, "fitz.rpc.worker_handler", c.conn.AsyncHandlerTimeout(), func(handlerCtx context.Context, span trace.Span) {
		requestCtx, cancel := context.WithCancel(handlerCtx)
		if remainingBudgetMS != nil {
			remaining := time.Duration(*remainingBudgetMS)*time.Millisecond - time.Since(receivedAt)
			remaining = max(remaining, 0)
			deadlineCtx, deadlineCancel := context.WithTimeout(requestCtx, remaining)
			requestCtx = deadlineCtx
			baseCancel := cancel
			cancel = func() {
				deadlineCancel()
				baseCancel()
			}
		}
		c.mu.Lock()
		active.cancel = cancel
		cancelImmediately := active.cancellationRequested
		c.mu.Unlock()
		if cancelImmediately {
			cancel()
			c.finishWorkerInvocation(correlationID, active)
			c.sendWorkerCleanupAck(correlationID)
			return
		}
		defer cancel()

		handlerErr := handler(requestCtx, req, w)
		if c.workerCancellationRequested(correlationID, active) {
			if c.finishWorkerInvocation(correlationID, active) {
				c.sendWorkerCleanupAck(correlationID)
			}
			return
		}
		if handlerErr != nil {
			err := handlerErr
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			if log := c.conn.Logger(); log != nil {
				log.Warn("rpc worker handler failed", "route", route, "error", err)
			}
			w.sendError(err)
		} else {
			// Send stream_end
			w.sendEnd()
		}
		c.finishWorkerInvocation(correlationID, active)
		// Cancellation may have been ordered before the terminal response while
		// its notification is still in transit. Negotiated cleanup acknowledgments
		// are safe even when the broker has already completed the call.
		c.sendWorkerCleanupAck(correlationID)
	}, trace.WithAttributes(
		attribute.String("fitz.route", route),
	)) {
		c.finishWorkerInvocation(correlationID, active)
		w.sendError(ErrRPCBackpressure)
		c.sendWorkerCleanupAck(correlationID)
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
func (c *client) RegisterWorker(ctx context.Context, route string, maxConcurrent uint32, handler RPCHandler) (*Subscription, error) {
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
	if maxConcurrent < 1 || maxConcurrent > 1024 {
		return nil, errors.New("maxConcurrent must be between 1 and 1024")
	}
	c.mu.Lock()
	_, alreadyRegistered := c.workers[route]
	c.mu.Unlock()
	if alreadyRegistered {
		return nil, fmt.Errorf("worker route %q is already registered", route)
	}

	c.initRPCHandler()

	sub, err := c.subscribeWorker(ctx, route, maxConcurrent, handler)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return sub, nil
}

// unsubscribeWorker removes a worker registration.
func (c *client) unsubscribeWorker(route string, version uint64) error {
	c.mu.Lock()
	if c.workerVersions[route] != version {
		c.mu.Unlock()
		return nil
	}
	delete(c.workers, route)
	delete(c.workerVersions, route)
	delete(c.workerConcurrency, route)
	c.mu.Unlock()

	ctx := c.conn.LifecycleContext()
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeRpcUnsubscribeWorker, rpcUnsubscribeWorkerPayloadWriter(route))
	if err != nil {
		return fmt.Errorf("UNSUBSCRIBE_WORKER request failed: %w", err)
	}
	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return fmt.Errorf("UNSUBSCRIBE_WORKER failed: %w", mapRPCError(err))
	}
	if !success {
		return errors.New("UNSUBSCRIBE_WORKER failed: unexpected status")
	}
	return nil
}

// Call per CLIENT_SPEC.md:
// Request: [correlation_id(16)][route_len][route][reply_route_len][reply_route][body_len][body]
// Response: [status] (ack that request was dispatched)
// Actual responses come via RPC RESPONSE (303) messages.
func (c *client) Call(ctx context.Context, route string, body []byte) (CallIterator, error) {
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
	iterator := &rpcIterator{
		stream:        stream,
		ctx:           ctx,
		correlationID: correlationID,
		client:        c,
		cancellation:  make(chan CancellationOutcome, 1),
	}
	var remainingBudgetMS *uint32
	_, capabilities := c.conn.ServerCapabilities()
	if capabilities&protocol.CapabilityRPCCancellation != 0 {
		if deadline, ok := ctx.Deadline(); ok {
			remaining := time.Until(deadline).Milliseconds()
			remaining = max(remaining, 0)
			remaining = min(remaining, 86_400_000)
			budget := uint32(remaining)
			remainingBudgetMS = &budget
		}
	}

	c.mu.Lock()
	if c.calls == nil {
		c.calls = make(map[[16]byte]*rpcIterator)
		c.cancelRequested = make(map[[16]byte]bool)
		c.cancelTimers = make(map[[16]byte]*time.Timer)
	}
	c.pendingRPCs[correlationID] = stream
	c.calls[correlationID] = iterator
	c.mu.Unlock()

	// RPC requests are one-way submissions; responses arrive asynchronously as
	// message type 303 frames correlated by the UUID above.
	err = c.conn.SendFireAndForgetWithWriter(ctx, protocol.MessageTypeRpcRequest, rpcRequestPayloadWriterWithBudget(correlationID, route, "", body, remainingBudgetMS))
	if err != nil {
		c.finishCall(correlationID, CancellationRequestNotSent)
		stream.close()
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("REQUEST failed: %w", err)
	}

	iterator.setStopContext(context.AfterFunc(ctx, func() {
		reason := byte(1)
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			reason = 2
		}
		c.cancelPendingRPC(correlationID, reason)
	}))
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

	// sendEnd is called from finalization paths (worker return, iterator close).
	// Errors here are intentionally dropped: the correlation ID has already
	// been removed from the in-flight map, so there is no state to roll back.
	// The caller observes the cancellation/end via iterator.Err() or context.
	_ = w.conn.SendFireAndForgetWithWriter(w.conn.LifecycleContext(), protocol.MessageTypeRpcResponse, rpcResponsePayloadWriter(w.correlationID, seq, nil, true))
}

func (w *responseWriter) sendError(err error) {
	w.mu.Lock()
	seq := w.seq
	w.mu.Unlock()
	message := err.Error()
	if len(message) > 1024 {
		message = strings.ToValidUTF8(message[:1024], "")
	}
	body := make([]byte, 9+len(message))
	body[0] = 1
	binary.BigEndian.PutUint32(body[1:5], 6010)
	binary.BigEndian.PutUint32(body[5:9], uint32(len(message)))
	copy(body[9:], message)
	_ = w.conn.SendFireAndForgetWithWriter(w.conn.LifecycleContext(), protocol.MessageTypeRpcResponse, rpcResponsePayloadWriter(w.correlationID, seq, body, true))
}

// rpcIterator iterates over response frames from a Call.
type rpcIterator struct {
	stream        *responseStream
	ctx           context.Context
	correlationID [16]byte
	client        *client
	cancellation  chan CancellationOutcome
	completed     bool
	stopContext   func() bool
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
		reason := byte(1)
		if errors.Is(err, context.DeadlineExceeded) {
			reason = 2
		}
		it.client.cancelPendingRPC(it.correlationID, reason)
		return false
	}

	frame, ok, err := it.stream.next(it.ctx)
	if err != nil {
		it.mu.Lock()
		it.err = err
		it.done = true
		it.mu.Unlock()
		if it.ctx.Err() != nil {
			reason := byte(1)
			if errors.Is(it.ctx.Err(), context.DeadlineExceeded) {
				reason = 2
			}
			it.client.cancelPendingRPC(it.correlationID, reason)
		}
		return false
	}
	if !ok {
		it.mu.Lock()
		if err := it.ctx.Err(); err != nil {
			it.err = err
		}
		it.done = true
		it.mu.Unlock()
		return false
	}
	it.current = frame
	return true
}

func (it *rpcIterator) CancellationResult() <-chan CancellationOutcome {
	return it.cancellation
}

func (it *rpcIterator) complete(outcome CancellationOutcome) {
	it.mu.Lock()
	if it.completed {
		it.mu.Unlock()
		return
	}
	it.completed = true
	stopContext := it.stopContext
	it.mu.Unlock()
	if stopContext != nil {
		stopContext()
	}
	if it.cancellation != nil {
		it.cancellation <- outcome
		close(it.cancellation)
	}
}

func (it *rpcIterator) setStopContext(stopContext func() bool) {
	it.mu.Lock()
	if it.completed {
		it.mu.Unlock()
		stopContext()
		return
	}
	it.stopContext = stopContext
	it.mu.Unlock()
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
	it.client.cancelPendingRPC(it.correlationID, 1)
	return nil
}

func (c *client) RestoreSubscriptions(ctx context.Context) error {
	c.mu.Lock()
	snapshot := make(map[string]RPCHandler, len(c.workers))
	concurrency := make(map[string]uint32, len(c.workerConcurrency))
	maps.Copy(snapshot, c.workers)
	maps.Copy(concurrency, c.workerConcurrency)
	c.mu.Unlock()

	restoredRoutes := make([]string, 0, len(snapshot))

	for route := range snapshot {
		if err := c.restoreSubscribeWorker(ctx, route, concurrency[route]); err != nil {
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

func (c *client) restoreSubscribeWorker(ctx context.Context, route string, maxConcurrent uint32) error {
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeRpcSubscribeWorker, rpcSubscribeWorkerPayloadWriterWithCancellation(route, maxConcurrent, c.supportsRPCCancellation()))
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

func (c *client) subscribeWorker(ctx context.Context, route string, maxConcurrent uint32, handler RPCHandler) (*Subscription, error) {
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeRpcSubscribeWorker, rpcSubscribeWorkerPayloadWriterWithCancellation(route, maxConcurrent, c.supportsRPCCancellation()))
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
	c.nextWorkerVersion++
	version := c.nextWorkerVersion
	c.workers[route] = handler
	c.workerConcurrency[route] = maxConcurrent
	c.workerVersions[route] = version
	c.mu.Unlock()
	return &Subscription{route: route, version: version, client: c}, nil
}
