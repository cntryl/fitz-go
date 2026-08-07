package connection

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
)

// pendingRequest represents one in-flight request awaiting response.
type pendingRequest struct {
	waiter     *requestWaiter
	cancelFunc context.CancelFunc
	discard    bool
}

type requestWaiter struct {
	ready       chan struct{}
	response    []byte
	hasResponse bool
	closed      bool
}

var requestWaiterPool = sync.Pool{
	New: func() any {
		return &requestWaiter{ready: make(chan struct{}, 1)}
	},
}

func acquireRequestWaiter() *requestWaiter {
	waiter := requestWaiterPool.Get().(*requestWaiter)
	waiter.reset()
	return waiter
}

func releaseRequestWaiter(waiter *requestWaiter) {
	if waiter == nil {
		return
	}
	waiter.reset()
	requestWaiterPool.Put(waiter)
}

func (w *requestWaiter) reset() {
	if w == nil {
		return
	}
	w.response = nil
	w.hasResponse = false
	w.closed = false
	select {
	case <-w.ready:
	default:
	}
}

func (w *requestWaiter) deliver(payload []byte) {
	if w == nil {
		return
	}
	w.response = payload
	w.hasResponse = true
	select {
	case w.ready <- struct{}{}:
	default:
	}
}

func (w *requestWaiter) fail() {
	if w == nil {
		return
	}
	w.closed = true
	select {
	case w.ready <- struct{}{}:
	default:
	}
}

type requestQueue struct {
	items       []pendingRequest
	head        int
	live        int
	removed     int
	waiterIndex map[*requestWaiter]int
}

func (q *requestQueue) push(req pendingRequest) {
	q.items = append(q.items, req)
	q.live++
	if req.waiter != nil {
		if q.waiterIndex == nil {
			q.waiterIndex = make(map[*requestWaiter]int, 4)
		}
		q.waiterIndex[req.waiter] = len(q.items) - 1
	}
}

func (q *requestQueue) pop() (pendingRequest, bool) {
	q.trimHead()
	if q.head >= len(q.items) {
		q.reset()
		return pendingRequest{}, false
	}
	req := q.items[q.head]
	q.items[q.head] = pendingRequest{}
	q.head++
	if req.waiter != nil {
		if q.waiterIndex != nil {
			delete(q.waiterIndex, req.waiter)
		}
	}
	q.live--
	q.trimHead()
	q.compactIfNeeded()
	return req, true
}

func (q *requestQueue) removeWaiter(waiter *requestWaiter, preserveResponseSlot bool) (pendingRequest, bool) {
	if q.waiterIndex == nil {
		return pendingRequest{}, false
	}
	idx, ok := q.waiterIndex[waiter]
	if !ok || idx < q.head || idx >= len(q.items) {
		delete(q.waiterIndex, waiter)
		return pendingRequest{}, false
	}
	req := q.items[idx]
	if req.waiter != waiter {
		delete(q.waiterIndex, waiter)
		return pendingRequest{}, false
	}

	delete(q.waiterIndex, waiter)
	if preserveResponseSlot {
		// Preserve a tombstone so a late FIFO response is discarded instead of
		// being delivered to the next request with the same message type.
		q.items[idx] = pendingRequest{discard: true}
	} else {
		q.items[idx] = pendingRequest{}
		q.live--
		q.removed++
		if idx == q.head {
			q.trimHead()
		}
	}
	q.compactIfNeeded()
	return req, true
}

func (q *requestQueue) len() int {
	return q.live
}

func (q *requestQueue) trimHead() {
	for q.head < len(q.items) && q.items[q.head].waiter == nil && !q.items[q.head].discard {
		q.head++
		if q.removed > 0 {
			q.removed--
		}
	}
	if q.head >= len(q.items) {
		q.reset()
		return
	}
}

func (q *requestQueue) compactIfNeeded() {
	if q.live == 0 {
		q.reset()
		return
	}
	if q.head < 32 && q.removed < 32 {
		return
	}
	if q.head*2 < len(q.items) && q.removed*2 < len(q.items) {
		return
	}
	q.compact()
}

func (q *requestQueue) compact() {
	if q.live == 0 {
		q.reset()
		return
	}
	items := make([]pendingRequest, 0, q.live)
	waiterIndex := make(map[*requestWaiter]int, q.live)
	for idx := q.head; idx < len(q.items); idx++ {
		req := q.items[idx]
		if req.waiter == nil && !req.discard {
			continue
		}
		if req.waiter != nil {
			waiterIndex[req.waiter] = len(items)
		}
		items = append(items, req)
	}
	for idx := range q.items {
		q.items[idx] = pendingRequest{}
	}
	q.items = items
	q.head = 0
	q.removed = 0
	q.waiterIndex = waiterIndex
}

func (q *requestQueue) reset() {
	for idx := range q.items {
		q.items[idx] = pendingRequest{}
	}
	q.items = q.items[:0]
	q.head = 0
	q.live = 0
	q.removed = 0
	q.waiterIndex = nil
}

// Multiplexer routes responses to pending requests using FIFO ordering.
// Per CLIENT_SPEC.md: Responses are matched to requests in order received.
// This matches the server's sequential processing model per actor/route.
type Multiplexer struct {
	// FIFO queue of pending requests per MessageType
	// Key = MessageType (100-199 for KV, 200-299 for Queue, etc.)
	// Value = queue of pendingRequest (oldest at front)
	pending   map[uint16]*requestQueue
	mu        sync.Mutex
	handlerMu sync.RWMutex

	// Async delivery handlers (Notice NOTIFY, Schedule NOTIFY, RPC REQUEST to worker, RPC RESPONSE per CLIENT_SPEC.md)
	// notifyHandlers are keyed by message type so every subscription-capable domain can register independently.
	notifyHandlers  map[uint16]func(subID uint64, route string, payload []byte)
	rawPushHandlers map[uint16]func(payload []byte)
	rpcReqHandler   func(payload []byte) // incoming RPC REQUEST (302) dispatched to worker
	rpcRespHandler  func(correlationID [16]byte, payload []byte)

	// Metrics for observability
	requestsInFlight atomic.Int64
	requestsTotal    atomic.Uint64
	responsesTotal   atomic.Uint64
	responsesDropped atomic.Uint64
	logger           *slog.Logger

	closed atomic.Bool
}

// NewMultiplexer creates a new multiplexer.
func NewMultiplexer() *Multiplexer {
	return &Multiplexer{
		pending:         make(map[uint16]*requestQueue),
		notifyHandlers:  make(map[uint16]func(subID uint64, route string, payload []byte)),
		rawPushHandlers: make(map[uint16]func(payload []byte)),
	}
}

func (m *Multiplexer) setLogger(logger *slog.Logger) {
	m.logger = logger
}

// RegisterRequest registers a pending request before sending.
// The responseChan will receive the response payload when it arrives.
// The cancelFunc is called if the request needs to be cleaned up.
func (m *Multiplexer) RegisterRequest(msgType uint16, responseChan chan []byte, cancelFunc context.CancelFunc) {
	waiter := acquireRequestWaiter()
	m.RegisterRequestWaiter(msgType, waiter, cancelFunc)
	go func() {
		defer releaseRequestWaiter(waiter)
		if responseChan == nil {
			<-waiter.ready
			return
		}

		<-waiter.ready
		if waiter.closed {
			close(responseChan)
			return
		}
		responseChan <- append([]byte(nil), waiter.response...)
	}()
}

func (m *Multiplexer) RegisterRequestWaiter(msgType uint16, waiter *requestWaiter, cancelFunc context.CancelFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed.Load() {
		waiter.fail()
		return
	}

	queue, exists := m.pending[msgType]
	if !exists {
		queue = &requestQueue{}
		m.pending[msgType] = queue
	}

	queue.push(pendingRequest{waiter: waiter, cancelFunc: cancelFunc})

	m.requestsInFlight.Add(1)
	m.requestsTotal.Add(1)
}

func (m *Multiplexer) UnregisterRequestWaiter(msgType uint16, waiter *requestWaiter) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	queue, exists := m.pending[msgType]
	if !exists {
		return false
	}

	if _, ok := queue.removeWaiter(waiter, false); ok {
		m.requestsInFlight.Add(-1)
		return true
	}
	return false
}

// AbandonRequestWaiter removes a canceled request while preserving its FIFO
// response slot so a late response cannot be delivered to a later request.
func (m *Multiplexer) AbandonRequestWaiter(msgType uint16, waiter *requestWaiter) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	queue, exists := m.pending[msgType]
	if !exists {
		return false
	}
	if _, ok := queue.removeWaiter(waiter, true); ok {
		m.requestsInFlight.Add(-1)
		return true
	}
	return false
}

// Dispatch routes a response to the appropriate handler.
// Called by the connection's dispatch loop when a frame arrives.
func (m *Multiplexer) Dispatch(msgType uint16, payload []byte) {
	// Handle async deliveries (per CLIENT_SPEC.md MessageType ranges)
	if msgType == 111 {
		if handler := m.notifyHandler(msgType); handler != nil {
			m.handleKVNotify(msgType, payload, handler)
		}
		return
	}
	// Queue NOTIFY (209) uses a queue-specific payload shape.
	if msgType == 209 {
		if handler := m.notifyHandler(msgType); handler != nil {
			m.handleQueueNotify(msgType, payload, handler)
		}
		return
	}
	// Lease, Notice, Stream, and Schedule use the shared route/payload shape.
	if msgType == 409 || msgType == 504 || msgType == 609 || msgType == 705 {
		if handler := m.notifyHandler(msgType); handler != nil {
			m.handleNotify(msgType, payload, handler)
		}
		return
	}
	if msgType == 302 {
		// RPC worker requests have the shape [uuid16 correlation_id][route][body].
		// Route those explicitly so an in-flight 302 call cannot steal an inbound worker request.
		if m.looksLikeRpcWorkerRequest(payload) {
			m.handleRpcRequest(payload)
			return
		}
	}
	if msgType == 303 {
		// 303 can be either: (1) sync response (ack) to our outgoing RESPONSE as worker, or (2) async RESPONSE forwarded to us as caller.
		// If we have a pending request for 303, this is the ack; otherwise deliver to caller's response handler.
		m.mu.Lock()
		queue303, exists303 := m.pending[303]
		hasPending303 := exists303 && queue303.len() > 0
		m.mu.Unlock()
		if hasPending303 {
			// Sync response to worker's SendRequest(303) — fall through to sync path below
		} else {
			m.handleRpcResponse(msgType, payload)
			return
		}
	}

	// Synchronous request/response - route to oldest pending request
	m.mu.Lock()
	queue, exists := m.pending[msgType]
	if !exists || queue.len() == 0 {
		m.mu.Unlock()
		if handler := m.rawPushHandler(msgType); handler != nil {
			handler(payload)
			return
		}
		// Unexpected response (no pending request)
		// This can happen if context was canceled but response arrived
		m.responsesDropped.Add(1)
		return
	}

	// Pop oldest pending request (FIFO order)
	req, _ := queue.pop()
	m.mu.Unlock()
	if req.discard {
		m.responsesDropped.Add(1)
		return
	}

	m.requestsInFlight.Add(-1)
	m.responsesTotal.Add(1)

	req.waiter.deliver(payload)
}

// handleKVNotify processes KV NOTIFY messages (111):
// [u64 subscription_id][string exact_route][u64 mutation_count].
func (m *Multiplexer) handleKVNotify(msgType uint16, payload []byte, handler func(subID uint64, route string, payload []byte)) {
	if len(payload) < 8+4+8 {
		m.dropFrame(msgType, payload, "kv notify missing fields")
		return
	}
	subID := binary.BigEndian.Uint64(payload[:8])
	routeBytes, offset, ok := readU32Bytes(payload, 8)
	if !ok || len(payload) != offset+8 {
		m.dropFrame(msgType, payload, "kv notify malformed route or mutation count")
		return
	}
	handler(subID, string(routeBytes), payload[offset:])
}

// handleNotify processes shared-shape NOTIFY messages (409 Lease, 504 Notice, 609 Stream, 705 Schedule).
// Per CLIENT_SPEC.md: [u64 BE subscription_id][u32 route_len][route][u32 payload_len][payload]
func (m *Multiplexer) handleNotify(msgType uint16, payload []byte, handler func(subID uint64, route string, payload []byte)) {
	if len(payload) < 8 {
		m.dropFrame(msgType, payload, "notify missing subscription_id")
		return // Malformed
	}

	offset := 0

	// Read subscription_id (u64 BE)
	subID := binary.BigEndian.Uint64(payload[offset : offset+8])
	offset += 8

	// Read route length and route
	if len(payload) < offset+4 {
		m.dropFrame(msgType, payload, "notify missing route length")
		return
	}
	routeBytes, offset, ok := readU32Bytes(payload, offset)
	if !ok {
		m.dropFrame(msgType, payload, "notify truncated route")
		return
	}
	route := string(routeBytes)

	// Read payload length and payload
	if len(payload) < offset+4 {
		m.dropFrame(msgType, payload, "notify missing payload length")
		return
	}
	msgPayload, _, ok := readU32Bytes(payload, offset)
	if !ok {
		m.dropFrame(msgType, payload, "notify truncated payload")
		return
	}

	handler(subID, route, msgPayload)
}

// handleQueueNotify processes Queue NOTIFY messages (209).
// Per queue sink wire format: [u64 BE subscription_id][u32 route_len][route][u64 ready][u64 delayed][u64 inflight]
func (m *Multiplexer) handleQueueNotify(msgType uint16, payload []byte, handler func(subID uint64, route string, payload []byte)) {
	if len(payload) < 8+4 {
		m.dropFrame(msgType, payload, "queue notify missing header")
		return
	}

	offset := 0
	subID := binary.BigEndian.Uint64(payload[offset : offset+8])
	offset += 8

	routeBytes, offset, ok := readU32Bytes(payload, offset)
	if !ok {
		m.dropFrame(msgType, payload, "queue notify truncated route")
		return
	}
	route := string(routeBytes)

	// Queue watch notifications carry three u64 counters after the route.
	if len(payload) != offset+24 {
		m.dropFrame(msgType, payload, "queue notify missing counters")
		return
	}

	handler(subID, route, payload[offset:])
}

// handleRpcRequest processes RPC REQUEST messages (async delivery to worker).
// Payload is full request: [u32 len=16][16 bytes correlation_id][route][reply_route][body]
func (m *Multiplexer) handleRpcRequest(payload []byte) {
	if handler := m.rpcRequestHandler(); handler != nil {
		handler(payload)
	}
}

func (m *Multiplexer) looksLikeRpcWorkerRequest(payload []byte) bool {
	if len(payload) < 24 {
		return false
	}
	routeLen := int(binary.BigEndian.Uint32(payload[16:20]))
	bodyLenOffset := 20 + routeLen
	if routeLen < 1 || bodyLenOffset > len(payload)-4 {
		return false
	}
	bodyLen := int(binary.BigEndian.Uint32(payload[bodyLenOffset : bodyLenOffset+4]))
	return bodyLenOffset+4+bodyLen == len(payload)
}

// handleRpcResponse processes RPC RESPONSE messages (async delivery).
// Per server rpc_codec.rs: [uuid16 correlation_id][u64 seq][u8 flags][bytes body].
func (m *Multiplexer) handleRpcResponse(msgType uint16, payload []byte) {
	if len(payload) < 16 {
		m.dropFrame(msgType, payload, "rpc response missing correlation_id")
		return
	}

	var correlationID [16]byte
	copy(correlationID[:], payload[:16])

	// Call registered handler with remaining payload (seq + body + stream_end)
	if handler := m.rpcResponseHandler(); handler != nil {
		handler(correlationID, payload[16:])
	}
}

func (m *Multiplexer) dropFrame(msgType uint16, payload []byte, reason string) {
	m.responsesDropped.Add(1)
	if m.logger != nil {
		m.logger.Warn("dropped malformed frame", "msg_type", msgType, "payload_len", len(payload), "reason", reason)
	}
}

func readU32Bytes(payload []byte, offset int) ([]byte, int, bool) {
	if offset < 0 || offset > len(payload) || len(payload)-offset < 4 {
		return nil, offset, false
	}
	length := binary.BigEndian.Uint32(payload[offset : offset+4])
	offset += 4
	if uint64(length) > uint64(len(payload)-offset) {
		return nil, offset, false
	}
	end := offset + int(length)
	return payload[offset:end], end, true
}

// SetNotifyHandler registers the handler for NOTIFY messages for the given message type.
// msgType should be 111 (KV), 209 (Queue), 409 (Lease), 504 (Notice),
// 609 (Stream), or 705 (Schedule).
func (m *Multiplexer) SetNotifyHandler(msgType uint16, handler func(subID uint64, route string, payload []byte)) {
	m.handlerMu.Lock()
	defer m.handlerMu.Unlock()

	if m.notifyHandlers == nil {
		m.notifyHandlers = make(map[uint16]func(subID uint64, route string, payload []byte))
	}
	m.notifyHandlers[msgType] = handler
}

func (m *Multiplexer) SetRawPushHandler(msgType uint16, handler func(payload []byte)) {
	m.handlerMu.Lock()
	defer m.handlerMu.Unlock()
	m.rawPushHandlers[msgType] = handler
}

// SetRPCRequestHandler registers the handler for RPC REQUEST messages (302).
// Called by the RPC domain client so workers receive forwarded requests.
func (m *Multiplexer) SetRPCRequestHandler(handler func(payload []byte)) {
	m.handlerMu.Lock()
	defer m.handlerMu.Unlock()
	m.rpcReqHandler = handler
}

// SetRPCResponseHandler registers the handler for RPC RESPONSE messages.
// Called by the RPC domain client.
func (m *Multiplexer) SetRPCResponseHandler(handler func(correlationID [16]byte, payload []byte)) {
	m.handlerMu.Lock()
	defer m.handlerMu.Unlock()
	m.rpcRespHandler = handler
}

func (m *Multiplexer) notifyHandler(msgType uint16) func(subID uint64, route string, payload []byte) {
	m.handlerMu.RLock()
	defer m.handlerMu.RUnlock()
	return m.notifyHandlers[msgType]
}

func (m *Multiplexer) rawPushHandler(msgType uint16) func(payload []byte) {
	m.handlerMu.RLock()
	defer m.handlerMu.RUnlock()
	return m.rawPushHandlers[msgType]
}

func (m *Multiplexer) rpcRequestHandler() func(payload []byte) {
	m.handlerMu.RLock()
	defer m.handlerMu.RUnlock()
	return m.rpcReqHandler
}

func (m *Multiplexer) rpcResponseHandler() func(correlationID [16]byte, payload []byte) {
	m.handlerMu.RLock()
	defer m.handlerMu.RUnlock()
	return m.rpcRespHandler
}

// Close shuts down the multiplexer and fails all pending requests.
func (m *Multiplexer) Close() error {
	if !m.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Fail all pending waiters so callers unblock on shutdown.
	for _, queue := range m.pending {
		for idx := queue.head; idx < len(queue.items); idx++ {
			req := queue.items[idx]
			if req.waiter == nil {
				continue
			}
			if req.cancelFunc != nil {
				req.cancelFunc()
			}
			m.requestsInFlight.Add(-1)
			req.waiter.fail()
		}
	}

	// Clear pending requests
	m.pending = make(map[uint16]*requestQueue)

	return nil
}

// Metrics returns current multiplexer statistics.
func (m *Multiplexer) Metrics() MultiplexerMetrics {
	return MultiplexerMetrics{
		RequestsInFlight: m.requestsInFlight.Load(),
		RequestsTotal:    m.requestsTotal.Load(),
		ResponsesTotal:   m.responsesTotal.Load(),
		ResponsesDropped: m.responsesDropped.Load(),
	}
}

// MultiplexerMetrics contains multiplexer statistics.
type MultiplexerMetrics struct {
	RequestsInFlight int64
	RequestsTotal    uint64
	ResponsesTotal   uint64
	ResponsesDropped uint64
}

// String provides a human-readable representation of metrics.
func (m MultiplexerMetrics) String() string {
	return fmt.Sprintf(
		"Mux[in_flight=%d, total_req=%d, total_resp=%d, dropped=%d]",
		m.RequestsInFlight, m.RequestsTotal, m.ResponsesTotal, m.ResponsesDropped,
	)
}
