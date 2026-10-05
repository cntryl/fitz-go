package rpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/core/encoding"
	"github.com/cntryl/fitz-go/internal/protocol"
	"github.com/cntryl/fitz-go/internal/testkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func TestShouldOrderMatchingWorkersBySpecificityThenLexically(t *testing.T) {
	// Arrange
	workers := map[string]RPCHandler{
		"rpc://realm/**":            nil,
		"rpc://realm/*/*":           nil,
		"rpc://realm/area/*":        nil,
		"rpc://realm/*/resource":    nil,
		"rpc://realm/area/resource": nil,
		"rpc://realm/area/**":       nil,
		"rpc://realm/**/resource":   nil,
	}

	// Act
	patterns := matchingWorkerPatterns("rpc://realm/area/resource", workers)

	// Assert
	require.Equal(t, []string{
		"rpc://realm/area/resource",
		"rpc://realm/*/resource",
		"rpc://realm/area/*",
		"rpc://realm/**/resource",
		"rpc://realm/area/**",
		"rpc://realm/*/*",
		"rpc://realm/**",
	}, patterns)
}

func TestShouldReturnBrokerErrorGivenRejectedWorkerUnsubscribe(t *testing.T) {
	transport := newScriptedRPCRestoreTransport()
	conn := connection.New(transport, connection.Config{ReadTimeout: time.Second})
	require.NoError(t, conn.Start(context.Background()))
	t.Cleanup(func() { _ = conn.Close() })
	client := NewClient(conn)
	baseWrites := scriptedRPCWriteCount(transport)
	go func() {
		waitForRPCWrites(t, transport, baseWrites+1)
		transport.enqueue(scriptedRPCFrame(t, protocol.MessageTypeRpcSubscribeWorker, rpcAckPayload()))
		waitForRPCWrites(t, transport, baseWrites+2)
		transport.enqueue(scriptedRPCFrame(t, protocol.MessageTypeRpcUnsubscribeWorker, rpcDomainErrorPayload(3002, "worker not found")))
	}()
	subscription, err := client.RegisterWorker(context.Background(), "rpc://realm/area/method", 7, func(context.Context, InboundRequest, ResponseWriter) error { return nil })
	require.NoError(t, err)

	err = subscription.Unsubscribe()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "worker not found")
}

func TestShouldRejectWorkerConcurrencyAboveWireLimit(t *testing.T) {
	// Arrange
	conn := connection.New(testkit.NewMockTransport(), connection.Config{})
	client := NewClient(conn)

	// Act
	_, err := client.RegisterWorker(context.Background(), "rpc://realm/area/method", 1025, func(context.Context, InboundRequest, ResponseWriter) error { return nil })

	// Assert
	require.EqualError(t, err, "maxConcurrent must be between 1 and 1024")
}

func rpcDomainErrorPayload(code uint32, message string) []byte {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	buf.WriteByte(1)
	connection.WriteU32BE(buf, code)
	connection.WriteString(buf, message)
	return append([]byte(nil), buf.Bytes()...)
}

type scriptedRPCRestoreTransport struct {
	mu      sync.Mutex
	written [][]byte
	readCh  chan []byte
	closed  chan struct{}
	once    sync.Once
}

func newScriptedRPCRestoreTransport() *scriptedRPCRestoreTransport {
	return &scriptedRPCRestoreTransport{
		readCh: make(chan []byte, 8),
		closed: make(chan struct{}),
	}
}

func (s *scriptedRPCRestoreTransport) Write(ctx context.Context, frame []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closed:
		return connection.ErrConnectionClosed
	default:
	}
	s.mu.Lock()
	s.written = append(s.written, append([]byte(nil), frame...))
	s.mu.Unlock()
	return nil
}

func (s *scriptedRPCRestoreTransport) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.closed:
		return nil, connection.ErrConnectionClosed
	case frame := <-s.readCh:
		return append([]byte(nil), frame...), nil
	}
}

func (s *scriptedRPCRestoreTransport) Close() error {
	s.once.Do(func() {
		close(s.closed)
	})
	return nil
}

func (s *scriptedRPCRestoreTransport) RemoteAddr() string {
	return "scripted://rpc"
}

func (s *scriptedRPCRestoreTransport) enqueue(frame []byte) {
	s.readCh <- append([]byte(nil), frame...)
}

func scriptedRPCFrame(t *testing.T, msgType uint16, payload []byte) []byte {
	t.Helper()
	frame := protocol.EncodeFrameOwned(msgType, payload)
	defer frame.Release()
	return append([]byte(nil), frame.Bytes()...)
}

func rpcAckPayload() []byte {
	return []byte{0}
}

func scriptedRPCWriteCount(trans *scriptedRPCRestoreTransport) int {
	trans.mu.Lock()
	defer trans.mu.Unlock()
	return len(trans.written)
}

func waitForRPCWrites(t *testing.T, trans *scriptedRPCRestoreTransport, expected int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return scriptedRPCWriteCount(trans) >= expected
	}, time.Second, 10*time.Millisecond)
}

func newStartedRPCConnection(t *testing.T) (*connection.Connection, *testkit.MockTransport) {
	t.Helper()
	transport := testkit.NewMockTransport()
	conn := connection.New(transport, connection.Config{Token: "", ReadTimeout: time.Second})
	require.NoError(t, conn.Start(context.Background()))
	t.Cleanup(func() {
		_ = conn.Close()
	})
	return conn, transport
}

func newStartedRPCConnectionWithCapabilities(t *testing.T, capabilities uint32) (*connection.Connection, *testkit.MockTransport) {
	t.Helper()
	return newStartedRPCConnectionWithConfig(t, capabilities, connection.Config{ReadTimeout: time.Second})
}

func newStartedRPCConnectionWithConfig(t *testing.T, capabilities uint32, config connection.Config) (*connection.Connection, *testkit.MockTransport) {
	t.Helper()
	transport := testkit.NewMockTransport()
	payload := make([]byte, 6)
	binary.BigEndian.PutUint16(payload[:2], 1)
	binary.BigEndian.PutUint32(payload[2:], capabilities)
	transport.SetReadFrames([][]byte{protocol.EncodeFrame(protocol.MessageTypeServerHello, payload)})
	conn := connection.New(transport, config)
	require.NoError(t, conn.Start(context.Background()))
	require.Eventually(t, func() bool {
		_, received := conn.ServerCapabilities()
		return received == capabilities
	}, time.Second, 10*time.Millisecond)
	t.Cleanup(func() { _ = conn.Close() })
	return conn, transport
}

func decodeWrittenRPCFrames(t *testing.T, transport *testkit.MockTransport) []protocol.Frame {
	t.Helper()
	frames := make([]protocol.Frame, 0)
	for _, written := range transport.GetWrittenFrames() {
		decoded, err := protocol.DecodeFrames(written)
		require.NoError(t, err)
		frames = append(frames, decoded...)
	}
	return frames
}

func TestShouldEncodeNegotiatedRPCBudgetAndWorkerSupportExtensions(t *testing.T) {
	// Arrange
	var requestID [16]byte
	requestID[0] = 9
	budget := uint32(321)

	// Act
	requestPayload, err := encodeRPCRequest(requestID, "rpc://realm/a", "", []byte("body"))
	require.NoError(t, err)
	var negotiatedRequest bytes.Buffer
	rpcRequestPayloadWriterWithBudget(requestID, "rpc://realm/a", "", []byte("body"), &budget)(&negotiatedRequest)
	var legacyRegistration, negotiatedRegistration bytes.Buffer
	rpcSubscribeWorkerPayloadWriter("rpc://realm/a", 4)(&legacyRegistration)
	rpcSubscribeWorkerPayloadWriterWithCancellation("rpc://realm/a", 4, true)(&negotiatedRegistration)

	// Assert
	assert.Equal(t, requestPayload, negotiatedRequest.Bytes()[:len(requestPayload)])
	assert.Equal(t, []byte{1, 1, 0, 0, 1, 65}, negotiatedRequest.Bytes()[len(requestPayload):])
	assert.Equal(t, legacyRegistration.Bytes(), negotiatedRegistration.Bytes()[:legacyRegistration.Len()])
	assert.Equal(t, []byte{1, 1}, negotiatedRegistration.Bytes()[legacyRegistration.Len():])
	decodedBudget, err := decodeRPCRequestBudget(negotiatedRequest.Bytes()[len(requestPayload):])
	require.NoError(t, err)
	require.NotNil(t, decodedBudget)
	assert.Equal(t, budget, *decodedBudget)
}

func TestShouldRequestAndResolveRemoteCancellationGivenCancelledCallerContext(t *testing.T) {
	// Arrange
	conn, transport := newStartedRPCConnectionWithCapabilities(t, protocol.CapabilityRPCCancellation)
	client := NewClient(conn).(*client)
	ctx, cancel := context.WithCancel(context.Background())

	// Act
	iterator, err := client.Call(ctx, "rpc://realm/area/resource", []byte("body"))
	require.NoError(t, err)
	cancel()
	require.Eventually(t, func() bool {
		for _, frame := range decodeWrittenRPCFrames(t, transport) {
			if frame.MessageType == protocol.MessageTypeRpcCancellation {
				return true
			}
		}
		return false
	}, time.Second, 10*time.Millisecond)
	var result []byte
	for _, frame := range decodeWrittenRPCFrames(t, transport) {
		if frame.MessageType == protocol.MessageTypeRpcCancellation {
			result = frame.Payload
			break
		}
	}
	client.handleRPCLifecycle(append(append([]byte{4}, result[1:17]...), 2))

	// Assert
	select {
	case outcome := <-iterator.CancellationResult():
		assert.Equal(t, CancellationForwarded, outcome)
	case <-time.After(time.Second):
		t.Fatal("cancellation outcome was not delivered")
	}
	assert.Equal(t, byte(1), result[0])
	assert.Equal(t, byte(1), result[17])
}

func TestShouldCancelWorkerAndAcknowledgeCleanupGivenBrokerCancellation(t *testing.T) {
	// Arrange
	conn, transport := newStartedRPCConnectionWithCapabilities(t, protocol.CapabilityRPCCancellation)
	started := make(chan struct{})
	canceled := make(chan struct{})
	route := "rpc://realm/area/resource"
	client := &client{
		conn: conn,
		workers: map[string]RPCHandler{route: func(ctx context.Context, _ InboundRequest, _ ResponseWriter) error {
			close(started)
			<-ctx.Done()
			close(canceled)
			return nil
		}},
		pendingRPCs:       make(map[[16]byte]*responseStream),
		activeInvocations: make(map[[16]byte]*activeRPCInvocation),
	}
	var correlationID [16]byte
	correlationID[0] = 7

	// Act
	client.handleWorkerRequest(correlationID, rpcWorkerPayload(route, "", []byte("body")))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker handler did not start")
	}
	client.handleRPCLifecycle(append(append([]byte{2}, correlationID[:]...), 1))
	require.Eventually(t, func() bool {
		select {
		case <-canceled:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		for _, frame := range decodeWrittenRPCFrames(t, transport) {
			if frame.MessageType == protocol.MessageTypeRpcCancellation && len(frame.Payload) == 17 && frame.Payload[0] == 3 {
				return true
			}
		}
		return false
	}, time.Second, 10*time.Millisecond)

	// Assert
	foundAck := false
	for _, frame := range decodeWrittenRPCFrames(t, transport) {
		if frame.MessageType == protocol.MessageTypeRpcCancellation && len(frame.Payload) == 17 && frame.Payload[0] == 3 {
			assert.Equal(t, correlationID[:], frame.Payload[1:])
			foundAck = true
		}
	}
	assert.True(t, foundAck)
}

func TestShouldAcknowledgeFinishedCleanupBeforeDelayedCancellationArrives(t *testing.T) {
	// Arrange
	conn, transport := newStartedRPCConnectionWithCapabilities(t, protocol.CapabilityRPCCancellation)
	cleanupStarted := make(chan struct{})
	cleanup := make(chan struct{})
	route := "rpc://realm/area/resource"
	c := &client{conn: conn, workers: map[string]RPCHandler{route: func(context.Context, InboundRequest, ResponseWriter) error {
		defer func() { close(cleanupStarted); <-cleanup }()
		return nil
	}}, activeInvocations: make(map[[16]byte]*activeRPCInvocation)}
	var id [16]byte
	id[0] = 8

	// Act
	c.handleWorkerRequest(id, rpcWorkerPayload(route, "", nil))
	select {
	case <-cleanupStarted:
	case <-time.After(time.Second):
		t.Fatal("handler cleanup did not start")
	}
	for _, frame := range decodeWrittenRPCFrames(t, transport) {
		require.NotEqual(t, protocol.MessageTypeRpcCancellation, frame.MessageType)
	}
	close(cleanup)

	// Assert
	require.Eventually(t, func() bool {
		for _, frame := range decodeWrittenRPCFrames(t, transport) {
			if frame.MessageType == protocol.MessageTypeRpcCancellation && bytes.Equal(frame.Payload, append([]byte{3}, id[:]...)) {
				return true
			}
		}
		return false
	}, time.Second, 10*time.Millisecond)
}

func TestShouldCountDispatchBufferingAgainstInboundDeadline(t *testing.T) {
	// Arrange
	transport := testkit.NewMockTransport()
	conn := connection.New(transport, connection.Config{AsyncHandlerMaxConcurrency: 1, AsyncHandlerQueueCapacity: 1})
	require.NoError(t, conn.Start(context.Background()))
	t.Cleanup(func() { _ = conn.Close() })
	started, release := make(chan struct{}), make(chan struct{})
	require.True(t, conn.LaunchAsyncHandler(context.Background(), "test.buffer", time.Second, func(context.Context, trace.Span) {
		close(started)
		<-release
	}))
	<-started
	received := make(chan error, 1)
	route := "rpc://realm/area/resource"
	c := &client{conn: conn, workers: map[string]RPCHandler{route: func(ctx context.Context, _ InboundRequest, _ ResponseWriter) error {
		received <- ctx.Err()
		return nil
	}}, activeInvocations: make(map[[16]byte]*activeRPCInvocation)}
	payload := append(rpcWorkerPayload(route, "", nil), 1, 1, 0, 0, 0, 1)

	// Act
	c.handleWorkerRequest([16]byte{}, payload)
	<-time.After(25 * time.Millisecond)
	close(release)

	// Assert
	select {
	case <-received:
		t.Fatal("worker handler started after its inherited deadline")
	case <-time.After(25 * time.Millisecond):
	}
}

func TestShouldClearAndCancelWorkerOnlyInvocationsOnDisconnect(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	id := [16]byte{9}
	c := &client{activeInvocations: map[[16]byte]*activeRPCInvocation{id: {cancel: cancel}}}

	// Act
	c.ClosePendingRPCs()

	// Assert
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Empty(t, c.activeInvocations)
}

func TestShouldStopRunningAndBufferedWorkerOnlyRequestsOnDisconnect(t *testing.T) {
	// Arrange
	conn, _ := newStartedRPCConnectionWithConfig(t, protocol.CapabilityRPCCancellation,
		connection.Config{AsyncHandlerMaxConcurrency: 1, AsyncHandlerQueueCapacity: 1})
	started, finished, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { close(release) })
	bufferedCalled := make(chan struct{}, 1)
	route := "rpc://realm/area/resource"
	c := &client{conn: conn, workers: map[string]RPCHandler{route: func(ctx context.Context, request InboundRequest, _ ResponseWriter) error {
		if string(request.Body) == "buffered" {
			bufferedCalled <- struct{}{}
			return nil
		}
		close(started)
		select {
		case <-ctx.Done():
		case <-release:
		}
		close(finished)
		return nil
	}}, activeInvocations: make(map[[16]byte]*activeRPCInvocation)}
	c.handleWorkerRequest([16]byte{1}, rpcWorkerPayload(route, "", []byte("running")))
	<-started
	c.handleWorkerRequest([16]byte{2}, rpcWorkerPayload(route, "", []byte("buffered")))

	// Act
	c.ClosePendingRPCs()

	// Assert
	select {
	case <-finished:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("worker-only disconnect did not cancel running handler")
	}
	select {
	case <-bufferedCalled:
		t.Fatal("disconnected buffered handler was started")
	case <-time.After(25 * time.Millisecond):
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	require.Empty(t, c.activeInvocations)
}

func TestShouldAcknowledgeCanceledBufferedWorkerWithoutStartingHandler(t *testing.T) {
	// Arrange
	conn, transport := newStartedRPCConnectionWithConfig(t, protocol.CapabilityRPCCancellation, connection.Config{AsyncHandlerMaxConcurrency: 1, AsyncHandlerQueueCapacity: 1})
	started, release := make(chan struct{}), make(chan struct{})
	require.True(t, conn.LaunchAsyncHandler(context.Background(), "test.buffer", time.Second, func(context.Context, trace.Span) { close(started); <-release }))
	<-started
	called := make(chan struct{}, 1)
	route := "rpc://realm/area/resource"
	c := &client{conn: conn, workers: map[string]RPCHandler{route: func(context.Context, InboundRequest, ResponseWriter) error { called <- struct{}{}; return nil }}, activeInvocations: make(map[[16]byte]*activeRPCInvocation)}
	var id [16]byte
	id[0] = 9

	// Act
	c.handleWorkerRequest(id, rpcWorkerPayload(route, "", nil))
	c.handleRPCLifecycle(append(append([]byte{2}, id[:]...), 1))
	require.Eventually(t, func() bool {
		for _, frame := range decodeWrittenRPCFrames(t, transport) {
			if frame.MessageType == protocol.MessageTypeRpcCancellation {
				return true
			}
		}
		return false
	}, time.Second, time.Millisecond)
	close(release)

	// Assert
	select {
	case <-called:
		t.Fatal("canceled buffered work was started")
	case <-time.After(25 * time.Millisecond):
	}
}

func rpcResponsePayload(sequence uint64, body []byte, streamEnd bool) []byte {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	encoding.WriteU64(buf, sequence)
	if streamEnd {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
	encoding.WriteBytes(buf, body)
	return append([]byte(nil), buf.Bytes()...)
}

func rpcWorkerPayload(route, replyRoute string, body []byte) []byte {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	encoding.WriteRoute(buf, route)
	encoding.WriteBytes(buf, body)
	return append([]byte(nil), buf.Bytes()...)
}

func TestShouldDeliverResponseFrameGivenPendingRequestWhenHandleRPCResponseCalled(t *testing.T) {
	// Arrange
	c := &client{pendingRPCs: make(map[[16]byte]*responseStream)}
	var correlationID [16]byte
	correlationID[0] = 1
	stream := newResponseStream()
	c.pendingRPCs[correlationID] = stream

	// Act
	c.handleRPCResponse(correlationID, rpcResponsePayload(3, []byte("payload"), false))

	// Assert
	frame, ok, err := stream.next(context.Background())
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, uint64(3), frame.Sequence)
	assert.Equal(t, []byte("payload"), frame.Body)
	_, stillPending := c.pendingRPCs[correlationID]
	assert.True(t, stillPending)
}

func TestShouldCleanupPendingResponseGivenStreamEndWhenHandleRPCResponseCalled(t *testing.T) {
	// Arrange
	c := &client{pendingRPCs: make(map[[16]byte]*responseStream)}
	var correlationID [16]byte
	correlationID[0] = 2
	stream := newResponseStream()
	c.pendingRPCs[correlationID] = stream

	// Act
	c.handleRPCResponse(correlationID, rpcResponsePayload(4, nil, true))

	// Assert
	_, stillPending := c.pendingRPCs[correlationID]
	assert.False(t, stillPending)
	_, ok, err := stream.next(context.Background())
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestShouldDeliverTerminalResponseFrameGivenBodyWhenHandleRPCResponseCalled(t *testing.T) {
	// Arrange
	c := &client{pendingRPCs: make(map[[16]byte]*responseStream)}
	var correlationID [16]byte
	correlationID[0] = 3
	stream := newResponseStream()
	c.pendingRPCs[correlationID] = stream

	// Act
	c.handleRPCResponse(correlationID, rpcResponsePayload(5, []byte("final"), true))

	// Assert
	frame, ok, err := stream.next(context.Background())
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, uint64(5), frame.Sequence)
	assert.Equal(t, []byte("final"), frame.Body)

	_, stillPending := c.pendingRPCs[correlationID]
	assert.False(t, stillPending)
	_, ok, err = stream.next(context.Background())
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestShouldNotBlockDispatchGivenStoppedConsumerWhenHandleRPCResponseCalled(t *testing.T) {
	c := &client{pendingRPCs: make(map[[16]byte]*responseStream)}
	var correlationID [16]byte
	correlationID[0] = 8
	stream := newResponseStream()
	c.pendingRPCs[correlationID] = stream
	payload := rpcResponsePayload(7, []byte("payload"), false)
	done := make(chan struct{})

	go func() {
		for range 128 {
			c.handleRPCResponse(correlationID, payload)
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handleRPCResponse blocked behind stopped consumer")
	}
	stream.close()
}

func TestShouldIgnoreUnexpectedResponseGivenRegisteredWorkerWhenHandleRPCResponseCalled(t *testing.T) {
	// Arrange
	called := make(chan struct{}, 1)
	route := "rpc://realm/area/resource"
	c := &client{
		workers: map[string]RPCHandler{
			route: func(context.Context, InboundRequest, ResponseWriter) error {
				called <- struct{}{}
				return nil
			},
		},
		pendingRPCs: make(map[[16]byte]*responseStream),
	}
	var correlationID [16]byte
	correlationID[0] = 10

	// Act
	c.handleRPCResponse(correlationID, rpcWorkerPayload(route, "rpc://realm/area/replies", []byte("body")))

	// Assert
	select {
	case <-called:
		t.Fatal("unexpected response routed to worker handler")
	case <-time.After(50 * time.Millisecond):
	}
	assert.Empty(t, c.pendingRPCs)
}

func TestShouldDispatchWorkerRequestGivenRegisteredWorkerWhenHandleWorkerRequestCalled(t *testing.T) {
	// Arrange
	conn, _ := newStartedRPCConnection(t)
	requests := make(chan InboundRequest, 1)
	c := &client{
		conn:        conn,
		workers:     make(map[string]RPCHandler),
		pendingRPCs: make(map[[16]byte]*responseStream),
	}
	route := "rpc://realm/area/resource"
	replyRoute := "rpc://realm/area/replies"
	c.workers[route] = func(_ context.Context, req InboundRequest, _ ResponseWriter) error {
		requests <- req
		return nil
	}
	var correlationID [16]byte
	correlationID[0] = 9

	// Act
	c.handleWorkerRequest(correlationID, rpcWorkerPayload(route, replyRoute, []byte("body")))

	// Assert
	select {
	case req := <-requests:
		assert.Equal(t, correlationID, req.CorrelationID)
		assert.Equal(t, route, req.Route)
		assert.Empty(t, req.ReplyRoute)
		assert.Equal(t, []byte("body"), req.Body)
	case <-time.After(time.Second):
		t.Fatal("worker request not delivered")
	}
}

func TestShouldIgnoreMalformedWorkerPayloadGivenShortPayloadWhenHandleWorkerRequestCalled(t *testing.T) {
	// Arrange
	called := false
	c := &client{
		workers:     map[string]RPCHandler{"rpc://realm/area/resource": func(context.Context, InboundRequest, ResponseWriter) error { called = true; return nil }},
		pendingRPCs: make(map[[16]byte]*responseStream),
	}

	// Act
	c.handleWorkerRequest([16]byte{}, []byte{0, 0, 0})

	// Assert
	assert.False(t, called)
}

func TestShouldReturnErrorGivenCorrelationIDGenerationFailureWhenCallCalled(t *testing.T) {
	// Arrange
	conn, transport := newStartedRPCConnection(t)
	client := &client{
		conn:        conn,
		workers:     make(map[string]RPCHandler),
		pendingRPCs: make(map[[16]byte]*responseStream),
	}
	originalReadRandom := readRandom
	readRandom = func([]byte) (int, error) {
		return 0, errors.New("entropy unavailable")
	}
	t.Cleanup(func() {
		readRandom = originalReadRandom
	})

	// Act
	iter, err := client.Call(context.Background(), "rpc://realm/area/resource", []byte("payload"))

	// Assert
	require.Error(t, err)
	require.Nil(t, iter)
	assert.Contains(t, err.Error(), "generate correlation id")
	assert.Len(t, transport.GetWrittenFrames(), 1)
	assert.Empty(t, client.pendingRPCs)
}

func TestShouldCleanupPendingRPCGivenContextDeadlineWhenNextCalled(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	stream := newResponseStream()
	c := &client{pendingRPCs: make(map[[16]byte]*responseStream)}
	var correlationID [16]byte
	correlationID[0] = 4
	c.pendingRPCs[correlationID] = stream
	it := &rpcIterator{
		stream:        stream,
		ctx:           ctx,
		correlationID: correlationID,
		client:        c,
	}

	// Act
	ok := it.Next()

	// Assert
	assert.False(t, ok)
	assert.ErrorIs(t, it.Err(), context.DeadlineExceeded)
	_, stillPending := c.pendingRPCs[correlationID]
	assert.False(t, stillPending)
}

func TestShouldCleanupPendingRPCGivenCanceledContextWhenNextCalled(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stream := newResponseStream()
	c := &client{pendingRPCs: make(map[[16]byte]*responseStream)}
	var correlationID [16]byte
	correlationID[0] = 5
	c.pendingRPCs[correlationID] = stream
	it := &rpcIterator{
		stream:        stream,
		ctx:           ctx,
		correlationID: correlationID,
		client:        c,
	}

	// Act
	ok := it.Next()

	// Assert
	assert.False(t, ok)
	assert.ErrorIs(t, it.Err(), context.Canceled)
	_, stillPending := c.pendingRPCs[correlationID]
	assert.False(t, stillPending)
}

func TestShouldFailPendingRPCGivenConnectionLossWhenClosePendingRPCsCalled(t *testing.T) {
	// Arrange
	c := &client{pendingRPCs: make(map[[16]byte]*responseStream)}
	var correlationID [16]byte
	correlationID[0] = 6
	stream := newResponseStream()
	c.pendingRPCs[correlationID] = stream

	// Act
	c.ClosePendingRPCs()

	// Assert
	_, stillPending := c.pendingRPCs[correlationID]
	assert.False(t, stillPending)
	_, ok, err := stream.next(context.Background())
	require.False(t, ok)
	require.ErrorIs(t, err, connection.ErrConnectionClosed)
}

func TestShouldPreserveWorkersGivenRestoreFailureWhenRestoreSubscriptionsCalled(t *testing.T) {
	trans := newScriptedRPCRestoreTransport()
	conn := connection.New(trans, connection.Config{Token: "", ReadTimeout: time.Second})
	require.NoError(t, conn.Start(context.Background()))
	t.Cleanup(func() {
		_ = conn.Close()
	})
	baseWrites := scriptedRPCWriteCount(trans)

	handlerAlpha := func(context.Context, InboundRequest, ResponseWriter) error { return nil }
	handlerBravo := func(context.Context, InboundRequest, ResponseWriter) error { return nil }
	c := &client{
		conn:        conn,
		workers:     map[string]RPCHandler{"rpc://realm/area/alpha": handlerAlpha, "rpc://realm/area/bravo": handlerBravo},
		pendingRPCs: make(map[[16]byte]*responseStream),
	}

	go func() {
		waitForRPCWrites(t, trans, baseWrites+1)
		trans.enqueue(scriptedRPCFrame(t, protocol.MessageTypeRpcSubscribeWorker, rpcAckPayload()))
		waitForRPCWrites(t, trans, baseWrites+2)
		trans.enqueue(scriptedRPCFrame(t, protocol.MessageTypeRpcSubscribeWorker, []byte{}))
		waitForRPCWrites(t, trans, baseWrites+3)
		trans.enqueue(scriptedRPCFrame(t, protocol.MessageTypeRpcUnsubscribeWorker, rpcAckPayload()))
	}()

	err := c.RestoreSubscriptions(context.Background())
	require.Error(t, err)

	c.mu.Lock()
	defer c.mu.Unlock()
	assert.Len(t, c.workers, 2)
	_, ok := c.workers["rpc://realm/area/alpha"]
	assert.True(t, ok)
	_, ok = c.workers["rpc://realm/area/bravo"]
	assert.True(t, ok)
	assert.Equal(t, baseWrites+3, len(trans.written))
}
