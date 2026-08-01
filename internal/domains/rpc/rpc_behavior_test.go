package rpc

import (
	"context"
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
