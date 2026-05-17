package rpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/core/encoding"
	"github.com/cntryl/fitz-go/internal/testkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	encoding.WriteBytes(buf, body)
	if streamEnd {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
	return append([]byte(nil), buf.Bytes()...)
}

func rpcWorkerPayload(route, replyRoute string, body []byte) []byte {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	encoding.WriteRoute(buf, route)
	encoding.WriteRoute(buf, replyRoute)
	encoding.WriteBytes(buf, body)
	return append([]byte(nil), buf.Bytes()...)
}

func TestShouldDeliverResponseFrameGivenPendingRequestWhenHandleRPCResponseCalled(t *testing.T) {
	// Arrange
	c := &client{pendingRPCs: make(map[[16]byte]chan ResponseFrame)}
	var correlationID [16]byte
	correlationID[0] = 1
	ch := make(chan ResponseFrame, 1)
	c.pendingRPCs[correlationID] = ch

	// Act
	c.handleRPCResponse(correlationID, rpcResponsePayload(3, []byte("payload"), false))

	// Assert
	select {
	case frame := <-ch:
		assert.Equal(t, uint64(3), frame.Sequence)
		assert.Equal(t, []byte("payload"), frame.Body)
	case <-time.After(time.Second):
		t.Fatal("response frame not delivered")
	}
	_, stillPending := c.pendingRPCs[correlationID]
	assert.True(t, stillPending)
}

func TestShouldCleanupPendingResponseGivenStreamEndWhenHandleRPCResponseCalled(t *testing.T) {
	// Arrange
	c := &client{pendingRPCs: make(map[[16]byte]chan ResponseFrame)}
	var correlationID [16]byte
	correlationID[0] = 2
	ch := make(chan ResponseFrame, 1)
	c.pendingRPCs[correlationID] = ch

	// Act
	c.handleRPCResponse(correlationID, rpcResponsePayload(4, nil, true))

	// Assert
	_, stillPending := c.pendingRPCs[correlationID]
	assert.False(t, stillPending)
	_, ok := <-ch
	assert.False(t, ok)
}

func TestShouldDeliverTerminalResponseFrameGivenBodyWhenHandleRPCResponseCalled(t *testing.T) {
	// Arrange
	c := &client{pendingRPCs: make(map[[16]byte]chan ResponseFrame)}
	var correlationID [16]byte
	correlationID[0] = 3
	ch := make(chan ResponseFrame, 1)
	c.pendingRPCs[correlationID] = ch

	// Act
	c.handleRPCResponse(correlationID, rpcResponsePayload(5, []byte("final"), true))

	// Assert
	frame, ok := <-ch
	require.True(t, ok)
	assert.Equal(t, uint64(5), frame.Sequence)
	assert.Equal(t, []byte("final"), frame.Body)

	_, stillPending := c.pendingRPCs[correlationID]
	assert.False(t, stillPending)
	_, ok = <-ch
	assert.False(t, ok)
}

func TestShouldDispatchWorkerRequestGivenRegisteredWorkerWhenHandleWorkerRequestCalled(t *testing.T) {
	// Arrange
	conn, _ := newStartedRPCConnection(t)
	requests := make(chan InboundRequest, 1)
	c := &client{
		conn:        conn,
		workers:     make(map[string]RPCHandler),
		pendingRPCs: make(map[[16]byte]chan ResponseFrame),
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
		assert.Equal(t, replyRoute, req.ReplyRoute)
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
		pendingRPCs: make(map[[16]byte]chan ResponseFrame),
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
		pendingRPCs: make(map[[16]byte]chan ResponseFrame),
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
	ch := make(chan ResponseFrame)
	c := &client{pendingRPCs: make(map[[16]byte]chan ResponseFrame)}
	var correlationID [16]byte
	correlationID[0] = 4
	c.pendingRPCs[correlationID] = ch
	it := &rpcIterator{
		ch:            ch,
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
	ch := make(chan ResponseFrame)
	c := &client{pendingRPCs: make(map[[16]byte]chan ResponseFrame)}
	var correlationID [16]byte
	correlationID[0] = 5
	c.pendingRPCs[correlationID] = ch
	it := &rpcIterator{
		ch:            ch,
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
