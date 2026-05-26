package connection

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestShouldRouteRpcWorkerRequestGivenPendingRpcRequestWhenDispatchCalled(t *testing.T) {
	mux := NewMultiplexer()
	t.Cleanup(func() {
		require.NoError(t, mux.Close())
	})

	responseChan := make(chan []byte, 1)
	mux.RegisterRequest(protocol.MessageTypeRpcRequest, responseChan, nil)

	workerRequests := make(chan []byte, 1)
	mux.SetRPCRequestHandler(func(payload []byte) {
		workerRequests <- append([]byte(nil), payload...)
	})

	payload := rpcWorkerRequestPayloadForTest(
		"rpc://realm/area/resource",
		"rpc://realm/area/replies",
		[]byte("body"),
	)

	mux.Dispatch(protocol.MessageTypeRpcRequest, payload)

	select {
	case got := <-workerRequests:
		require.Equal(t, payload, got)
	case <-time.After(time.Second):
		t.Fatal("worker request was not routed to the RPC request handler")
	}

	select {
	case got := <-responseChan:
		t.Fatalf("unexpected response delivery for worker request: %q", got)
	default:
	}
}

func TestShouldUnregisterMiddleWaiterGivenPendingRequestsWhenDispatchCalled(t *testing.T) {
	mux := NewMultiplexer()
	t.Cleanup(func() {
		require.NoError(t, mux.Close())
	})

	waiter1 := acquireRequestWaiter()
	waiter2 := acquireRequestWaiter()
	waiter3 := acquireRequestWaiter()
	t.Cleanup(func() {
		releaseRequestWaiter(waiter1)
		releaseRequestWaiter(waiter2)
		releaseRequestWaiter(waiter3)
	})

	mux.RegisterRequestWaiter(100, waiter1, nil)
	mux.RegisterRequestWaiter(100, waiter2, nil)
	mux.RegisterRequestWaiter(100, waiter3, nil)

	require.True(t, mux.UnregisterRequestWaiter(100, waiter2))
	require.Equal(t, int64(2), mux.Metrics().RequestsInFlight)

	mux.Dispatch(100, []byte("first"))
	mux.Dispatch(100, []byte("third"))

	select {
	case <-waiter1.ready:
		require.False(t, waiter1.closed)
		require.Equal(t, []byte("first"), waiter1.response)
	case <-time.After(time.Second):
		t.Fatal("first waiter did not receive response")
	}

	select {
	case <-waiter3.ready:
		require.False(t, waiter3.closed)
		require.Equal(t, []byte("third"), waiter3.response)
	case <-time.After(time.Second):
		t.Fatal("third waiter did not receive response")
	}

	select {
	case <-waiter2.ready:
		t.Fatal("removed waiter should not be signaled")
	default:
	}

	require.Equal(t, int64(0), mux.Metrics().RequestsInFlight)
}

func rpcWorkerRequestPayloadForTest(route string, replyRoute string, body []byte) []byte {
	payload := make([]byte, 0, 20+4+len(route)+4+len(replyRoute)+4+len(body))

	corrLen := make([]byte, 4)
	binary.BigEndian.PutUint32(corrLen, 16)
	payload = append(payload, corrLen...)
	payload = append(payload, make([]byte, 16)...)

	writeTLVString := func(value string) {
		encoded := []byte(value)
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(encoded)))
		payload = append(payload, lenBuf...)
		payload = append(payload, encoded...)
	}

	writeTLVBytes := func(value []byte) {
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(value)))
		payload = append(payload, lenBuf...)
		payload = append(payload, value...)
	}

	writeTLVString(route)
	writeTLVString(replyRoute)
	writeTLVBytes(body)
	return payload
}
