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
