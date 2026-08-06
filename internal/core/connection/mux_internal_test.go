package connection

import (
	"encoding/binary"
	"log/slog"
	"math"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/v2/internal/protocol"
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

func TestShouldDropMalformedNotifyFrameGivenBoundsFailureWhenDispatchCalled(t *testing.T) {
	tests := []struct {
		name     string
		msgType  uint16
		payload  []byte
		register func(*Multiplexer, func())
	}{
		{
			name:    "notice notify missing subscription id",
			msgType: protocol.MessageTypeNoticeNotify,
			payload: []byte{0x01},
			register: func(mux *Multiplexer, called func()) {
				mux.SetNotifyHandler(protocol.MessageTypeNoticeNotify, func(uint64, string, []byte) {
					called()
				})
			},
		},
		{
			name:    "queue notify missing header",
			msgType: protocol.MessageTypeQueueNotify,
			payload: []byte{0x01},
			register: func(mux *Multiplexer, called func()) {
				mux.SetNotifyHandler(protocol.MessageTypeQueueNotify, func(uint64, string, []byte) {
					called()
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := NewMultiplexer()
			t.Cleanup(func() {
				require.NoError(t, mux.Close())
			})
			recorder := newLogRecorder()
			mux.setLogger(slog.New(recorder))

			called := false
			tt.register(mux, func() {
				called = true
			})

			mux.Dispatch(tt.msgType, tt.payload)

			require.False(t, called)
			require.Equal(t, uint64(1), mux.Metrics().ResponsesDropped)
			assertLogEntry(t, recorder.snapshot(), slog.LevelWarn, "dropped malformed frame")
		})
	}
}

func TestShouldDropMalformedRpcResponseGivenShortCorrelationPayloadWhenDispatchCalled(t *testing.T) {
	mux := NewMultiplexer()
	t.Cleanup(func() {
		require.NoError(t, mux.Close())
	})
	recorder := newLogRecorder()
	mux.setLogger(slog.New(recorder))

	called := false
	mux.SetRPCResponseHandler(func([16]byte, []byte) {
		called = true
	})

	for payloadLen := range 16 {
		mux.Dispatch(protocol.MessageTypeRpcResponse, make([]byte, payloadLen))
	}

	require.False(t, called)
	require.Equal(t, uint64(16), mux.Metrics().ResponsesDropped)
	assertLogEntry(t, recorder.snapshot(), slog.LevelWarn, "dropped malformed frame")
}

func TestShouldDropMalformedNotifyFrameGivenUint32LengthOverflowWhenDispatchCalled(t *testing.T) {
	tests := []struct {
		name    string
		msgType uint16
		payload []byte
		setup   func(*Multiplexer, func())
	}{
		{
			name:    "notice route length max uint32",
			msgType: protocol.MessageTypeNoticeNotify,
			payload: notifyPayloadWithLengths(math.MaxUint32),
			setup: func(mux *Multiplexer, called func()) {
				mux.SetNotifyHandler(protocol.MessageTypeNoticeNotify, func(uint64, string, []byte) {
					called()
				})
			},
		},
		{
			name:    "notice payload length max uint32",
			msgType: protocol.MessageTypeNoticeNotify,
			payload: notifyPayloadWithLengths(0, math.MaxUint32),
			setup: func(mux *Multiplexer, called func()) {
				mux.SetNotifyHandler(protocol.MessageTypeNoticeNotify, func(uint64, string, []byte) {
					called()
				})
			},
		},
		{
			name:    "queue route length max uint32",
			msgType: protocol.MessageTypeQueueNotify,
			payload: notifyPayloadWithLengths(math.MaxUint32),
			setup: func(mux *Multiplexer, called func()) {
				mux.SetNotifyHandler(protocol.MessageTypeQueueNotify, func(uint64, string, []byte) {
					called()
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := NewMultiplexer()
			t.Cleanup(func() {
				require.NoError(t, mux.Close())
			})
			recorder := newLogRecorder()
			mux.setLogger(slog.New(recorder))

			called := false
			tt.setup(mux, func() {
				called = true
			})

			mux.Dispatch(tt.msgType, tt.payload)

			require.False(t, called)
			require.Equal(t, uint64(1), mux.Metrics().ResponsesDropped)
			assertLogEntry(t, recorder.snapshot(), slog.LevelWarn, "dropped malformed frame")
		})
	}
}

func rpcWorkerRequestPayloadForTest(route string, _ string, body []byte) []byte {
	payload := make([]byte, 0, 16+4+len(route)+4+len(body))
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
	writeTLVBytes(body)
	return payload
}

func notifyPayloadWithLengths(lengths ...uint32) []byte {
	payload := make([]byte, 0, 8+(4*len(lengths)))
	payload = append(payload, make([]byte, 8)...)
	for _, length := range lengths {
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], length)
		payload = append(payload, lenBuf[:]...)
	}
	return payload
}

func TestShouldDropResponseForUnregisteredWaiterWhenLaterRequestExists(t *testing.T) {
	mux := NewMultiplexer()
	defer func() { _ = mux.Close() }()

	waiter1 := acquireRequestWaiter()
	waiter2 := acquireRequestWaiter()
	defer releaseRequestWaiter(waiter1)
	defer releaseRequestWaiter(waiter2)

	mux.RegisterRequestWaiter(100, waiter1, nil)
	mux.RegisterRequestWaiter(100, waiter2, nil)

	if !mux.AbandonRequestWaiter(100, waiter1) {
		t.Fatal("expected first waiter to be unregistered")
	}

	mux.Dispatch(100, []byte("stale"))
	mux.Dispatch(100, []byte("live"))

	select {
	case <-waiter1.ready:
		t.Fatal("unregistered waiter should not receive a response")
	default:
	}

	select {
	case <-waiter2.ready:
		if !waiter2.hasResponse {
			t.Fatal("live waiter was signaled without a response")
		}
		if got := string(waiter2.response); got != "live" {
			t.Fatalf("live waiter received %q, want %q", got, "live")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("live waiter did not receive response")
	}
}
