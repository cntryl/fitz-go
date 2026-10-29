package connection_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestShouldSendRPCWorkerResponseWithoutHeaderCorrelation(t *testing.T) {
	transport := newAdmissionBlockingTransport()
	conn := connection.New(transport, connection.DefaultConfig())
	require.NoError(t, conn.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	require.NoError(t, conn.SendFireAndForget(context.Background(), protocol.MessageTypeRpcResponse, []byte("response")))

	transport.mu.Lock()
	frame := append([]byte(nil), transport.written[len(transport.written)-1]...)
	transport.mu.Unlock()
	decoded, err := protocol.DecodeMessage(frame)
	require.NoError(t, err)
	require.Zero(t, decoded.CorrelationID)
}

func TestShouldSendRPCWorkerResponseWithoutHeaderCorrelationWhenUsingPayloadWriter(t *testing.T) {
	transport := newAdmissionBlockingTransport()
	conn := connection.New(transport, connection.DefaultConfig())
	require.NoError(t, conn.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	require.NoError(t, conn.SendFireAndForgetWithWriter(context.Background(), protocol.MessageTypeRpcResponse, func(payload *bytes.Buffer) {
		payload.WriteString("response")
	}))

	transport.mu.Lock()
	frame := append([]byte(nil), transport.written[len(transport.written)-1]...)
	transport.mu.Unlock()
	decoded, err := protocol.DecodeMessage(frame)
	require.NoError(t, err)
	require.Zero(t, decoded.CorrelationID)
}

func TestShouldDistinguishOneWayCorrelationPolicies(t *testing.T) {
	transport := newAdmissionBlockingTransport()
	conn := connection.New(transport, connection.DefaultConfig())
	require.NoError(t, conn.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	cases := []struct {
		name       string
		kind       uint16
		payload    []byte
		correlated bool
	}{
		{name: "rpc request", kind: protocol.MessageTypeRpcRequest, correlated: true},
		{name: "caller cancel", kind: protocol.MessageTypeRpcCancellation, payload: []byte{1}, correlated: true},
		{name: "worker cleanup acknowledgement", kind: protocol.MessageTypeRpcCancellation, payload: []byte{3}},
		{name: "unacknowledged notice", kind: protocol.MessageTypeNoticePublish},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, conn.SendFireAndForget(context.Background(), tc.kind, tc.payload))
			transport.mu.Lock()
			frame := append([]byte(nil), transport.written[len(transport.written)-1]...)
			transport.mu.Unlock()
			decoded, err := protocol.DecodeMessage(frame)
			require.NoError(t, err)
			require.Equal(t, tc.correlated, decoded.CorrelationID != 0)
		})
	}
}
