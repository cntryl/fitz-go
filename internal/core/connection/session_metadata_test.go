package connection

import (
	"encoding/binary"
	"testing"

	"github.com/cntryl/fitz-go/v2/internal/protocol"
	"github.com/cntryl/fitz-go/v2/internal/testkit"
	"github.com/stretchr/testify/require"
)

func TestShouldReportServiceNameGivenMetadataCapabilityWhenServerHelloIsReceived(t *testing.T) {
	transport := testkit.NewMockTransport()
	conn := New(transport, Config{ServiceName: "orders-worker"})
	payload := make([]byte, 6)
	binary.BigEndian.PutUint16(payload[:2], 1)
	binary.BigEndian.PutUint32(payload[2:], protocol.CapabilitySessionMetadata)

	_, err := conn.dispatchTransportFrame(protocol.EncodeFrame(protocol.MessageTypeServerHello, payload))
	require.NoError(t, err)

	written := transport.GetWrittenFrames()
	require.Len(t, written, 1)
	messageType, metadata, err := protocol.DecodeFrame(written[0])
	require.NoError(t, err)
	require.Equal(t, protocol.MessageTypeSessionMetadata, messageType)
	require.Equal(t, []byte("\x00\x00\x00\x0dorders-worker"), metadata)
}

func TestShouldOmitServiceNameGivenMetadataCapabilityIsMissing(t *testing.T) {
	transport := testkit.NewMockTransport()
	conn := New(transport, Config{ServiceName: "orders-worker"})
	payload := make([]byte, 6)
	binary.BigEndian.PutUint16(payload[:2], 1)

	_, err := conn.dispatchTransportFrame(protocol.EncodeFrame(protocol.MessageTypeServerHello, payload))
	require.NoError(t, err)
	require.Empty(t, transport.GetWrittenFrames())
}
