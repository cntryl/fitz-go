package connection

import (
	"context"
	"strings"
	"testing"

	"github.com/cntryl/fitz-go/internal/protocol"
	"github.com/cntryl/fitz-go/internal/testkit"
	"github.com/stretchr/testify/require"
)

func TestShouldSendServiceNameInConnectGivenZeroOptionalCapabilities(t *testing.T) {
	// Arrange
	transport := testkit.NewWire2MockTransport()
	name := strings.Repeat("é", 64)
	conn := New(transport, Config{ServiceName: " " + name + " "})
	t.Cleanup(func() { _ = conn.Close() })
	// Act
	require.NoError(t, conn.Start(context.Background()))
	frames := transport.GetWrittenFrames()
	// Assert
	require.Len(t, frames, 1)
	kind, payload, err := protocol.DecodeFrame(frames[0])
	require.NoError(t, err)
	require.Equal(t, uint16(1), kind)
	jwt, next, err := ReadString(payload, 8)
	require.NoError(t, err)
	require.Empty(t, jwt)
	actual, _, err := ReadString(payload, next)
	require.NoError(t, err)
	require.Equal(t, name, actual)
	version, bits := conn.ServerCapabilities()
	require.Equal(t, uint16(2), version)
	require.Zero(t, bits)
}
func TestShouldRejectLegacyHelloGivenTruncatedNegotiation(t *testing.T) {
	// Arrange
	conn := New(testkit.NewMockTransport(), Config{})
	t.Cleanup(func() { _ = conn.Close() })
	// Act
	_, err := conn.dispatchTransportFrame(protocol.EncodeFrame(2, []byte{0, 1, 0, 0, 0, 0}))
	// Assert
	require.Error(t, err)
}
