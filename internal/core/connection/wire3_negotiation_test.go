package connection

import (
	"testing"

	"github.com/cntryl/fitz-go/internal/testkit"
	"github.com/stretchr/testify/require"
)

func TestShouldRejectRepeatedHelloWithoutReusingCorrelationIDs(t *testing.T) {
	// Arrange
	conn := New(testkit.NewMockTransport(), Config{})
	t.Cleanup(func() { _ = conn.Close() })
	hello := testkit.HelloFrame(testkit.HelloPayload())
	_, err := conn.dispatchTransportFrame(hello)
	require.NoError(t, err)
	require.Equal(t, uint64(1), conn.mux.NextCorrelationID())
	// Act
	_, err = conn.dispatchTransportFrame(hello)
	// Assert
	require.Error(t, err)
	require.Equal(t, uint64(2), conn.mux.NextCorrelationID())
}
