package rpc

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShouldDecodeTerminalErrorResponseGivenValidPayload(t *testing.T) {
	// Arrange
	body := []byte{1, 0, 0, 0, 3, 'b', 'a', 'd'}
	payload := make([]byte, 13+len(body))
	binary.BigEndian.PutUint64(payload[:8], 0)
	payload[8] = 1
	binary.BigEndian.PutUint32(payload[9:13], uint32(len(body)))
	copy(payload[13:], body)

	// Act
	decoded, err := decodeRPCResponsePayload(payload)

	// Assert
	require.NoError(t, err)
	require.True(t, decoded.streamEnd)
	require.Equal(t, uint64(0), decoded.sequence)
	require.Equal(t, body, decoded.body)
}

func TestShouldRejectRPCResponseGivenUnknownFlags(t *testing.T) {
	// Arrange
	payload := make([]byte, 13)
	payload[8] = 0x81

	// Act
	_, err := decodeRPCResponsePayload(payload)

	// Assert
	require.ErrorContains(t, err, "unsupported response flags")
}

func TestShouldRejectRPCResponseGivenTrailingBytes(t *testing.T) {
	// Arrange
	payload := make([]byte, 14)
	payload[8] = 1
	payload[13] = 0xff

	// Act
	_, err := decodeRPCResponsePayload(payload)

	// Assert
	require.ErrorContains(t, err, "does not match remaining bytes")
}
