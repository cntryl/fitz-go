package kv

import (
	"encoding/binary"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestShouldOmitPersistenceGivenBeginWhenEncoding(t *testing.T) {
	// Arrange
	route := "kv://r/a/x"
	// Act
	payload, err := encodeBegin(route, TxModeReadWrite)
	// Assert
	require.NoError(t, err)
	require.Len(t, payload, 4+len(route)+1)
	require.Equal(t, byte(TxModeReadWrite), payload[len(payload)-1])
}

func TestShouldEncodePersistenceGivenCommitWhenEncoding(t *testing.T) {
	for _, mode := range []uint8{DurabilityBuffered, DurabilitySync} {
		// Arrange
		route := "kv://r/a/x"
		// Act
		payload, err := encodeCommit(7, route, mode)
		// Assert
		require.NoError(t, err)
		require.Equal(t, uint64(7), binary.BigEndian.Uint64(payload))
		require.Len(t, payload, 8+4+len(route)+1)
		require.Equal(t, mode, payload[len(payload)-1])
	}
}
