package kv

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShouldRejectImpossibleScanCountBeforeAllocating(t *testing.T) {
	// Arrange
	response := make([]byte, 5)
	binary.BigEndian.PutUint32(response[:4], ^uint32(0))

	// Act
	_, _, err := parseScanResponse(response)

	// Assert
	require.ErrorContains(t, err, "exceeds the remaining response payload")
}
