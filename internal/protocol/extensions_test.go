package protocol

import (
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShouldDecodeSharedWire3Vectors(t *testing.T) {
	// Arrange
	data, err := os.ReadFile("testdata/wire3_vectors.txt")
	require.NoError(t, err)
	for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		parts := strings.Fields(line)
		t.Run(parts[0], func(t *testing.T) {
			// Act
			bytes, err := hex.DecodeString(parts[1])
			require.NoError(t, err)
			frame, err := DecodeMessage(bytes)
			// Assert
			require.NoError(t, err)
			core, _, err := DecodeExtensions(frame)
			require.NoError(t, err)
			require.NotEmpty(t, core)
		})
	}
}

func TestShouldRejectMalformedExtensions(t *testing.T) {
	// Arrange
	inputs := [][]byte{nil, {0, 0, 0, 1}, {0, 0, 0, 0}, {0, 0, 0, 0, 0}, {0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, {0, 0, 0, 0, 0, 9, 255, 255, 255, 255}}
	for _, payload := range inputs {
		// Act
		_, _, err := DecodeExtensions(Frame{MessageType: 104, Flags: 4, Payload: payload})
		// Assert
		require.Error(t, err)
	}
}

func TestShouldRejectDuplicateHelloCapabilities(t *testing.T) {
	// Arrange
	payload := []byte{0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0}
	// Act
	_, _, err := DecodeExtensions(Frame{MessageType: 2, Flags: 4, Payload: payload})
	// Assert
	require.Error(t, err)
}

func TestShouldPreserveCoreOnlyPayloadWithoutAllocation(t *testing.T) {
	// Arrange
	core := []byte("hi")
	// Act
	result, err := EncodeExtensions(core, nil)
	// Assert
	require.NoError(t, err)
	require.Equal(t, &core[0], &result[0])
}
