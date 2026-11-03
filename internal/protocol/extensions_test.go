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

func TestShouldRejectEveryTruncatedPrefixOfWire3Hello(t *testing.T) {
	// Arrange
	data, err := os.ReadFile("testdata/wire3_vectors.txt")
	require.NoError(t, err)
	for line := range strings.SplitSeq(string(data), "\n") {
		if !strings.HasPrefix(line, "hello_wire3_queue_capabilities ") {
			continue
		}
		raw, err := hex.DecodeString(strings.Fields(line)[1])
		require.NoError(t, err)
		frame, err := DecodeMessage(raw)
		require.NoError(t, err)
		for size := range len(frame.Payload) {
			// Act
			_, _, err = DecodeExtensions(Frame{MessageType: 2, Flags: 4, Payload: frame.Payload[:size]})
			// Assert
			require.Error(t, err, "prefix size %d", size)
		}
	}
}

func TestShouldSkipRepeatedUnknownFieldsInAnyOrder(t *testing.T) {
	// Arrange
	payload, err := hex.DecodeString("000000026869100100000000100000000000100000000000")
	require.NoError(t, err)
	// Act
	core, _, err := DecodeExtensions(Frame{MessageType: 104, Flags: 4, Payload: payload})
	// Assert
	require.NoError(t, err)
	require.Equal(t, "hi", string(core))
}
