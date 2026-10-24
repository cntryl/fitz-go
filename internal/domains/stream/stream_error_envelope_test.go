package stream

import (
	"encoding/binary"
	"errors"
	"testing"

	coreerrors "github.com/cntryl/fitz-go/internal/core/errors"
	"github.com/stretchr/testify/require"
)

func TestShouldPreserveStructuredStreamCodeAndCause(t *testing.T) {
	for _, tc := range []struct {
		code     uint32
		message  string
		conflict bool
	}{
		{2050, "unrelated wording", true},
		{2051, "concurrency conflict", false},
		{2006, "backend unavailable", false},
	} {
		t.Run(tc.message, func(t *testing.T) {
			// Arrange
			payload := make([]byte, 9+len(tc.message))
			payload[0] = 1
			binary.BigEndian.PutUint16(payload[1:3], uint16(tc.code))
			payload[3] = 0
			payload[4] = 0
			binary.BigEndian.PutUint32(payload[5:9], uint32(len(tc.message)))
			copy(payload[9:], tc.message)

			// Act
			success, _, raw := parsePlainStreamResponse(payload)
			mapped := mapStreamError(raw)

			// Assert
			require.False(t, success)
			var cause *coreerrors.DomainError
			require.ErrorAs(t, mapped, &cause)
			require.Equal(t, tc.code, uint32(cause.Code))
			require.Equal(t, tc.conflict, errors.Is(mapped, ErrStreamConflict))
			require.ErrorIs(t, mapped, raw)
		})
	}
}

func TestShouldLeaveLegacyConflictWordingUnclassified(t *testing.T) {
	// Arrange
	original := errors.New("concurrency conflict")
	// Act
	mapped := mapStreamError(original)
	// Assert
	require.Same(t, original, mapped)
	require.NotErrorIs(t, mapped, ErrStreamConflict)
}

func TestShouldRejectTruncatedVersionedStreamError(t *testing.T) {
	// Arrange
	payload := []byte{2, 0, 0, 7, 209}
	// Act
	success, _, err := parsePlainStreamResponse(payload)
	// Assert
	require.False(t, success)
	var cause *coreerrors.DomainError
	require.Error(t, err)
	require.NotErrorAs(t, err, &cause)
}
