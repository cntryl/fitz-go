package connection

import (
	"encoding/binary"
	"errors"

	"testing"

	coreerrors "github.com/cntryl/fitz-go/internal/core/errors"
)

func TestShouldPreserveCodedCapacityForPlainScheduleResponse(t *testing.T) {
	// Arrange
	payload := make([]byte, 9)
	payload[0] = 1
	binary.BigEndian.PutUint32(payload[1:5], 7010)
	// Act
	_, _, err := ParsePlainResponse(payload)
	// Assert
	var domainError *coreerrors.DomainError
	if !errors.As(err, &domainError) {
		t.Fatalf("expected domain error, got %v", err)
	}
}
