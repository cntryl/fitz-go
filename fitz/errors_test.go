package fitz

import (
	"fmt"
	"testing"

	coreerrors "github.com/cntryl/fitz-go/internal/core/errors"
	"github.com/stretchr/testify/assert"
)

func TestShouldClassifyScheduleBackendErrorAsTerminal(t *testing.T) {
	err := coreerrors.NewDomainErrorWithDisposition(uint16(ErrCodeScheduleBackendError), 6, 1, "backend busy")

	assert.Equal(t, uint32(7006), ErrCodeScheduleBackendError)
	assert.False(t, IsRetryable(err))
}

func TestShouldClassifyWrappedQueueFullSentinelAsRetryable(t *testing.T) {
	// Arrange
	err := fmt.Errorf("complete failed: %w", ErrQueueFull)
	// Act
	retryable := IsRetryable(err)
	// Assert
	assert.True(t, retryable)
}
