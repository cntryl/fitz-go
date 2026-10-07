package fitz

import (
	"fmt"
	"testing"

	coreerrors "github.com/cntryl/fitz-go/internal/core/errors"
	"github.com/stretchr/testify/assert"
)

func TestShouldClassifyScheduleBackendErrorAsRetryable(t *testing.T) {
	err := coreerrors.NewDomainError(ErrCodeScheduleBackendError, "backend busy")

	assert.Equal(t, uint32(7010), ErrCodeScheduleBackendError)
	assert.True(t, IsRetryable(err))
}

func TestShouldClassifyWrappedQueueFullSentinelAsRetryable(t *testing.T) {
	// Arrange
	err := fmt.Errorf("complete failed: %w", ErrQueueFull)
	// Act
	retryable := IsRetryable(err)
	// Assert
	assert.True(t, retryable)
}
