package fitz

import (
	"testing"

	coreerrors "github.com/cntryl/fitz-go/v2/internal/core/errors"
	"github.com/stretchr/testify/assert"
)

func TestShouldClassifyScheduleBackendErrorAsRetryable(t *testing.T) {
	err := coreerrors.NewDomainError(ErrCodeScheduleBackendError, "backend busy")

	assert.Equal(t, uint32(7010), ErrCodeScheduleBackendError)
	assert.True(t, IsRetryable(err))
}
