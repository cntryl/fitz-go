package errors

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShouldIdentifyBackpressureGivenKnownCodesWhenIsBackpressureCalled(t *testing.T) {
	// Arrange
	queueCode := uint32(QueueFull)
	rpcCode := uint32(RpcBackpressure)
	otherCode := uint32(KvKeyNotFound)

	// Act / Assert
	assert.True(t, IsBackpressure(queueCode))
	assert.True(t, IsBackpressure(rpcCode))
	assert.False(t, IsBackpressure(otherCode))
}

func TestShouldFormatDomainErrorGivenKnownCodeWhenErrorCalled(t *testing.T) {
	// Arrange
	err := NewDomainError(uint32(QueueFull), "queue saturated")

	// Act
	actual := err.Error()

	// Assert
	assert.Equal(t, "queue_full: queue saturated", actual)
}

func TestShouldReturnUnknownLabelGivenUnknownCodeWhenErrorCodeStringCalled(t *testing.T) {
	// Arrange
	code := ErrorCode(999999)

	// Act
	actual := code.String()

	// Assert
	assert.Equal(t, "unknown_error_999999", actual)
}
