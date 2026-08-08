package fitz

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShouldConfigureDelayedEnqueueGivenDelayOption(t *testing.T) {
	// Arrange
	config := queueEnqueueConfig{}

	// Act
	WithQueueEnqueueDelaySeconds(45)(&config)

	// Assert
	assert.Equal(t, uint64(45), config.delaySeconds)
}
