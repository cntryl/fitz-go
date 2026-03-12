package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldReturnNilGivenTransientFailuresWhenDoEventuallySucceeds(t *testing.T) {
	// Arrange
	attempts := 0
	cfg := BackoffConfig{
		InitialDelay: time.Millisecond,
		MaxDelay:     2 * time.Millisecond,
		Multiplier:   1,
		JitterFactor: 0,
	}

	// Act
	err := Do(context.Background(), cfg, 3, func() error {
		attempts++
		if attempts < 3 {
			return errors.New("retryable")
		}
		return nil
	}, func(err error) bool {
		return err != nil
	})

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 3, attempts)
}

func TestShouldStopImmediatelyGivenNonRetryableErrorWhenDoCalled(t *testing.T) {
	// Arrange
	expected := errors.New("fatal")
	attempts := 0

	// Act
	err := Do(context.Background(), DefaultBackoff, 5, func() error {
		attempts++
		return expected
	}, func(err error) bool {
		return false
	})

	// Assert
	require.ErrorIs(t, err, expected)
	assert.Equal(t, 1, attempts)
}

func TestShouldReturnLastErrorGivenMaxRetriesExceededWhenDoCalled(t *testing.T) {
	// Arrange
	expected := errors.New("still failing")
	attempts := 0
	cfg := BackoffConfig{
		InitialDelay: time.Millisecond,
		MaxDelay:     time.Millisecond,
		Multiplier:   1,
		JitterFactor: 0,
	}

	// Act
	err := Do(context.Background(), cfg, 2, func() error {
		attempts++
		return expected
	}, func(err error) bool {
		return true
	})

	// Assert
	require.ErrorIs(t, err, expected)
	assert.Equal(t, 3, attempts)
}

func TestShouldReturnContextErrorGivenCanceledContextWhenDoWaitingToRetry(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithCancel(context.Background())
	cfg := BackoffConfig{
		InitialDelay: 25 * time.Millisecond,
		MaxDelay:     25 * time.Millisecond,
		Multiplier:   1,
		JitterFactor: 0,
	}
	done := make(chan error, 1)

	// Act
	go func() {
		done <- Do(ctx, cfg, 1, func() error {
			return errors.New("retryable")
		}, func(err error) bool {
			return true
		})
	}()
	time.Sleep(5 * time.Millisecond)
	cancel()
	err := <-done

	// Assert
	require.ErrorIs(t, err, context.Canceled)
}

func TestShouldBoundDelayGivenJitterWhenCalculateDelayCalled(t *testing.T) {
	// Arrange
	cfg := BackoffConfig{
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     40 * time.Millisecond,
		Multiplier:   2,
		JitterFactor: 0.25,
	}

	// Act
	delay := calculateDelay(cfg, 2)

	// Assert
	assert.GreaterOrEqual(t, delay, 30*time.Millisecond)
	assert.LessOrEqual(t, delay, 50*time.Millisecond)
}
