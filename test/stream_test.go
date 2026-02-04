package integration

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/cntryl-go/test/fixture"
	"github.com/stretchr/testify/require"
)

// TestShouldAppendRecordsGivenValidSessionWhenAppendCalled verifies the
// basic stream lifecycle: BEGIN â†’ APPEND â†’ COMMIT.
func TestShouldAppendRecordsGivenValidSessionWhenAppendCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		// Act & Assert
		t.Fatal("Stream BEGIN/APPEND/COMMIT not yet implemented")
	})
}

// TestShouldReadRecordsInOrderGivenOffsetRangeWhenReadCalled verifies READ
// operation returns records in strict offset order.
func TestShouldReadRecordsInOrderGivenOffsetRangeWhenReadCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		// Act & Assert
		t.Fatal("Stream READ operation not yet implemented")
	})
}

// TestShouldRejectAppendGivenMismatchedOffsetWhenOptimisticConcurrency verifies
// optimistic concurrency control using expected_offset.
func TestShouldRejectAppendGivenMismatchedOffsetWhenOptimisticConcurrency(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		// Act & Assert
		t.Fatal("Stream optimistic concurrency not yet implemented")
	})
}

// TestShouldRollbackUncommittedAppendsGivenActiveSessionWhenRollbackCalled
// verifies ROLLBACK discards uncommitted appends.
func TestShouldRollbackUncommittedAppendsGivenActiveSessionWhenRollbackCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		// Act & Assert
		t.Fatal("Stream ROLLBACK operation not yet implemented")
	})
}

// TestShouldReturnLastRecordGivenExistingStreamWhenLastCalled verifies
// LAST operation returns the most recent record.
func TestShouldReturnLastRecordGivenExistingStreamWhenLastCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		// Act & Assert
		t.Fatal("Stream LAST operation not yet implemented")
	})
}

// TestShouldReceiveNotificationsGivenActiveSubscriptionWhenRecordsAppended
// verifies SUBSCRIBE delivers notifications for new appends.
func TestShouldReceiveNotificationsGivenActiveSubscriptionWhenRecordsAppended(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		// Act & Assert
		t.Fatal("Stream SUBSCRIBE operation not yet implemented")
	})
}
