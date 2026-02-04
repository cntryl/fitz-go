package integration

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/cntryl-go/test/fixture"
	"github.com/stretchr/testify/require"
)

// TestShouldReceiveNotificationGivenActiveSubscriptionWhenPublishMatches
// verifies the basic pub/sub lifecycle: SUBSCRIBE â†’ PUBLISH â†’ NOTIFY.
func TestShouldReceiveNotificationGivenActiveSubscriptionWhenPublishMatches(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		// Act & Assert
		t.Fatal("Notice SUBSCRIBE/PUBLISH not yet implemented")
	})
}

// TestShouldFanoutToAllSubscribersGivenMultipleSubscriptionsWhenPublish
// verifies a single PUBLISH reaches all matching subscriptions.
func TestShouldFanoutToAllSubscribersGivenMultipleSubscriptionsWhenPublish(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		// Act & Assert
		t.Fatal("Notice fanout not yet implemented")
	})
}

// TestShouldMatchWildcardGivenStarPatternWhenSubscribe verifies wildcard
// pattern matching with * (single segment).
func TestShouldMatchWildcardGivenStarPatternWhenSubscribe(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		// Act & Assert
		t.Fatal("Notice wildcard matching not yet implemented")
	})
}

// TestShouldMatchMultiSegmentGivenDoubleStarPatternWhenSubscribe verifies
// multi-segment wildcard matching with ** (zero or more segments).
func TestShouldMatchMultiSegmentGivenDoubleStarPatternWhenSubscribe(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		// Act & Assert
		t.Fatal("Notice ** wildcard not yet implemented")
	})
}

// TestShouldStopReceivingGivenUnsubscribeWhenUnsubscribeCalled verifies
// UNSUBSCRIBE stops notification delivery.
func TestShouldStopReceivingGivenUnsubscribeWhenUnsubscribeCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		// Act & Assert
		t.Fatal("Notice UNSUBSCRIBE not yet implemented")
	})
}

// TestShouldClearAllSubscriptionsGivenActiveSubscriptionsWhenUnsubscribeAllCalled
// verifies UNSUBSCRIBE_ALL removes all subscriptions.
func TestShouldClearAllSubscriptionsGivenActiveSubscriptionsWhenUnsubscribeAllCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		// Act & Assert
		t.Fatal("Notice UNSUBSCRIBE_ALL not yet implemented")
	})
}

// TestShouldSucceedGivenNoSubscribersWhenPublish verifies PUBLISH returns
// success even when no subscribers exist.
func TestShouldSucceedGivenNoSubscribersWhenPublish(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		// Act & Assert
		t.Fatal("Notice PUBLISH without subscribers not yet implemented")
	})
}

// TestShouldDropSubscriptionsGivenDisconnectWhenReconnect verifies subscriptions
// are session-scoped and lost on disconnect per CLIENT_SPEC.md.
func TestShouldDropSubscriptionsGivenDisconnectWhenReconnect(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		// Act & Assert
		t.Fatal("Notice subscription lifecycle not yet implemented")
	})
}
