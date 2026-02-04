package integration

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/cntryl-go/test/fixture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShouldReceiveNotificationGivenActiveSubscriptionWhenPublishMatches
// verifies the basic pub/sub lifecycle: SUBSCRIBE â†’ PUBLISH â†’ NOTIFY.
func TestShouldReceiveNotificationGivenActiveSubscriptionWhenPublishMatches(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange: start simulator broker and configure fixture
		addr, stop, err := fixture.StartSimBroker(string(transport))
		if err != nil {
			t.Fatalf("failed to start sim broker: %v", err)
		}
		defer stop()

		f := fixture.NewTestFixture(t, transport)
		f.SetBrokerAddr(addr)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		subscriber := f.Client().Notice()
		ch, err := subscriber.SubscribeChan(ctx, "realm/area/resource")
		require.NoError(t, err)

		// Publisher
		pub := fixture.NewTestFixture(t, transport)
		pub.SetBrokerAddr(addr)
		pubCtx, pubCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer pubCancel()
		require.NoError(t, pub.Connect(pubCtx))

		// Act: publish
		payload := []byte("hello")
		require.NoError(t, pub.Client().Notice().Publish(context.Background(), "realm/area/resource", payload))

		// Assert: subscriber receives notification
		select {
		case got := <-ch:
			assert.Equal(t, payload, got)
		case <-ctx.Done():
			t.Fatal("timed out waiting for notification")
		}
	})
}

// TestShouldFanoutToAllSubscribersGivenMultipleSubscriptionsWhenPublish
// verifies a single PUBLISH reaches all matching subscriptions.
func TestShouldFanoutToAllSubscribersGivenMultipleSubscriptionsWhenPublish(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		addr, stop, err := fixture.StartSimBroker(string(transport))
		if err != nil {
			t.Fatalf("failed to start sim broker: %v", err)
		}
		defer stop()

		f := fixture.NewTestFixture(t, transport)
		f.SetBrokerAddr(addr)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		sub1 := f.Client().Notice()
		ch1, _ := sub1.SubscribeChan(ctx, "realm/area/resource")
		sub2 := f.Client().Notice()
		ch2, _ := sub2.SubscribeChan(ctx, "realm/area/resource")

		pub := fixture.NewTestFixture(t, transport)
		pub.SetBrokerAddr(addr)
		pubCtx, pubCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer pubCancel()
		require.NoError(t, pub.Connect(pubCtx))

		payload := []byte("fanout")
		require.NoError(t, pub.Client().Notice().Publish(context.Background(), "realm/area/resource", payload))

		received := 0
		for received < 2 {
			select {
			case <-ch1:
				received++
			case <-ch2:
				received++
			case <-ctx.Done():
				t.Fatal("timed out waiting for fanout")
			}
		}
	})
}

// TestShouldMatchWildcardGivenStarPatternWhenSubscribe verifies wildcard
// pattern matching with * (single segment).
func TestShouldMatchWildcardGivenStarPatternWhenSubscribe(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		addr, stop, err := fixture.StartSimBroker(string(transport))
		if err != nil {
			t.Fatalf("failed to start sim broker: %v", err)
		}
		defer stop()

		f := fixture.NewTestFixture(t, transport)
		f.SetBrokerAddr(addr)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		sub := f.Client().Notice()
		ch, _ := sub.SubscribeChan(ctx, "realm/*/resource")

		pub := fixture.NewTestFixture(t, transport)
		pub.SetBrokerAddr(addr)
		pubCtx, pubCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer pubCancel()
		require.NoError(t, pub.Connect(pubCtx))

		// Matching publish
		require.NoError(t, pub.Client().Notice().Publish(context.Background(), "realm/area/resource", []byte("ok")))
		select {
		case got := <-ch:
			assert.Equal(t, []byte("ok"), got)
		case <-ctx.Done():
			t.Fatal("timed out waiting for wildcard match")
		}

		// Non-matching publish should not be delivered
		require.NoError(t, pub.Client().Notice().Publish(context.Background(), "other/area/resource", []byte("no")))
		select {
		case <-ch:
			t.Fatal("unexpected notification for non-matching route")
		case <-time.After(50 * time.Millisecond):
			// OK: no delivery
		}
	})
}

// TestShouldMatchMultiSegmentGivenDoubleStarPatternWhenSubscribe verifies
// multi-segment wildcard matching with ** (zero or more segments).
func TestShouldMatchMultiSegmentGivenDoubleStarPatternWhenSubscribe(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		addr, stop, err := fixture.StartSimBroker(string(transport))
		if err != nil {
			t.Fatalf("failed to start sim broker: %v", err)
		}
		defer stop()

		f := fixture.NewTestFixture(t, transport)
		f.SetBrokerAddr(addr)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		sub := f.Client().Notice()
		ch, _ := sub.SubscribeChan(ctx, "realm/**/resource")

		pub := fixture.NewTestFixture(t, transport)
		pub.SetBrokerAddr(addr)
		pubCtx, pubCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer pubCancel()
		require.NoError(t, pub.Connect(pubCtx))

		// Matching publish with multiple segments
		require.NoError(t, pub.Client().Notice().Publish(context.Background(), "realm/a/b/resource", []byte("ok")))
		select {
		case got := <-ch:
			assert.Equal(t, []byte("ok"), got)
		case <-ctx.Done():
			t.Fatal("timed out waiting for ** match")
		}
	})
}

// TestShouldStopReceivingGivenUnsubscribeWhenUnsubscribeCalled verifies
// UNSUBSCRIBE stops notification delivery.
func TestShouldStopReceivingGivenUnsubscribeWhenUnsubscribeCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		addr, stop, err := fixture.StartSimBroker(string(transport))
		if err != nil {
			t.Fatalf("failed to start sim broker: %v", err)
		}
		defer stop()

		f := fixture.NewTestFixture(t, transport)
		f.SetBrokerAddr(addr)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		sub := f.Client().Notice()
		ch, _ := sub.SubscribeChan(ctx, "realm/area/resource")
		// Unsubscribe
		require.NoError(t, sub.Unsubscribe(context.Background(), "realm/area/resource"))
		// Unsubscribe processed (broker acked)

		pub := fixture.NewTestFixture(t, transport)
		pub.SetBrokerAddr(addr)
		pubCtx, pubCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer pubCancel()
		require.NoError(t, pub.Connect(pubCtx))

		require.NoError(t, pub.Client().Notice().Publish(context.Background(), "realm/area/resource", []byte("x")))
		select {
		case got, ok := <-ch:
			if !ok {
				// channel closed as expected
				break
			}
			t.Fatalf("unexpected notification after unsubscribe: %v", got)
		case <-time.After(50 * time.Millisecond):
			// OK
		}
	})
}

// TestShouldClearAllSubscriptionsGivenActiveSubscriptionsWhenUnsubscribeAllCalled
// verifies UNSUBSCRIBE_ALL removes all subscriptions.
func TestShouldClearAllSubscriptionsGivenActiveSubscriptionsWhenUnsubscribeAllCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		addr, stop, err := fixture.StartSimBroker(string(transport))
		if err != nil {
			t.Fatalf("failed to start sim broker: %v", err)
		}
		defer stop()

		f := fixture.NewTestFixture(t, transport)
		f.SetBrokerAddr(addr)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		sub := f.Client().Notice()
		ch, _ := sub.SubscribeChan(ctx, "realm/area/resource")
		// UnsubscribeAll
		require.NoError(t, sub.UnsubscribeAll(context.Background()))
		// UnsubscribeAll processed (broker acked)

		pub := fixture.NewTestFixture(t, transport)
		pub.SetBrokerAddr(addr)
		pubCtx, pubCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer pubCancel()
		require.NoError(t, pub.Connect(pubCtx))

		require.NoError(t, pub.Client().Notice().Publish(context.Background(), "realm/area/resource", []byte("x")))
		select {
		case got, ok := <-ch:
			if !ok {
				// channel closed as expected
				break
			}
			t.Fatalf("unexpected notification after unsubscribe all: %v", got)
		case <-time.After(50 * time.Millisecond):
			// OK
		}
	})
}

// TestShouldSucceedGivenNoSubscribersWhenPublish verifies PUBLISH returns
// success even when no subscribers exist.
func TestShouldSucceedGivenNoSubscribersWhenPublish(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		addr, stop, err := fixture.StartSimBroker(string(transport))
		if err != nil {
			t.Fatalf("failed to start sim broker: %v", err)
		}
		defer stop()

		f := fixture.NewTestFixture(t, transport)
		f.SetBrokerAddr(addr)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		// Act: publish when no one subscribed
		require.NoError(t, f.Client().Notice().Publish(context.Background(), "realm/area/resource", []byte("x")))
		// No error and no panic = success
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
