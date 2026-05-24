package integration

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/fitz"
	"github.com/cntryl/fitz-go/test/fixture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldAcquireLeaseGivenAvailableLeaseWhenAcquireCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		l, err := f.Client().Lease().Acquire(ctx, f.UniqueRoute("lease"), 30)
		require.NoError(t, err)
		require.NotNil(t, l)
		assert.Greater(t, l.ExpiresAt, time.Now().Unix()-1)
	})
}

func TestShouldRejectAcquireGivenHeldLeaseWhenAcquireCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f1 := fixture.NewTestFixture(t, transport)
		f2 := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f1.ConnectOrFail(ctx)
		f2.ConnectOrFail(ctx)
		route := f1.UniqueRoute("lease")

		_, err := f1.Client().Lease().Acquire(ctx, route, 30)
		require.NoError(t, err)

		l2, err2 := f2.Client().Lease().Acquire(ctx, route, 30)
		if err2 != nil {
			assert.ErrorIs(t, err2, fitz.ErrLeaseHeld)
			return
		}
		assert.Nil(t, l2)
	})
}

func TestShouldExtendTTLGivenValidTokenWhenRenewCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		l, err := f.Client().Lease().Acquire(ctx, f.UniqueRoute("lease"), 10)
		require.NoError(t, err)
		newExpiry, err := l.Extend(ctx, 60)
		require.NoError(t, err)
		assert.Greater(t, newExpiry, time.Now().Unix())
	})
}

func TestShouldRejectRenewGivenInvalidTokenWhenTokenMismatch(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		l, err := f.Client().Lease().Acquire(ctx, f.UniqueRoute("lease"), 30)
		require.NoError(t, err)
		_, err = l.ExtendWithToken(ctx, []byte("wrong-token"), 60)
		require.Error(t, err)
	})
}

func TestShouldReleaseLeaseGivenValidTokenWhenReleaseCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("lease")
		l, err := f.Client().Lease().Acquire(ctx, route, 30)
		require.NoError(t, err)
		require.NoError(t, l.Release(ctx))

		l2, err := f.Client().Lease().Acquire(ctx, route, 30)
		require.NoError(t, err)
		require.NotNil(t, l2)
		assert.Greater(t, l2.ExpiresAt, time.Now().Unix()-1)
	})
}

func TestShouldRejectReleaseGivenInvalidTokenWhenTokenMismatch(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		l, err := f.Client().Lease().Acquire(ctx, f.UniqueRoute("lease"), 30)
		require.NoError(t, err)
		require.Error(t, l.ReleaseWithToken(ctx, []byte("wrong-token")))
	})
}

func TestShouldExpireLeaseGivenTTLElapsedWhenNoRenew(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("lease")
		l, err := f.Client().Lease().Acquire(ctx, route, 2)
		require.NoError(t, err)
		require.NotNil(t, l)

		time.Sleep(3 * time.Second)

		l2, err := f.Client().Lease().Acquire(ctx, route, 30)
		require.NoError(t, err)
		require.NotNil(t, l2)
		assert.Greater(t, l2.ExpiresAt, time.Now().Unix()-1)
	})
}

func TestShouldQueryLeaseStatusGivenExistingLeaseWhenQueryCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("lease")
		l, err := f.Client().Lease().Acquire(ctx, route, 30)
		require.NoError(t, err)
		require.NotNil(t, l)

		info, err := f.Client().Lease().Query(ctx, route)
		require.NoError(t, err)
		require.NotNil(t, info)
		assert.True(t, info.Held)
		assert.True(t, info.TTLRemainingSecs > 0 || info.OwnerID != "")
	})
}

func TestShouldNotifyGivenSubscriptionWhenLeaseReleased(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("lease")
		notifications := make(chan fitz.LeaseChangeNotification, 1)

		sub, err := f.Client().Lease().Subscribe(ctx, route, func(_ context.Context, notif fitz.LeaseChangeNotification) error {
			notifications <- notif
			return nil
		})
		require.NoError(t, err)
		defer sub.Unsubscribe()

		l, err := f.Client().Lease().Acquire(ctx, route, 30)
		require.NoError(t, err)
		require.NoError(t, l.Release(ctx))

		select {
		case notif := <-notifications:
			assert.Equal(t, route, notif.Route)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for lease change notification")
		}
	})
}

func TestShouldRestoreLeaseSubscriptionGivenLiveDisconnectWhenReconnectEnabled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		harness := fixture.NewProxyReconnectHarness(t, transport, fixture.AuthModeForTestName(t.Name()))
		subscriber := harness.Proxied
		actor := harness.Stable

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		harness.Connect(ctx, fixture.DefaultReconnectOptions()...)

		route := subscriber.UniqueRoute("lease")
		var err error
		notifications := make(chan string, 4)
		_, err = subscriber.Client().Lease().Subscribe(ctx, route, func(_ context.Context, notif fitz.LeaseChangeNotification) error {
			notifications <- notif.Route
			return nil
		})
		require.NoError(t, err)

		triggerChange := func() {
			lease, err := actor.Client().Lease().Acquire(ctx, route, 30)
			require.NoError(t, err)
			require.NoError(t, lease.Release(ctx))
		}

		triggerChange()

		select {
		case notifiedRoute := <-notifications:
			require.Equal(t, route, notifiedRoute)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for initial lease change notification")
		}

		harness.WaitForInitialConnection(5 * time.Second)
		harness.DropAndWaitForReconnect(10 * time.Second)

		require.Eventually(t, func() bool {
			triggerChange()

			select {
			case notifiedRoute := <-notifications:
				return notifiedRoute == route
			default:
				return false
			}
		}, 10*time.Second, 100*time.Millisecond)
	})
}
