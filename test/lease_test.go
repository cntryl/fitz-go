package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cntryl/cntryl-go/internal/domains/lease"
	"github.com/cntryl/cntryl-go/test/fixture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// connectOrSkip connects to the broker and skips the test if unavailable.
func connectOrSkipLease(t *testing.T, f *fixture.TestFixture, ctx context.Context) {
	t.Helper()
	if err := f.Connect(ctx); err != nil {
		t.Skipf("broker not available: %v", err)
	}
}

// uniqueLeaseRoute returns a unique lease route to avoid collisions between tests.
func uniqueLeaseRoute(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("lease://test/integration/res-%d", time.Now().UnixNano())
}

// TestShouldAcquireLeaseGivenAvailableLeaseWhenAcquireCalled verifies
// ACQUIRE operation succeeds when lease is free.
func TestShouldAcquireLeaseGivenAvailableLeaseWhenAcquireCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		connectOrSkipLease(t, f, ctx)
		route := uniqueLeaseRoute(t)

		// Act
		token, expiresAt, held, err := f.Client().Lease().Acquire(ctx, route, 30)

		// Assert
		require.NoError(t, err)
		assert.True(t, held, "lease should be granted when free")
		assert.NotEmpty(t, token, "token should be non-empty on successful acquire")
		assert.Greater(t, expiresAt, time.Now().Unix()-1, "expiresAt should be in the future")
	})
}

// TestShouldRejectAcquireGivenHeldLeaseWhenAcquireCalled verifies
// ACQUIRE operation fails when lease is already held by a different owner.
func TestShouldRejectAcquireGivenHeldLeaseWhenAcquireCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange — two independent clients to simulate different owners.
		f1 := fixture.NewTestFixture(t, transport)
		f2 := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		connectOrSkipLease(t, f1, ctx)
		connectOrSkipLease(t, f2, ctx)

		route := uniqueLeaseRoute(t)

		// First client acquires successfully.
		token, _, held, err := f1.Client().Lease().Acquire(ctx, route, 30)
		require.NoError(t, err)
		require.True(t, held)
		require.NotEmpty(t, token)

		// Act — second client attempts to acquire the same lease.
		_, _, held2, err2 := f2.Client().Lease().Acquire(ctx, route, 30)

		// Assert — should either error with ErrLeaseHeld or return held=false.
		if err2 != nil {
			assert.ErrorIs(t, err2, lease.ErrLeaseHeld)
		} else {
			assert.False(t, held2, "second acquire should not be granted while lease is held")
		}
	})
}

// TestShouldExtendTTLGivenValidTokenWhenRenewCalled verifies RENEW
// operation extends lease TTL with valid fencing token.
func TestShouldExtendTTLGivenValidTokenWhenRenewCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		connectOrSkipLease(t, f, ctx)
		route := uniqueLeaseRoute(t)

		token, _, held, err := f.Client().Lease().Acquire(ctx, route, 10)
		require.NoError(t, err)
		require.True(t, held)

		// Act
		newExpiry, err := f.Client().Lease().Renew(ctx, route, token, 60)

		// Assert
		require.NoError(t, err)
		assert.Greater(t, newExpiry, time.Now().Unix(), "renewed expiry should be in the future")
	})
}

// TestShouldRejectRenewGivenInvalidTokenWhenTokenMismatch verifies RENEW
// operation rejects invalid fencing tokens.
func TestShouldRejectRenewGivenInvalidTokenWhenTokenMismatch(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		connectOrSkipLease(t, f, ctx)
		route := uniqueLeaseRoute(t)

		_, _, held, err := f.Client().Lease().Acquire(ctx, route, 30)
		require.NoError(t, err)
		require.True(t, held)

		// Act — renew with a fabricated (wrong) token.
		_, err = f.Client().Lease().Renew(ctx, route, []byte("wrong-token"), 60)

		// Assert
		require.Error(t, err, "renew with invalid token should fail")
	})
}

// TestShouldReleaseLeaseGivenValidTokenWhenReleaseCalled verifies RELEASE
// operation frees the lease with valid fencing token.
func TestShouldReleaseLeaseGivenValidTokenWhenReleaseCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		connectOrSkipLease(t, f, ctx)
		route := uniqueLeaseRoute(t)

		token, _, held, err := f.Client().Lease().Acquire(ctx, route, 30)
		require.NoError(t, err)
		require.True(t, held)

		// Act
		err = f.Client().Lease().Release(ctx, route, token)

		// Assert
		require.NoError(t, err)

		// Re-acquire should succeed after release.
		token2, _, held2, err2 := f.Client().Lease().Acquire(ctx, route, 30)
		require.NoError(t, err2)
		assert.True(t, held2, "lease should be acquirable after release")
		assert.NotEmpty(t, token2)
	})
}

// TestShouldRejectReleaseGivenInvalidTokenWhenTokenMismatch verifies RELEASE
// operation rejects invalid fencing tokens.
func TestShouldRejectReleaseGivenInvalidTokenWhenTokenMismatch(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		connectOrSkipLease(t, f, ctx)
		route := uniqueLeaseRoute(t)

		_, _, held, err := f.Client().Lease().Acquire(ctx, route, 30)
		require.NoError(t, err)
		require.True(t, held)

		// Act — release with a fabricated (wrong) token.
		err = f.Client().Lease().Release(ctx, route, []byte("wrong-token"))

		// Assert
		require.Error(t, err, "release with invalid token should fail")
	})
}

// TestShouldExpireLeaseGivenTTLElapsedWhenNoRenew verifies expired leases
// automatically release and become acquirable by new owners.
func TestShouldExpireLeaseGivenTTLElapsedWhenNoRenew(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		connectOrSkipLease(t, f, ctx)
		route := uniqueLeaseRoute(t)

		// Acquire with very short TTL.
		token, _, held, err := f.Client().Lease().Acquire(ctx, route, 2)
		require.NoError(t, err)
		require.True(t, held)
		require.NotEmpty(t, token)

		// Wait for TTL to expire.
		time.Sleep(3 * time.Second)

		// Act — re-acquire after expiry should succeed.
		token2, _, held2, err2 := f.Client().Lease().Acquire(ctx, route, 30)

		// Assert
		require.NoError(t, err2)
		assert.True(t, held2, "expired lease should be re-acquirable")
		assert.NotEmpty(t, token2)
	})
}

// TestShouldQueryLeaseStatusGivenExistingLeaseWhenQueryCalled verifies
// QUERY operation returns current lease holder information.
func TestShouldQueryLeaseStatusGivenExistingLeaseWhenQueryCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		connectOrSkipLease(t, f, ctx)
		route := uniqueLeaseRoute(t)

		// Acquire a lease so there is active state to query.
		_, _, held, err := f.Client().Lease().Acquire(ctx, route, 30)
		require.NoError(t, err)
		require.True(t, held)

		// Act
		info, err := f.Client().Lease().Query(ctx, route)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, info)
		assert.True(t, info.Held, "query should report lease as held")
		assert.NotEmpty(t, info.Token, "query should return the lease token")
		assert.Greater(t, info.TTL, uint32(0), "query should return remaining TTL")
	})
}
