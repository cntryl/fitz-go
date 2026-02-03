package integration

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/cntryl-go/internal/kv"
	"github.com/cntryl/cntryl-go/test/fixture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShouldOpenAndCommitTransactionGivenValidRouteWhenBeginCalled verifies the
// basic KV transaction lifecycle: BEGIN â†’ PUT â†’ GET â†’ COMMIT.
func TestShouldOpenAndCommitTransactionGivenValidRouteWhenBeginCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := f.Connect(ctx); err != nil {
			t.Fatalf("failed to connect: %v", err)
		}

		realm := f.UniqueRealm()
		area := f.UniqueArea()
		resource := f.UniqueResource()
		route := kv.Route{
			Realm:    realm,
			Area:     area,
			Resource: resource,
		}

		// Act
		tx, err := f.Client().KV().Begin(ctx, route)

		// Assert
		require.NoError(t, err, "Begin failed")
		require.NotNil(t, tx, "expected non-nil transaction")

		require.NoError(t, tx.Commit(ctx), "Commit failed")

		// Begin a new read-only transaction and verify value persisted
		trx, err := f.Client().KV().BeginRead(ctx, route)
		require.NoError(t, err, "BeginRead failed")
		val, found, err := trx.Get(ctx, []byte("user:123"))
		require.NoError(t, err, "Get failed")
		assert.True(t, found, "expected key to be found after commit")
		assert.Equal(t, "Alice", string(val), "unexpected value")
	})
}

// TestShouldReadValueGivenExistingKeyWhenGetCalled verifies GET operation
// returns the correct value for an existing key.
func TestShouldReadValueGivenExistingKeyWhenGetCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := f.Connect(ctx); err != nil {
			t.Fatalf("failed to connect: %v", err)
		}

		// Act & Assert
		t.Fatal("KV GET operation not yet implemented")
	})
}

// TestShouldReturnNotFoundGivenNonExistentKeyWhenGetCalled verifies GET
// operation handles missing keys correctly.
func TestShouldReturnNotFoundGivenNonExistentKeyWhenGetCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := f.Connect(ctx); err != nil {
			t.Fatalf("failed to connect: %v", err)
		}

		// Act & Assert
		t.Fatal("KV GET operation not yet implemented")
	})
}

// TestShouldWriteValueGivenValidKeyWhenPutCalled verifies PUT operation
// writes values correctly.
func TestShouldWriteValueGivenValidKeyWhenPutCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := f.Connect(ctx); err != nil {
			t.Fatalf("failed to connect: %v", err)
		}

		// Act & Assert
		t.Fatal("KV PUT operation not yet implemented")
	})
}

// TestShouldInsertNewKeyGivenNonExistentKeyWhenInsertCalled verifies INSERT
// operation creates new keys.
func TestShouldInsertNewKeyGivenNonExistentKeyWhenInsertCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := f.Connect(ctx); err != nil {
			t.Fatalf("failed to connect: %v", err)
		}

		// Act & Assert
		t.Fatal("KV INSERT operation not yet implemented")
	})
}

// TestShouldFailGivenExistingKeyWhenInsertCalled verifies INSERT operation
// rejects duplicate keys.
func TestShouldFailGivenExistingKeyWhenInsertCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := f.Connect(ctx); err != nil {
			t.Fatalf("failed to connect: %v", err)
		}

		// Act & Assert
		t.Fatal("KV INSERT operation not yet implemented")
	})
}

// TestShouldDeleteKeyGivenExistingKeyWhenDeleteCalled verifies DELETE
// operation removes keys.
func TestShouldDeleteKeyGivenExistingKeyWhenDeleteCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := f.Connect(ctx); err != nil {
			t.Fatalf("failed to connect: %v", err)
		}

		// Act & Assert
		t.Fatal("KV DELETE operation not yet implemented")
	})
}

// TestShouldScanKeysInOrderGivenRangeWhenScanCalled verifies SCAN operation
// returns lexicographically ordered key-value pairs.
func TestShouldScanKeysInOrderGivenRangeWhenScanCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := f.Connect(ctx); err != nil {
			t.Fatalf("failed to connect: %v", err)
		}

		// Act & Assert
		t.Fatal("KV SCAN operation not yet implemented")
	})
}

// TestShouldRollbackChangesGivenActiveTransactionWhenRollbackCalled verifies
// ROLLBACK discards all uncommitted changes.
func TestShouldRollbackChangesGivenActiveTransactionWhenRollbackCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := f.Connect(ctx); err != nil {
			t.Fatalf("failed to connect: %v", err)
		}

		// Act & Assert
		t.Fatal("KV ROLLBACK operation not yet implemented")
	})
}

// TestShouldIsolateTransactionsGivenConcurrentAccessWhenMultipleTransactions
// verifies transaction isolation per CLIENT_SPEC.md.
func TestShouldIsolateTransactionsGivenConcurrentAccessWhenMultipleTransactions(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := f.Connect(ctx); err != nil {
			t.Fatalf("failed to connect: %v", err)
		}

		// Act & Assert
		t.Fatal("KV transaction isolation not yet implemented")
	})
}
