//nolint:gosec,errcheck,dupl
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

func TestShouldOpenAndCommitTransactionGivenValidRouteWhenBeginCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("kv")

		tx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
		require.NoError(t, err)
		require.NoError(t, tx.Put(ctx, []byte("user:123"), []byte("Alice")))
		require.NoError(t, tx.Commit(ctx))

		verifyTx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync, fitz.WithKVMode(fitz.KVModeReadOnly))
		require.NoError(t, err)
		result, err := verifyTx.Get(ctx, []byte("user:123"))
		require.NoError(t, err)
		assert.True(t, result.Found)
		assert.Equal(t, "Alice", string(result.Value))
	})
}

func TestShouldReadValueGivenExistingKeyWhenGetCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("kv")

		tx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
		require.NoError(t, err)
		require.NoError(t, tx.Put(ctx, []byte("colour"), []byte("blue")))
		require.NoError(t, tx.Commit(ctx))

		readTx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync, fitz.WithKVMode(fitz.KVModeReadOnly))
		require.NoError(t, err)
		result, err := readTx.Get(ctx, []byte("colour"))
		require.NoError(t, err)
		assert.True(t, result.Found)
		assert.Equal(t, "blue", string(result.Value))
	})
}

func TestShouldReturnNotFoundGivenNonExistentKeyWhenGetCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("kv")

		tx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync, fitz.WithKVMode(fitz.KVModeReadOnly))
		require.NoError(t, err)
		result, err := tx.Get(ctx, []byte("missing"))
		require.NoError(t, err)
		assert.False(t, result.Found)
		assert.Nil(t, result.Value)
	})
}

func TestShouldWriteValueGivenValidKeyWhenPutCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("kv")

		tx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
		require.NoError(t, err)
		require.NoError(t, tx.Put(ctx, []byte("k1"), []byte("v1")))

		result, err := tx.Get(ctx, []byte("k1"))
		require.NoError(t, err)
		assert.True(t, result.Found)
		assert.Equal(t, "v1", string(result.Value))
		require.NoError(t, tx.Commit(ctx))
	})
}

func TestShouldInsertNewKeyGivenNonExistentKeyWhenInsertCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("kv")

		tx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
		require.NoError(t, err)
		require.NoError(t, tx.Insert(ctx, []byte("new-key"), []byte("new-value")))

		result, err := tx.Get(ctx, []byte("new-key"))
		require.NoError(t, err)
		assert.True(t, result.Found)
		assert.Equal(t, "new-value", string(result.Value))
		require.NoError(t, tx.Commit(ctx))
	})
}

func TestShouldFailGivenExistingKeyWhenInsertCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("kv")

		tx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
		require.NoError(t, err)
		require.NoError(t, tx.Insert(ctx, []byte("dup"), []byte("first")))
		require.NoError(t, tx.Commit(ctx))

		tx2, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
		require.NoError(t, err)
		err = tx2.Insert(ctx, []byte("dup"), []byte("second"))
		assert.ErrorIs(t, err, fitz.ErrKVKeyExists)
		_ = tx2.Rollback(ctx)
	})
}

func TestShouldDeleteKeyGivenExistingKeyWhenDeleteCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("kv")

		tx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
		require.NoError(t, err)
		require.NoError(t, tx.Put(ctx, []byte("to-delete"), []byte("doomed")))
		require.NoError(t, tx.Commit(ctx))

		tx2, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
		require.NoError(t, err)
		require.NoError(t, tx2.Delete(ctx, []byte("to-delete")))
		require.NoError(t, tx2.Commit(ctx))

		readTx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync, fitz.WithKVMode(fitz.KVModeReadOnly))
		require.NoError(t, err)
		result, err := readTx.Get(ctx, []byte("to-delete"))
		require.NoError(t, err)
		assert.False(t, result.Found)
	})
}

func TestShouldScanKeysInOrderGivenRangeWhenScanCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("kv")

		tx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
		require.NoError(t, err)
		require.NoError(t, tx.Put(ctx, []byte("b"), []byte("2")))
		require.NoError(t, tx.Put(ctx, []byte("a"), []byte("1")))
		require.NoError(t, tx.Put(ctx, []byte("c"), []byte("3")))
		require.NoError(t, tx.Commit(ctx))

		readTx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync, fitz.WithKVMode(fitz.KVModeReadOnly))
		require.NoError(t, err)
		iter, _, err := readTx.Scan(ctx, fitz.KVScanQuery{StartKey: []byte("a"), EndKey: []byte("d"), Limit: 10})
		require.NoError(t, err)
		defer iter.Close()

		var keys []string
		for iter.Next() {
			keys = append(keys, string(iter.Value().Key))
		}
		require.NoError(t, iter.Err())
		assert.Equal(t, []string{"a", "b", "c"}, keys)
	})
}

func TestShouldDeleteRangeGivenRangeWhenDeleteRangeCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("kv")

		tx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
		require.NoError(t, err)
		require.NoError(t, tx.Put(ctx, []byte("a"), []byte("1")))
		require.NoError(t, tx.Put(ctx, []byte("b"), []byte("2")))
		require.NoError(t, tx.Put(ctx, []byte("c"), []byte("3")))
		require.NoError(t, tx.Put(ctx, []byte("d"), []byte("4")))
		require.NoError(t, tx.Commit(ctx))

		tx2, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
		require.NoError(t, err)
		require.NoError(t, tx2.DeleteRange(ctx, []byte("b"), []byte("d")))
		require.NoError(t, tx2.Commit(ctx))

		readTx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync, fitz.WithKVMode(fitz.KVModeReadOnly))
		require.NoError(t, err)
		iter, _, err := readTx.Scan(ctx, fitz.KVScanQuery{StartKey: []byte("a"), EndKey: []byte("z"), Limit: 10})
		require.NoError(t, err)
		defer iter.Close()

		var keys []string
		for iter.Next() {
			keys = append(keys, string(iter.Value().Key))
		}
		require.NoError(t, iter.Err())
		assert.Equal(t, []string{"a", "d"}, keys)
	})
}

func TestShouldRespectLimitGivenScanLimitWhenScanCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("kv")

		tx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
		require.NoError(t, err)
		require.NoError(t, tx.Put(ctx, []byte("a"), []byte("1")))
		require.NoError(t, tx.Put(ctx, []byte("b"), []byte("2")))
		require.NoError(t, tx.Put(ctx, []byte("c"), []byte("3")))
		require.NoError(t, tx.Commit(ctx))

		readTx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync, fitz.WithKVMode(fitz.KVModeReadOnly))
		require.NoError(t, err)
		iter, _, err := readTx.Scan(ctx, fitz.KVScanQuery{StartKey: []byte("a"), EndKey: []byte("z"), Limit: 2})
		require.NoError(t, err)
		defer iter.Close()

		count := 0
		for iter.Next() {
			count++
		}
		require.NoError(t, iter.Err())
		assert.Equal(t, 2, count)
	})
}

func TestShouldRejectInvertedRangeGivenScanQueryWhenScanCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("kv")

		tx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
		require.NoError(t, err)
		_, _, err = tx.Scan(ctx, fitz.KVScanQuery{StartKey: []byte("z"), EndKey: []byte("a"), Limit: 10})
		require.Error(t, err)
		assert.ErrorIs(t, err, fitz.ErrKVInvalidRange)
	})
}

func TestShouldRollbackChangesGivenActiveTransactionWhenRollbackCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("kv")

		tx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
		require.NoError(t, err)
		require.NoError(t, tx.Put(ctx, []byte("ephemeral"), []byte("gone")))
		require.NoError(t, tx.Rollback(ctx))

		readTx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync, fitz.WithKVMode(fitz.KVModeReadOnly))
		require.NoError(t, err)
		result, err := readTx.Get(ctx, []byte("ephemeral"))
		require.NoError(t, err)
		assert.False(t, result.Found)
	})
}

func TestShouldIsolateTransactionsGivenConcurrentAccessWhenMultipleTransactions(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f1 := fixture.NewTestFixture(t, transport)
		f2 := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f1.ConnectOrFail(ctx)
		f2.ConnectOrFail(ctx)
		route := f1.UniqueRoute("kv")

		tx1, err := f1.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
		require.NoError(t, err)
		defer func() { _ = tx1.Rollback(ctx) }()

		tx2, err := f2.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
		if err != nil {
			assert.Nil(t, tx2)
			assert.Contains(t, err.Error(), "conflict")
			return
		}
		if tx2 != nil {
			_ = tx2.Rollback(ctx)
			t.Fatal("second Begin on same resource should have failed with conflict")
		}
	})
}

func TestShouldRejectWriteGivenReadOnlyModeWhenPutCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("kv")

		tx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync, fitz.WithKVMode(fitz.KVModeReadOnly))
		require.NoError(t, err)
		assert.ErrorIs(t, tx.Put(ctx, []byte("k"), []byte("v")), fitz.ErrKVReadOnly)
	})
}

func TestShouldRejectBeginGivenInvalidRouteWhenBeginCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		_, err := f.Client().KV().Begin(ctx, "invalid-route-not-kv-format", fitz.KVDurabilitySync)
		require.Error(t, err)
	})
}

func TestShouldRejectSecondCommitGivenAlreadyCommittedTransactionWhenCommitCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("kv")

		tx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
		require.NoError(t, err)
		require.NoError(t, tx.Put(ctx, []byte("k"), []byte("v")))
		require.NoError(t, tx.Commit(ctx))

		err = tx.Commit(ctx)
		if err != nil {
			assert.Error(t, err)
		}
	})
}
