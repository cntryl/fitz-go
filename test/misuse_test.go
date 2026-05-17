package integration

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/test/fixture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldReturnClearErrorGivenCommitAfterRollbackWhenTxMisused(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		tx, err := f.Client().KV().Begin(ctx, f.UniqueRoute("kv"))
		require.NoError(t, err)

		require.NoError(t, tx.Rollback(ctx))
		err = tx.Commit(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "rolled back")
	})
}

func TestShouldReturnClearErrorGivenMutationAfterCommitWhenTxMisused(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		tx, err := f.Client().KV().Begin(ctx, f.UniqueRoute("kv"))
		require.NoError(t, err)

		require.NoError(t, tx.Commit(ctx))
		err = tx.Put(ctx, []byte("k"), []byte("v"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "committed")
	})
}
