//go:build integration

package integration

import (
	"context"
	"github.com/cntryl/fitz-go/fitz"
	"github.com/cntryl/fitz-go/test/fixture"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestShouldCommitBothDomainsGivenExplicitPersistenceWhenUsingDevelopBroker(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		f.ConnectOrFail(ctx)
		for _, mode := range []fitz.KVDurabilityMode{fitz.KVDurabilityBuffered, fitz.KVDurabilitySync} {
			tx, err := f.Client().KV().Begin(ctx, f.UniqueRoute("kv"))
			require.NoError(t, err)
			require.NoError(t, tx.Put(ctx, []byte("key"), []byte("value")))
			require.NoError(t, tx.Commit(ctx, mode))
			session, err := f.Client().Stream().Begin(ctx, f.UniqueRoute("stream"))
			require.NoError(t, err)
			_, err = session.Append(ctx, 0, []byte("record"))
			require.NoError(t, err)
			require.NoError(t, session.Commit(ctx, fitz.StreamCommitMode(mode)))
		}
	})
}
