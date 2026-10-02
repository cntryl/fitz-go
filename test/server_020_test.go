//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/fitz"
	"github.com/cntryl/fitz-go/test/fixture"
	"github.com/stretchr/testify/require"
)

func TestShouldAdvertiseServer020Capabilities(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		t.Helper()
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Act
		f.ConnectOrFail(ctx)
		require.Eventually(t, f.Client().CorrelationEnabled, time.Second, 10*time.Millisecond)
		version, capabilities := f.Client().ServerCapabilities()

		// Assert
		require.Equal(t, uint16(1), version)
		require.Equal(t, uint32(7), capabilities&7, "server 0.2.0 requires capability bits 0, 1 and 2")
	})
}

func TestShouldExcludeResumeKeyInDirectionalKVScan(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		t.Helper()
		for _, reverse := range []bool{false, true} {
			name := "forward"
			if reverse {
				name = "reverse"
			}
			t.Run(name, func(t *testing.T) {
				// Arrange
				f := fixture.NewTestFixture(t, transport)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				f.ConnectOrFail(ctx)
				route := f.UniqueRoute("kv")
				seed, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
				require.NoError(t, err)
				for _, key := range [][]byte{{0x10}, {0x10, 0}, {0x20}} {
					require.NoError(t, seed.Put(ctx, key, key))
				}
				require.NoError(t, seed.Commit(ctx))
				tx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync, fitz.WithKVMode(fitz.KVModeReadOnly))
				require.NoError(t, err)
				defer rollbackQuietly(ctx, tx)
				first, more, err := tx.Scan(ctx, fitz.KVScanQuery{Limit: 1, Reverse: reverse})
				require.NoError(t, err)
				defer closeQuietly(first)
				require.True(t, first.Next())
				start := first.Value().Key
				require.False(t, first.Next())
				require.NoError(t, first.Err())
				require.True(t, more)

				// Act
				resumed, more, err := tx.Scan(ctx, fitz.KVScanQuery{StartKey: start, StartExclusive: true, Limit: 2, Reverse: reverse})
				require.NoError(t, err)
				defer closeQuietly(resumed)
				var keys [][]byte
				for resumed.Next() {
					keys = append(keys, resumed.Value().Key)
				}

				// Assert
				require.NoError(t, resumed.Err())
				expected := [][]byte{{0x10, 0}, {0x20}}
				if reverse {
					expected = [][]byte{{0x10, 0}, {0x10}}
				}
				require.Equal(t, expected, keys)
				require.False(t, more)
			})
		}
	})
}
