//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/v2/fitz"
	"github.com/cntryl/fitz-go/v2/test/fixture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldRouteRequestToWorkerGivenRegisteredWorkerWhenRequestCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		fWorker := fixture.NewTestFixture(t, transport)
		fCaller := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		fWorker.ConnectOrFail(ctx)
		fCaller.ConnectOrFail(ctx)
		route := fWorker.UniqueRoute("rpc")

		sub, err := fWorker.Client().RPC().RegisterWorker(ctx, route, 7, func(_ context.Context, req fitz.RPCInboundRequest, w fitz.RPCResponseWriter) error {
			return w.Send(req.Body)
		})

		require.NoError(t, err)
		defer sub.Deregister()

		iter, err := fCaller.Client().RPC().Call(ctx, route, []byte("ping"))
		require.NoError(t, err)
		defer closeQuietly(iter)

		require.True(t, iter.Next())
		assert.Equal(t, []byte("ping"), iter.Value().Body)
	})
}

func TestShouldRestoreWorkerRegistrationGivenLiveDisconnectWhenReconnectEnabled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		harness := fixture.NewProxyReconnectHarness(t, transport, fixture.AuthModeForTestName(t.Name()))
		worker := harness.Proxied
		caller := harness.Stable

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		harness.Connect(ctx, fixture.DefaultReconnectOptions()...)

		route := worker.UniqueRoute("rpc")
		var err error
		_, err = worker.Client().RPC().RegisterWorker(ctx, route, 7, func(_ context.Context, req fitz.RPCInboundRequest, w fitz.RPCResponseWriter) error {
			return w.Send(req.Body)
		})

		require.NoError(t, err)

		callAndExpect := func(payload string) {
			iter, err := caller.Client().RPC().Call(ctx, route, []byte(payload))
			require.NoError(t, err)
			defer closeQuietly(iter)

			require.True(t, iter.Next())
			assert.Equal(t, payload, string(iter.Value().Body))
			require.NoError(t, iter.Err())
		}

		callAndExpect("before-disconnect")

		harness.WaitForInitialConnection(5 * time.Second)
		harness.DropAndWaitForReconnect(10 * time.Second)

		require.Eventually(t, func() bool {
			iter, err := caller.Client().RPC().Call(ctx, route, []byte("after-disconnect"))
			if err != nil {
				return false
			}
			defer closeQuietly(iter)

			if !iter.Next() {
				return false
			}

			if string(iter.Value().Body) != "after-disconnect" {
				return false
			}

			return iter.Err() == nil
		}, 10*time.Second, 100*time.Millisecond)
	})
}

func TestShouldReassembleStreamingResponseGivenMultiFrameResponseWhenSequenced(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		fWorker := fixture.NewTestFixture(t, transport)
		fCaller := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		fWorker.ConnectOrFail(ctx)
		fCaller.ConnectOrFail(ctx)
		route := fWorker.UniqueRoute("rpc")

		sub, err := fWorker.Client().RPC().RegisterWorker(ctx, route, 7, func(_ context.Context, _ fitz.RPCInboundRequest, w fitz.RPCResponseWriter) error {
			for i := range 3 {
				if err := w.Send([]byte{byte(i)}); err != nil {
					return err
				}
			}
			return nil
		})

		require.NoError(t, err)
		defer sub.Deregister()

		iter, err := fCaller.Client().RPC().Call(ctx, route, []byte("stream-me"))
		require.NoError(t, err)
		defer closeQuietly(iter)

		var seqs []uint64
		for iter.Next() {
			seqs = append(seqs, iter.Value().Sequence)
		}
		require.NoError(t, iter.Err())
		assert.Equal(t, []uint64{0, 1, 2}, seqs)
	})
}

func TestShouldReturnTimeoutGivenNoWorkerResponseWhenRequestTimeout(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		callCtx, callCancel := context.WithTimeout(ctx, time.Second)
		defer callCancel()
		iter, err := f.Client().RPC().Call(callCtx, f.UniqueRoute("rpc"), []byte("nobody-home"))
		require.NoError(t, err)
		assert.False(t, iter.Next())
		assert.Error(t, iter.Err())
	})
}

func TestShouldLoadBalanceGivenMultipleWorkersWhenConcurrentRequests(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		fW1 := fixture.NewTestFixture(t, transport)
		fW2 := fixture.NewTestFixture(t, transport)
		fCaller := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		fW1.ConnectOrFail(ctx)
		fW2.ConnectOrFail(ctx)
		fCaller.ConnectOrFail(ctx)
		route := fW1.UniqueRoute("rpc")

		var mu sync.Mutex
		workerIDs := make(map[string]int)
		echoHandler := func(id string) fitz.RPCHandler {
			return func(_ context.Context, req fitz.RPCInboundRequest, w fitz.RPCResponseWriter) error {
				mu.Lock()
				workerIDs[id]++
				mu.Unlock()
				return w.Send(req.Body)
			}
		}

		sub1, err := fW1.Client().RPC().RegisterWorker(ctx, route, 7, echoHandler("w1"))
		require.NoError(t, err)
		defer sub1.Deregister()

		sub2, err := fW2.Client().RPC().RegisterWorker(ctx, route, 7, echoHandler("w2"))
		require.NoError(t, err)
		defer sub2.Deregister()

		for range 4 {
			iter, err := fCaller.Client().RPC().Call(ctx, route, []byte("req"))
			require.NoError(t, err)
			for iter.Next() {
			}
			require.NoError(t, iter.Close())
		}

		mu.Lock()
		total := workerIDs["w1"] + workerIDs["w2"]
		mu.Unlock()
		assert.Equal(t, 4, total)
	})
}

func TestShouldCorrelateResponseGivenCorrectCorrelationIDWhenMultipleRequests(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		fWorker := fixture.NewTestFixture(t, transport)
		fCaller := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		fWorker.ConnectOrFail(ctx)
		fCaller.ConnectOrFail(ctx)
		route := fWorker.UniqueRoute("rpc")

		sub, err := fWorker.Client().RPC().RegisterWorker(ctx, route, 7, func(_ context.Context, req fitz.RPCInboundRequest, w fitz.RPCResponseWriter) error {
			return w.Send(req.Body)
		})

		require.NoError(t, err)
		defer sub.Deregister()

		for _, payload := range []string{"req-A", "req-B"} {
			iter, err := fCaller.Client().RPC().Call(ctx, route, []byte(payload))
			require.NoError(t, err)
			require.True(t, iter.Next())
			assert.Equal(t, payload, string(iter.Value().Body))
			require.NoError(t, iter.Close())
		}
	})
}

func TestShouldUnregisterWorkerGivenActiveSubscriptionWhenDeregisterCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		fWorker := fixture.NewTestFixture(t, transport)
		fCaller := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		fWorker.ConnectOrFail(ctx)
		fCaller.ConnectOrFail(ctx)
		route := fWorker.UniqueRoute("rpc")

		sub, err := fWorker.Client().RPC().RegisterWorker(ctx, route, 7, func(_ context.Context, req fitz.RPCInboundRequest, w fitz.RPCResponseWriter) error {
			return w.Send(req.Body)
		})

		require.NoError(t, err)

		iter, err := fCaller.Client().RPC().Call(ctx, route, []byte("alive"))
		require.NoError(t, err)
		require.True(t, iter.Next())
		require.NoError(t, iter.Close())

		sub.Deregister()
		deadCtx, deadCancel := context.WithTimeout(ctx, 2*time.Second)
		defer deadCancel()
		deadIter, err := fCaller.Client().RPC().Call(deadCtx, route, []byte("dead"))
		require.NoError(t, err)
		assert.False(t, deadIter.Next())
		assert.Error(t, deadIter.Err())
	})
}
