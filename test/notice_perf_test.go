package integration

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/cntryl/cntryl-go/test/fixture"
	"github.com/stretchr/testify/require"
)

// TestNoticePerf measures round-trip latency for Publish -> subscriber delivery
// for both TCP and WebSocket transports. It's a lightweight smoke benchmark,
// not a full microbenchmark. Runs 100 iterations and reports avg/p95 in logs.
func TestNoticePerf(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		addr, stop, err := fixture.StartSimBroker(string(transport))
		require.NoError(t, err)
		defer stop()

		f := fixture.NewTestFixture(t, transport)
		f.SetBrokerAddr(addr)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, f.Connect(ctx))

		sub := f.Client().Notice()
		subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer subCancel()
		ch, err := sub.SubscribeChan(subCtx, "perf/route")
		require.NoError(t, err)

		pub := fixture.NewTestFixture(t, transport)
		pub.SetBrokerAddr(addr)
		pubCtx, pubCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer pubCancel()
		require.NoError(t, pub.Connect(pubCtx))

		const iters = 100
		durations := make([]time.Duration, 0, iters)
		for i := 0; i < iters; i++ {
			payload := []byte(fmt.Sprintf("m-%d", i))
			start := time.Now()
			require.NoError(t, pub.Client().Notice().Publish(context.Background(), "perf/route", payload))
			select {
			case got := <-ch:
				_ = got
				durations = append(durations, time.Since(start))
			case <-time.After(2 * time.Second):
				t.Fatalf("timeout waiting for notification on iter %d", i)
			}
		}

		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
		total := time.Duration(0)
		for _, d := range durations {
			total += d
		}
		avg := total / time.Duration(len(durations))
		p95 := durations[int(float64(len(durations))*0.95)-1]
		t.Logf("Notice perf (%s): avg=%s p95=%s (n=%d)", transport, avg, p95, len(durations))
	})
}
