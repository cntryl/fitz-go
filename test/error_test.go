//nolint:errcheck
package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/fitz"
	coreerrors "github.com/cntryl/fitz-go/internal/core/errors"
	"github.com/cntryl/fitz-go/test/fixture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldReturnExpectedDomainErrorsGivenRejectedOperations(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f1 := fixture.NewTestFixture(t, transport)
		f2 := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f1.ConnectOrFail(ctx)
		f2.ConnectOrFail(ctx)

		t.Run("kv read-only write rejected", func(t *testing.T) {
			route := f1.UniqueRoute("kv")
			tx, err := f1.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync, fitz.WithKVMode(fitz.KVModeReadOnly))
			require.NoError(t, err)
			err = tx.Put(ctx, []byte("k"), []byte("v"))
			assert.ErrorIs(t, err, fitz.ErrKVReadOnly)
		})

		t.Run("queue invalid token rejected", func(t *testing.T) {
			route := f1.UniqueRoute("queue")
			_, err := f1.Client().Queue().Enqueue(ctx, route, []byte("job"))
			require.NoError(t, err)

			items, err := f1.Client().Queue().Reserve(ctx, route, 5, 1)
			require.NoError(t, err)
			require.Len(t, items, 1)

			err = items[0].CompleteWithToken(ctx, ^uint64(0))
			assert.ErrorIs(t, err, fitz.ErrQueueInvalidToken)
		})

		t.Run("lease contention rejected", func(t *testing.T) {
			route := f1.UniqueRoute("lease")
			lease, err := f1.Client().Lease().Acquire(ctx, route, 3)
			require.NoError(t, err)
			defer lease.Release(ctx)

			_, err = f2.Client().Lease().Acquire(ctx, route, 3)
			require.Error(t, err)
			assert.True(t, errors.Is(err, fitz.ErrLeaseHeld) || errors.Is(err, fitz.ErrLeaseQueued))
		})

		t.Run("rpc no worker returns error", func(t *testing.T) {
			callCtx, callCancel := context.WithTimeout(ctx, time.Second)
			defer callCancel()

			_, err := f1.Client().RPC().Call(callCtx, f1.UniqueRoute("rpc"), []byte("nobody"))
			require.Error(t, err)
		})
	})
}

func TestShouldClassifyRetryabilityGivenErrorCodeWhenIsRetryableCalled(t *testing.T) {
	tests := []struct {
		name string
		code uint32
		want bool
	}{
		{name: "kv isolation conflict", code: fitz.ErrCodeKvIsolationConflict, want: true},
		{name: "stream read beyond watermark", code: fitz.ErrCodeStreamReadBeyondWatermark, want: true},
		{name: "queue full", code: fitz.ErrCodeQueueFull, want: true},
		{name: "lease held", code: fitz.ErrCodeLeaseHeld, want: true},
		{name: "rpc timeout", code: fitz.ErrCodeRpcTimeout, want: true},
		{name: "rpc no worker", code: fitz.ErrCodeRpcWorkerNotFound, want: true},
		{name: "kv key not found", code: fitz.ErrCodeKvKeyNotFound, want: false},
		{name: "schedule not found", code: fitz.ErrCodeScheduleNotFound, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := coreerrors.NewDomainError(tc.code, tc.name)
			assert.Equal(t, tc.want, fitz.IsRetryable(err))
			assert.Equal(t, tc.want, fitz.IsRetryable(errors.Join(errors.New("outer"), err)))
		})
	}
}
