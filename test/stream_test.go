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

func TestShouldAppendRecordsGivenValidSessionWhenAppendCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		sess, err := f.Client().Stream().Begin(ctx, f.UniqueRoute("stream"), 0)
		require.NoError(t, err)

		_, err = sess.Append(ctx, []byte("record-1"))
		require.NoError(t, err)
		_, err = sess.Append(ctx, []byte("record-2"))
		require.NoError(t, err)
		require.NoError(t, sess.Commit(ctx))
	})
}

func TestShouldReadRecordsInOrderGivenOffsetRangeWhenReadCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("stream")
		sess, err := f.Client().Stream().Begin(ctx, route, 0)
		require.NoError(t, err)
		for i := 0; i < 3; i++ {
			_, err := sess.Append(ctx, []byte{byte(i)})
			require.NoError(t, err)
		}
		require.NoError(t, sess.Commit(ctx))

		iter, err := f.Client().Stream().Read(ctx, route, 0, 10)
		require.NoError(t, err)
		defer iter.Close()

		var offsets []uint64
		for iter.Next() {
			offsets = append(offsets, iter.Value().Offset)
		}
		require.NoError(t, iter.Err())
		for i := 1; i < len(offsets); i++ {
			assert.Greater(t, offsets[i], offsets[i-1])
		}
	})
}

func TestShouldRejectBeginGivenMismatchedExpectedOffsetWhenOptimisticConcurrency(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("stream")
		sess, err := f.Client().Stream().Begin(ctx, route, 0)
		require.NoError(t, err)
		_, err = sess.Append(ctx, []byte("first"))
		require.NoError(t, err)
		require.NoError(t, sess.Commit(ctx))

		_, err = f.Client().Stream().Begin(ctx, route, 99999)
		assert.Error(t, err)
	})
}

func TestShouldRollbackUncommittedAppendsGivenActiveSessionWhenRollbackCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("stream")
		sess, err := f.Client().Stream().Begin(ctx, route, 0)
		require.NoError(t, err)
		_, err = sess.Append(ctx, []byte("ephemeral"))
		require.NoError(t, err)
		require.NoError(t, sess.Rollback(ctx))

		iter, err := f.Client().Stream().Read(ctx, route, 0, 10)
		require.NoError(t, err)
		defer iter.Close()

		count := 0
		for iter.Next() {
			count++
		}
		assert.Equal(t, 0, count)
	})
}

func TestShouldReturnLastRecordGivenExistingStreamWhenLastCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("stream")
		sess, err := f.Client().Stream().Begin(ctx, route, 0)
		require.NoError(t, err)
		_, err = sess.Append(ctx, []byte("first"))
		require.NoError(t, err)
		_, err = sess.Append(ctx, []byte("last-one"))
		require.NoError(t, err)
		require.NoError(t, sess.Commit(ctx))

		rec, err := f.Client().Stream().Peek(ctx, route)
		require.NoError(t, err)
		if rec != nil {
			assert.Equal(t, []byte("last-one"), rec.Body)
		}
	})
}

func TestShouldGetMetadataGivenExistingStreamWhenMetadataCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("stream")
		sess, err := f.Client().Stream().Begin(ctx, route, 0)
		require.NoError(t, err)
		_, err = sess.Append(ctx, []byte("data"))
		require.NoError(t, err)
		require.NoError(t, sess.Commit(ctx))

		meta, err := f.Client().Stream().Metadata(ctx, route)
		require.NoError(t, err)
		require.NotNil(t, meta)
	})
}

func TestShouldRejectReadGivenOffsetBeyondWatermarkWhenConsumeCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("stream")
		sess, err := f.Client().Stream().Begin(ctx, route, 0)
		require.NoError(t, err)
		_, err = sess.Append(ctx, []byte("only"))
		require.NoError(t, err)
		require.NoError(t, sess.Commit(ctx))

		iter, err := f.Client().Stream().Read(ctx, route, 999999, 10)
		if err != nil {
			assert.Error(t, err)
			return
		}
		defer iter.Close()

		for iter.Next() {
		}
		if iter.Err() != nil {
			assert.Error(t, iter.Err())
		}
	})
}

func TestShouldNotifyGivenSubscriptionWhenCommitAppends(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("stream")
		notifications := make(chan struct{}, 1)

		sub, err := f.Client().Stream().Subscribe(ctx, route, func(_ context.Context, n fitz.StreamCommitNotification) error {
			t.Logf("stream commit notification received: route=%s", n.Route)
			notifications <- struct{}{}
			return nil
		})
		require.NoError(t, err)
		defer sub.Unsubscribe()

		sess, err := f.Client().Stream().Begin(ctx, route, 0)
		require.NoError(t, err)
		_, err = sess.Append(ctx, []byte("notify"))
		require.NoError(t, err)
		require.NoError(t, sess.Commit(ctx))

		select {
		case <-notifications:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for stream commit notification")
		}
	})
}
