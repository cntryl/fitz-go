//go:build integration

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
		sess, err := f.Client().Stream().Begin(ctx, f.UniqueRoute("stream"))
		require.NoError(t, err)

		_, err = sess.Append(ctx, 0, []byte("record-1"))
		require.NoError(t, err)
		_, err = sess.Append(ctx, 1, []byte("record-2"))
		require.NoError(t, err)
		require.NoError(t, sess.Commit(ctx, fitz.StreamCommitSync))
	})
}

func TestShouldReadRecordsInOrderGivenOffsetRangeWhenReadCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("stream")
		sess, err := f.Client().Stream().Begin(ctx, route)
		require.NoError(t, err)
		expectedOffset := uint64(0)
		for i := range 3 {
			_, err := sess.Append(ctx, expectedOffset, []byte{byte(i)})
			require.NoError(t, err)
			expectedOffset++
		}
		require.NoError(t, sess.Commit(ctx, fitz.StreamCommitSync))

		iter, err := f.Client().Stream().Read(ctx, route, 0, 10)
		require.NoError(t, err)
		defer closeQuietly(iter)

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

func TestShouldReadMatchingDiscriminatorRecordsGivenFilterWhenReadCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("stream")
		sess, err := f.Client().Stream().Begin(ctx, route)
		require.NoError(t, err)

		alpha := "proj.alpha"
		_, err = sess.Append(ctx, 0, []byte("alpha"), &fitz.StreamAppendOptions{Discriminator: &alpha})
		require.NoError(t, err)
		beta := "audit.beta"
		_, err = sess.Append(ctx, 1, []byte("beta"), &fitz.StreamAppendOptions{Discriminator: &beta})
		require.NoError(t, err)
		require.NoError(t, sess.Commit(ctx, fitz.StreamCommitSync))

		filter := &fitz.StreamFilterSet{Clauses: []fitz.StreamFilterClause{{Kind: fitz.StreamFilterEquals, Value: "proj.alpha"}}}
		iter, err := f.Client().Stream().Read(ctx, route, 0, 10, &fitz.StreamReadOptions{Filter: filter})
		require.NoError(t, err)
		defer closeQuietly(iter)

		var bodies [][]byte
		for iter.Next() {
			bodies = append(bodies, append([]byte(nil), iter.Value().Body...))
		}
		require.NoError(t, iter.Err())
		require.Len(t, bodies, 1)
		assert.Equal(t, []byte("alpha"), bodies[0])

		page, err := f.Client().Stream().ReadPage(ctx, route, 0, 10, &fitz.StreamReadOptions{Filter: filter})
		require.NoError(t, err)
		require.NotNil(t, page)
		assert.Equal(t, uint64(1), page.Cursor.LastResourceOffset)
		assert.False(t, page.Cursor.HasMore)
		require.Len(t, page.Items, 2)
		assert.Equal(t, fitz.StreamReadItemEvent, page.Items[0].Kind)
		require.NotNil(t, page.Items[0].Record)
		assert.Equal(t, []byte("alpha"), page.Items[0].Record.Body)
		assert.Equal(t, fitz.StreamReadItemFiltered, page.Items[1].Kind)
		assert.Equal(t, uint64(1), page.Items[1].Offset)
		require.NotNil(t, page.Items[1].Reason)
		assert.Equal(t, fitz.StreamFilteredReasonServerFilter, *page.Items[1].Reason)
	})
}

func TestShouldRejectAppendGivenMismatchedExpectedOffsetWhenOptimisticConcurrency(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("stream")
		sess, err := f.Client().Stream().Begin(ctx, route)
		require.NoError(t, err)
		_, err = sess.Append(ctx, 0, []byte("first"))
		require.NoError(t, err)
		require.NoError(t, sess.Commit(ctx, fitz.StreamCommitSync))

		sess, err = f.Client().Stream().Begin(ctx, route)
		require.NoError(t, err)
		_, err = sess.Append(ctx, 0, []byte("second"))
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
		sess, err := f.Client().Stream().Begin(ctx, route)
		require.NoError(t, err)
		_, err = sess.Append(ctx, 0, []byte("ephemeral"))
		require.NoError(t, err)
		require.NoError(t, sess.Rollback(ctx))

		iter, err := f.Client().Stream().Read(ctx, route, 0, 10)
		require.NoError(t, err)
		defer closeQuietly(iter)

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
		sess, err := f.Client().Stream().Begin(ctx, route)
		require.NoError(t, err)
		_, err = sess.Append(ctx, 0, []byte("first"))
		require.NoError(t, err)
		_, err = sess.Append(ctx, 1, []byte("last-one"))
		require.NoError(t, err)
		require.NoError(t, sess.Commit(ctx, fitz.StreamCommitSync))

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
		sess, err := f.Client().Stream().Begin(ctx, route)
		require.NoError(t, err)
		_, err = sess.Append(ctx, 0, []byte("data"))
		require.NoError(t, err)
		require.NoError(t, sess.Commit(ctx, fitz.StreamCommitSync))

		meta, err := f.Client().Stream().Metadata(ctx, route)
		require.NoError(t, err)
		require.NotNil(t, meta)
	})
}

func TestShouldRejectReadGivenOffsetBeyondWatermarkWhenReadCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("stream")
		sess, err := f.Client().Stream().Begin(ctx, route)
		require.NoError(t, err)
		_, err = sess.Append(ctx, 0, []byte("only"))
		require.NoError(t, err)
		require.NoError(t, sess.Commit(ctx, fitz.StreamCommitSync))

		iter, err := f.Client().Stream().Read(ctx, route, 999999, 10)
		require.NoError(t, err)
		require.NotNil(t, iter)
		defer closeQuietly(iter)
		assert.False(t, iter.Next())
		assert.NoError(t, iter.Err())
	})
}

func TestShouldNotifyGivenSubscriptionWhenCommitAppends(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("stream")
		notifications := make(chan fitz.StreamCommitNotification, 1)

		sub, err := f.Client().Stream().Subscribe(ctx, route, func(_ context.Context, n fitz.StreamCommitNotification) error {
			t.Logf("stream commit notification received: route=%s event=%s", n.Route, n.Event)
			notifications <- n
			return nil
		})
		require.NoError(t, err)
		defer sub.Unsubscribe()

		sess, err := f.Client().Stream().Begin(ctx, route)
		require.NoError(t, err)
		_, err = sess.Append(ctx, 0, []byte("notify"))
		require.NoError(t, err)
		require.NoError(t, sess.Commit(ctx, fitz.StreamCommitSync))

		select {
		case notification := <-notifications:
			assert.Equal(t, route, notification.Route)
			assert.Equal(t, "committed", notification.Event)
			assert.Equal(t, uint64(1), notification.BatchSize)
			assert.Equal(t, uint64(0), notification.FirstResourceOffset)
			assert.Equal(t, uint64(0), notification.LastResourceOffset)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for stream commit notification")
		}
	})
}

func TestShouldRestoreCommitSubscriptionGivenLiveDisconnectWhenReconnectEnabled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		harness := fixture.NewProxyReconnectHarness(t, transport, fixture.AuthModeForTestName(t.Name()))
		subscriber := harness.Proxied
		producer := harness.Stable

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		harness.Connect(ctx, fixture.DefaultReconnectOptions()...)

		route := subscriber.UniqueRoute("stream")
		var err error
		notifications := make(chan string, 4)
		_, err = subscriber.Client().Stream().Subscribe(ctx, route, func(_ context.Context, n fitz.StreamCommitNotification) error {
			notifications <- n.Route
			return nil
		})
		require.NoError(t, err)

		commitRecord := func(body string) {
			nextOffset := uint64(0)
			rec, err := producer.Client().Stream().Peek(ctx, route)
			require.NoError(t, err)
			if rec != nil {
				nextOffset = rec.Offset + 1
			}

			sess, err := producer.Client().Stream().Begin(ctx, route)
			require.NoError(t, err)
			_, err = sess.Append(ctx, nextOffset, []byte(body))
			require.NoError(t, err)
			require.NoError(t, sess.Commit(ctx, fitz.StreamCommitSync))
		}

		commitRecord("before-disconnect")

		select {
		case notifiedRoute := <-notifications:
			require.Equal(t, route, notifiedRoute)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for initial stream notification")
		}

		harness.WaitForInitialConnection(5 * time.Second)
		harness.DropAndWaitForReconnect(10 * time.Second)

		require.Eventually(t, func() bool {
			commitRecord("after-disconnect")

			select {
			case notifiedRoute := <-notifications:
				return notifiedRoute == route
			default:
				return false
			}
		}, 10*time.Second, 100*time.Millisecond)
	})
}
