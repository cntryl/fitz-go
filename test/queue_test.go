//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/v2/fitz"
	"github.com/cntryl/fitz-go/v2/test/fixture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldSendAndReceiveMessageGivenValidQueueWhenBasicWorkflow(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("queue")

		msgID, err := f.Client().Queue().Enqueue(ctx, route, []byte("task-payload"))
		require.NoError(t, err)
		assert.NotZero(t, msgID)

		items, err := f.Client().Queue().Reserve(ctx, route, 30, 1)
		require.NoError(t, err)
		require.Len(t, items, 1)
		assert.Equal(t, []byte("task-payload"), items[0].Body)
		require.NoError(t, items[0].Complete(ctx))
	})
}

func TestShouldDelayVisibilityGivenDelayedEnqueueWhenReserved(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("queue")
		_, err := f.Client().Queue().EnqueueWithOptions(
			ctx,
			route,
			[]byte("delayed"),
			fitz.WithQueueEnqueueDelaySeconds(1),
		)
		require.NoError(t, err)

		immediate, err := f.Client().Queue().Reserve(ctx, route, 30, 1)
		require.NoError(t, err)
		assert.Empty(t, immediate)

		var delayed []*fitz.QueueItem
		require.Eventually(t, func() bool {
			delayed, err = f.Client().Queue().Reserve(ctx, route, 30, 1)
			return err == nil && len(delayed) == 1
		}, 3*time.Second, 100*time.Millisecond)
		assert.Equal(t, []byte("delayed"), delayed[0].Body)
		require.NoError(t, delayed[0].Complete(ctx))
	})
}

func TestShouldReturnMessageToQueueGivenExpiredLeaseWhenLeaseExpires(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("queue")

		_, err := f.Client().Queue().Enqueue(ctx, route, []byte("expire-me"))
		require.NoError(t, err)

		items, err := f.Client().Queue().Reserve(ctx, route, 1, 1)
		require.NoError(t, err)
		require.Len(t, items, 1)

		time.Sleep(1 * time.Second)

		var items2 []*fitz.QueueItem
		require.Eventually(t, func() bool {
			candidate, err := f.Client().Queue().Reserve(ctx, route, 30, 1)
			if err != nil || len(candidate) < 1 {
				return false
			}
			items2 = candidate
			return true
		}, 3*time.Second, 100*time.Millisecond)
		assert.GreaterOrEqual(t, len(items2), 1)
		if len(items2) >= 1 {
			assert.Equal(t, []byte("expire-me"), items2[0].Body)
		}
	})
}

func TestShouldExtendLeaseGivenValidTokenWhenExtendCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("queue")

		_, err := f.Client().Queue().Enqueue(ctx, route, []byte("extend-me"))
		require.NoError(t, err)

		items, err := f.Client().Queue().Reserve(ctx, route, 5, 1)
		require.NoError(t, err)
		require.Len(t, items, 1)
		require.NoError(t, items[0].Extend(ctx, 60))
	})
}

func TestShouldRejectCompleteGivenInvalidTokenWhenTokenMismatch(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("queue")
		_, err := f.Client().Queue().Enqueue(ctx, route, []byte("token-check"))
		require.NoError(t, err)

		items, err := f.Client().Queue().Reserve(ctx, route, 30, 1)
		require.NoError(t, err)
		require.Len(t, items, 1)
		assert.ErrorIs(t, items[0].CompleteWithToken(ctx, 9999999), fitz.ErrQueueInvalidToken)
	})
}

func TestShouldReserveBatchGivenMultipleMessagesWhenBatchSizeSpecified(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("queue")

		for range 5 {
			_, err := f.Client().Queue().Enqueue(ctx, route, []byte("batch-msg"))
			require.NoError(t, err)
		}

		items, err := f.Client().Queue().Reserve(ctx, route, 30, 3)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(items), 3)
		assert.GreaterOrEqual(t, len(items), 1)
	})
}

func TestShouldLongPollGivenReserveWithOptionsWhenMessageArrivesLater(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		consumer := fixture.NewTestFixture(t, transport)
		producer := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		consumer.ConnectOrFail(ctx)
		producer.ConnectOrFail(ctx)
		route := consumer.UniqueRoute("queue")

		go func() {
			time.Sleep(250 * time.Millisecond)
			_, _ = producer.Client().Queue().Enqueue(ctx, route, []byte("late-msg"))
		}()

		items, err := consumer.Client().Queue().ReserveWithOptions(
			ctx,
			route,
			30,
			fitz.WithQueueReserveBatchSize(1),
			fitz.WithQueueReserveWaitSeconds(2),
		)
		require.NoError(t, err)
		require.Len(t, items, 1)
		assert.Equal(t, []byte("late-msg"), items[0].Body)
	})
}

func TestShouldDistributeMessagesGivenMultipleConsumersWhenConcurrentReserve(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f1 := fixture.NewTestFixture(t, transport)
		f2 := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f1.ConnectOrFail(ctx)
		f2.ConnectOrFail(ctx)
		route := f1.UniqueRoute("queue")

		for range 2 {
			_, err := f1.Client().Queue().Enqueue(ctx, route, []byte("concurrent-msg"))
			require.NoError(t, err)
		}

		items1, err1 := f1.Client().Queue().Reserve(ctx, route, 30, 1)
		items2, err2 := f2.Client().Queue().Reserve(ctx, route, 30, 1)
		require.NoError(t, err1)
		require.NoError(t, err2)
		assert.Equal(t, 2, len(items1)+len(items2))
	})
}

func TestShouldRejectCompleteGivenExpiredLeaseWhenCompleteCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("queue")

		_, err := f.Client().Queue().Enqueue(ctx, route, []byte("expire-then-complete"))
		require.NoError(t, err)

		items, err := f.Client().Queue().Reserve(ctx, route, 1, 1)
		require.NoError(t, err)
		require.Len(t, items, 1)

		time.Sleep(1 * time.Second)

		require.Eventually(t, func() bool {
			err = items[0].Complete(ctx)
			return err != nil
		}, 3*time.Second, 100*time.Millisecond)
		assert.True(t, errors.Is(err, fitz.ErrQueueLeaseExpired) || errors.Is(err, fitz.ErrQueueMessageNotFound))
	})
}

func TestShouldReturnEmptyGivenEmptyQueueWhenReserveCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		items, err := f.Client().Queue().Reserve(ctx, f.UniqueRoute("queue"), 30, 1)
		require.NoError(t, err)
		require.NotNil(t, items)
		assert.Empty(t, items)
	})
}

func TestShouldNotifyGivenSubscribeWhenMessageEnqueued(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("queue")

		notifications := make(chan struct{}, 2)
		sub, err := f.Client().Queue().Subscribe(ctx, route, func(_ context.Context, n fitz.QueueAvailabilityNotification) error {
			t.Logf("queue notification received: route=%s", n.Route)
			notifications <- struct{}{}
			return nil
		})
		require.NoError(t, err)
		defer sub.Unsubscribe()

		_, err = f.Client().Queue().Enqueue(ctx, route, []byte("notify-me"))
		require.NoError(t, err)

		select {
		case <-notifications:
		case <-time.After(2 * time.Second):
			t.Fatal("expected availability notification")
		}

		sub.Unsubscribe()
		_, err = f.Client().Queue().Enqueue(ctx, route, []byte("no-notify"))
		require.NoError(t, err)

		select {
		case <-notifications:
			t.Fatal("did not expect notification after unsubscribe")
		case <-time.After(500 * time.Millisecond):
		}
	})
}

func TestShouldRestoreAvailabilitySubscriptionGivenLiveDisconnectWhenReconnectEnabled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		harness := fixture.NewProxyReconnectHarness(t, transport, fixture.AuthModeForTestName(t.Name()))
		subscriber := harness.Proxied
		producer := harness.Stable

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		harness.Connect(ctx, fixture.DefaultReconnectOptions()...)

		route := subscriber.UniqueRoute("queue")
		var err error
		notifications := make(chan string, 4)
		_, err = subscriber.Client().Queue().Subscribe(ctx, route, func(_ context.Context, n fitz.QueueAvailabilityNotification) error {
			notifications <- n.Route
			return nil
		})
		require.NoError(t, err)

		_, err = producer.Client().Queue().Enqueue(ctx, route, []byte("before-disconnect"))
		require.NoError(t, err)

		select {
		case notifiedRoute := <-notifications:
			require.Equal(t, route, notifiedRoute)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for initial queue notification")
		}

		items, err := subscriber.Client().Queue().Reserve(ctx, route, 30, 1)
		require.NoError(t, err)
		require.Len(t, items, 1)
		require.NoError(t, items[0].Complete(ctx))

		harness.WaitForInitialConnection(5 * time.Second)
		harness.DropAndWaitForReconnect(10 * time.Second)

		require.Eventually(t, func() bool {
			_, err := producer.Client().Queue().Enqueue(ctx, route, []byte("after-disconnect"))
			if err != nil {
				return false
			}

			select {
			case notifiedRoute := <-notifications:
				return notifiedRoute == route
			default:
				items, reserveErr := producer.Client().Queue().Reserve(ctx, route, 30, 1)
				if reserveErr == nil && len(items) == 1 {
					_ = items[0].Complete(ctx)
				}
				return false
			}
		}, 10*time.Second, 100*time.Millisecond)
	})
}
