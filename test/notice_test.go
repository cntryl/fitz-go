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

func TestShouldReceiveNotificationGivenActiveSubscriptionWhenPublishMatches(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("notice")
		received := make(chan fitz.NoticeMsg, 1)

		sub, err := f.Client().Notice().Subscribe(ctx, route, func(_ context.Context, msg fitz.NoticeMsg) error {
			received <- msg
			return nil
		})
		require.NoError(t, err)
		defer sub.Unsubscribe()

		require.NoError(t, f.Client().Notice().Publish(ctx, route, []byte("hello")))
		select {
		case msg := <-received:
			assert.Equal(t, route, msg.Route)
			assert.Equal(t, []byte("hello"), msg.Body)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for notification")
		}
	})
}

func TestShouldFanoutToAllSubscribersGivenMultipleSubscriptionsWhenPublish(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f1 := fixture.NewTestFixture(t, transport)
		f2 := fixture.NewTestFixture(t, transport)
		publisher := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f1.ConnectOrFail(ctx)
		f2.ConnectOrFail(ctx)
		publisher.ConnectOrFail(ctx)
		route := f1.UniqueRoute("notice")

		var mu sync.Mutex
		count := 0
		handler := func(_ context.Context, _ fitz.NoticeMsg) error {
			mu.Lock()
			count++
			mu.Unlock()
			return nil
		}

		sub1, err := f1.Client().Notice().Subscribe(ctx, route, handler)
		require.NoError(t, err)
		defer sub1.Unsubscribe()

		sub2, err := f2.Client().Notice().Subscribe(ctx, route, handler)
		require.NoError(t, err)
		defer sub2.Unsubscribe()

		require.NoError(t, publisher.Client().Notice().Publish(ctx, route, []byte("fanout")))
		time.Sleep(500 * time.Millisecond)

		mu.Lock()
		assert.Equal(t, 2, count)
		mu.Unlock()
	})
}

func TestShouldSucceedGivenNoSubscribersWhenPublishCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		assert.NoError(t, f.Client().Notice().Publish(ctx, f.UniqueRoute("notice"), []byte("nobody-listening")))
	})
}

func TestShouldStopReceivingGivenUnsubscribeWhenPublish(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("notice")
		received := make(chan fitz.NoticeMsg, 10)

		sub, err := f.Client().Notice().Subscribe(ctx, route, func(_ context.Context, msg fitz.NoticeMsg) error {
			received <- msg
			return nil
		})
		require.NoError(t, err)

		require.NoError(t, f.Client().Notice().Publish(ctx, route, []byte("before")))
		select {
		case <-received:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for first notification")
		}

		sub.Unsubscribe()
		require.NoError(t, f.Client().Notice().Publish(ctx, route, []byte("after")))
		select {
		case msg := <-received:
			t.Fatalf("received unexpected notification after unsubscribe: %s", msg.Body)
		case <-time.After(500 * time.Millisecond):
		}
	})
}

func TestShouldMatchWildcardGivenPatternSubscriptionWhenPublishToConcreteRoute(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		realm := f.UniqueRealm()
		area := f.UniqueArea()
		pattern := "notice://" + realm + "/" + area + "/*"
		concrete := "notice://" + realm + "/" + area + "/events"

		received := make(chan fitz.NoticeMsg, 1)
		sub, err := f.Client().Notice().Subscribe(ctx, pattern, func(_ context.Context, msg fitz.NoticeMsg) error {
			received <- msg
			return nil
		})
		require.NoError(t, err)
		defer sub.Unsubscribe()

		require.NoError(t, f.Client().Notice().Publish(ctx, concrete, []byte("wildcard-test")))
		select {
		case msg := <-received:
			assert.Equal(t, concrete, msg.Route)
			assert.Equal(t, []byte("wildcard-test"), msg.Body)
		case <-time.After(5 * time.Second):
			t.Fatal("wildcard subscription did not match concrete publish")
		}
	})
}

func TestShouldNotMatchSingleStarGivenDifferentAreaWhenPublishCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		realm := f.UniqueRealm()
		pattern := "notice://" + realm + "/expected/*"
		concrete := "notice://" + realm + "/other/events"
		received := make(chan fitz.NoticeMsg, 1)
		sub, err := f.Client().Notice().Subscribe(ctx, pattern, func(_ context.Context, msg fitz.NoticeMsg) error {
			received <- msg
			return nil
		})
		require.NoError(t, err)
		defer sub.Unsubscribe()

		require.NoError(t, f.Client().Notice().Publish(ctx, concrete, []byte("single-star")))

		select {
		case msg := <-received:
			t.Fatalf("single-star subscription unexpectedly matched %s", msg.Route)
		case <-time.After(300 * time.Millisecond):
		}
	})
}

func TestShouldMatchDoubleStarGivenDifferentAreaWhenPublishCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		realm := f.UniqueRealm()
		pattern := "notice://" + realm + "/**"
		concrete := "notice://" + realm + "/other/events"
		received := make(chan fitz.NoticeMsg, 1)
		sub, err := f.Client().Notice().Subscribe(ctx, pattern, func(_ context.Context, msg fitz.NoticeMsg) error {
			received <- msg
			return nil
		})
		require.NoError(t, err)
		defer sub.Unsubscribe()

		require.NoError(t, f.Client().Notice().Publish(ctx, concrete, []byte("double-star")))

		select {
		case msg := <-received:
			assert.Equal(t, concrete, msg.Route)
		case <-time.After(5 * time.Second):
			t.Fatal("double-star subscription did not match across areas")
		}
	})
}

func TestShouldIsolateRealmsGivenStagingSubscriptionWhenProdPublishCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		stagingRealm := f.UniqueRealm()
		stagingPattern := "notice://" + stagingRealm + "/**"
		prodRoute := "notice://" + stagingRealm + "-prod/area/resource"
		received := make(chan fitz.NoticeMsg, 1)
		sub, err := f.Client().Notice().Subscribe(ctx, stagingPattern, func(_ context.Context, msg fitz.NoticeMsg) error {
			received <- msg
			return nil
		})
		require.NoError(t, err)
		defer sub.Unsubscribe()

		require.NoError(t, f.Client().Notice().Publish(ctx, prodRoute, []byte("prod")))

		select {
		case msg := <-received:
			t.Fatalf("staging subscription unexpectedly received prod route %s", msg.Route)
		case <-time.After(300 * time.Millisecond):
		}
	})
}
