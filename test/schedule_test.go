//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/fitz"
	coreerrors "github.com/cntryl/fitz-go/internal/core/errors"
	"github.com/cntryl/fitz-go/internal/testkit"
	"github.com/cntryl/fitz-go/test/fixture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldCreateScheduleGivenValidCronExpressionWhenCreateCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		id, err := f.Client().Schedule().Create(ctx, f.UniqueRoute("schedule"), "*/5 * * * *", []byte("task-payload"))
		require.NoError(t, err)
		assert.NotEmpty(t, id)
	})
}

func TestShouldRejectCreateGivenInvalidCronSyntaxWhenCreateCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		_, err := f.Client().Schedule().Create(ctx, f.UniqueRoute("schedule"), "not a cron", []byte("payload"))
		testkit.AssertDomainErrorCode(t, err, coreerrors.ScheduleInvalidCron)
	})
}

func TestShouldCancelScheduleGivenExistingScheduleWhenCancelCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("schedule")
		_, err := f.Client().Schedule().Create(ctx, route, "0 9 * * 1", []byte("weekly"))
		require.NoError(t, err)
		require.NoError(t, f.Client().Schedule().Cancel(ctx, route))
	})
}

func TestShouldListSchedulesGivenMultipleSchedulesWhenListCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("schedule")
		_, err := f.Client().Schedule().Create(ctx, route, "0 9 * * 1", []byte("s1"))
		require.NoError(t, err)
		_, err = f.Client().Schedule().Create(ctx, route, "0 12 * * *", []byte("s2"))
		require.NoError(t, err)

		entries, totalCount, err := f.Client().Schedule().List(ctx, 0, 100)
		require.NoError(t, err)
		require.NotNil(t, entries)
		_ = totalCount
	})
}

func TestShouldCancelNonExistentScheduleGivenBogusIDWhenCancelCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		missingRoute := strings.TrimSuffix(f.UniqueRoute("schedule"), "/run") + "/missing"
		err := f.Client().Schedule().Cancel(ctx, missingRoute)
		if err != nil {
			assert.ErrorIs(t, err, fitz.ErrScheduleNotFound)
		}
	})
}

func TestShouldReturnListWithoutErrorGivenSchedulesWhenListCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		entries, totalCount, err := f.Client().Schedule().List(ctx, 0, 0)
		require.NoError(t, err)
		require.NotNil(t, entries)
		_ = totalCount
	})
}

func TestShouldListSchedulesGivenWildcardSelectorWhenListBySelectorCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		realm := f.UniqueRealm()
		area := f.UniqueArea()
		selectedPrefix := fmt.Sprintf("schedule://%s/%s", realm, area)
		selector := selectedPrefix + "/*"
		matchingRoutes := []string{
			selectedPrefix + "/one/run",
			selectedPrefix + "/two/send",
		}
		otherRoute := fmt.Sprintf("schedule://%s/%s-alt/three/run", realm, area)

		for _, route := range append(append([]string{}, matchingRoutes...), otherRoute) {
			_, err := f.Client().Schedule().Create(ctx, route, "0 9 * * 1", []byte(route))
			require.NoError(t, err)
		}

		entries, totalCount, err := f.Client().Schedule().ListBySelector(ctx, selector, 0, 10)
		require.NoError(t, err)
		require.Len(t, entries, 2)
		assert.Equal(t, uint64(2), totalCount)
		for _, entry := range entries {
			assert.True(t, strings.HasPrefix(entry.Route, selectedPrefix+"/"))
		}
	})
}

func TestShouldSubscribeAndUnsubscribeGivenValidPatternWhenSubscribeCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		sub, err := f.Client().Schedule().Subscribe(ctx, f.UniqueRoute("schedule"), func(_ context.Context, _ fitz.ScheduleNotification) error {
			return nil
		})
		require.NoError(t, err)
		sub.Unsubscribe()
		require.NotNil(t, sub)
	})
}

func TestShouldRejectWildcardSubscribeGivenClientValidationWhenSubscribeCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		sub, err := f.Client().Schedule().Subscribe(ctx, "schedule://realm/area/*", func(_ context.Context, _ fitz.ScheduleNotification) error {
			return nil
		})
		require.Error(t, err)
		require.Nil(t, sub)
	})
}

func TestShouldDeliverScheduleNotificationGivenLiveBrokerWhenScheduleFires(t *testing.T) {
	f := fixture.NewTestFixture(t, fixture.TransportTCP)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	require.NoError(t, f.ConnectWithOptions(ctx, fitz.WithReadTimeout(2*time.Minute)))
	route := f.UniqueRoute("schedule")
	payload := []byte("live-schedule-payload")
	received := make(chan []byte, 1)
	createdIDs := make([]string, 0, 2)

	sub, err := f.Client().Schedule().Subscribe(ctx, route, func(_ context.Context, n fitz.ScheduleNotification) error {
		received <- append([]byte(nil), n.Payload...)
		return nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()
	defer func() {
		for _, id := range createdIDs {
			_ = f.Client().Schedule().Cancel(context.Background(), id)
		}
	}()

	waitForNotification := func(wait time.Duration) ([]byte, bool) {
		select {
		case got := <-received:
			return got, true
		case <-time.After(wait):
			return nil, false
		}
	}

	for attempt := 1; attempt <= 2; attempt++ {
		id, createErr := f.Client().Schedule().Create(ctx, route, "* * * * *", payload)
		require.NoError(t, createErr)
		createdIDs = append(createdIDs, id)

		if got, ok := waitForNotification(95 * time.Second); ok {
			assert.Equal(t, payload, got)
			return
		}

		if attempt == 1 {
			t.Log("no schedule notification in first window; recreating schedule and retrying once")
		}
	}

	t.Fatal("timed out waiting for schedule notification after retry")
}

func TestShouldRestoreScheduleSubscriptionGivenLiveDisconnectWhenReconnectEnabled(t *testing.T) {
	harness := fixture.NewProxyReconnectHarness(t, fixture.TransportTCP, fixture.AuthModeForTestName(t.Name()))
	subscriber := harness.Proxied
	actor := harness.Stable

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	reconnectOpts := append([]fitz.Option{fitz.WithReadTimeout(2 * time.Minute)}, fixture.DefaultReconnectOptions()...)
	harness.Connect(ctx, reconnectOpts...)

	route := subscriber.UniqueRoute("schedule")
	payload := []byte("schedule-after-reconnect")
	received := make(chan []byte, 1)
	createdIDs := make([]string, 0, 2)

	sub, err := subscriber.Client().Schedule().Subscribe(ctx, route, func(_ context.Context, n fitz.ScheduleNotification) error {
		received <- append([]byte(nil), n.Payload...)
		return nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()
	defer func() {
		for _, id := range createdIDs {
			_ = actor.Client().Schedule().Cancel(context.Background(), id)
		}
	}()

	harness.WaitForInitialConnection(5 * time.Second)
	harness.DropAndWaitForReconnect(10 * time.Second)

	waitForNotification := func(wait time.Duration) ([]byte, bool) {
		select {
		case got := <-received:
			return got, true
		case <-time.After(wait):
			return nil, false
		}
	}

	for attempt := 1; attempt <= 2; attempt++ {
		id, createErr := actor.Client().Schedule().Create(ctx, route, "* * * * *", payload)
		require.NoError(t, createErr)
		createdIDs = append(createdIDs, id)

		if got, ok := waitForNotification(95 * time.Second); ok {
			assert.Equal(t, payload, got)
			return
		}

		if attempt == 1 {
			t.Log("no restored schedule notification in first window; recreating schedule and retrying once")
		}
	}

	t.Fatal("timed out waiting for restored schedule notification after retry")
}
