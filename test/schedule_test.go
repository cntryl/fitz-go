package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/fitz"
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
		assert.Error(t, err)
	})
}

func TestShouldCancelScheduleGivenExistingScheduleWhenCancelCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("schedule")
		id, err := f.Client().Schedule().Create(ctx, route, "0 9 * * 1", []byte("weekly"))
		require.NoError(t, err)
		require.NoError(t, f.Client().Schedule().Cancel(ctx, id))
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
		err := f.Client().Schedule().Cancel(ctx, f.UniqueRoute("schedule")+"-nonexistent")
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

func TestShouldListSchedulesGivenAreaSelectorWhenListBySelectorCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		realm := f.UniqueRealm()
		area := f.UniqueArea()
		selectedPrefix := fmt.Sprintf("schedule://%s/%s", realm, area)
		matchingRoutes := []string{
			selectedPrefix + "/one",
			selectedPrefix + "/two",
		}
		otherRoute := fmt.Sprintf("schedule://%s/%s-alt/three", realm, area)

		for _, route := range append(append([]string{}, matchingRoutes...), otherRoute) {
			_, err := f.Client().Schedule().Create(ctx, route, "0 9 * * 1", []byte(route))
			require.NoError(t, err)
		}

		entries, totalCount, err := f.Client().Schedule().ListBySelector(ctx, selectedPrefix, 0, 10)
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
		sub, err := f.Client().Schedule().Subscribe(ctx, f.UniqueRoute("schedule"), func(_ context.Context, _ fitz.ScheduleNotification) {})
		require.NoError(t, err)
		sub.Unsubscribe()
		require.NotNil(t, sub)
	})
}

func TestShouldDeliverScheduleNotificationGivenLiveBrokerWhenScheduleFires(t *testing.T) {
	f := fixture.NewTestFixture(t, fixture.TransportTCP)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	f.ConnectOrFail(ctx)
	route := f.UniqueRoute("schedule")
	payload := []byte("live-schedule-payload")
	received := make(chan []byte, 1)

	sub, err := f.Client().Schedule().Subscribe(ctx, route, func(_ context.Context, n fitz.ScheduleNotification) {
		received <- append([]byte(nil), n.Payload...)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()
	defer func() {
		_ = f.Client().Schedule().Cancel(context.Background(), route)
	}()

	_, err = f.Client().Schedule().Create(ctx, route, "* * * * *", payload)
	require.NoError(t, err)

	select {
	case got := <-received:
		assert.Equal(t, payload, got)
	case <-ctx.Done():
		t.Fatal("timed out waiting for schedule notification")
	}
}
