package subscriptions

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldReuseServerSubscriptionGivenDuplicatePatternWhenSubscribeCalled(t *testing.T) {
	registry := NewRegistry[string]()
	subscribeCalls := 0

	firstSubID, firstHandlerID, err := registry.Subscribe("notice://realm/area/resource", "first", func(string) (uint64, error) {
		subscribeCalls++
		return 42, nil
	})
	require.NoError(t, err)

	secondSubID, secondHandlerID, err := registry.Subscribe("notice://realm/area/resource", "second", func(string) (uint64, error) {
		subscribeCalls++
		return 99, nil
	})
	require.NoError(t, err)

	assert.Equal(t, uint64(42), firstSubID)
	assert.Equal(t, uint64(42), secondSubID)
	assert.NotEqual(t, firstHandlerID, secondHandlerID)
	assert.Equal(t, 1, subscribeCalls)

	handlers := registry.Handlers(42)
	require.Len(t, handlers, 2)
	assert.ElementsMatch(t, []string{"first", "second"}, handlers)
}

func TestShouldSendWireUnsubscribeOnlyForLastHandlerWhenUnsubscribeCalled(t *testing.T) {
	registry := NewRegistry[string]()
	subID, firstHandlerID, err := registry.Subscribe("stream://realm/area/resource", "first", func(string) (uint64, error) {
		return 7, nil
	})
	require.NoError(t, err)
	_, secondHandlerID, err := registry.Subscribe("stream://realm/area/resource", "second", func(string) (uint64, error) {
		return 7, nil
	})
	require.NoError(t, err)

	assert.False(t, registry.Unsubscribe("stream://realm/area/resource", firstHandlerID))
	assert.ElementsMatch(t, []string{"second"}, registry.Handlers(subID))
	assert.True(t, registry.Unsubscribe("stream://realm/area/resource", secondHandlerID))
	assert.Empty(t, registry.Handlers(subID))
}

func TestShouldPreserveHandlersGivenReconnectWhenRestoreCalled(t *testing.T) {
	registry := NewRegistry[string]()
	_, firstHandlerID, err := registry.Subscribe("queue://realm/area/resource", "first", func(string) (uint64, error) {
		return 10, nil
	})
	require.NoError(t, err)
	_, secondHandlerID, err := registry.Subscribe("queue://realm/area/resource", "second", func(string) (uint64, error) {
		return 10, nil
	})
	require.NoError(t, err)

	restoreCalls := 0
	require.NoError(t, registry.Restore(
		func(pattern string) (uint64, error) {
			restoreCalls++
			assert.Equal(t, "queue://realm/area/resource", pattern)
			return 55, nil
		},
		func(string, uint64) error { return nil },
	))
	assert.Equal(t, 1, restoreCalls)
	assert.ElementsMatch(t, []string{"first", "second"}, registry.Handlers(55))

	assert.False(t, registry.Unsubscribe("queue://realm/area/resource", firstHandlerID))
	assert.True(t, registry.Unsubscribe("queue://realm/area/resource", secondHandlerID))
}

func TestShouldReuseInFlightSubscriptionGivenConcurrentDuplicatePatternWhenSubscribeCalled(t *testing.T) {
	registry := NewRegistry[string]()
	started := make(chan struct{})
	release := make(chan struct{})
	var subscribeCalls atomic.Int32
	type subscribeResult struct {
		subID     uint64
		handlerID uint64
		err       error
	}
	firstDone := make(chan subscribeResult, 1)
	secondDone := make(chan subscribeResult, 1)

	go func() {
		subID, handlerID, err := registry.Subscribe("notice://realm/area/resource", "first", func(string) (uint64, error) {
			subscribeCalls.Add(1)
			close(started)
			<-release
			return 42, nil
		})
		firstDone <- subscribeResult{subID: subID, handlerID: handlerID, err: err}
	}()

	<-started
	go func() {
		subID, handlerID, err := registry.Subscribe("notice://realm/area/resource", "second", func(string) (uint64, error) {
			subscribeCalls.Add(1)
			return 99, nil
		})
		secondDone <- subscribeResult{subID: subID, handlerID: handlerID, err: err}
	}()

	close(release)
	first := <-firstDone
	second := <-secondDone

	require.NoError(t, first.err)
	require.NoError(t, second.err)
	assert.Equal(t, uint64(42), first.subID)
	assert.Equal(t, uint64(42), second.subID)
	assert.NotEqual(t, first.handlerID, second.handlerID)
	assert.Equal(t, int32(1), subscribeCalls.Load())
	assert.ElementsMatch(t, []string{"first", "second"}, registry.Handlers(42))
}

func TestShouldAllowHandlerLookupGivenBlockedRestoreWhenHandlersCalled(t *testing.T) {
	registry := NewRegistry[string]()
	_, _, err := registry.Subscribe("queue://realm/area/resource", "first", func(string) (uint64, error) {
		return 10, nil
	})
	require.NoError(t, err)

	started := make(chan struct{})
	release := make(chan struct{})
	restoreDone := make(chan error, 1)
	go func() {
		restoreDone <- registry.Restore(
			func(string) (uint64, error) {
				close(started)
				<-release
				return 55, nil
			},
			func(string, uint64) error { return nil },
		)
	}()

	<-started
	handlersDone := make(chan []string, 1)
	go func() {
		handlersDone <- registry.Handlers(10)
	}()

	select {
	case handlers := <-handlersDone:
		assert.ElementsMatch(t, []string{"first"}, handlers)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Handlers blocked while restore was waiting on wire subscribe")
	}

	close(release)
	require.NoError(t, <-restoreDone)
	assert.ElementsMatch(t, []string{"first"}, registry.Handlers(55))
}

func TestShouldRollbackRestoredSubscriptionsGivenRestoreFailureWhenWireSubscribeFails(t *testing.T) {
	registry := NewRegistry[string]()
	_, _, err := registry.Subscribe("stream://realm/area/alpha", "first", func(string) (uint64, error) { return 10, nil })
	require.NoError(t, err)
	_, _, err = registry.Subscribe("stream://realm/area/bravo", "second", func(string) (uint64, error) { return 11, nil })
	require.NoError(t, err)
	_, _, err = registry.Subscribe("stream://realm/area/charlie", "third", func(string) (uint64, error) { return 12, nil })
	require.NoError(t, err)

	var rollback []string
	restoreCalls := 0
	restoreErr := registry.Restore(
		func(pattern string) (uint64, error) {
			restoreCalls++
			switch pattern {
			case "stream://realm/area/alpha":
				return 101, nil
			case "stream://realm/area/bravo":
				return 102, nil
			case "stream://realm/area/charlie":
				return 0, errors.New("boom")
			default:
				t.Fatalf("unexpected pattern %q", pattern)
				return 0, nil
			}
		},
		func(pattern string, subID uint64) error {
			rollback = append(rollback, pattern+":"+fmt.Sprintf("%d", subID))
			return nil
		},
	)
	require.Error(t, restoreErr)
	assert.Equal(t, 3, restoreCalls)
	assert.Equal(t, []string{
		"stream://realm/area/bravo:102",
		"stream://realm/area/alpha:101",
	}, rollback)
	assert.ElementsMatch(t, []string{"first"}, registry.Handlers(10))
	assert.ElementsMatch(t, []string{"second"}, registry.Handlers(11))
	assert.ElementsMatch(t, []string{"third"}, registry.Handlers(12))
}
