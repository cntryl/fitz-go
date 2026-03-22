package subscriptions

import (
	"testing"

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
	require.NoError(t, registry.Restore(func(pattern string) (uint64, error) {
		restoreCalls++
		assert.Equal(t, "queue://realm/area/resource", pattern)
		return 55, nil
	}))
	assert.Equal(t, 1, restoreCalls)
	assert.ElementsMatch(t, []string{"first", "second"}, registry.Handlers(55))

	assert.False(t, registry.Unsubscribe("queue://realm/area/resource", firstHandlerID))
	assert.True(t, registry.Unsubscribe("queue://realm/area/resource", secondHandlerID))
}
