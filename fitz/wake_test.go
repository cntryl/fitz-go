package fitz

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldWakeWaiterGivenWakeCalled(t *testing.T) {
	gate := NewWakeGate()
	done := make(chan uint64, 1)
	go func() {
		version, _ := gate.Wait(context.Background())
		done <- version
	}()
	time.Sleep(10 * time.Millisecond)

	wokenVersion := gate.Wake()

	select {
	case observed := <-done:
		assert.Equal(t, wokenVersion, observed)
	case <-time.After(time.Second):
		t.Fatal("waiter was not woken")
	}
}

func TestShouldReturnImmediatelyGivenNewerVersionWhenWaitAfterCalled(t *testing.T) {
	gate := NewWakeGate()
	version := gate.Wake()

	observed, err := gate.WaitAfter(context.Background(), version-1)

	require.NoError(t, err)
	assert.Equal(t, version, observed)
}

func TestShouldReturnContextErrorGivenCanceledContextWhenWaitAfterCalled(t *testing.T) {
	gate := NewWakeGate()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := gate.WaitAfter(ctx, 0)

	require.ErrorIs(t, err, context.Canceled)
}
