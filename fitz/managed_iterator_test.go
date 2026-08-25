package fitz

import (
	"context"
	"testing"

	coreerrors "github.com/cntryl/fitz-go/v2/internal/core/errors"
	"github.com/stretchr/testify/require"
)

func TestShouldRejectLateCallbackWithoutPanicAfterManagedPushIteratorCloses(t *testing.T) {
	var handler func(int) error
	unsubscribing := make(chan struct{})
	releaseUnsubscribe := make(chan struct{})
	iterator, err := startManagedPushIterator(context.Background(), 1,
		func(_ context.Context, callback func(int) error) (func(), <-chan error, error) {
			handler = callback
			return func() {
				close(unsubscribing)
				<-releaseUnsubscribe
			}, nil, nil
		})
	require.NoError(t, err)

	require.NoError(t, iterator.Close())
	<-unsubscribing
	close(releaseUnsubscribe)
	require.False(t, iterator.Next()) // waits until shutdown closes the value channel
	require.Error(t, handler(1))
}

func TestShouldSurfaceSubscriptionOverflowAsManagedIteratorError(t *testing.T) {
	completion := make(chan error, 1)
	iterator, err := startManagedPushIterator(context.Background(), 1,
		func(_ context.Context, _ func(int) error) (func(), <-chan error, error) {
			return func() {}, completion, nil
		})
	require.NoError(t, err)
	overflow := &coreerrors.AsyncHandlerOverflowError{Domain: "schedule", SubscriptionID: 7}
	completion <- overflow
	close(completion)

	require.False(t, iterator.Next())
	require.ErrorIs(t, iterator.Err(), coreerrors.ErrAsyncHandlerOverflow)
}
