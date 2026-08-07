package fitz

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShouldRejectLateCallbackWithoutPanicAfterManagedPushIteratorCloses(t *testing.T) {
	var handler func(int) error
	unsubscribing := make(chan struct{})
	releaseUnsubscribe := make(chan struct{})
	iterator, err := startManagedPushIterator(context.Background(), 1,
		func(_ context.Context, callback func(int) error) (func(), error) {
			handler = callback
			return func() {
				close(unsubscribing)
				<-releaseUnsubscribe
			}, nil
		})
	require.NoError(t, err)

	require.NoError(t, iterator.Close())
	<-unsubscribing
	close(releaseUnsubscribe)
	require.False(t, iterator.Next()) // waits until shutdown closes the value channel
	require.Error(t, handler(1))
}
