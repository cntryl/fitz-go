package connection

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/testkit"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func TestShouldRejectAsyncHandlerLaunchGivenQueueFullWhenLaunchAsyncHandlerCalled(t *testing.T) {
	conn := New(testkit.NewMockTransport(), Config{AsyncHandlerMaxConcurrency: 1})
	t.Cleanup(func() {
		_ = conn.Close()
	})

	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	secondStarted := make(chan struct{})
	thirdReturned := make(chan struct{})

	require.True(t, conn.LaunchAsyncHandler(context.Background(), "fitz.test.first", time.Second, func(context.Context, trace.Span) {
		close(firstStarted)
		<-firstRelease
	}))

	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first async handler did not start")
	}

	require.True(t, conn.LaunchAsyncHandler(context.Background(), "fitz.test.second", time.Second, func(context.Context, trace.Span) {
		close(secondStarted)
	}))

	thirdResult := make(chan bool, 1)
	go func() {
		thirdResult <- conn.LaunchAsyncHandler(context.Background(), "fitz.test.third", time.Second, func(context.Context, trace.Span) {})
		close(thirdReturned)
	}()

	select {
	case <-thirdReturned:
		select {
		case result := <-thirdResult:
			require.False(t, result)
		default:
			t.Fatal("third async handler returned without reporting a result")
		}
	case <-time.After(25 * time.Millisecond):
		t.Fatal("third async handler launch did not return promptly when the queue was full")
	}

	close(firstRelease)

	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("second async handler did not start after capacity was released")
	}
}

func TestShouldExpireQueuedAsyncHandlerGivenTimeoutBeforeWorkerStartsWhenLaunchAsyncHandlerCalled(t *testing.T) {
	conn := New(testkit.NewMockTransport(), Config{AsyncHandlerMaxConcurrency: 1})
	t.Cleanup(func() {
		_ = conn.Close()
	})

	firstRelease := make(chan struct{})
	firstStarted := make(chan struct{})
	secondRan := make(chan struct{})

	require.True(t, conn.LaunchAsyncHandler(context.Background(), "fitz.test.first", time.Second, func(context.Context, trace.Span) {
		close(firstStarted)
		<-firstRelease
	}))

	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first async handler did not start")
	}

	require.True(t, conn.LaunchAsyncHandler(context.Background(), "fitz.test.second", 25*time.Millisecond, func(context.Context, trace.Span) {
		close(secondRan)
	}))

	time.Sleep(50 * time.Millisecond)
	close(firstRelease)

	select {
	case <-secondRan:
		t.Fatal("queued async handler ran after its timeout elapsed")
	case <-time.After(100 * time.Millisecond):
	}
}
