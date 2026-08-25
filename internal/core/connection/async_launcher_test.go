package connection

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/v2/internal/testkit"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestShouldRejectAsyncHandlerLaunchGivenQueueFullWhenLaunchAsyncHandlerCalled(t *testing.T) {
	conn := New(testkit.NewMockTransport(), Config{AsyncHandlerMaxConcurrency: 1, AsyncHandlerQueueCapacity: 1})
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
	conn := New(testkit.NewMockTransport(), Config{AsyncHandlerMaxConcurrency: 1, AsyncHandlerQueueCapacity: 1})
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

func TestShouldEndQueuedAsyncHandlerSpanGivenShutdownWhenJobDrained(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider()
	tp.RegisterSpanProcessor(recorder)
	t.Cleanup(func() {
		require.NoError(t, tp.Shutdown(context.Background()))
	})

	conn := New(testkit.NewMockTransport(), Config{
		AsyncHandlerMaxConcurrency: 1,
		AsyncHandlerQueueCapacity:  1,
		Tracer:                     tp.Tracer("fitz-go-async-test"),
	})

	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	secondRan := make(chan struct{})
	defer func() {
		close(firstRelease)
		_ = conn.Close()
	}()

	require.True(t, conn.LaunchAsyncHandler(context.Background(), "fitz.test.first", time.Second, func(context.Context, trace.Span) {
		close(firstStarted)
		<-firstRelease
	}))

	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first async handler did not start")
	}

	require.True(t, conn.LaunchAsyncHandler(context.Background(), "fitz.test.queued", time.Minute, func(context.Context, trace.Span) {
		close(secondRan)
	}))

	conn.beginAsyncHandlerShutdown()

	require.False(t, conn.LaunchAsyncHandler(context.Background(), "fitz.test.after_shutdown", time.Second, func(context.Context, trace.Span) {}))
	require.Eventually(t, func() bool {
		for _, span := range recorder.Ended() {
			if span.Name() == "fitz.test.queued" {
				return true
			}
		}
		return false
	}, time.Second, time.Millisecond)

	select {
	case <-secondRan:
		t.Fatal("queued async handler ran after shutdown drain")
	default:
	}
}

func TestShouldCancelReceivedAsyncHandlerJobGivenShutdownBeforeSlotAcquired(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider()
	tp.RegisterSpanProcessor(recorder)
	t.Cleanup(func() {
		require.NoError(t, tp.Shutdown(context.Background()))
	})

	conn := New(testkit.NewMockTransport(), Config{
		AsyncHandlerMaxConcurrency: 1,
		AsyncHandlerQueueCapacity:  1,
		Tracer:                     tp.Tracer("fitz-go-async-test"),
	})
	t.Cleanup(func() {
		_ = conn.Close()
	})

	conn.asyncHandlerSem <- struct{}{}
	defer func() {
		select {
		case <-conn.asyncHandlerSem:
		default:
		}
	}()

	ran := make(chan struct{})
	require.True(t, conn.LaunchAsyncHandler(context.Background(), "fitz.test.received_before_shutdown", time.Minute, func(context.Context, trace.Span) {
		close(ran)
	}))

	require.Eventually(t, func() bool {
		return len(conn.asyncHandlerJobs) == 0
	}, time.Second, time.Millisecond)

	conn.beginAsyncHandlerShutdown()
	conn.cancel()

	require.Eventually(t, func() bool {
		for _, span := range recorder.Ended() {
			if span.Name() == "fitz.test.received_before_shutdown" {
				return span.Status().Code == codes.Error
			}
		}
		return false
	}, time.Second, time.Millisecond)

	select {
	case <-ran:
		t.Fatal("async handler ran after shutdown before acquiring a slot")
	default:
	}
}
