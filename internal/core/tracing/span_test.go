package tracing

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestShouldLinkParentWithoutChildChainGivenDetachedSpanWhenStarted(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider()
	tp.RegisterSpanProcessor(recorder)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	tracer := tp.Tracer("test")
	parentCtx, parent := tracer.Start(context.Background(), "parent")
	parentSC := parent.SpanContext()

	childCtx, cancel, detached := StartDetachedSpan(parentCtx, tracer, "detached", time.Second)
	defer cancel()
	require.NotNil(t, childCtx)
	detached.End()
	parent.End()

	ended := recorder.Ended()
	require.Len(t, ended, 2)

	var detachedSpan trace.ReadOnlySpan
	for _, sp := range ended {
		if sp.Name() == "detached" {
			detachedSpan = sp
			break
		}
	}
	require.NotNil(t, detachedSpan)

	// Detached spans should not be parented by the source context span.
	require.False(t, detachedSpan.Parent().IsValid())

	links := detachedSpan.Links()
	require.Len(t, links, 1)
	require.Equal(t, parentSC.TraceID(), links[0].SpanContext.TraceID())
	require.Equal(t, parentSC.SpanID(), links[0].SpanContext.SpanID())
}

func TestShouldCancelDetachedContextGivenTimeoutWhenSpanStarted(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider()
	tp.RegisterSpanProcessor(recorder)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	tracer := tp.Tracer("test")
	ctx, cancel, span := StartDetachedSpan(context.Background(), tracer, "detached-timeout", 15*time.Millisecond)
	defer cancel()
	defer span.End()

	select {
	case <-ctx.Done():
		require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("detached span context did not timeout")
	}
}
