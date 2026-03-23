package tracing

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const (
	// DefaultAsyncSpanTimeout bounds async callback spans to prevent long-lived traces.
	DefaultAsyncSpanTimeout = 30 * time.Second
)

var noopCancel = func() {}

// StartDetachedSpan starts a span from a detached root context and links it to
// the parent span context when available. This prevents unbounded parent-child
// chains in long-lived async flows while preserving correlation.
func StartDetachedSpan(
	parent context.Context,
	tracer trace.Tracer,
	name string,
	timeout time.Duration,
	opts ...trace.SpanStartOption,
) (context.Context, context.CancelFunc, trace.Span) {
	if tracer == nil {
		tracer = otel.Tracer("github.com/cntryl/fitz-go")
	}

	baseCtx := context.Background()
	cancel := noopCancel
	if timeout > 0 {
		var timeoutCancel context.CancelFunc
		baseCtx, timeoutCancel = context.WithTimeout(baseCtx, timeout)
		cancel = timeoutCancel
	}

	if parent != nil {
		if parentSC := trace.SpanContextFromContext(parent); parentSC.IsValid() {
			opts = append([]trace.SpanStartOption{trace.WithLinks(trace.Link{SpanContext: parentSC})}, opts...)
		}
	}

	spanCtx, span := tracer.Start(baseCtx, name, opts...)
	return spanCtx, cancel, span
}
