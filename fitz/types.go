package fitz

import (
	"context"
	"log/slog"
	"time"

	coreclient "github.com/cntryl/fitz-go/v2/internal/core/client"
	"github.com/cntryl/fitz-go/v2/internal/core/connection"
	coretypes "github.com/cntryl/fitz-go/v2/internal/core/types"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// TokenProvider supplies a token for each connect attempt. Return an empty
// string for anonymous mode.
type TokenProvider func(context.Context) (string, error)

// Iterator is the canonical iterator shape used by the public Fitz Go SDK.
//
// Iterators are resource-backed. Call Close when finished, especially when
// breaking early, to release pending request/subscription state promptly.
type Iterator[T any] interface {
	Next() bool
	Value() T
	Err() error
	// Close releases iterator resources. It is safe to call more than once.
	Close() error
}

// ConnectionState describes the lifecycle state of the broker connection.
type ConnectionState uint8

const (
	ConnectionStateDisconnected ConnectionState = iota
	ConnectionStateConnecting
	ConnectionStateConnected
	ConnectionStateAuthenticating
	ConnectionStateAuthenticated
	ConnectionStateReconnecting
	ConnectionStateClosed
)

// TransportType selects the underlying transport implementation.
type TransportType uint8

const (
	TransportAuto TransportType = iota
	TransportWebSocket
	TransportTCP
)

// LifecycleEvent is emitted by WithLifecycleHandler for connection lifecycle
// transitions. Shared names match fitz-ts; reconnect_scheduled and
// reconnect_exhausted are Go-specific extensions.
type LifecycleEvent struct {
	Event     string
	State     ConnectionState
	Transport string
	URL       string
	Attempt   int
	Error     error
}

type clientConfig struct {
	coreOptions []coreclient.Option
}

// Option configures the public client at construction time.
type Option func(*clientConfig)

func applyOptions(opts []Option) []coreclient.Option {
	cfg := &clientConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}
	return append([]coreclient.Option(nil), cfg.coreOptions...)
}

// WithAuthSettleDelay configures the silent CONNECT settle window used to infer
// successful auth on brokers that do not emit CONNECT_OK.
func WithAuthSettleDelay(delay time.Duration) Option {
	return func(cfg *clientConfig) {
		cfg.coreOptions = append(cfg.coreOptions, coreclient.WithAuthSettleDelay(delay))
	}
}

// WithReadTimeout sets the per-read deadline on the underlying transport
// connection. A zero duration disables the timeout.
func WithReadTimeout(timeout time.Duration) Option {
	return func(cfg *clientConfig) {
		cfg.coreOptions = append(cfg.coreOptions, coreclient.WithReadTimeout(timeout))
	}
}

// WithWriteTimeout sets the per-write deadline on the underlying transport
// connection. A zero duration disables the timeout.
func WithWriteTimeout(timeout time.Duration) Option {
	return func(cfg *clientConfig) {
		cfg.coreOptions = append(cfg.coreOptions, coreclient.WithWriteTimeout(timeout))
	}
}

// WithMaxInFlightRequests sets the maximum number of concurrently admitted
// outbound request operations on a connection.
func WithMaxInFlightRequests(limit int) Option {
	return func(cfg *clientConfig) {
		cfg.coreOptions = append(cfg.coreOptions, coreclient.WithMaxInFlightRequests(limit))
	}
}

// WithMaxRequestQueueSize sets how many outbound operations may wait for an
// in-flight slot before new operations fail fast with ErrRequestQueueFull.
func WithMaxRequestQueueSize(limit int) Option {
	return func(cfg *clientConfig) {
		cfg.coreOptions = append(cfg.coreOptions, coreclient.WithMaxRequestQueueSize(limit))
	}
}

// WithAsyncHandlerTimeout sets the timeout used for detached async handler
// spans in subscription callbacks and RPC worker handlers.
func WithAsyncHandlerTimeout(timeout time.Duration) Option {
	return func(cfg *clientConfig) {
		cfg.coreOptions = append(cfg.coreOptions, coreclient.WithAsyncHandlerTimeout(timeout))
	}
}

// WithAsyncHandlerMaxConcurrency sets the maximum number of concurrent detached
// async handlers used by subscription callbacks and RPC worker handlers.
func WithAsyncHandlerMaxConcurrency(limit int) Option {
	return func(cfg *clientConfig) {
		cfg.coreOptions = append(cfg.coreOptions, coreclient.WithAsyncHandlerMaxConcurrency(limit))
	}
}

// WithReconnect controls the automatic reconnect behavior. When enabled, the
// client will attempt to re-establish the transport connection using exponential
// backoff starting at backoff, doubling up to the ceiling configured by
// WithReconnectMaxDelay (default 5s), up to maxAttempts times (0 = unlimited).
func WithReconnect(enabled bool, backoff time.Duration, maxAttempts int) Option {
	return func(cfg *clientConfig) {
		cfg.coreOptions = append(cfg.coreOptions, coreclient.WithReconnect(enabled, backoff, maxAttempts))
	}
}

// WithReconnectMaxDelay sets the ceiling for exponential reconnect backoff.
// After each failed attempt the delay grows (with jitter) until it reaches
// this maximum. Defaults to 5s.
func WithReconnectMaxDelay(d time.Duration) Option {
	return func(cfg *clientConfig) {
		cfg.coreOptions = append(cfg.coreOptions, coreclient.WithReconnectMaxDelay(d))
	}
}

// WithReconnectTimeout bounds the total duration of one automatic reconnect loop.
func WithReconnectTimeout(timeout time.Duration) Option {
	return func(cfg *clientConfig) {
		cfg.coreOptions = append(cfg.coreOptions, coreclient.WithReconnectTimeout(timeout))
	}
}

// WithRetry controls automatic retries for replay-safe operations. maxAttempts
// is the total number of attempts; values <= 0 use the default.
func WithRetry(enabled bool, backoff time.Duration, maxAttempts int) Option {
	return func(cfg *clientConfig) {
		cfg.coreOptions = append(cfg.coreOptions, coreclient.WithRetry(enabled, backoff, maxAttempts))
	}
}

// WithRetryMaxDelay sets the ceiling for automatic retry backoff.
func WithRetryMaxDelay(d time.Duration) Option {
	return func(cfg *clientConfig) {
		cfg.coreOptions = append(cfg.coreOptions, coreclient.WithRetryMaxDelay(d))
	}
}

// WithHeartbeat configures idle liveness checks. WebSocket uses ping/pong;
// TCP uses socket keepalive.
func WithHeartbeat(enabled bool, interval time.Duration, timeout time.Duration) Option {
	return func(cfg *clientConfig) {
		cfg.coreOptions = append(cfg.coreOptions, coreclient.WithHeartbeat(enabled, interval, timeout))
	}
}

// WithMaxFrameSize sets the per-transport inbound frame size limit in bytes.
func WithMaxFrameSize(bytes int) Option {
	return func(cfg *clientConfig) {
		cfg.coreOptions = append(cfg.coreOptions, coreclient.WithMaxFrameSize(bytes))
	}
}

// WithLifecycleHandler registers a callback for connection lifecycle events.
func WithLifecycleHandler(handler func(LifecycleEvent)) Option {
	return func(cfg *clientConfig) {
		cfg.coreOptions = append(cfg.coreOptions, coreclient.WithLifecycleHandler(func(event coreclient.LifecycleEvent) {
			if handler == nil {
				return
			}
			handler(LifecycleEvent{
				Event:     event.Event,
				State:     fromCoreConnectionState(event.State),
				Transport: event.Transport,
				URL:       event.URL,
				Attempt:   event.Attempt,
				Error:     event.Error,
			})
		}))
	}
}

// WithTransport selects the transport implementation. TransportAuto (the
// default) chooses WebSocket when the address starts with ws:// or wss://,
// and TCP otherwise.
func WithTransport(transportType TransportType) Option {
	return func(cfg *clientConfig) {
		cfg.coreOptions = append(cfg.coreOptions, coreclient.WithTransport(toCoreTransportType(transportType)))
	}
}

// WithLogger attaches a structured [slog.Logger] to the client. If nil, the
// client operates silently without emitting any log output.
func WithLogger(logger *slog.Logger) Option {
	return func(cfg *clientConfig) {
		cfg.coreOptions = append(cfg.coreOptions, coreclient.WithLogger(logger))
	}
}

// WithTracer attaches an OpenTelemetry [trace.Tracer] for distributed tracing.
// Spans are created for each domain operation when a tracer is provided.
func WithTracer(tracer trace.Tracer) Option {
	return func(cfg *clientConfig) {
		cfg.coreOptions = append(cfg.coreOptions, coreclient.WithTracer(tracer))
	}
}

// WithMeter attaches an OpenTelemetry [metric.Meter] for client metrics.
// Metrics are emitted for core operations such as detached async handler sloting.
func WithMeter(meter metric.Meter) Option {
	return func(cfg *clientConfig) {
		cfg.coreOptions = append(cfg.coreOptions, coreclient.WithMeter(meter))
	}
}

func toCoreTransportType(transportType TransportType) coreclient.TransportType {
	switch transportType {
	case TransportAuto:
		return coreclient.TransportAuto
	case TransportWebSocket:
		return coreclient.TransportWebSocket
	case TransportTCP:
		return coreclient.TransportTCP
	default:
		return coreclient.TransportAuto
	}
}

func fromCoreConnectionState(state connection.State) ConnectionState {
	switch state {
	case connection.StateDisconnected:
		return ConnectionStateDisconnected
	case connection.StateConnecting:
		return ConnectionStateConnecting
	case connection.StateConnected:
		return ConnectionStateConnected
	case connection.StateAuthenticating:
		return ConnectionStateAuthenticating
	case connection.StateAuthenticated:
		return ConnectionStateAuthenticated
	case connection.StateClosed:
		return ConnectionStateClosed
	default:
		return ConnectionStateDisconnected
	}
}

var (
	ErrConnectionClosed      = connection.ErrConnectionClosed
	ErrNotAuthenticated      = connection.ErrNotAuthenticated
	ErrAuthenticationFailed  = connection.ErrAuthenticationFailed
	ErrAuthenticationTimeout = connection.ErrAuthenticationTimeout
	ErrRequestQueueFull      = connection.ErrRequestQueueFull
	ErrStaleHandle           = connection.ErrStaleHandle
	ErrInvalidRouteShape     = coretypes.ErrInvalidRouteShape
)
