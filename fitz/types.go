package fitz

import (
	"context"
	"log/slog"
	"time"

	coreclient "github.com/cntryl/fitz-go/internal/core/client"
	"github.com/cntryl/fitz-go/internal/core/connection"
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
	ConnectionStateClosed
)

// TransportType selects the underlying transport implementation.
type TransportType uint8

const (
	TransportAuto TransportType = iota
	TransportWebSocket
	TransportTCP
)

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

// WithReconnect controls the automatic reconnect behaviour. When enabled, the
// client will attempt to re-establish the transport connection using a fixed
// backoff interval up to maxAttempts times (0 = unlimited).
func WithReconnect(enabled bool, backoff time.Duration, maxAttempts int) Option {
	return func(cfg *clientConfig) {
		cfg.coreOptions = append(cfg.coreOptions, coreclient.WithReconnect(enabled, backoff, maxAttempts))
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

func toCoreTransportType(transportType TransportType) coreclient.TransportType {
	switch transportType {
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
)
