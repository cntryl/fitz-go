package fitz

import (
	"log/slog"
	"time"

	coreclient "github.com/cntryl/fitz-go/internal/core/client"
	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/core/iter"
	"github.com/cntryl/fitz-go/internal/core/types"
	"go.opentelemetry.io/otel/trace"
)

// TokenProvider supplies a JWT for each connect attempt. Return an empty string
// for anonymous mode.
type TokenProvider = types.TokenProvider

// Iterator is the canonical streaming iterator shape used by the Fitz Go SDK.
type Iterator[T any] = iter.Iterator[T]

// ConnectionState describes the lifecycle state of the broker connection.
type ConnectionState = connection.State

const (
	ConnectionStateDisconnected   = connection.StateDisconnected
	ConnectionStateConnecting     = connection.StateConnecting
	ConnectionStateConnected      = connection.StateConnected
	ConnectionStateAuthenticating = connection.StateAuthenticating
	ConnectionStateAuthenticated  = connection.StateAuthenticated
	ConnectionStateClosed         = connection.StateClosed
)

// TransportType selects the underlying transport implementation.
type TransportType = coreclient.TransportType

const (
	TransportAuto      = coreclient.TransportAuto
	TransportWebSocket = coreclient.TransportWebSocket
	TransportTCP       = coreclient.TransportTCP
)

// Option configures the public client at construction time.
type Option = coreclient.Option

// WithAuthSettleDelay configures the silent CONNECT settle window used to infer
// successful auth on brokers that do not emit CONNECT_OK.
func WithAuthSettleDelay(delay time.Duration) Option {
	return coreclient.WithAuthTimeout(delay)
}

func WithReadTimeout(timeout time.Duration) Option {
	return coreclient.WithReadTimeout(timeout)
}

func WithWriteTimeout(timeout time.Duration) Option {
	return coreclient.WithWriteTimeout(timeout)
}

func WithReconnect(enabled bool, backoff time.Duration, maxAttempts int) Option {
	return coreclient.WithReconnect(enabled, backoff, maxAttempts)
}

func WithTransport(transportType TransportType) Option {
	return coreclient.WithTransport(transportType)
}

func WithLogger(logger *slog.Logger) Option {
	return coreclient.WithLogger(logger)
}

func WithTracer(tracer trace.Tracer) Option {
	return coreclient.WithTracer(tracer)
}

var (
	ErrConnectionClosed      = connection.ErrConnectionClosed
	ErrNotAuthenticated      = connection.ErrNotAuthenticated
	ErrAuthenticationFailed  = connection.ErrAuthenticationFailed
	ErrAuthenticationTimeout = connection.ErrAuthenticationTimeout
)
