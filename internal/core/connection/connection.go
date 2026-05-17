//nolint:dupl,gosec,unused,errcheck
package connection

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	coreerrors "github.com/cntryl/fitz-go/internal/core/errors"
	"github.com/cntryl/fitz-go/internal/core/transport"
	"github.com/cntryl/fitz-go/internal/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	traceNoop "go.opentelemetry.io/otel/trace/noop"
)

// State represents the connection lifecycle state.
type State int32

const (
	StateDisconnected   State = iota
	StateConnecting           // Transport dialing
	StateConnected            // Transport open
	StateAuthenticating       // CONNECT sent, awaiting confirmation
	StateAuthenticated        // First response received OR immediate auth (anonymous)
	StateClosed               // Connection terminated
)

// String returns the state name for logging.
func (s State) String() string {
	switch s {
	case StateDisconnected:
		return "DISCONNECTED"
	case StateConnecting:
		return "CONNECTING"
	case StateConnected:
		return "CONNECTED"
	case StateAuthenticating:
		return "AUTHENTICATING"
	case StateAuthenticated:
		return "AUTHENTICATED"
	case StateClosed:
		return "CLOSED"
	default:
		return "UNKNOWN"
	}
}

// Connection manages a single connection to the Fitz server.
// Handles authentication, dispatch loop, and request/response correlation.
type Connection struct {
	transport       transport.Transport
	state           atomic.Int32 // State enum
	stateMu         sync.RWMutex // Protects state transitions
	writeMu         sync.Mutex   // Serializes outbound frames so FIFO request queues match wire order
	requestSem      chan struct{}
	asyncHandlerSem chan struct{}

	// CONNECT configuration (per CLIENT_SPEC.md)
	token string

	// Authentication confirmation
	authConfirmed chan struct{} // Closed when auth succeeds
	authError     error         // Set if auth fails

	// Multiplexer for request/response correlation
	mux *Multiplexer

	// Dispatch loop control
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{} // Closed when dispatch loop exits
	started atomic.Bool   // Set once Start has launched the dispatch loop
	closed  atomic.Bool   // Set once Close has begun shutdown

	// Connection error (set when connection closes)
	connError atomic.Value // stores error

	// Configuration
	cfg Config

	asyncSlotWaitMs          metric.Int64Histogram
	asyncSlotAcquireFailures metric.Int64Counter
	asyncHandlersActive      metric.Int64UpDownCounter
	asyncSlotOccupancyMs     metric.Int64Histogram

	// Observability (optional)
	logger         *slog.Logger
	tracer         trace.Tracer
	meter          metric.Meter
	metricsEnabled bool
	tracingEnabled bool
	// Request observability instruments (REQ-OBS-006)
	requestDuration metric.Int64Histogram
	requestErrors   metric.Int64Counter

	// Subscription tracking for fitz.subscriptions.active gauge
	activeSubscriptions atomic.Int64
}

// Config contains connection configuration.
type Config struct {
	Token                      string
	AuthSettleDelay            time.Duration // CONNECT silent-success settle window (default 500ms)
	ReadTimeout                time.Duration // Default 30s (per-read timeout)
	WriteTimeout               time.Duration // Default 10s
	MaxInFlightRequests        int           // Default 256 concurrently admitted outbound requests
	AsyncHandlerTimeout        time.Duration // Default 30s for detached async handler spans
	AsyncHandlerMaxConcurrency int           // Default 256 concurrent async handlers
	ReconnectEnabled           bool
	ReconnectBackoff           time.Duration

	// Observability (optional)
	Logger *slog.Logger // When nil, no logging.
	Tracer trace.Tracer // When nil, otel.Tracer(module) is used.
	Meter  metric.Meter // When nil, otel.Meter(module) is used.
}

// DefaultConfig returns default configuration.
func DefaultConfig() Config {
	return Config{
		AuthSettleDelay:     500 * time.Millisecond,
		ReadTimeout:         30 * time.Second,
		WriteTimeout:        10 * time.Second,
		MaxInFlightRequests: 256,
	}
}

// Common errors
var (
	ErrNotAuthenticated         = errors.New("not authenticated")
	ErrAuthenticationFailed     = errors.New("authentication failed")
	ErrAuthenticationTimeout    = errors.New("authentication timeout")
	ErrConnectionClosed         = errors.New("connection closed")
	ErrConnectionAlreadyStarted = errors.New("connection already started")
)

// New creates a new connection with the given transport.
func New(trans transport.Transport, cfg Config) *Connection {
	ctx, cancel := context.WithCancel(context.Background())

	// Apply defaults
	if cfg.AuthSettleDelay == 0 {
		cfg.AuthSettleDelay = 500 * time.Millisecond
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 30 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 10 * time.Second
	}
	if cfg.MaxInFlightRequests <= 0 {
		cfg.MaxInFlightRequests = 256
	}
	if cfg.AsyncHandlerTimeout == 0 {
		cfg.AsyncHandlerTimeout = 30 * time.Second
	}
	if cfg.AsyncHandlerMaxConcurrency <= 0 {
		cfg.AsyncHandlerMaxConcurrency = 256
	}

	tracer := cfg.Tracer
	if tracer == nil {
		tracer = traceNoop.NewTracerProvider().Tracer("")
	}
	meter := cfg.Meter
	if meter == nil {
		meter = noop.NewMeterProvider().Meter("")
	}

	conn := &Connection{
		transport:       trans,
		requestSem:      make(chan struct{}, cfg.MaxInFlightRequests),
		asyncHandlerSem: make(chan struct{}, cfg.AsyncHandlerMaxConcurrency),
		token:           cfg.Token,
		authConfirmed:   make(chan struct{}),
		mux:             NewMultiplexer(),
		ctx:             ctx,
		cancel:          cancel,
		done:            make(chan struct{}),
		cfg:             cfg,
		logger:          cfg.Logger,
		tracer:          tracer,
		meter:           meter,
		metricsEnabled:  cfg.Meter != nil,
		tracingEnabled:  cfg.Tracer != nil,
	}
	conn.initAsyncHandlerMetrics()
	return conn
}

// Logger returns the structured logger, or nil if not set.
func (c *Connection) Logger() *slog.Logger {
	return c.logger
}

// Tracer returns the OpenTelemetry tracer (never nil).
func (c *Connection) Tracer() trace.Tracer {
	return c.tracer
}

// Meter returns the OpenTelemetry meter (never nil).
func (c *Connection) Meter() metric.Meter {
	return c.meter
}

func (c *Connection) initAsyncHandlerMetrics() {
	if c == nil || c.meter == nil || !c.metricsEnabled {
		return
	}
	if histogram, err := c.meter.Int64Histogram(
		"fitz.async_handler.slot_wait_ms",
		metric.WithDescription("Wait time to acquire an async handler concurrency slot"),
		metric.WithUnit("ms"),
	); err == nil {
		c.asyncSlotWaitMs = histogram
	}
	if counter, err := c.meter.Int64Counter(
		"fitz.async_handler.slot_acquire_failures",
		metric.WithDescription("Count of async handler slot acquisition failures"),
		metric.WithUnit("{failure}"),
	); err == nil {
		c.asyncSlotAcquireFailures = counter
	}
	if active, err := c.meter.Int64UpDownCounter(
		"fitz.async_handler.active_handlers",
		metric.WithDescription("Current number of active detached async handlers"),
		metric.WithUnit("{handler}"),
	); err == nil {
		c.asyncHandlersActive = active
	}
	if occupancy, err := c.meter.Int64Histogram(
		"fitz.async_handler.slot_occupancy_ms",
		metric.WithDescription("Time an acquired async handler slot is held"),
		metric.WithUnit("ms"),
	); err == nil {
		c.asyncSlotOccupancyMs = occupancy
	}

	if hist, err := c.meter.Int64Histogram(
		"fitz.request.duration",
		metric.WithDescription("Duration of synchronous request-response round-trips"),
		metric.WithUnit("ms"),
	); err == nil {
		c.requestDuration = hist
	}
	if ctr, err := c.meter.Int64Counter(
		"fitz.request.errors",
		metric.WithDescription("Count of synchronous request errors"),
		metric.WithUnit("{error}"),
	); err == nil {
		c.requestErrors = ctr
	}

	if stateGauge, err := c.meter.Int64ObservableGauge(
		"fitz.connection.state",
		metric.WithDescription("Current connection lifecycle state (0=disconnected,1=connecting,2=connected,3=authenticating,4=authenticated,5=closed)"),
	); err == nil {
		_, _ = c.meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
			o.ObserveInt64(stateGauge, int64(c.state.Load()))
			return nil
		}, stateGauge)
	}
	if subGauge, err := c.meter.Int64ObservableGauge(
		"fitz.subscriptions.active",
		metric.WithDescription("Number of active server-side subscriptions across all domains"),
		metric.WithUnit("{subscription}"),
	); err == nil {
		_, _ = c.meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
			o.ObserveInt64(subGauge, c.activeSubscriptions.Load())
			return nil
		}, subGauge)
	}
}

// Start begins the connection lifecycle.
// Starts dispatch loop and performs CONNECT handshake.
// Blocks until authentication is confirmed or fails.
func (c *Connection) Start(ctx context.Context) error {
	if c.closed.Load() {
		return ErrConnectionClosed
	}
	if !c.started.CompareAndSwap(false, true) {
		return ErrConnectionAlreadyStarted
	}

	ctx, span := c.tracer.Start(ctx, "fitz.connection.start")
	defer span.End()

	c.setState(StateAuthenticating)
	if c.logger != nil {
		c.logger.InfoContext(ctx, "connection authenticating")
	}

	// Start dispatch loop
	go c.dispatchLoop()

	// Send CONNECT
	if err := c.sendConnect(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		c.Close()
		return fmt.Errorf("send CONNECT: %w", err)
	}

	// Wait for authentication confirmation
	authSettleDelay := c.cfg.AuthSettleDelay

	select {
	case <-c.authConfirmed:
		// Auth succeeded (first response received or immediate for anonymous)
		if c.logger != nil {
			c.logger.InfoContext(ctx, "connection authenticated")
		}
		return nil

	case <-c.done:
		// Connection closed during auth (likely invalid JWT)
		if c.authError != nil {
			span.RecordError(c.authError)
			span.SetStatus(codes.Error, c.authError.Error())
			return c.authError
		}
		span.SetStatus(codes.Error, ErrAuthenticationFailed.Error())
		return ErrAuthenticationFailed

	case <-ctx.Done():
		// Caller canceled
		span.RecordError(ctx.Err())
		span.SetStatus(codes.Error, ctx.Err().Error())
		c.Close()
		return ctx.Err()

	case <-time.After(authSettleDelay):
		// Valid JWT CONNECT is silently accepted by the broker; if the transport
		// remains open through the auth window, treat the connection as
		// authenticated.
		if c.getConnError() != nil {
			err := c.getConnError()
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		c.confirmAuthentication()
		if c.logger != nil {
			c.logger.InfoContext(ctx, "connection authenticated after silent CONNECT window")
		}
		return nil
	}
}

// sendConnect sends the CONNECT message (MessageType=1).
// Per CLIENT_SPEC.md: [MessageType=1][Length][JWT bytes UTF-8]
// Empty JWT for anonymous mode.
func (c *Connection) sendConnect(ctx context.Context) error {
	ctx, span := c.tracer.Start(ctx, "fitz.connection.send_connect", trace.WithAttributes(attribute.Int("fitz.msg_type", int(protocol.MessageTypeConnect))))
	defer span.End()

	payload := []byte(c.token)
	frame := protocol.EncodeFrameOwned(protocol.MessageTypeConnect, payload)
	if frame == nil {
		err := errors.New("encode CONNECT frame")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	defer frame.Release()

	writeCtx := ctx
	if c.cfg.WriteTimeout > 0 {
		var cancel context.CancelFunc
		writeCtx, cancel = context.WithTimeout(ctx, c.cfg.WriteTimeout)
		defer cancel()
	}

	c.writeMu.Lock()
	err := c.transport.Write(writeCtx, frame.Bytes())
	c.writeMu.Unlock()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// For anonymous mode (empty JWT), confirm immediately
	// Per CLIENT_SPEC.md: Server stays silent on valid JWT
	if c.token == "" {
		c.confirmAuthentication()
	}

	return nil
}

// confirmAuthentication marks authentication as successful.
// Called when first valid response arrives (or immediately for anonymous).
func (c *Connection) confirmAuthentication() {
	select {
	case <-c.authConfirmed:
		// Already confirmed
	default:
		c.setState(StateAuthenticated)
		close(c.authConfirmed)
	}
}

// dispatchLoop reads frames from transport and routes to multiplexer.
// Runs in its own goroutine, started by Start().
func (c *Connection) dispatchLoop() {
	defer close(c.done)
	defer c.mux.Close()

	firstResponse := true

	for {
		// Check if connection is closed
		if c.ctx.Err() != nil {
			return
		}

		// Read next frame from transport (context-aware)
		readCtx := c.ctx
		if c.cfg.ReadTimeout > 0 {
			var cancel context.CancelFunc
			readCtx, cancel = context.WithTimeout(c.ctx, c.cfg.ReadTimeout)
			frame, err := c.transport.Read(readCtx)
			cancel()
			if err != nil {
				c.handleReadError(err)
				return
			}

			// Decode frame (MessageType + payload)
			msgType, payload, err := protocol.DecodeFrame(frame)
			if err != nil {
				if c.logger != nil {
					c.logger.Error("decode frame failed", "error", err)
				}
				c.setConnError(fmt.Errorf("decode frame: %w", err))
				return
			}
			if c.logger != nil {
				c.logger.Debug("frame received", "msg_type", msgType)
			}

			// First valid response confirms authentication
			if firstResponse {
				c.confirmAuthentication()
				firstResponse = false
			}

			// Route to multiplexer (non-blocking dispatch)
			c.mux.Dispatch(msgType, payload)
			continue
		}

		frame, err := c.transport.Read(readCtx)
		if err != nil {
			c.handleReadError(err)
			return
		}

		// Decode frame (MessageType + payload)
		msgType, payload, err := protocol.DecodeFrame(frame)
		if err != nil {
			if c.logger != nil {
				c.logger.Error("decode frame failed", "error", err)
			}
			c.setConnError(fmt.Errorf("decode frame: %w", err))
			return
		}
		if c.logger != nil {
			c.logger.Debug("frame received", "msg_type", msgType)
		}

		// First valid response confirms authentication
		if firstResponse {
			c.confirmAuthentication()
			firstResponse = false
		}

		// Route to multiplexer (non-blocking dispatch)
		c.mux.Dispatch(msgType, payload)
	}
}

// handleReadError processes transport read errors.
func (c *Connection) handleReadError(err error) {
	if errors.Is(err, context.Canceled) {
		// Clean shutdown
		c.setConnError(nil)
		return
	}
	if c.logger != nil {
		c.logger.Warn("read error", "error", err)
	}

	// Connection closed by server (might be auth failure)
	if !c.isAuthenticated() {
		c.authError = ErrAuthenticationFailed
	}

	c.setConnError(err)
}

// SendRequest sends a synchronous request and waits for response.
// Used by domain client implementations.
func (c *Connection) SendRequest(ctx context.Context, msgType uint16, payload []byte) (_ []byte, retErr error) {
	var span trace.Span = traceNoop.Span{}
	if c.tracingEnabled {
		ctx, span = c.tracer.Start(ctx, "fitz.connection.send_request", trace.WithAttributes(attribute.Int("fitz.msg_type", int(msgType))))
	}
	defer span.End()
	if c.metricsEnabled {
		metricCtx := c.LifecycleContext()

		reqStart := time.Now()
		defer func() {
			attrs := metric.WithAttributes(attribute.Int("fitz.msg_type", int(msgType)))
			if c.requestDuration != nil {
				c.requestDuration.Record(metricCtx, time.Since(reqStart).Milliseconds(), attrs)
			}
			if retErr != nil && c.requestErrors != nil {
				errorAttrs := []attribute.KeyValue{attribute.Int("fitz.msg_type", int(msgType))}
				var de *coreerrors.DomainError
				if errors.As(retErr, &de) {
					errorAttrs = append(errorAttrs, attribute.Int64("fitz.error_code", int64(de.Code)))
				} else {
					errorAttrs = append(errorAttrs, attribute.Int64("fitz.error_code", 0))
				}
				c.requestErrors.Add(metricCtx, 1, metric.WithAttributes(errorAttrs...))
			}
		}()
	}

	if !c.isAuthenticated() {
		span.SetStatus(codes.Error, ErrNotAuthenticated.Error())
		return nil, ErrNotAuthenticated
	}

	ok := c.AcquireRequestSlot(ctx)
	if !ok {
		err := c.outboundAdmissionError(ctx)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	defer c.ReleaseRequestSlot()

	frame := protocol.EncodeFrameOwned(msgType, payload)
	if frame == nil {
		err := errors.New("encode frame")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	defer frame.Release()

	responseChan := make(chan []byte, 1)
	if c.logger != nil {
		c.logger.DebugContext(ctx, "request sent", "msg_type", msgType)
	}

	writeCtx := ctx
	if c.cfg.WriteTimeout > 0 {
		var cancel context.CancelFunc
		writeCtx, cancel = context.WithTimeout(ctx, c.cfg.WriteTimeout)
		defer cancel()
	}

	c.writeMu.Lock()
	c.mux.RegisterRequest(msgType, responseChan, nil)
	defer c.mux.UnregisterRequest(msgType, responseChan)
	err := c.transport.Write(writeCtx, frame.Bytes())
	c.writeMu.Unlock()
	if err != nil {
		wrapped := fmt.Errorf("write request: %w", err)
		span.RecordError(wrapped)
		span.SetStatus(codes.Error, wrapped.Error())
		return nil, wrapped
	}

	select {
	case resp, ok := <-responseChan:
		if !ok {
			if err := c.getConnError(); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}
			span.SetStatus(codes.Error, ErrConnectionClosed.Error())
			return nil, ErrConnectionClosed
		}
		return resp, nil
	case <-ctx.Done():
		span.RecordError(ctx.Err())
		span.SetStatus(codes.Error, ctx.Err().Error())
		return nil, ctx.Err()
	case <-c.done:
		select {
		case resp, ok := <-responseChan:
			if ok {
				return resp, nil
			}
		default:
		}
		if err := c.getConnError(); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		span.SetStatus(codes.Error, ErrConnectionClosed.Error())
		return nil, ErrConnectionClosed
	}
}

// SendRequestWithWriter sends a request using a payload writer to avoid allocations.
func (c *Connection) SendRequestWithWriter(ctx context.Context, msgType uint16, writePayload func(*bytes.Buffer)) (_ []byte, retErr error) {
	var span trace.Span = traceNoop.Span{}
	if c.tracingEnabled {
		ctx, span = c.tracer.Start(ctx, "fitz.connection.send_request_with_writer", trace.WithAttributes(attribute.Int("fitz.msg_type", int(msgType))))
	}
	defer span.End()
	if c.metricsEnabled {
		metricCtx := c.LifecycleContext()

		reqStart := time.Now()
		defer func() {
			attrs := metric.WithAttributes(attribute.Int("fitz.msg_type", int(msgType)))
			if c.requestDuration != nil {
				c.requestDuration.Record(metricCtx, time.Since(reqStart).Milliseconds(), attrs)
			}
			if retErr != nil && c.requestErrors != nil {
				errorAttrs := []attribute.KeyValue{attribute.Int("fitz.msg_type", int(msgType))}
				var de *coreerrors.DomainError
				if errors.As(retErr, &de) {
					errorAttrs = append(errorAttrs, attribute.Int64("fitz.error_code", int64(de.Code)))
				} else {
					errorAttrs = append(errorAttrs, attribute.Int64("fitz.error_code", 0))
				}
				c.requestErrors.Add(metricCtx, 1, metric.WithAttributes(errorAttrs...))
			}
		}()
	}

	if !c.isAuthenticated() {
		span.SetStatus(codes.Error, ErrNotAuthenticated.Error())
		return nil, ErrNotAuthenticated
	}

	ok := c.AcquireRequestSlot(ctx)
	if !ok {
		err := c.outboundAdmissionError(ctx)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	defer c.ReleaseRequestSlot()

	frame, err := protocol.EncodeFrameWithPayloadWriter(msgType, writePayload)
	if err != nil {
		wrapped := fmt.Errorf("encode frame: %w", err)
		span.RecordError(wrapped)
		span.SetStatus(codes.Error, wrapped.Error())
		return nil, wrapped
	}
	defer frame.Release()

	responseChan := make(chan []byte, 1)
	if c.logger != nil {
		c.logger.DebugContext(ctx, "request sent", "msg_type", msgType)
	}

	writeCtx := ctx
	if c.cfg.WriteTimeout > 0 {
		var cancel context.CancelFunc
		writeCtx, cancel = context.WithTimeout(ctx, c.cfg.WriteTimeout)
		defer cancel()
	}

	c.writeMu.Lock()
	c.mux.RegisterRequest(msgType, responseChan, nil)
	defer c.mux.UnregisterRequest(msgType, responseChan)
	err = c.transport.Write(writeCtx, frame.Bytes())
	c.writeMu.Unlock()
	if err != nil {
		wrapped := fmt.Errorf("write request: %w", err)
		span.RecordError(wrapped)
		span.SetStatus(codes.Error, wrapped.Error())
		return nil, wrapped
	}

	select {
	case resp, ok := <-responseChan:
		if !ok {
			if err := c.getConnError(); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}
			span.SetStatus(codes.Error, ErrConnectionClosed.Error())
			return nil, ErrConnectionClosed
		}
		return resp, nil
	case <-ctx.Done():
		span.RecordError(ctx.Err())
		span.SetStatus(codes.Error, ctx.Err().Error())
		return nil, ctx.Err()
	case <-c.done:
		select {
		case resp, ok := <-responseChan:
			if ok {
				return resp, nil
			}
		default:
		}
		if err := c.getConnError(); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		span.SetStatus(codes.Error, ErrConnectionClosed.Error())
		return nil, ErrConnectionClosed
	}
}

// SendFireAndForget sends a frame without expecting a response.
// Used for operations like Notice PUBLISH or RPC response where the server does not reply.
func (c *Connection) SendFireAndForget(ctx context.Context, msgType uint16, payload []byte) error {
	var span trace.Span = traceNoop.Span{}
	if c.tracingEnabled {
		ctx, span = c.tracer.Start(ctx, "fitz.connection.send_fire_and_forget", trace.WithAttributes(attribute.Int("fitz.msg_type", int(msgType))))
	}
	defer span.End()

	if !c.isAuthenticated() {
		span.SetStatus(codes.Error, ErrNotAuthenticated.Error())
		return ErrNotAuthenticated
	}

	ok := c.AcquireRequestSlot(ctx)
	if !ok {
		err := c.outboundAdmissionError(ctx)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	defer c.ReleaseRequestSlot()

	frame := protocol.EncodeFrameOwned(msgType, payload)
	if frame == nil {
		err := errors.New("encode frame")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	defer frame.Release()

	writeCtx := ctx
	if c.cfg.WriteTimeout > 0 {
		var cancel context.CancelFunc
		writeCtx, cancel = context.WithTimeout(ctx, c.cfg.WriteTimeout)
		defer cancel()
	}

	c.writeMu.Lock()
	err := c.transport.Write(writeCtx, frame.Bytes())
	c.writeMu.Unlock()
	if err != nil {
		wrapped := fmt.Errorf("write fire-and-forget: %w", err)
		span.RecordError(wrapped)
		span.SetStatus(codes.Error, wrapped.Error())
		return wrapped
	}

	return nil
}

// SendFireAndForgetWithWriter sends a fire-and-forget frame using a payload writer.
func (c *Connection) SendFireAndForgetWithWriter(ctx context.Context, msgType uint16, writePayload func(*bytes.Buffer)) error {
	var span trace.Span = traceNoop.Span{}
	if c.tracingEnabled {
		ctx, span = c.tracer.Start(ctx, "fitz.connection.send_fire_and_forget_with_writer", trace.WithAttributes(attribute.Int("fitz.msg_type", int(msgType))))
	}
	defer span.End()

	if !c.isAuthenticated() {
		span.SetStatus(codes.Error, ErrNotAuthenticated.Error())
		return ErrNotAuthenticated
	}

	ok := c.AcquireRequestSlot(ctx)
	if !ok {
		err := c.outboundAdmissionError(ctx)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	defer c.ReleaseRequestSlot()

	frame, err := protocol.EncodeFrameWithPayloadWriter(msgType, writePayload)
	if err != nil {
		wrapped := fmt.Errorf("encode frame: %w", err)
		span.RecordError(wrapped)
		span.SetStatus(codes.Error, wrapped.Error())
		return wrapped
	}
	defer frame.Release()

	writeCtx := ctx
	if c.cfg.WriteTimeout > 0 {
		var cancel context.CancelFunc
		writeCtx, cancel = context.WithTimeout(ctx, c.cfg.WriteTimeout)
		defer cancel()
	}

	c.writeMu.Lock()
	err = c.transport.Write(writeCtx, frame.Bytes())
	c.writeMu.Unlock()
	if err != nil {
		wrapped := fmt.Errorf("write fire-and-forget: %w", err)
		span.RecordError(wrapped)
		span.SetStatus(codes.Error, wrapped.Error())
		return wrapped
	}

	return nil
}

func (c *Connection) outboundAdmissionError(ctx context.Context) error {
	if err := c.getConnError(); err != nil {
		return err
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return ErrConnectionClosed
}

// Close cleanly shuts down the connection.
func (c *Connection) Close() error {
	if c == nil {
		return nil
	}
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}

	// Cancel context (signals dispatch loop to stop)
	c.cancel()

	// Close transport FIRST to unblock any pending Read() calls.
	// Without this, the dispatch loop would block forever on transport.Read()
	// because the TCP read is a blocking I/O call that ignores context cancellation.
	err := c.transport.Close()

	// Wait for dispatch loop to exit if it has been started.
	if c.started.Load() {
		<-c.done
	}

	c.setState(StateClosed)
	if c.logger != nil {
		c.logger.Info("connection closed")
	}

	return err
}

// RegisterNotifyHandler registers handler for NOTIFY messages for the given message type.
// msgType should be protocol.MessageTypeQueueNotify (209), MessageTypeLeaseNotify (409),
// MessageTypeNoticeNotify (504), or MessageTypeStreamNotify (609).
func (c *Connection) RegisterNotifyHandler(msgType uint16, handler func(subID uint64, route string, payload []byte)) {
	c.mux.SetNotifyHandler(msgType, handler)
}

// RegisterScheduleNotifyHandler registers handler for Schedule NOTIFY messages (705).
func (c *Connection) RegisterScheduleNotifyHandler(handler func(subID uint64, payload []byte)) {
	c.mux.SetScheduleNotifyHandler(handler)
}

// RegisterRPCRequestHandler registers handler for RPC REQUEST messages (302).
func (c *Connection) RegisterRPCRequestHandler(handler func(payload []byte)) {
	c.mux.SetRPCRequestHandler(handler)
}

// RegisterRPCResponseHandler registers handler for RPC RESPONSE messages.
func (c *Connection) RegisterRPCResponseHandler(handler func(correlationID [16]byte, payload []byte)) {
	c.mux.SetRPCResponseHandler(handler)
}

// State management helpers

func (c *Connection) setState(state State) {
	c.state.Store(int32(state))
}

// State returns the current lifecycle state.
func (c *Connection) State() State {
	return State(c.state.Load())
}

func (c *Connection) isAuthenticated() bool {
	select {
	case <-c.authConfirmed:
		return true
	default:
		return false
	}
}

func (c *Connection) setConnError(err error) {
	if err != nil {
		c.connError.Store(err)
	}
}

func (c *Connection) getConnError() error {
	if val := c.connError.Load(); val != nil {
		return val.(error)
	}
	return nil
}

// Metrics returns multiplexer metrics.
func (c *Connection) Metrics() MultiplexerMetrics {
	return c.mux.Metrics()
}

// Done closes when the dispatch loop exits.
func (c *Connection) Done() <-chan struct{} {
	return c.done
}

// LifecycleContext returns the connection lifecycle context.
// It is canceled when the connection begins shutdown.
func (c *Connection) LifecycleContext() context.Context {
	if c == nil || c.ctx == nil {
		return context.Background()
	}
	return c.ctx
}

// AsyncHandlerTimeout returns the configured timeout for detached async handler spans.
func (c *Connection) AsyncHandlerTimeout() time.Duration {
	if c == nil {
		return 30 * time.Second
	}
	if c.cfg.AsyncHandlerTimeout <= 0 {
		return 30 * time.Second
	}
	return c.cfg.AsyncHandlerTimeout
}

// AsyncHandlerMaxConcurrency returns the configured maximum number of
// concurrently executing detached async handlers.
func (c *Connection) AsyncHandlerMaxConcurrency() int {
	if c == nil {
		return 256
	}
	if c.cfg.AsyncHandlerMaxConcurrency <= 0 {
		return 256
	}
	return c.cfg.AsyncHandlerMaxConcurrency
}

// MaxInFlightRequests returns the configured maximum number of concurrently
// admitted outbound request operations.
func (c *Connection) MaxInFlightRequests() int {
	if c == nil {
		return 256
	}
	if c.cfg.MaxInFlightRequests <= 0 {
		return 256
	}
	return c.cfg.MaxInFlightRequests
}

// AcquireRequestSlot acquires a concurrency slot for outbound request admission.
// It returns false if acquisition was canceled by context shutdown/deadline.
func (c *Connection) AcquireRequestSlot(ctx context.Context) bool {
	if c == nil || c.requestSem == nil {
		return true
	}
	if ctx == nil {
		ctx = c.LifecycleContext()
	}
	select {
	case c.requestSem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	case <-c.done:
		return false
	}
}

// ReleaseRequestSlot releases a previously acquired outbound request slot.
func (c *Connection) ReleaseRequestSlot() {
	if c == nil || c.requestSem == nil {
		return
	}
	<-c.requestSem
}

// AcquireAsyncHandlerSlot acquires a concurrency slot for async handler execution.
// It returns false if acquisition was canceled by context shutdown/deadline.
func (c *Connection) AcquireAsyncHandlerSlot(ctx context.Context) (release func(), ok bool) {
	noop := func() {}
	if c == nil || c.asyncHandlerSem == nil {
		return noop, true
	}
	if ctx == nil {
		ctx = c.LifecycleContext()
	}
	metricCtx := c.LifecycleContext()

	start := time.Now()
	recordWait := func(recordCtx context.Context) {
		if c.asyncSlotWaitMs != nil {
			c.asyncSlotWaitMs.Record(recordCtx, time.Since(start).Milliseconds())
		}
	}
	recordFailure := func(recordCtx context.Context, reason string) {
		if c.asyncSlotAcquireFailures != nil {
			c.asyncSlotAcquireFailures.Add(recordCtx, 1, metric.WithAttributes(attribute.String("fitz.reason", reason)))
		}
	}

	select {
	case c.asyncHandlerSem <- struct{}{}:
		recordWait(ctx)
		if c.asyncHandlersActive != nil {
			c.asyncHandlersActive.Add(metricCtx, 1)
		}
		acquiredAt := time.Now()
		return func() {
			<-c.asyncHandlerSem
			if c.asyncHandlersActive != nil {
				c.asyncHandlersActive.Add(metricCtx, -1)
			}
			if c.asyncSlotOccupancyMs != nil {
				c.asyncSlotOccupancyMs.Record(metricCtx, time.Since(acquiredAt).Milliseconds())
			}
		}, true
	case <-ctx.Done():
		recordWait(ctx)
		recordFailure(ctx, "context_done")
		return noop, false
	case <-c.ctx.Done():
		recordWait(c.ctx)
		recordFailure(c.ctx, "connection_shutdown")
		return noop, false
	}
}

// Err returns the terminal connection error, if any.
func (c *Connection) Err() error {
	return c.getConnError()
}

// AddSubscriptions adjusts the active subscription count by delta.
// Call with delta=+1 when a server-side subscription is established,
// and delta=-1 when it is removed. Used to populate the fitz.subscriptions.active metric.
func (c *Connection) AddSubscriptions(delta int64) {
	c.activeSubscriptions.Add(delta)
}
