package connection

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	coreerrors "github.com/cntryl/fitz-go/internal/core/errors"
	"github.com/cntryl/fitz-go/internal/core/retry"
	coretracing "github.com/cntryl/fitz-go/internal/core/tracing"
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
	transport        transport.Transport
	state            atomic.Int32 // State enum
	writeMu          sync.Mutex   // Serializes outbound frames so FIFO request queues match wire order
	requestSem       chan struct{}
	oneWaySem        chan struct{}
	queuedRequests   atomic.Int64
	queuedWrites     atomic.Int64
	asyncHandlerSem  chan struct{}
	asyncHandlerJobs chan asyncHandlerJob
	asyncHandlerMu   sync.Mutex
	asyncClosing     atomic.Bool
	asyncWorkersOnce sync.Once
	heartbeatOnce    sync.Once
	lastActivityUnix atomic.Int64

	// CONNECT configuration (per CLIENT_SPEC.md)
	token string

	// Authentication confirmation
	authConfirmed   chan struct{} // Closed when auth succeeds
	authConfirmOnce sync.Once
	authError       error // Set if auth fails
	serverHello     chan struct{}
	serverHelloOnce sync.Once
	maxMessageBytes atomic.Uint32
	maxInFlight     atomic.Uint32
	sessionID       atomic.Uint64
	onGoAway        func(code uint16, grace time.Duration, reason string)

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
	asyncHandlersSaturated   metric.Int64Counter

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
	ServiceName                string
	ClientBuild                string
	OnGoAway                   func(code uint16, grace time.Duration, reason string)
	MaxMessageBytes            int
	AuthSettleDelay            time.Duration // Optional CONNECT rejection-observation window.
	ReadTimeout                time.Duration // Default 30s (per-read timeout)
	WriteTimeout               time.Duration // Default 10s
	MaxInFlightRequests        int           // Default 256 concurrently admitted outbound requests
	MaxRequestQueueSize        int           // Default 1024 waiters beyond MaxInFlightRequests
	AsyncHandlerTimeout        time.Duration // Default 30s for detached async handler spans
	AsyncHandlerMaxConcurrency int           // Default 256 concurrent async handlers
	AsyncHandlerQueueCapacity  int           // Default 1024 queued async handlers
	ReconnectEnabled           bool
	ReconnectBackoff           time.Duration
	RetryEnabled               bool
	RetryConfigured            bool
	RetryMaxAttempts           int
	RetryBackoff               time.Duration
	RetryMaxBackoff            time.Duration
	HeartbeatEnabled           bool
	HeartbeatConfigured        bool
	HeartbeatInterval          time.Duration
	HeartbeatTimeout           time.Duration

	// Observability (optional)
	Logger *slog.Logger // When nil, no logging.
	Tracer trace.Tracer // When nil, otel.Tracer(module) is used.
	Meter  metric.Meter // When nil, otel.Meter(module) is used.
}

func (c *Connection) CorrelationEnabled() bool { return c.mux.CorrelationEnabled() }

func (c *Connection) ServerCapabilities() (uint16, uint32) { return c.mux.Capabilities() }

func (c *Connection) ServerLimits() (uint32, uint32) {
	return c.maxMessageBytes.Load(), c.maxInFlight.Load()
}

// DefaultConfig returns default configuration.
func DefaultConfig() Config {
	return Config{
		AuthSettleDelay:           500 * time.Millisecond,
		ReadTimeout:               30 * time.Second,
		WriteTimeout:              10 * time.Second,
		MaxInFlightRequests:       256,
		MaxRequestQueueSize:       1024,
		AsyncHandlerQueueCapacity: 1024,
		RetryEnabled:              true,
		RetryConfigured:           true,
		RetryMaxAttempts:          3,
		RetryBackoff:              100 * time.Millisecond,
		RetryMaxBackoff:           time.Second,
		HeartbeatEnabled:          true,
		HeartbeatConfigured:       true,
		HeartbeatInterval:         10 * time.Second,
		HeartbeatTimeout:          30 * time.Second,
		ClientBuild:               "fitz-go/0.3.0",
	}
}

// Common errors
var (
	ErrNotAuthenticated         = errors.New("not authenticated")
	ErrAuthenticationFailed     = errors.New("authentication failed")
	ErrVersionMismatch          = errors.New("wire version mismatch")
	ErrAuthenticationTimeout    = errors.New("authentication timeout")
	ErrConnectionClosed         = errors.New("connection closed")
	ErrConnectionAlreadyStarted = errors.New("connection already started")
	ErrRequestQueueFull         = errors.New("request queue full")
	ErrStaleHandle              = errors.New("stale handle")
)

// New creates a new connection with the given transport.
func New(trans transport.Transport, cfg Config) *Connection {
	ctx, cancel := context.WithCancel(context.Background())

	// Apply defaults
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 30 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 10 * time.Second
	}
	if cfg.MaxInFlightRequests <= 0 {
		cfg.MaxInFlightRequests = 256
	}
	if cfg.MaxRequestQueueSize < 0 {
		cfg.MaxRequestQueueSize = 0
	}
	if cfg.MaxRequestQueueSize == 0 {
		cfg.MaxRequestQueueSize = 1024
	}
	if cfg.AsyncHandlerTimeout == 0 {
		cfg.AsyncHandlerTimeout = 30 * time.Second
	}
	if cfg.AsyncHandlerMaxConcurrency <= 0 {
		cfg.AsyncHandlerMaxConcurrency = 256
	}
	if cfg.AsyncHandlerQueueCapacity <= 0 {
		cfg.AsyncHandlerQueueCapacity = 1024
	}
	if !cfg.RetryConfigured {
		cfg.RetryEnabled = true
	}
	if cfg.RetryMaxAttempts <= 0 {
		cfg.RetryMaxAttempts = 3
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = 100 * time.Millisecond
	}
	if cfg.RetryMaxBackoff <= 0 {
		cfg.RetryMaxBackoff = time.Second
	}
	if !cfg.HeartbeatConfigured {
		cfg.HeartbeatEnabled = true
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 10 * time.Second
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = 30 * time.Second
	}
	if cfg.ClientBuild == "" {
		cfg.ClientBuild = "fitz-go/0.3.0"
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
		transport:        trans,
		requestSem:       make(chan struct{}, cfg.MaxInFlightRequests),
		oneWaySem:        make(chan struct{}, cfg.MaxInFlightRequests),
		asyncHandlerSem:  make(chan struct{}, cfg.AsyncHandlerMaxConcurrency),
		asyncHandlerJobs: make(chan asyncHandlerJob, cfg.AsyncHandlerQueueCapacity),
		token:            cfg.Token,
		authConfirmed:    make(chan struct{}),
		serverHello:      make(chan struct{}),
		mux:              NewMultiplexer(),
		ctx:              ctx,
		cancel:           cancel,
		done:             make(chan struct{}),
		cfg:              cfg,
		onGoAway:         cfg.OnGoAway,
		logger:           cfg.Logger,
		tracer:           tracer,
		meter:            meter,
		metricsEnabled:   cfg.Meter != nil,
		tracingEnabled:   cfg.Tracer != nil,
	}
	conn.maxMessageBytes.Store(uint32(protocol.MaxMessageSize))
	conn.mux.setLogger(cfg.Logger)
	conn.recordActivity()
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
	if saturated, err := c.meter.Int64Counter(
		"fitz.async_handlers.saturated",
		metric.WithDescription("Count of callback jobs rejected because the async handler queue is full"),
		metric.WithUnit("{failure}"),
	); err == nil {
		c.asyncHandlersSaturated = saturated
	}
	if active, err := c.meter.Int64ObservableGauge(
		"fitz.async_handlers.active",
		metric.WithDescription("Current number of executing async handlers"),
		metric.WithUnit("{handler}"),
	); err == nil {
		_, _ = c.meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
			observer.ObserveInt64(active, int64(len(c.asyncHandlerSem)))
			return nil
		}, active)
	}
	if queued, err := c.meter.Int64ObservableGauge(
		"fitz.async_handlers.queued",
		metric.WithDescription("Current number of callback jobs waiting in the async handler queue"),
		metric.WithUnit("{handler}"),
	); err == nil {
		_, _ = c.meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
			observer.ObserveInt64(queued, int64(len(c.asyncHandlerJobs)))
			return nil
		}, queued)
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
		if closeErr := c.Close(); closeErr != nil && c.logger != nil {
			c.logger.WarnContext(ctx, "close after CONNECT failure", "error", closeErr)
		}
		return fmt.Errorf("send CONNECT: %w", err)
	}

	// HELLO is the only successful wire-2 handshake response. A silent socket
	// is not an authenticated or negotiated session.
	negotiationTimeout := c.cfg.ReadTimeout
	if negotiationTimeout <= 0 {
		negotiationTimeout = 30 * time.Second
	}

	select {
	case <-c.serverHello:
		if c.closed.Load() || c.ctx.Err() != nil {
			return ErrConnectionClosed
		}
		c.confirmAuthentication()
		if c.logger != nil {
			c.logger.InfoContext(ctx, "wire-2 connection negotiated")
		}
		c.startHeartbeat()
		return nil

	case <-c.ctx.Done():
		// Session canceled during authentication, including explicit Close.
		if c.authError != nil {
			span.RecordError(c.authError)
			span.SetStatus(codes.Error, c.authError.Error())
			return c.authError
		}
		if c.closed.Load() {
			span.SetStatus(codes.Error, ErrConnectionClosed.Error())
			return ErrConnectionClosed
		}
		span.SetStatus(codes.Error, ErrAuthenticationFailed.Error())
		return ErrAuthenticationFailed

	case <-ctx.Done():
		// Caller canceled
		span.RecordError(ctx.Err())
		span.SetStatus(codes.Error, ctx.Err().Error())
		if closeErr := c.Close(); closeErr != nil && c.logger != nil {
			c.logger.WarnContext(ctx, "close after start cancellation", "error", closeErr)
		}
		return ctx.Err()

	case <-time.After(negotiationTimeout):
		span.RecordError(ErrAuthenticationTimeout)
		span.SetStatus(codes.Error, ErrAuthenticationTimeout.Error())
		_ = c.Close()
		return ErrAuthenticationTimeout
	}
}

// sendConnect sends the wire-2 version and client identity fields.
func (c *Connection) sendConnect(ctx context.Context) error {
	ctx, span := c.tracer.Start(ctx, "fitz.connection.send_connect", trace.WithAttributes(attribute.Int("fitz.msg_type", int(protocol.MessageTypeConnect))))
	defer span.End()

	var payload bytes.Buffer
	var versions [8]byte
	binary.BigEndian.PutUint16(versions[:2], 2)
	binary.BigEndian.PutUint16(versions[2:4], 2)
	// The current client has no optional wire capabilities; features that were
	// formerly gated by capability bits are part of the wire-2 base layouts.
	payload.Write(versions[:])
	writeConnectString := func(value string) {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(value)))
		payload.Write(length[:])
		payload.WriteString(value)
	}
	writeConnectString(c.token)
	writeConnectString(strings.TrimSpace(c.cfg.ServiceName))
	writeConnectString(c.cfg.ClientBuild)
	frame := protocol.EncodeFrameOwned(protocol.MessageTypeConnect, payload.Bytes())
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
	c.recordActivity()

	// Both anonymous and authenticated connections use the bounded silent
	// CONNECT window. HELLO supplies the Wire-2 limits before callers register
	// workers; a missing HELLO is not an authenticated negotiated session.

	return nil
}

// confirmAuthentication marks authentication as successful.
// Called when first valid response arrives (or immediately for anonymous).
func (c *Connection) confirmAuthentication() {
	c.authConfirmOnce.Do(func() {
		c.setState(StateAuthenticated)
		close(c.authConfirmed)
	})
}

// dispatchLoop reads frames from transport and routes to multiplexer.
// Runs in its own goroutine, started by Start().
func (c *Connection) dispatchLoop() {
	defer func() {
		c.setState(StateClosed)
		if err := c.mux.Close(); err != nil && c.logger != nil {
			c.logger.Warn("close multiplexer", "error", err)
		}
		c.beginAsyncHandlerShutdown()
		c.cancel()
		close(c.done)
	}()

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
				if errors.Is(err, context.DeadlineExceeded) && c.ctx.Err() == nil {
					continue
				}
				c.handleReadError(err)
				return
			}
			c.recordActivity()

			hasResponse, err := c.dispatchTransportFrame(frame)
			if err != nil {
				if c.logger != nil {
					c.logger.Error("decode frame failed", "error", err)
				}
				c.setConnError(fmt.Errorf("decode frame: %w", err))
				return
			}

			// First valid response confirms authentication
			if firstResponse && hasResponse {
				c.confirmAuthentication()
				firstResponse = false
			}

			continue
		}

		frame, err := c.transport.Read(readCtx)
		if err != nil {
			c.handleReadError(err)
			return
		}
		c.recordActivity()

		hasResponse, err := c.dispatchTransportFrame(frame)
		if err != nil {
			if c.logger != nil {
				c.logger.Error("decode frame failed", "error", err)
			}
			c.setConnError(fmt.Errorf("decode frame: %w", err))
			return
		}
		// First valid response confirms authentication
		if firstResponse && hasResponse {
			c.confirmAuthentication()
			firstResponse = false
		}
	}
}

func (c *Connection) dispatchTransportFrame(data []byte) (bool, error) {
	frame, err := protocol.DecodeMessage(data)
	if err != nil {
		return false, err
	}
	if c.logger != nil {
		c.logger.Debug("frame received", "msg_type", frame.MessageType, "correlation", frame.CorrelationID)
	}
	switch frame.MessageType {
	case protocol.MessageTypeHello:
		if frame.CorrelationID != 0 || len(frame.Payload) < 22 {
			return false, errors.New("malformed HELLO")
		}
		version := binary.BigEndian.Uint16(frame.Payload[:2])
		if version != 2 {
			return false, fmt.Errorf("unsupported negotiated wire version %d", version)
		}
		capabilities := binary.BigEndian.Uint32(frame.Payload[2:6])
		maxMessageBytes := binary.BigEndian.Uint32(frame.Payload[6:10])
		maxInFlight := binary.BigEndian.Uint32(frame.Payload[10:14])
		sessionID := binary.BigEndian.Uint64(frame.Payload[14:22])
		if maxMessageBytes < uint32(protocol.HeaderLen) || maxInFlight == 0 {
			return false, errors.New("HELLO contains invalid negotiated limits")
		}
		localLimit := uint32(protocol.MaxMessageSize)
		localLimit = min(localLimit, maxMessageBytes)
		if c.cfg.MaxMessageBytes > 0 && uint32(c.cfg.MaxMessageBytes) < localLimit {
			localLimit = uint32(c.cfg.MaxMessageBytes)
		}
		c.maxMessageBytes.Store(localLimit)
		c.maxInFlight.Store(maxInFlight)
		c.sessionID.Store(sessionID)
		if uint64(maxInFlight) < uint64(cap(c.requestSem)) {
			limit := int(maxInFlight)
			c.requestSem = make(chan struct{}, limit)
			c.oneWaySem = make(chan struct{}, limit)
		}
		c.mux.SetCapabilities(version, capabilities)
		if sizer, ok := c.transport.(interface{ SetMaxFrameSize(size int) }); ok {
			sizer.SetMaxFrameSize(int(localLimit))
		}
		c.serverHelloOnce.Do(func() { close(c.serverHello) })
		return true, nil
	case protocol.MessageTypeConnectRejected:
		if frame.CorrelationID != 0 {
			return false, errors.New("CONNECT_REJECTED has a correlation")
		}
		code, offset, readErr := ReadU16BE(frame.Payload, 0)
		if readErr != nil {
			return false, fmt.Errorf("decode CONNECT_REJECTED: %w", readErr)
		}
		message, end, readErr := ReadString(frame.Payload, offset)
		if readErr != nil || end != len(frame.Payload) {
			return false, errors.New("malformed CONNECT_REJECTED")
		}
		rejection := fmt.Errorf("connection rejected with code %d: %s", code, message)
		if code == 50 {
			rejection = fmt.Errorf("%w (code %d): %s", ErrVersionMismatch, code, message)
		}
		c.authError = rejection
		return false, rejection
	case protocol.MessageTypeGoAway:
		if frame.CorrelationID != 0 {
			return false, errors.New("GOAWAY has a correlation")
		}
		code, offset, readErr := ReadU16BE(frame.Payload, 0)
		if readErr != nil {
			return false, fmt.Errorf("decode GOAWAY: %w", readErr)
		}
		graceMS, offset, readErr := ReadU32BE(frame.Payload, offset)
		if readErr != nil {
			return false, fmt.Errorf("decode GOAWAY: %w", readErr)
		}
		reason, end, readErr := ReadString(frame.Payload, offset)
		if readErr != nil || end != len(frame.Payload) {
			return false, errors.New("malformed GOAWAY")
		}
		if c.onGoAway != nil {
			c.onGoAway(code, time.Duration(graceMS)*time.Millisecond, reason)
		}
		return false, nil
	}
	if frame.CorrelationID != 0 {
		final := frame.Flags&protocol.FlagResponseFinal != 0
		if !c.mux.DispatchCorrelated(frame.CorrelationID, frame.MessageType, frame.Payload, final) {
			return false, fmt.Errorf("unexpected response correlation %d", frame.CorrelationID)
		}
	} else {
		c.mux.Dispatch(frame.MessageType, frame.Payload)
	}
	return true, nil
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
	return c.SendRequestWithFlags(ctx, msgType, 0, payload)
}

// SendRequestWithFlags sends a request with negotiated wire header flags.
func (c *Connection) SendRequestWithFlags(ctx context.Context, msgType uint16, flags uint8, payload []byte) (_ []byte, retErr error) {
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

	if err := c.AcquireRequestSlotErr(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	defer c.ReleaseRequestSlot()

	correlationID := c.mux.NextCorrelationID()
	if correlationID == 0 {
		return nil, errors.New("correlation IDs exhausted; reconnect before sending another request")
	}
	frame := protocol.EncodeFrameOwnedWithHeader(msgType, flags, correlationID, payload)
	if frame == nil {
		err := errors.New("encode frame")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	defer frame.Release()
	if uint32(frame.Len()) > c.maxMessageBytes.Load() {
		return nil, fmt.Errorf("message exceeds negotiated max_message_bytes (%d)", c.maxMessageBytes.Load())
	}

	waiter := acquireRequestWaiter()
	releaseWaiter := false
	defer func() {
		if releaseWaiter {
			releaseRequestWaiter(waiter)
		}
	}()
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
	if !c.mux.RegisterCorrelatedRequest(correlationID, waiter) {
		c.writeMu.Unlock()
		return nil, errors.New("register correlated request")
	}
	defer func() {
		removed := c.mux.UnregisterCorrelatedRequest(correlationID, waiter)
		if removed {
			releaseWaiter = true
		}
	}()
	err := c.transport.Write(writeCtx, frame.Bytes())
	c.writeMu.Unlock()
	if err != nil {
		wrapped := fmt.Errorf("write request: %w", err)
		span.RecordError(wrapped)
		span.SetStatus(codes.Error, wrapped.Error())
		return nil, wrapped
	}
	c.recordActivity()

	select {
	case <-waiter.ready:
		releaseWaiter = true
		if waiter.closed {
			if err := c.getConnError(); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}
			span.SetStatus(codes.Error, ErrConnectionClosed.Error())
			return nil, ErrConnectionClosed
		}
		return waiter.response, nil
	case <-ctx.Done():
		removed := c.mux.UnregisterCorrelatedRequest(correlationID, waiter)
		if removed {
			releaseWaiter = true
		}
		span.RecordError(ctx.Err())
		span.SetStatus(codes.Error, ctx.Err().Error())
		return nil, ctx.Err()
	case <-c.done:
		select {
		case <-waiter.ready:
			releaseWaiter = true
			if !waiter.closed {
				return waiter.response, nil
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
	return c.SendRequestWithWriterAndFlags(ctx, msgType, 0, writePayload)
}

// SendRequestWithWriterAndFlags sends a request with flags and direct payload encoding.
func (c *Connection) SendRequestWithWriterAndFlags(ctx context.Context, msgType uint16, flags uint8, writePayload func(*bytes.Buffer)) (_ []byte, retErr error) {
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

	if err := c.AcquireRequestSlotErr(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	defer c.ReleaseRequestSlot()

	correlationID := c.mux.NextCorrelationID()
	if correlationID == 0 {
		return nil, errors.New("correlation IDs exhausted; reconnect before sending another request")
	}
	frame, err := protocol.EncodeFrameWithHeaderAndPayloadWriter(msgType, flags, correlationID, writePayload)
	if err != nil {
		wrapped := fmt.Errorf("encode frame: %w", err)
		span.RecordError(wrapped)
		span.SetStatus(codes.Error, wrapped.Error())
		return nil, wrapped
	}
	defer frame.Release()
	if uint32(frame.Len()) > c.maxMessageBytes.Load() {
		return nil, fmt.Errorf("message exceeds negotiated max_message_bytes (%d)", c.maxMessageBytes.Load())
	}

	waiter := acquireRequestWaiter()
	releaseWaiter := false
	defer func() {
		if releaseWaiter {
			releaseRequestWaiter(waiter)
		}
	}()
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
	if !c.mux.RegisterCorrelatedRequest(correlationID, waiter) {
		c.writeMu.Unlock()
		return nil, errors.New("register correlated request")
	}
	defer func() {
		removed := c.mux.UnregisterCorrelatedRequest(correlationID, waiter)
		if removed {
			releaseWaiter = true
		}
	}()
	err = c.transport.Write(writeCtx, frame.Bytes())
	c.writeMu.Unlock()
	if err != nil {
		wrapped := fmt.Errorf("write request: %w", err)
		span.RecordError(wrapped)
		span.SetStatus(codes.Error, wrapped.Error())
		return nil, wrapped
	}
	c.recordActivity()

	select {
	case <-waiter.ready:
		releaseWaiter = true
		if waiter.closed {
			if err := c.getConnError(); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}
			span.SetStatus(codes.Error, ErrConnectionClosed.Error())
			return nil, ErrConnectionClosed
		}
		return waiter.response, nil
	case <-ctx.Done():
		removed := c.mux.UnregisterCorrelatedRequest(correlationID, waiter)
		if removed {
			releaseWaiter = true
		}
		span.RecordError(ctx.Err())
		span.SetStatus(codes.Error, ctx.Err().Error())
		return nil, ctx.Err()
	case <-c.done:
		select {
		case <-waiter.ready:
			releaseWaiter = true
			if !waiter.closed {
				return waiter.response, nil
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
func fireAndForgetNeedsCorrelation(messageType uint16, payload []byte) bool {
	if messageType < protocol.MessageTypeRpcSubscribeWorker || messageType > protocol.MessageTypeRpcLifecycle {
		return false
	}
	switch messageType {
	case protocol.MessageTypeRpcResponse, protocol.MessageTypeRpcLifecycle:
		return false
	case protocol.MessageTypeRpcCancellation:
		return len(payload) > 0 && payload[0] == 1
	default:
		return true
	}
}

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

	if err := c.AcquireWriteSlotErr(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	defer c.ReleaseWriteSlot()

	correlationID := uint64(0)
	if fireAndForgetNeedsCorrelation(msgType, payload) {
		correlationID = c.mux.NextCorrelationID()
		if correlationID == 0 {
			return errors.New("correlation IDs exhausted; reconnect before sending another request")
		}
	}
	frame := protocol.EncodeFrameOwnedWithHeader(msgType, 0, correlationID, payload)
	if frame == nil {
		err := errors.New("encode frame")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	defer frame.Release()
	if uint32(frame.Len()) > c.maxMessageBytes.Load() {
		return fmt.Errorf("message exceeds negotiated max_message_bytes (%d)", c.maxMessageBytes.Load())
	}

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
	c.recordActivity()

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

	if err := c.AcquireWriteSlotErr(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	defer c.ReleaseWriteSlot()

	payloadBuffer := bytes.NewBuffer(make([]byte, 0, 128))
	writePayload(payloadBuffer)
	payload := payloadBuffer.Bytes()
	correlationID := uint64(0)
	if fireAndForgetNeedsCorrelation(msgType, payload) {
		correlationID = c.mux.NextCorrelationID()
		if correlationID == 0 {
			return errors.New("correlation IDs exhausted; reconnect before sending another request")
		}
	}
	frame := protocol.EncodeFrameOwnedWithHeader(msgType, 0, correlationID, payload)
	if frame == nil {
		return errors.New("encode frame")
	}
	defer frame.Release()
	if uint32(frame.Len()) > c.maxMessageBytes.Load() {
		return fmt.Errorf("message exceeds negotiated max_message_bytes (%d)", c.maxMessageBytes.Load())
	}

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
	c.recordActivity()

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

	c.beginAsyncHandlerShutdown()
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

// RegisterRawPushHandler registers a handler for deferred responses that use
// their request message type but arrive after the synchronous response.
func (c *Connection) RegisterRawPushHandler(msgType uint16, handler func(payload []byte)) {
	c.mux.SetRawPushHandler(msgType, handler)
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
		c.connError.Store(connErrorValue{err: err})
	}
}

func (c *Connection) getConnError() error {
	if val := c.connError.Load(); val != nil {
		return val.(connErrorValue).err
	}
	return nil
}

type connErrorValue struct{ err error }

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

// AsyncHandlerQueueCapacity returns the configured number of callback jobs
// that may wait for an execution slot.
func (c *Connection) AsyncHandlerQueueCapacity() int {
	if c == nil || c.cfg.AsyncHandlerQueueCapacity <= 0 {
		return 1024
	}
	return c.cfg.AsyncHandlerQueueCapacity
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

// MaxRequestQueueSize returns the configured number of waiters admitted beyond
// the active in-flight limit.
func (c *Connection) MaxRequestQueueSize() int {
	if c == nil {
		return 1024
	}
	if c.cfg.MaxRequestQueueSize <= 0 {
		return 1024
	}
	return c.cfg.MaxRequestQueueSize
}

// RetryEnabled reports whether automatic retry helpers are enabled.
func (c *Connection) RetryEnabled() bool {
	return c != nil && c.cfg.RetryEnabled
}

// AcquireRequestSlot acquires a concurrency slot for outbound request admission.
// It returns false if acquisition was canceled by context shutdown/deadline.
func (c *Connection) AcquireRequestSlot(ctx context.Context) bool {
	return c.AcquireRequestSlotErr(ctx) == nil
}

// AcquireRequestSlotErr acquires a request slot or returns the admission error.
func (c *Connection) AcquireRequestSlotErr(ctx context.Context) error {
	if c == nil || c.requestSem == nil {
		return nil
	}
	if ctx == nil {
		ctx = c.LifecycleContext()
	}
	select {
	case c.requestSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.outboundAdmissionError(ctx)
	default:
	}

	queued := c.queuedRequests.Add(1)
	if queued > int64(c.MaxRequestQueueSize()) {
		c.queuedRequests.Add(-1)
		return ErrRequestQueueFull
	}
	defer c.queuedRequests.Add(-1)

	select {
	case c.requestSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.outboundAdmissionError(ctx)
	}
}

// ReleaseRequestSlot releases a previously acquired outbound request slot.
func (c *Connection) ReleaseRequestSlot() {
	if c == nil || c.requestSem == nil {
		return
	}
	<-c.requestSem
}

// AcquireWriteSlot acquires a concurrency slot for one-way outbound writes.
// It returns false if acquisition was canceled by context shutdown/deadline.
func (c *Connection) AcquireWriteSlot(ctx context.Context) bool {
	return c.AcquireWriteSlotErr(ctx) == nil
}

// AcquireWriteSlotErr acquires a one-way write slot or returns the admission error.
func (c *Connection) AcquireWriteSlotErr(ctx context.Context) error {
	if c == nil || c.oneWaySem == nil {
		return nil
	}
	if ctx == nil {
		ctx = c.LifecycleContext()
	}
	select {
	case c.oneWaySem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.outboundAdmissionError(ctx)
	default:
	}

	queued := c.queuedWrites.Add(1)
	if queued > int64(c.MaxRequestQueueSize()) {
		c.queuedWrites.Add(-1)
		return ErrRequestQueueFull
	}
	defer c.queuedWrites.Add(-1)

	select {
	case c.oneWaySem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.outboundAdmissionError(ctx)
	}
}

// ReleaseWriteSlot releases a previously acquired one-way outbound write slot.
func (c *Connection) ReleaseWriteSlot() {
	if c == nil || c.oneWaySem == nil {
		return
	}
	<-c.oneWaySem
}

type asyncHandlerJob struct {
	ctx    context.Context
	cancel context.CancelFunc
	span   trace.Span
	run    func(context.Context, trace.Span)
}

// LaunchAsyncHandler schedules an async callback on the connection's bounded handler pool.
// It returns false when the job cannot be accepted because the connection is shutting down
// the caller's context has already been canceled, or the bounded queue is full.
func (c *Connection) LaunchAsyncHandler(parent context.Context, spanName string, timeout time.Duration, run func(context.Context, trace.Span), opts ...trace.SpanStartOption) bool {
	if c == nil || c.asyncHandlerJobs == nil || run == nil {
		return false
	}
	if parent == nil {
		parent = c.LifecycleContext()
	}
	if c.closed.Load() || c.asyncClosing.Load() || c.State() == StateClosed {
		return false
	}
	if err := c.ctx.Err(); err != nil {
		return false
	}
	if err := parent.Err(); err != nil {
		return false
	}
	c.ensureAsyncHandlerWorkers()
	handlerCtx, cancel, span := coretracing.StartDetachedSpan(parent, c.tracer, spanName, timeout, opts...)
	job := asyncHandlerJob{
		ctx:    handlerCtx,
		cancel: cancel,
		span:   span,
		run:    run,
	}

	c.asyncHandlerMu.Lock()
	defer c.asyncHandlerMu.Unlock()
	if c.closed.Load() || c.asyncClosing.Load() || c.State() == StateClosed {
		c.cancelAsyncHandlerJob(job, "connection shutting down")
		return false
	}
	if err := c.ctx.Err(); err != nil {
		c.cancelAsyncHandlerJob(job, err.Error())
		return false
	}
	if err := parent.Err(); err != nil {
		c.cancelAsyncHandlerJob(job, err.Error())
		return false
	}

	select {
	case c.asyncHandlerJobs <- job:
		return true
	default:
		queueErr := errors.New("async handler queue full")
		if c.asyncHandlersSaturated != nil {
			c.asyncHandlersSaturated.Add(parent, 1)
		}
		span.RecordError(queueErr)
		span.SetStatus(codes.Error, queueErr.Error())
		cancel()
		span.End()
		return false
	}
}

func (c *Connection) ensureAsyncHandlerWorkers() {
	if c == nil || c.asyncHandlerJobs == nil {
		return
	}
	c.asyncWorkersOnce.Do(func() {
		workerCount := c.AsyncHandlerMaxConcurrency()
		for range workerCount {
			go c.asyncHandlerWorker()
		}
	})
}

func (c *Connection) asyncHandlerWorker() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case job := <-c.asyncHandlerJobs:
			if c.claimAsyncHandlerJob(job) {
				c.executeAsyncHandlerJob(job)
			}
		}
	}
}

func (c *Connection) claimAsyncHandlerJob(job asyncHandlerJob) bool {
	c.asyncHandlerMu.Lock()
	defer c.asyncHandlerMu.Unlock()
	jobDone := job.ctx != nil && job.ctx.Err() != nil
	if c.asyncClosing.Load() || c.closed.Load() || c.State() == StateClosed || c.ctx.Err() != nil || jobDone {
		c.cancelAsyncHandlerJob(job, c.asyncHandlerStopError(job).Error())
		return false
	}
	return true
}

func (c *Connection) executeAsyncHandlerJob(job asyncHandlerJob) {
	defer job.cancel()
	defer job.span.End()

	release, ok := c.AcquireAsyncHandlerSlot(job.ctx)
	if !ok {
		err := c.asyncHandlerStopError(job)
		job.span.RecordError(err)
		job.span.SetStatus(codes.Error, err.Error())
		return
	}
	defer release()

	job.run(job.ctx, job.span)
}

func (c *Connection) beginAsyncHandlerShutdown() {
	if c == nil || c.asyncHandlerJobs == nil {
		return
	}
	c.asyncHandlerMu.Lock()
	defer c.asyncHandlerMu.Unlock()
	c.asyncClosing.Store(true)
	c.drainAsyncHandlerJobsLocked()
}

func (c *Connection) drainAsyncHandlerJobsLocked() {
	for {
		select {
		case job := <-c.asyncHandlerJobs:
			c.cancelAsyncHandlerJob(job, "connection shutting down")
		default:
			return
		}
	}
}

func (c *Connection) cancelAsyncHandlerJob(job asyncHandlerJob, reason string) {
	if job.cancel != nil {
		job.cancel()
	}
	if job.span != nil {
		if reason != "" {
			err := errors.New(reason)
			job.span.RecordError(err)
			job.span.SetStatus(codes.Error, err.Error())
		}
		job.span.End()
	}
}

func (c *Connection) asyncHandlerStopError(job asyncHandlerJob) error {
	if job.ctx != nil {
		if err := job.ctx.Err(); err != nil {
			return err
		}
	}
	if c != nil && c.ctx != nil {
		if err := c.ctx.Err(); err != nil {
			return err
		}
	}
	return errors.New("connection shutting down")
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
		if err := ctx.Err(); err != nil {
			<-c.asyncHandlerSem
			recordWait(ctx)
			recordFailure(ctx, "context_done")
			return noop, false
		}
		if err := c.ctx.Err(); err != nil {
			<-c.asyncHandlerSem
			recordWait(c.ctx)
			recordFailure(c.ctx, "connection_shutdown")
			return noop, false
		}
		if c.asyncClosing.Load() || c.closed.Load() || c.State() == StateClosed {
			<-c.asyncHandlerSem
			recordWait(c.LifecycleContext())
			recordFailure(c.LifecycleContext(), "connection_shutdown")
			return noop, false
		}
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

// ActiveSubscriptions returns the current active subscription count.
func (c *Connection) ActiveSubscriptions() int64 {
	if c == nil {
		return 0
	}
	return c.activeSubscriptions.Load()
}

// CheckLiveHandle returns ErrStaleHandle when a handle is bound to a
// connection that has already closed. Stateful handles created before a
// reconnect must fail fast instead of silently using the replacement
// connection.
func (c *Connection) CheckLiveHandle() error {
	if c == nil {
		return ErrStaleHandle
	}
	if c.closed.Load() {
		return fmt.Errorf("%w: %w", ErrStaleHandle, ErrConnectionClosed)
	}
	select {
	case <-c.done:
		if err := c.getConnError(); err != nil {
			return fmt.Errorf("%w: %w", ErrStaleHandle, err)
		}
		return fmt.Errorf("%w: %w", ErrStaleHandle, ErrConnectionClosed)
	default:
		return nil
	}
}

// RunWithRetry executes fn with the connection retry policy. maxAttempts is
// total attempts, not retries; the default is three attempts.
func (c *Connection) RunWithRetry(ctx context.Context, fn func() error, isRetryable func(error) bool) error {
	if c == nil || fn == nil {
		return nil
	}
	if !c.cfg.RetryEnabled {
		return fn()
	}
	if isRetryable == nil {
		isRetryable = IsTransientRetryable
	}
	maxAttempts := c.cfg.RetryMaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	backoff := retry.BackoffConfig{
		InitialDelay: c.cfg.RetryBackoff,
		MaxDelay:     c.cfg.RetryMaxBackoff,
		Multiplier:   2.0,
		JitterFactor: 0.1,
	}
	if backoff.InitialDelay <= 0 {
		backoff.InitialDelay = 100 * time.Millisecond
	}
	if backoff.MaxDelay <= 0 {
		backoff.MaxDelay = time.Second
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if !isRetryable(lastErr) || attempt == maxAttempts {
			return lastErr
		}

		timer := time.NewTimer(retry.CalculateDelay(backoff, attempt-1))
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-c.done:
			if !timer.Stop() {
				<-timer.C
			}
			return c.outboundAdmissionError(ctx)
		}
	}
	return lastErr
}

// IsTransientRetryable reports whether err is safe for replayable read retries.
func IsTransientRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrConnectionClosed) ||
		errors.Is(err, ErrNotAuthenticated) {
		return true
	}
	var transportErr *transport.TransportError
	if errors.As(err, &transportErr) {
		return true
	}
	var domainErr *coreerrors.DomainError
	if errors.As(err, &domainErr) {
		if domainErr.Effect != 0 {
			return false
		}
		if domainErr.Class == 5 {
			return true
		}
		switch uint32(domainErr.Code) {
		case coreerrors.KvIsolationConflict,
			coreerrors.KvBusy,
			coreerrors.StreamBusy,
			coreerrors.NoticeBusy,
			coreerrors.QueueFull,
			coreerrors.LeaseQueueFull,
			coreerrors.LeaseWaitersLimit,
			coreerrors.RpcRouteNotRegistered,
			coreerrors.ScheduleBusy:
			return true
		case coreerrors.LeaseHeld:
			return true
		}
	}
	return false
}

type heartbeatSender interface {
	SendHeartbeat(ctx context.Context) error
}

type keepAliveEnabler interface {
	EnableKeepAlive(interval time.Duration) error
}

func (c *Connection) startHeartbeat() {
	if c == nil || !c.cfg.HeartbeatEnabled {
		return
	}
	c.heartbeatOnce.Do(func() {
		if enabler, ok := c.transport.(keepAliveEnabler); ok {
			_ = enabler.EnableKeepAlive(c.cfg.HeartbeatInterval)
		}
		sender, ok := c.transport.(heartbeatSender)
		if !ok {
			return
		}
		go c.heartbeatLoop(sender)
	})
}

func (c *Connection) heartbeatLoop(sender heartbeatSender) {
	interval := c.cfg.HeartbeatInterval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	timeout := c.cfg.HeartbeatTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	for {
		last := time.Unix(0, c.lastActivityUnix.Load())
		delay := max(interval-time.Since(last), 0)
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-c.done:
			timer.Stop()
			return
		case <-c.ctx.Done():
			timer.Stop()
			return
		}

		if time.Since(time.Unix(0, c.lastActivityUnix.Load())) < interval {
			continue
		}

		heartbeatCtx, cancel := context.WithTimeout(c.ctx, timeout)
		err := sender.SendHeartbeat(heartbeatCtx)
		cancel()
		if err == nil {
			c.recordActivity()
			continue
		}
		if c.ctx.Err() != nil {
			return
		}
		c.setConnError(fmt.Errorf("heartbeat failed: %w", err))
		c.cancel()
		_ = c.transport.Close()
		return
	}
}

func (c *Connection) recordActivity() {
	if c == nil {
		return
	}
	c.lastActivityUnix.Store(time.Now().UnixNano())
}
