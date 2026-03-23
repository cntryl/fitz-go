package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/core/reconnect"
	"github.com/cntryl/fitz-go/internal/core/transport"
	"github.com/cntryl/fitz-go/internal/core/types"
	"github.com/cntryl/fitz-go/internal/domains/kv"
	"github.com/cntryl/fitz-go/internal/domains/lease"
	"github.com/cntryl/fitz-go/internal/domains/notice"
	"github.com/cntryl/fitz-go/internal/domains/queue"
	"github.com/cntryl/fitz-go/internal/domains/rpc"
	"github.com/cntryl/fitz-go/internal/domains/schedule"
	"github.com/cntryl/fitz-go/internal/domains/stream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// TransportType specifies the transport protocol.
type TransportType int

const (
	TransportAuto TransportType = iota // Auto-detect from URL
	TransportWebSocket
	TransportTCP
)

var (
	dialTCPTransport       = transport.DialTCP
	dialWebSocketTransport = transport.DialWebSocket
)

// Client implements the Fitz client with connection management.
// Per CLIENT_SPEC.md: Handles authentication, request/response correlation, and domain routing.
type Client struct {
	addr          string
	tokenProvider types.TokenProvider
	config        *Config
	meter         metric.Meter

	mu     sync.RWMutex
	conn   *connection.Connection
	closed atomic.Bool

	// Domain clients
	kvClient       kv.Client
	noticeClient   notice.Client
	queueClient    queue.Client
	rpcClient      rpc.Client
	streamClient   stream.Client
	leaseClient    lease.Client
	scheduleClient schedule.Client

	closeOnce       sync.Once
	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc
	reconnectMu     sync.Mutex
	reconnecting    bool
	reconnectCount  metric.Int64Counter
	reconnectTimeMs metric.Int64Histogram
}

// Config contains client configuration.
type Config struct {
	// Connection
	URL                        string
	AuthSettleDelay            time.Duration
	ReadTimeout                time.Duration
	WriteTimeout               time.Duration
	AsyncHandlerTimeout        time.Duration
	AsyncHandlerMaxConcurrency int

	// Reconnection
	ReconnectEnabled bool
	ReconnectBackoff time.Duration
	MaxReconnects    int

	// Transport
	TransportType TransportType // Auto, WebSocket, or TCP

	// Observability (optional)
	Logger *slog.Logger // When nil, no logging.
	Tracer trace.Tracer // When nil, otel.Tracer(module) is used for spans.
	Meter  metric.Meter // When nil, otel.Meter(module) is used for metrics.
}

// defaultConfig returns default client configuration.
func defaultConfig() *Config {
	return &Config{
		TransportType:              TransportAuto,
		AuthSettleDelay:            500 * time.Millisecond,
		ReadTimeout:                30 * time.Second,
		WriteTimeout:               10 * time.Second,
		AsyncHandlerTimeout:        30 * time.Second,
		AsyncHandlerMaxConcurrency: 256,
	}
}

// Option is a functional option for configuring the client.
type Option func(*Config)

// WithURL sets the server URL.
func WithURL(url string) Option {
	return func(c *Config) { c.URL = url }
}

// WithAuthSettleDelay sets the silent CONNECT settle window.
func WithAuthSettleDelay(delay time.Duration) Option {
	return func(c *Config) { c.AuthSettleDelay = delay }
}

// WithReadTimeout sets the per-read timeout.
func WithReadTimeout(timeout time.Duration) Option {
	return func(c *Config) { c.ReadTimeout = timeout }
}

// WithWriteTimeout sets the write timeout.
func WithWriteTimeout(timeout time.Duration) Option {
	return func(c *Config) { c.WriteTimeout = timeout }
}

// WithAsyncHandlerTimeout sets the timeout used for detached async handler spans.
// A zero duration uses the default timeout.
func WithAsyncHandlerTimeout(timeout time.Duration) Option {
	return func(c *Config) { c.AsyncHandlerTimeout = timeout }
}

// WithAsyncHandlerMaxConcurrency sets the maximum number of concurrent
// detached async handlers. A value <= 0 uses the default limit.
func WithAsyncHandlerMaxConcurrency(max int) Option {
	return func(c *Config) { c.AsyncHandlerMaxConcurrency = max }
}

// WithReconnect enables/disables automatic reconnection.
func WithReconnect(enabled bool, backoff time.Duration, maxAttempts int) Option {
	return func(c *Config) {
		c.ReconnectEnabled = enabled
		c.ReconnectBackoff = backoff
		c.MaxReconnects = maxAttempts
	}
}

// WithTransport sets the transport type.
func WithTransport(transportType TransportType) Option {
	return func(c *Config) { c.TransportType = transportType }
}

// WithLogger sets the structured logger for connection and domain logging.
// When nil or not set, no logs are emitted.
func WithLogger(logger *slog.Logger) Option {
	return func(c *Config) { c.Logger = logger }
}

// WithTracer sets the OpenTelemetry tracer for spans.
// When nil or not set, otel.Tracer(module) is used (no-op if no TracerProvider is set).
func WithTracer(tracer trace.Tracer) Option {
	return func(c *Config) { c.Tracer = tracer }
}

// WithMeter sets the OpenTelemetry meter for metrics.
// When nil or not set, otel.Meter(module) is used (no-op if no MeterProvider is set).
func WithMeter(meter metric.Meter) Option {
	return func(c *Config) { c.Meter = meter }
}

// NewClient creates a new Fitz client targeting the given address.
// Call Connect() to establish the connection.
func NewClient(addr string, tokenProvider types.TokenProvider) *Client {
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	cfg := defaultConfig()
	cfg.URL = addr
	client := &Client{
		addr:            addr,
		tokenProvider:   tokenProvider,
		config:          cfg,
		lifecycleCtx:    lifecycleCtx,
		lifecycleCancel: lifecycleCancel,
	}
	client.initMetrics()
	return client
}

// NewClientWithOptions creates a new Fitz client targeting the given address
// and applies functional options before the first Connect call.
func NewClientWithOptions(addr string, tokenProvider types.TokenProvider, opts ...Option) *Client {
	c := NewClient(addr, tokenProvider)
	c.config.URL = addr
	for _, opt := range opts {
		opt(c.config)
	}
	c.initMetrics()
	return c
}

func (c *Client) initMetrics() {
	if c == nil || c.config == nil {
		return
	}
	meter := c.config.Meter
	if meter == nil {
		meter = otel.Meter("github.com/cntryl/fitz-go")
	}
	c.meter = meter
	if counter, err := meter.Int64Counter(
		"fitz.reconnect.attempts",
		metric.WithDescription("Count of Fitz reconnect attempts by outcome"),
		metric.WithUnit("{attempt}"),
	); err == nil {
		c.reconnectCount = counter
	}
	if histogram, err := meter.Int64Histogram(
		"fitz.reconnect.attempt_duration_ms",
		metric.WithDescription("Duration of Fitz reconnect attempts by outcome"),
		metric.WithUnit("ms"),
	); err == nil {
		c.reconnectTimeMs = histogram
	}
}

func (c *Client) recordReconnectAttempt(outcome string, transportType TransportType, started time.Time) {
	if c == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("fitz.outcome", outcome),
		attribute.String("fitz.transport", transportTypeString(transportType)),
	)
	if c.reconnectCount != nil {
		c.reconnectCount.Add(context.Background(), 1, attrs)
	}
	if c.reconnectTimeMs != nil {
		c.reconnectTimeMs.Record(context.Background(), time.Since(started).Milliseconds(), attrs)
	}
}

// Connect establishes a connection to the broker using the address and
// TokenProvider configured during client construction.
func (c *Client) Connect(ctx context.Context) error {
	if c.closed.Load() {
		return connection.ErrConnectionClosed
	}
	tracer := c.config.Tracer
	if tracer == nil {
		tracer = otel.Tracer("github.com/cntryl/fitz-go")
	}
	transportType := c.config.TransportType
	if transportType == TransportAuto {
		transportType = detectTransport(c.addr)
	}
	var span trace.Span
	ctx, span = tracer.Start(ctx, "fitz.Connect", trace.WithAttributes(
		attribute.String("fitz.addr", c.addr),
		attribute.String("fitz.transport", transportTypeString(transportType)),
	))
	defer span.End()

	if c.config.Logger != nil {
		c.config.Logger.Info("connect started", "addr", c.addr)
	}

	if err := c.config.validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	conn, err := c.dialConnection(ctx, transportType)
	if err != nil {
		return err
	}

	if c.config.Logger != nil {
		c.config.Logger.Info("connect success", "addr", c.addr)
	}
	c.attachConnection(conn)
	return nil
}

// Dial connects to a Fitz server and returns a ready-to-use client.
func Dial(ctx context.Context, addr string, tokenProvider types.TokenProvider, opts ...Option) (*Client, error) {
	client := NewClientWithOptions(addr, tokenProvider, opts...)
	if err := client.Connect(ctx); err != nil {
		return nil, err
	}
	return client, nil
}

// detectTransport determines transport type from URL scheme.
func detectTransport(url string) TransportType {
	if strings.HasPrefix(url, "ws://") || strings.HasPrefix(url, "wss://") {
		return TransportWebSocket
	}
	return TransportTCP
}

func transportTypeString(t TransportType) string {
	switch t {
	case TransportWebSocket:
		return "websocket"
	case TransportTCP:
		return "tcp"
	default:
		return "auto"
	}
}

// validate checks if the configuration is valid.
func (c *Config) validate() error {
	if c.URL == "" {
		return errors.New("URL is required")
	}
	return nil
}

// Close gracefully shuts down the connection.
// Safe to call multiple times (idempotent).
func (c *Client) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		if c.lifecycleCancel != nil {
			c.lifecycleCancel()
		}
		if c.config.Logger != nil {
			c.config.Logger.Info("client close")
		}
		if conn := c.currentConnection(); conn != nil {
			err = conn.Close()
		}
	})
	return err
}

// State returns the current connection lifecycle state.
func (c *Client) State() connection.State {
	if conn := c.currentConnection(); conn != nil {
		return conn.State()
	}
	if c.closed.Load() {
		return connection.StateClosed
	}
	return connection.StateDisconnected
}

// SendRequest is a low-level API for domain implementations.
// Sends a synchronous request and waits for response.
// Per CLIENT_SPEC.md: Responses matched via FIFO correlation.
func (c *Client) SendRequest(ctx context.Context, msgType uint16, payload []byte) ([]byte, error) {
	conn := c.currentConnection()
	if conn == nil {
		return nil, connection.ErrConnectionClosed
	}
	return conn.SendRequest(ctx, msgType, payload)
}

// RegisterNotifyHandler registers handler for NOTIFY messages for the given message type.
// msgType should be protocol.MessageTypeQueueNotify (209), MessageTypeLeaseNotify (409),
// MessageTypeNoticeNotify (504), or MessageTypeStreamNotify (609).
func (c *Client) RegisterNotifyHandler(msgType uint16, handler func(subID uint64, route string, payload []byte)) {
	if conn := c.currentConnection(); conn != nil {
		conn.RegisterNotifyHandler(msgType, handler)
	}
}

// RegisterRPCResponseHandler registers handler for RPC RESPONSE messages.
// Called by RPC domain client.
func (c *Client) RegisterRPCResponseHandler(handler func(correlationID [16]byte, payload []byte)) {
	if conn := c.currentConnection(); conn != nil {
		conn.RegisterRPCResponseHandler(handler)
	}
}

// Metrics returns connection metrics.
func (c *Client) Metrics() connection.MultiplexerMetrics {
	if conn := c.currentConnection(); conn != nil {
		return conn.Metrics()
	}
	return connection.MultiplexerMetrics{}
}

// Domain client accessors.

// KV returns the KV domain client.
func (c *Client) KV() kv.Client {
	return c.kvClient
}

// Notice returns the Notice domain client.
func (c *Client) Notice() notice.Client {
	return c.noticeClient
}

// Queue returns the Queue domain client.
func (c *Client) Queue() queue.Client {
	return c.queueClient
}

// RPC returns the RPC domain client.
func (c *Client) RPC() rpc.Client {
	return c.rpcClient
}

// Stream returns the Stream domain client.
func (c *Client) Stream() stream.Client {
	return c.streamClient
}

// Lease returns the Lease domain client.
func (c *Client) Lease() lease.Client {
	return c.leaseClient
}

// Schedule returns the Schedule domain client.
func (c *Client) Schedule() schedule.Client {
	return c.scheduleClient
}

func (c *Client) currentConnection() *connection.Connection {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn
}

func (c *Client) dialConnection(ctx context.Context, transportType TransportType) (*connection.Connection, error) {
	token := ""
	if c.tokenProvider != nil {
		var err error
		token, err = c.tokenProvider(ctx)
		if err != nil {
			return nil, fmt.Errorf("get token: %w", err)
		}
	}

	var (
		trans transport.Transport
		err   error
	)
	switch transportType {
	case TransportWebSocket:
		trans, err = dialWebSocketTransport(ctx, c.config.URL)
	case TransportTCP:
		trans, err = dialTCPTransport(ctx, c.config.URL)
	default:
		return nil, fmt.Errorf("unsupported transport type: %d", transportType)
	}
	if err != nil {
		return nil, fmt.Errorf("dial transport: %w", err)
	}

	connCfg := connection.Config{
		Token:                      token,
		AuthSettleDelay:            c.config.AuthSettleDelay,
		ReadTimeout:                c.config.ReadTimeout,
		WriteTimeout:               c.config.WriteTimeout,
		AsyncHandlerTimeout:        c.config.AsyncHandlerTimeout,
		AsyncHandlerMaxConcurrency: c.config.AsyncHandlerMaxConcurrency,
		Logger:                     c.config.Logger,
		Tracer:                     c.config.Tracer,
		Meter:                      c.config.Meter,
	}
	conn := connection.New(trans, connCfg)
	if err := conn.Start(ctx); err != nil {
		_ = trans.Close()
		return nil, fmt.Errorf("start connection: %w", err)
	}

	return conn, nil
}

func (c *Client) attachConnection(conn *connection.Connection) {
	c.mu.Lock()
	c.conn = conn
	if c.kvClient == nil {
		c.kvClient = kv.NewClient(conn)
	}
	if c.noticeClient == nil {
		c.noticeClient = notice.NewClient(conn)
	}
	if c.queueClient == nil {
		c.queueClient = queue.NewClient(conn)
	}
	if c.rpcClient == nil {
		c.rpcClient = rpc.NewClient(conn)
	}
	if c.streamClient == nil {
		c.streamClient = stream.NewClient(conn)
	}
	if c.leaseClient == nil {
		c.leaseClient = lease.NewClient(conn)
	}
	if c.scheduleClient == nil {
		c.scheduleClient = schedule.NewClient(conn)
	}
	c.mu.Unlock()

	c.replaceDomainConnections(conn)
	go c.monitorConnection(conn)
}

func (c *Client) replaceDomainConnections(conn *connection.Connection) {
	for _, candidate := range []any{
		c.noticeClient,
		c.queueClient,
		c.rpcClient,
		c.streamClient,
		c.leaseClient,
		c.scheduleClient,
	} {
		if restorer, ok := candidate.(reconnect.DomainRestorer); ok {
			restorer.ReplaceConnection(conn)
		}
	}
}

func (c *Client) monitorConnection(conn *connection.Connection) {
	<-conn.Done()

	if c.closed.Load() {
		return
	}
	if c.currentConnection() != conn {
		return
	}
	if !c.config.ReconnectEnabled {
		return
	}

	_ = conn.Close()
	c.beginReconnect(conn.Err())
}

func (c *Client) beginReconnect(cause error) {
	c.reconnectMu.Lock()
	if c.reconnecting || c.closed.Load() {
		c.reconnectMu.Unlock()
		return
	}
	c.reconnecting = true
	c.reconnectMu.Unlock()
	defer func() {
		c.reconnectMu.Lock()
		c.reconnecting = false
		c.reconnectMu.Unlock()
	}()

	transportType := c.config.TransportType
	if transportType == TransportAuto {
		transportType = detectTransport(c.addr)
	}
	backoff := c.config.ReconnectBackoff
	if backoff <= 0 {
		backoff = 250 * time.Millisecond
	}
	maxAttempts := c.config.MaxReconnects
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	tracer := c.config.Tracer
	if tracer == nil {
		tracer = otel.Tracer("github.com/cntryl/fitz-go")
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if c.closed.Load() {
			return
		}
		started := time.Now()

		attemptCtx, attemptSpan := tracer.Start(c.lifecycleCtx, "fitz.reconnect.attempt", trace.WithAttributes(
			attribute.Int("fitz.attempt", attempt),
			attribute.String("fitz.transport", transportTypeString(transportType)),
		))
		if cause != nil && c.config.Logger != nil {
			c.config.Logger.Warn("reconnect attempt", "attempt", attempt, "error", cause)
		}
		if cause != nil {
			attemptSpan.RecordError(cause)
		}

		conn, err := c.dialConnection(attemptCtx, transportType)
		if err == nil {
			c.attachConnection(conn)
			restoreCtx, restoreSpan := tracer.Start(attemptCtx, "fitz.reconnect.restore_subscriptions")
			err := c.restoreDomainSubscriptions(restoreCtx)
			restoreSpan.End()
			if err == nil {
				if c.config.Logger != nil {
					c.config.Logger.Info("reconnect success", "attempt", attempt)
				}
				c.recordReconnectAttempt("success", transportType, started)
				attemptSpan.End()
				return
			} else {
				attemptSpan.RecordError(err)
				attemptSpan.SetStatus(codes.Error, err.Error())
				outcome := "restore_error"
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					outcome = "canceled"
				}
				c.recordReconnectAttempt(outcome, transportType, started)
				cause = err
				_ = conn.Close()
			}
		} else {
			attemptSpan.RecordError(err)
			attemptSpan.SetStatus(codes.Error, err.Error())
			outcome := "dial_error"
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				outcome = "canceled"
			}
			c.recordReconnectAttempt(outcome, transportType, started)
			cause = err
		}
		attemptSpan.End()

		timer := time.NewTimer(backoff)
		select {
		case <-c.lifecycleCtx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (c *Client) restoreDomainSubscriptions(ctx context.Context) error {
	for _, candidate := range []any{
		c.noticeClient,
		c.queueClient,
		c.rpcClient,
		c.streamClient,
		c.leaseClient,
		c.scheduleClient,
	} {
		if restorer, ok := candidate.(reconnect.DomainRestorer); ok {
			if err := restorer.RestoreSubscriptions(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}
