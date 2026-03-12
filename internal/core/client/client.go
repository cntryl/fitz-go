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
}

// Config contains client configuration.
type Config struct {
	// Connection
	URL          string
	JWT          string
	AuthTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// Reconnection (not yet implemented)
	ReconnectEnabled bool
	ReconnectBackoff time.Duration
	MaxReconnects    int

	// Transport
	TransportType TransportType // Auto, WebSocket, or TCP

	// Observability (optional)
	Logger *slog.Logger // When nil, no logging.
	Tracer trace.Tracer // When nil, otel.Tracer(module) is used for spans.
}

// defaultConfig returns default client configuration.
func defaultConfig() *Config {
	return &Config{
		TransportType: TransportAuto,
		AuthTimeout:   5 * time.Second,
		ReadTimeout:   30 * time.Second,
		WriteTimeout:  10 * time.Second,
	}
}

// Option is a functional option for configuring the client.
type Option func(*Config)

// WithURL sets the server URL.
func WithURL(url string) Option {
	return func(c *Config) { c.URL = url }
}

// WithJWT sets the JWT token for authentication.
// Use empty string for anonymous mode.
func WithJWT(jwt string) Option {
	return func(c *Config) { c.JWT = jwt }
}

// WithAuthTimeout sets the authentication timeout.
func WithAuthTimeout(timeout time.Duration) Option {
	return func(c *Config) { c.AuthTimeout = timeout }
}

// WithReadTimeout sets the per-read timeout.
func WithReadTimeout(timeout time.Duration) Option {
	return func(c *Config) { c.ReadTimeout = timeout }
}

// WithWriteTimeout sets the write timeout.
func WithWriteTimeout(timeout time.Duration) Option {
	return func(c *Config) { c.WriteTimeout = timeout }
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

// NewClient creates a new Fitz client targeting the given address.
// Call Connect() to establish the connection.
func NewClient(addr string, tokenProvider types.TokenProvider) *Client {
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	return &Client{
		addr:            addr,
		tokenProvider:   tokenProvider,
		config:          defaultConfig(),
		lifecycleCtx:    lifecycleCtx,
		lifecycleCancel: lifecycleCancel,
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
	c.config.URL = c.addr

	if err := c.config.validate(); err != nil {
		if c.config.Logger != nil {
			c.config.Logger.Error("invalid config", "error", err)
		}
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
// Per CLIENT_SPEC.md: Performs CONNECT handshake and waits for authentication confirmation.
func Dial(ctx context.Context, opts ...Option) (*Client, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	if err := cfg.validate(); err != nil {
		if cfg.Logger != nil {
			cfg.Logger.Error("invalid config", "error", err)
		}
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	transportType := cfg.TransportType
	if transportType == TransportAuto {
		transportType = detectTransport(cfg.URL)
	}
	tracer := cfg.Tracer
	if tracer == nil {
		tracer = otel.Tracer("github.com/cntryl/fitz-go")
	}
	var span trace.Span
	ctx, span = tracer.Start(ctx, "fitz.Connect", trace.WithAttributes(
		attribute.String("fitz.addr", cfg.URL),
		attribute.String("fitz.transport", transportTypeString(transportType)),
	))
	defer span.End()

	c := &Client{
		addr:   cfg.URL,
		config: cfg,
	}
	c.lifecycleCtx, c.lifecycleCancel = context.WithCancel(context.Background())

	conn, err := c.dialConnection(ctx, transportType)
	if err != nil {
		return nil, err
	}
	c.attachConnection(conn)

	if cfg.Logger != nil {
		cfg.Logger.Info("dial success", "url", cfg.URL)
	}
	return c, nil
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

// Domain client accessors (implements fitz.Client interface)

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
	// Arrange
	jwt := c.config.JWT
	if c.tokenProvider != nil {
		token, err := c.tokenProvider(ctx)
		if err != nil {
			if c.config.Logger != nil {
				c.config.Logger.Error("get token failed", "error", err)
			}
			return nil, fmt.Errorf("get token: %w", err)
		}
		jwt = token
	}
	c.config.JWT = jwt

	// Act
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
		if c.config.Logger != nil {
			c.config.Logger.Error("dial transport failed", "error", err)
		}
		return nil, fmt.Errorf("dial transport: %w", err)
	}

	connCfg := connection.Config{
		JWT:          jwt,
		AuthTimeout:  c.config.AuthTimeout,
		ReadTimeout:  c.config.ReadTimeout,
		WriteTimeout: c.config.WriteTimeout,
		Logger:       c.config.Logger,
		Tracer:       c.config.Tracer,
	}
	conn := connection.New(trans, connCfg)
	if err := conn.Start(ctx); err != nil {
		if c.config.Logger != nil {
			c.config.Logger.Error("start connection failed", "error", err)
		}
		_ = trans.Close()
		return nil, fmt.Errorf("start connection: %w", err)
	}

	// Assert
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

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if c.closed.Load() {
			return
		}
		if cause != nil && c.config.Logger != nil {
			c.config.Logger.Warn("reconnect attempt", "attempt", attempt, "error", cause)
		}

		conn, err := c.dialConnection(c.lifecycleCtx, transportType)
		if err == nil {
			c.attachConnection(conn)
			if err := c.restoreDomainSubscriptions(c.lifecycleCtx); err == nil {
				if c.config.Logger != nil {
					c.config.Logger.Info("reconnect success", "attempt", attempt)
				}
				return
			} else {
				cause = err
				_ = conn.Close()
			}
		} else {
			cause = err
		}

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
