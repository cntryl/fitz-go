package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	fitz "github.com/cntryl/cntryl-go"
	"github.com/cntryl/cntryl-go/internal/kv"
	"github.com/cntryl/cntryl-go/internal/lease"
	"github.com/cntryl/cntryl-go/internal/notice"
	"github.com/cntryl/cntryl-go/internal/queue"
	"github.com/cntryl/cntryl-go/internal/rpc"
	"github.com/cntryl/cntryl-go/internal/stream"
	"github.com/cntryl/cntryl-go/internal/transport"
)

// Client implements the fitz.Client interface with connection management,
// retry logic, and mux-based domain routing.
type Client struct {
	addr          string
	tokenProvider fitz.TokenProvider
	dialer        Dialer
	mux           *transport.Mux
	mu            sync.RWMutex
	closed        bool
	retryBackoff  time.Duration
	maxRetries    int
	domainClients struct {
		notice notice.Client
		stream stream.Client
		queue  queue.Client
		rpc    rpc.Client
		kv     kv.Client
		lease  lease.Client
	}
}

// NewClient creates a new Fitz client for the given address and token provider.
// Address format determines transport protocol:
//   - "host:port" or "tcp://host:port" for TCP
//   - "ws://host:port/path" for WebSocket
//   - "wss://host:port/path" for secure WebSocket (recommended)
//
// TokenProvider is called during connection to obtain JWT for authentication.
// Pass nil for unauthenticated connections.
func NewClient(addr string, tokenProvider fitz.TokenProvider, opts ...ClientOption) *Client {
	c := &Client{
		addr:          addr,
		tokenProvider: tokenProvider,
		dialer:        &DefaultDialer{},
		retryBackoff:  100 * time.Millisecond,
		maxRetries:    5,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// ClientOption is a functional option for configuring the client.
type ClientOption func(*Client)

// WithRetryBackoff sets the initial retry backoff duration.
func WithRetryBackoff(d time.Duration) ClientOption {
	return func(c *Client) { c.retryBackoff = d }
}

// WithMaxRetries sets the maximum number of connection retries.
func WithMaxRetries(n int) ClientOption {
	return func(c *Client) { c.maxRetries = n }
}

// WithDialer sets a custom dialer (primarily for testing).
func WithDialer(d Dialer) ClientOption {
	return func(c *Client) { c.dialer = d }
}

// Connect opens a connection to the broker with exponential backoff retry.
// On success, it launches the mux and initializes domain clients.
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mux != nil {
		return errors.New("already connected")
	}

	// Try to establish connection with retries.
	var conn net.Conn
	var err error
	backoff := c.retryBackoff
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		conn, err = c.dialer.Dial(ctx, c.addr)
		if err == nil {
			break
		}
		if attempt < c.maxRetries {
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
			backoff *= 2
		}
	}
	if conn == nil {
		return fmt.Errorf("failed to connect after %d attempts: %w", c.maxRetries+1, err)
	}

	// Create mux and start loops.
	c.mux = transport.NewMux(conn)
	c.mux.Start()

	// TODO: Send CONN_OPEN handshake with token and wait for ACK.
	// Token should be obtained by calling c.tokenProvider(ctx)
	// For now, we'll initialize domain clients.
	c.initializeDomainClients()

	return nil
}

// Close gracefully closes the connection and domain clients.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed || c.mux == nil {
		return nil
	}
	c.closed = true
	return c.mux.Close()
}

// Notice returns the notice domain client.
func (c *Client) Notice() notice.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.domainClients.notice
}

// Stream returns the stream domain client.
func (c *Client) Stream() stream.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.domainClients.stream
}

// Queue returns the queue domain client.
func (c *Client) Queue() queue.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.domainClients.queue
}

// RPC returns the RPC domain client.
func (c *Client) RPC() rpc.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.domainClients.rpc
}

// KV returns the KV domain client.
func (c *Client) KV() kv.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.domainClients.kv
}

// Lease returns the lease domain client.
func (c *Client) Lease() lease.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.domainClients.lease
}

// initializeDomainClients creates concrete domain clients that use the mux.
// This is called after a successful connection.
func (c *Client) initializeDomainClients() {
	c.domainClients.kv = kv.NewClient(c.mux)
	// TODO: Initialize remaining domain clients (notice, stream, queue, rpc, lease)
}
