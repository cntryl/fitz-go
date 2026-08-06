package fitz

import (
	"context"

	coreclient "github.com/cntryl/fitz-go/v2/internal/core/client"
)

// Client is the canonical public Fitz Go client.
type Client struct {
	inner *coreclient.Client
}

// NewClient constructs a disconnected client. Call Connect before using the
// domain accessors.
func NewClient(addr string, tokenProvider TokenProvider, opts ...Option) *Client {
	coreOpts := applyOptions(opts)
	return &Client{
		inner: coreclient.NewClientWithOptions(addr, coreclientTokenProvider(tokenProvider), coreOpts...),
	}
}

// Dial constructs and connects a client in one step.
func Dial(ctx context.Context, addr string, tokenProvider TokenProvider, opts ...Option) (*Client, error) {
	coreOpts := applyOptions(opts)
	inner, err := coreclient.Dial(ctx, addr, coreclientTokenProvider(tokenProvider), coreOpts...)
	if err != nil {
		return nil, err
	}
	return &Client{inner: inner}, nil
}

func coreclientTokenProvider(tokenProvider TokenProvider) func(context.Context) (string, error) {
	if tokenProvider == nil {
		return nil
	}
	return func(ctx context.Context) (string, error) {
		return tokenProvider(ctx)
	}
}

// Connect performs the Fitz handshake over the configured transport, exchanging
// CONNECT / CONNECT_OK frames and waiting for the connection to reach the
// Authenticated state. The context deadline governs the full connect attempt.
func (c *Client) Connect(ctx context.Context) error {
	return c.inner.Connect(ctx)
}

// ConnectWhenReady retries initial connection attempts using the configured
// reconnect backoff and attempt limit. The context owns the total wait, and
// authentication failures terminate immediately.
func (c *Client) ConnectWhenReady(ctx context.Context) error {
	return c.inner.ConnectWhenReady(ctx)
}

// Close tears down the underlying transport connection and releases all
// in-flight request state. It is safe to call more than once.
func (c *Client) Close() error {
	return c.inner.Close()
}

// State returns the current lifecycle state of the broker connection.
func (c *Client) State() ConnectionState {
	if c.inner.IsReconnecting() {
		return ConnectionStateReconnecting
	}
	return fromCoreConnectionState(c.inner.State())
}

// Notice returns the Notice domain client for publish/subscribe messaging.
func (c *Client) Notice() NoticeClient {
	return &noticeClient{inner: c.inner.Notice()}
}

// Stream returns the Stream domain client for append-only log streams.
func (c *Client) Stream() StreamClient {
	return &streamClient{inner: c.inner.Stream()}
}

// Queue returns the Queue domain client for reliable work queues with
// at-least-once delivery semantics.
func (c *Client) Queue() QueueClient {
	return &queueClient{inner: c.inner.Queue()}
}

// RPC returns the RPC domain client for request/response messaging with
// worker registration and fan-out.
func (c *Client) RPC() RPCClient {
	return &rpcClient{inner: c.inner.RPC()}
}

// KV returns the KV domain client for strongly-consistent key-value
// transactions.
func (c *Client) KV() KVClient {
	return &kvClient{inner: c.inner.KV()}
}

// Lease returns the Lease domain client for distributed mutual-exclusion
// leases with TTL and renewal.
func (c *Client) Lease() LeaseClient {
	return &leaseClient{inner: c.inner.Lease()}
}

// Schedule returns the Schedule domain client for cron-based recurring
// task scheduling with subscribe notification delivery.
func (c *Client) Schedule() ScheduleClient {
	return &scheduleClient{inner: c.inner.Schedule()}
}
