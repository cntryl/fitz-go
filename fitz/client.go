package fitz

import (
	"context"

	coreclient "github.com/cntryl/fitz-go/internal/core/client"
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

func (c *Client) Connect(ctx context.Context) error {
	return c.inner.Connect(ctx)
}

func (c *Client) Close() error {
	return c.inner.Close()
}

func (c *Client) State() ConnectionState {
	return fromCoreConnectionState(c.inner.State())
}

func (c *Client) Notice() NoticeClient {
	return &noticeClient{inner: c.inner.Notice()}
}

func (c *Client) Stream() StreamClient {
	return &streamClient{inner: c.inner.Stream()}
}

func (c *Client) Queue() QueueClient {
	return &queueClient{inner: c.inner.Queue()}
}

func (c *Client) RPC() RPCClient {
	return &rpcClient{inner: c.inner.RPC()}
}

func (c *Client) KV() KVClient {
	return &kvClient{inner: c.inner.KV()}
}

func (c *Client) Lease() LeaseClient {
	return &leaseClient{inner: c.inner.Lease()}
}

func (c *Client) Schedule() ScheduleClient {
	return &scheduleClient{inner: c.inner.Schedule()}
}
