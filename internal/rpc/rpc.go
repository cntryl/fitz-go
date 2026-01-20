package rpc

import (
	"context"
	"time"
)

// Client provides request/response and streaming RPC primitives.
type Client interface {
	Request(ctx context.Context, route string, body []byte, timeout time.Duration) ([]byte, error)
	RequestStream(ctx context.Context, route string, body []byte) (<-chan []byte, error)
}
