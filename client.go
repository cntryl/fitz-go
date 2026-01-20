package fitz

import (
	"context"

	"github.com/cntryl/cntryl-go/internal/kv"
	"github.com/cntryl/cntryl-go/internal/lease"
	"github.com/cntryl/cntryl-go/internal/notice"
	"github.com/cntryl/cntryl-go/internal/queue"
	"github.com/cntryl/cntryl-go/internal/rpc"
	"github.com/cntryl/cntryl-go/internal/stream"
)

// TokenProvider is a function that returns a JWT token for authentication.
// It is called during connection establishment and reconnection attempts,
// allowing for token renewal and refresh logic. Return an empty string for
// unauthenticated connections.
type TokenProvider func(ctx context.Context) (string, error)

// Client is the primary top-level Fitz client exposing each domain client.
// Implementations SHOULD provide a constructor (e.g., NewClient) that accepts:
//   - addr: broker address determining transport ("host:port", "tcp://...", "ws://...", "wss://...")
//   - tokenProvider: function for obtaining JWT tokens (supports renewal on reconnection)
type Client interface {
	// Connect establishes a connection to the broker using the address and
	// TokenProvider configured during client construction.
	//
	// Connect will call the TokenProvider to obtain a JWT token for the
	// CONN_OPEN handshake. This allows tokens to be refreshed on each
	// connection attempt.
	//
	// Per CLIENT_SPEC.md, both TCP and WebSocket transports MUST be supported
	// with identical wire protocol semantics (TLV framing).
	Connect(ctx context.Context) error

	// Close cleanly shuts down the client and associated resources.
	// This should send a CONN_CLOSE frame if the connection is active,
	// then close the underlying transport.
	Close() error

	// Domain clients
	Notice() notice.Client
	Stream() stream.Client
	Queue() queue.Client
	RPC() rpc.Client
	KV() kv.Client
	Lease() lease.Client
}
