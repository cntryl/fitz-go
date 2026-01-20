package kv

import (
	"context"
	"sync"

	"github.com/cntryl/cntryl-go/internal/transport"
)

// Client provides transaction-based key-value operations only. All data
// interactions MUST occur through transactions returned by Begin/BeginRead.
// Convenience helpers were intentionally removed to avoid accidental
// non-transactional use.
type Client interface {
	// Begin opens a read/write transaction scoped to the provided route.
	Begin(ctx context.Context, route Route) (Tx, error)

	// BeginRead opens a read-only transaction scoped to the provided route.
	BeginRead(ctx context.Context, route Route) (ReadTx, error)
}

// client is a concrete implementation of Client using the transport mux.
type client struct {
	mux *transport.Mux
	mu  sync.RWMutex
}

// NewClient creates a new KV domain client backed by the transport mux.
func NewClient(mux *transport.Mux) Client {
	return &client{
		mux: mux,
	}
}

// Begin opens a read/write transaction scoped to the provided route.
func (c *client) Begin(ctx context.Context, route Route) (Tx, error) {
	if err := route.Validate(); err != nil {
		return nil, err
	}
	txID := nextTxID.Add(1)
	return &transaction{
		route:    route,
		mux:      c.mux,
		readOnly: false,
		txID:     txID,
	}, nil
}

// BeginRead opens a read-only transaction scoped to the provided route.
func (c *client) BeginRead(ctx context.Context, route Route) (ReadTx, error) {
	if err := route.Validate(); err != nil {
		return nil, err
	}
	txID := nextTxID.Add(1)
	return &transaction{
		route:    route,
		mux:      c.mux,
		readOnly: true,
		txID:     txID,
	}, nil
}
