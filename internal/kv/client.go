package kv

import "context"

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
