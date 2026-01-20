package kv

import (
	"context"

	"github.com/cntryl/cntryl-go/internal/iter"
)

// KVPair is a simple key/value pair returned by Scan operations.
type KVPair struct {
	Key   []byte
	Value []byte
}

// ReadTx exposes read-only operations within a transaction and supports
// Commit/Rollback semantics. Implementations SHOULD make Commit a no-op for
// read-only transactions if the underlying store does not require it.
//
// Scan returns a streaming `iter.Iterator[KVPair]` for results matching `query`.
type ReadTx interface {
	Get(ctx context.Context, key []byte) ([]byte, bool, error)
	Scan(ctx context.Context, query []byte, limit uint32) (iter.Iterator[KVPair], error)

	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// Tx is a read/write transaction. It embeds ReadTx and adds mutation methods.
type Tx interface {
	ReadTx
	Put(ctx context.Context, key []byte, value []byte) error
	Insert(ctx context.Context, key []byte, value []byte) error
	Delete(ctx context.Context, key []byte) error
	DeleteRange(ctx context.Context, startKey []byte, endKey []byte) (int, error)
}

// Client provides transaction-based key-value operations only. All data
// interactions MUST occur through transactions returned by Begin/BeginRead.
// Convenience helpers were intentionally removed to avoid accidental
// non-transactional use.
type Client interface {
	// Begin opens a read/write transaction scoped to the provided route.
	Begin(ctx context.Context, route string) (Tx, error)

	// BeginRead opens a read-only transaction scoped to the provided route.
	BeginRead(ctx context.Context, route string) (ReadTx, error)
}
