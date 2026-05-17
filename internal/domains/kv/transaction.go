package kv

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/core/iter"
	"github.com/cntryl/fitz-go/internal/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// KVPair is a key/value pair returned by Scan operations.
type KVPair struct {
	Key   []byte
	Value []byte
}

// ReadTx exposes read-only operations within a transaction.
//
// CRITICAL: Per CLIENT_SPEC.md, operations within a single transaction
// MUST be sequential. Do NOT call methods concurrently on the same transaction.
type ReadTx interface {
	Get(ctx context.Context, key []byte) (value []byte, found bool, err error)
	Scan(ctx context.Context, query ScanQuery) (iter.Iterator[KVPair], bool, error)
}

// Tx is a read/write transaction. It embeds ReadTx and adds mutations.
//
// CRITICAL: Per CLIENT_SPEC.md lines 323-361, operations within a single
// transaction (same tx_id) MUST be sequential. Concurrent calls will corrupt
// transaction state on the server.
type Tx interface {
	ReadTx
	Put(ctx context.Context, key, value []byte) error
	Insert(ctx context.Context, key, value []byte) error
	Delete(ctx context.Context, key []byte) error
	DeleteRange(ctx context.Context, startKey, endKey []byte) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// transaction is a concrete implementation of both ReadTx and Tx.
// NOT thread-safe - caller must serialize operations per CLIENT_SPEC.md.
type transaction struct {
	route      string
	conn       *connection.Connection
	readOnly   bool
	txID       uint64 // Server-assigned per CLIENT_SPEC.md
	committed  atomic.Bool
	rolledback atomic.Bool
}

// Get retrieves a value by key.
// Returns (value, true, nil) if key exists.
// Returns (nil, false, nil) if key does not exist (not an error per CLIENT_SPEC.md).
// Returns (nil, false, err) on actual errors.
func (t *transaction) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	ctx, span := t.conn.Tracer().Start(ctx, "fitz.kv.Get", trace.WithAttributes(
		attribute.String("fitz.route", t.route),
		attribute.Int64("fitz.tx_id", int64(t.txID)),
	))
	defer span.End()
	// Validate state
	if err := t.checkState(); err != nil {
		return nil, false, err
	}

	// Validate key
	if err := ValidateKeySize(key); err != nil {
		return nil, false, err
	}

	// Encode request per CLIENT_SPEC.md
	writer, err := getPayloadWriter(t.txID, t.route, key)
	if err != nil {
		return nil, false, fmt.Errorf("encode get: %w", err)
	}

	// Send request
	resp, err := t.conn.SendRequestWithWriter(ctx, protocol.MessageTypeKvGet, writer)
	if err != nil {
		return nil, false, fmt.Errorf("get request failed: %w", err)
	}

	// Parse standard response status
	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return nil, false, fmt.Errorf("get failed: %w", mapKVError(err))
	}
	if !success {
		return nil, false, fmt.Errorf("get failed: unexpected status")
	}

	// Parse GET-specific response: [found][value_len?][value?]
	if len(remaining) < 1 {
		return nil, false, fmt.Errorf("invalid get response: expected at least 1 byte for found flag")
	}

	found := remaining[0] == 1

	if !found {
		return nil, false, nil // Key not found (normal case, not an error)
	}

	// Extract value
	if len(remaining) < 5 { // found(1) + value_len(4)
		return nil, false, fmt.Errorf("invalid get response: missing value length")
	}

	valueLen, offset, err := connection.ReadU32BE(remaining, 1)
	if err != nil {
		return nil, false, fmt.Errorf("parse value length: %w", err)
	}

	if len(remaining) < offset+int(valueLen) {
		return nil, false, fmt.Errorf("invalid get response: truncated value")
	}

	value := make([]byte, valueLen)
	copy(value, remaining[offset:offset+int(valueLen)])

	return value, true, nil
}

// Put upserts a key/value pair (create or overwrite).
func (t *transaction) Put(ctx context.Context, key, value []byte) error {
	ctx, span := t.conn.Tracer().Start(ctx, "fitz.kv.Put", trace.WithAttributes(
		attribute.String("fitz.route", t.route),
		attribute.Int64("fitz.tx_id", int64(t.txID)),
	))
	defer span.End()
	// Validate state
	if err := t.checkState(); err != nil {
		return err
	}
	if err := t.checkWritable(); err != nil {
		return err
	}

	// Validate inputs
	if err := ValidateKeySize(key); err != nil {
		return err
	}
	if err := ValidateValueSize(value); err != nil {
		return err
	}

	// Encode request
	writer, err := putPayloadWriter(t.txID, t.route, key, value)
	if err != nil {
		return fmt.Errorf("encode put: %w", err)
	}

	// Send request
	resp, err := t.conn.SendRequestWithWriter(ctx, protocol.MessageTypeKvPut, writer)
	if err != nil {
		return fmt.Errorf("put request failed: %w", err)
	}

	// Parse standard response
	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return fmt.Errorf("put failed: %w", mapKVError(err))
	}
	if !success {
		return fmt.Errorf("put failed: unexpected status")
	}

	return nil
}

// Insert creates a new key/value pair. Fails if key already exists.
func (t *transaction) Insert(ctx context.Context, key, value []byte) error {
	ctx, span := t.conn.Tracer().Start(ctx, "fitz.kv.Insert", trace.WithAttributes(
		attribute.String("fitz.route", t.route),
		attribute.Int64("fitz.tx_id", int64(t.txID)),
	))
	defer span.End()
	// Validate state
	if err := t.checkState(); err != nil {
		return err
	}
	if err := t.checkWritable(); err != nil {
		return err
	}

	// Validate inputs
	if err := ValidateKeySize(key); err != nil {
		return err
	}
	if err := ValidateValueSize(value); err != nil {
		return err
	}

	// Encode request (wire format same as PUT)
	writer, err := insertPayloadWriter(t.txID, t.route, key, value)
	if err != nil {
		return fmt.Errorf("encode insert: %w", err)
	}

	// Send request
	resp, err := t.conn.SendRequestWithWriter(ctx, protocol.MessageTypeKvInsert, writer)
	if err != nil {
		return fmt.Errorf("insert request failed: %w", err)
	}

	// Parse standard response
	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return fmt.Errorf("insert failed: %w", mapKVError(err))
	}
	if !success {
		return fmt.Errorf("insert failed: unexpected status")
	}

	return nil
}

// Delete removes a key. Idempotent (deleting non-existent key succeeds).
func (t *transaction) Delete(ctx context.Context, key []byte) error {
	ctx, span := t.conn.Tracer().Start(ctx, "fitz.kv.Delete", trace.WithAttributes(
		attribute.String("fitz.route", t.route),
		attribute.Int64("fitz.tx_id", int64(t.txID)),
	))
	defer span.End()
	// Validate state
	if err := t.checkState(); err != nil {
		return err
	}
	if err := t.checkWritable(); err != nil {
		return err
	}

	// Validate key
	if err := ValidateKeySize(key); err != nil {
		return err
	}

	// Encode request
	writer, err := deletePayloadWriter(t.txID, t.route, key)
	if err != nil {
		return fmt.Errorf("encode delete: %w", err)
	}

	// Send request
	resp, err := t.conn.SendRequestWithWriter(ctx, protocol.MessageTypeKvDelete, writer)
	if err != nil {
		return fmt.Errorf("delete request failed: %w", err)
	}

	// Parse standard response
	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return fmt.Errorf("delete failed: %w", mapKVError(err))
	}
	if !success {
		return fmt.Errorf("delete failed: unexpected status")
	}

	return nil
}

// DeleteRange removes all keys in range [startKey, endKey) (exclusive end).
func (t *transaction) DeleteRange(ctx context.Context, startKey, endKey []byte) error {
	ctx, span := t.conn.Tracer().Start(ctx, "fitz.kv.DeleteRange", trace.WithAttributes(
		attribute.String("fitz.route", t.route),
		attribute.Int64("fitz.tx_id", int64(t.txID)),
	))
	defer span.End()
	// Validate state
	if err := t.checkState(); err != nil {
		return err
	}
	if err := t.checkWritable(); err != nil {
		return err
	}

	// Validate keys
	if err := ValidateKeySize(startKey); err != nil {
		return fmt.Errorf("invalid start key: %w", err)
	}
	if err := ValidateKeySize(endKey); err != nil {
		return fmt.Errorf("invalid end key: %w", err)
	}

	// Encode request
	writer, err := deleteRangePayloadWriter(t.txID, t.route, startKey, endKey)
	if err != nil {
		return fmt.Errorf("encode delete_range: %w", err)
	}

	// Send request
	resp, err := t.conn.SendRequestWithWriter(ctx, protocol.MessageTypeKvDeleteRange, writer)
	if err != nil {
		return fmt.Errorf("delete_range request failed: %w", err)
	}

	// Parse standard response
	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return fmt.Errorf("delete_range failed: %w", mapKVError(err))
	}
	if !success {
		return fmt.Errorf("delete_range failed: unexpected status")
	}

	return nil
}

// Scan returns an iterator over key/value pairs matching the query.
// Returns (iterator, hasMore, error) where hasMore indicates server has additional results.
//
// Per CLIENT_SPEC.md: SCAN returns batch results in one response (not streaming).
// Use SliceIterator for simple in-memory iteration.
func (t *transaction) Scan(ctx context.Context, query ScanQuery) (iter.Iterator[KVPair], bool, error) {
	ctx, span := t.conn.Tracer().Start(ctx, "fitz.kv.Scan", trace.WithAttributes(
		attribute.String("fitz.route", t.route),
		attribute.Int64("fitz.tx_id", int64(t.txID)),
	))
	defer span.End()
	// Validate state
	if err := t.checkState(); err != nil {
		return nil, false, err
	}

	// Encode request
	writer, err := scanPayloadWriter(t.txID, t.route, query)
	if err != nil {
		return nil, false, fmt.Errorf("encode scan: %w", err)
	}

	// Send request
	resp, err := t.conn.SendRequestWithWriter(ctx, protocol.MessageTypeKvScan, writer)
	if err != nil {
		return nil, false, fmt.Errorf("scan request failed: %w", err)
	}

	// Parse standard response status
	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return nil, false, fmt.Errorf("scan failed: %w", mapKVError(err))
	}
	if !success {
		return nil, false, fmt.Errorf("scan failed: unexpected status")
	}

	// Parse SCAN response: [item_count][items...][has_more]
	pairs, hasMore, err := parseScanResponse(remaining)
	if err != nil {
		return nil, false, fmt.Errorf("parse scan response: %w", err)
	}

	// Return SliceIterator for batch results
	return iter.NewSliceIterator(pairs), hasMore, nil
}

// parseScanResponse parses batch SCAN results per CLIENT_SPEC.md.
// Response: [item_count][key_len][key][value_len][value]...[has_more]
func parseScanResponse(remaining []byte) ([]KVPair, bool, error) {
	if len(remaining) < 4 { // item_count(4)
		return nil, false, fmt.Errorf("invalid scan response: too short")
	}

	itemCount, offset, err := connection.ReadU32BE(remaining, 0)
	if err != nil {
		return nil, false, fmt.Errorf("parse item_count: %w", err)
	}

	pairs := make([]KVPair, 0, itemCount)

	for i := uint32(0); i < itemCount; i++ {
		// Parse key
		if offset+4 > len(remaining) {
			return nil, false, fmt.Errorf("truncated key length at item %d", i)
		}
		keyLen, newOffset, err := connection.ReadU32BE(remaining, offset)
		if err != nil {
			return nil, false, fmt.Errorf("parse key length: %w", err)
		}
		offset = newOffset

		if offset+int(keyLen) > len(remaining) {
			return nil, false, fmt.Errorf("truncated key at item %d", i)
		}
		key := make([]byte, keyLen)
		copy(key, remaining[offset:offset+int(keyLen)])
		offset += int(keyLen)

		// Parse value
		if offset+4 > len(remaining) {
			return nil, false, fmt.Errorf("truncated value length at item %d", i)
		}
		valueLen, newOffset, err := connection.ReadU32BE(remaining, offset)
		if err != nil {
			return nil, false, fmt.Errorf("parse value length: %w", err)
		}
		offset = newOffset

		if offset+int(valueLen) > len(remaining) {
			return nil, false, fmt.Errorf("truncated value at item %d", i)
		}
		value := make([]byte, valueLen)
		copy(value, remaining[offset:offset+int(valueLen)])
		offset += int(valueLen)

		pairs = append(pairs, KVPair{Key: key, Value: value})
	}

	// Parse has_more flag
	if offset+1 > len(remaining) {
		return nil, false, fmt.Errorf("missing has_more flag")
	}
	var hasMore bool
	switch remaining[offset] {
	case 0:
		hasMore = false
	case 1:
		hasMore = true
	default:
		return nil, false, fmt.Errorf("invalid has_more flag: %d", remaining[offset])
	}
	offset++
	if offset != len(remaining) {
		return nil, false, fmt.Errorf("unexpected trailing bytes: %d", len(remaining)-offset)
	}

	return pairs, hasMore, nil
}

// Commit finalizes the transaction durably.
func (t *transaction) Commit(ctx context.Context) error {
	ctx, span := t.conn.Tracer().Start(ctx, "fitz.kv.Commit", trace.WithAttributes(
		attribute.String("fitz.route", t.route),
		attribute.Int64("fitz.tx_id", int64(t.txID)),
	))
	defer span.End()
	// Validate state
	if err := t.checkState(); err != nil {
		return err
	}
	if err := t.checkWritable(); err != nil {
		return err
	}

	// Encode request
	// Send request
	resp, err := t.conn.SendRequestWithWriter(ctx, protocol.MessageTypeKvCommit, commitPayloadWriter(t.txID, t.route))
	if err != nil {
		return fmt.Errorf("commit request failed: %w", err)
	}

	// Parse standard response
	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return fmt.Errorf("commit failed: %w", mapKVError(err))
	}
	if !success {
		return fmt.Errorf("commit failed: unexpected status")
	}

	// Mark transaction as committed
	t.committed.Store(true)

	return nil
}

// Rollback aborts the transaction, discarding all changes.
func (t *transaction) Rollback(ctx context.Context) error {
	ctx, span := t.conn.Tracer().Start(ctx, "fitz.kv.Rollback", trace.WithAttributes(
		attribute.String("fitz.route", t.route),
		attribute.Int64("fitz.tx_id", int64(t.txID)),
	))
	defer span.End()
	// Validate state (allow rollback even if committed)
	if t.rolledback.Load() {
		return fmt.Errorf("transaction already rolled back")
	}

	// Encode request
	// Send request
	resp, err := t.conn.SendRequestWithWriter(ctx, protocol.MessageTypeKvRollback, rollbackPayloadWriter(t.txID, t.route))
	if err != nil {
		return fmt.Errorf("rollback request failed: %w", err)
	}

	// Parse standard response
	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return fmt.Errorf("rollback failed: %w", mapKVError(err))
	}
	if !success {
		return fmt.Errorf("rollback failed: unexpected status")
	}

	// Mark transaction as rolled back
	t.rolledback.Store(true)

	return nil
}

// checkState validates transaction state before operations.
func (t *transaction) checkState() error {
	if t.committed.Load() {
		return fmt.Errorf("transaction already committed")
	}
	if t.rolledback.Load() {
		return fmt.Errorf("transaction already rolled back")
	}
	return nil
}

func (t *transaction) checkWritable() error {
	if t.readOnly {
		return ErrReadOnlyTransaction
	}
	return nil
}
