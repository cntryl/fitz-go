package fitz

import (
	"context"

	internalkv "github.com/cntryl/fitz-go/internal/domains/kv"
)

type KVPair struct {
	Key   []byte
	Value []byte
}

type KVScanQuery struct {
	StartKey []byte
	EndKey   []byte
	Limit    uint32
	Reverse  bool
}

type KVDurabilityMode uint8

const (
	KVDurabilityBuffered KVDurabilityMode = KVDurabilityMode(internalkv.DurabilityBuffered)
	KVDurabilitySync     KVDurabilityMode = KVDurabilityMode(internalkv.DurabilitySync)
)

type KVMode uint8

const (
	KVModeReadOnly  KVMode = KVMode(internalkv.TxModeReadOnly)
	KVModeReadWrite KVMode = KVMode(internalkv.TxModeReadWrite)
)

type kvBeginConfig struct {
	mode KVMode
}

type KVBeginOption func(*kvBeginConfig)

func WithKVMode(mode KVMode) KVBeginOption {
	return func(cfg *kvBeginConfig) {
		cfg.mode = mode
	}
}

type KVGetResult struct {
	Found bool
	Value []byte
}

type KVTx interface {
	Get(ctx context.Context, key []byte) (KVGetResult, error)
	Scan(ctx context.Context, query KVScanQuery) (Iterator[KVPair], bool, error)
	Put(ctx context.Context, key, value []byte) error
	Insert(ctx context.Context, key, value []byte) error
	Delete(ctx context.Context, key []byte) error
	DeleteRange(ctx context.Context, startKey, endKey []byte) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type KVClient interface {
	Begin(ctx context.Context, route string, durability KVDurabilityMode, opts ...KVBeginOption) (KVTx, error)
}

type kvClient struct {
	inner internalkv.Client
}

type kvTx struct {
	inner internalkv.Tx
}

type kvPairIterator struct {
	inner   Iterator[internalkv.KVPair]
	current KVPair
}

func (c *kvClient) Begin(ctx context.Context, route string, durability KVDurabilityMode, opts ...KVBeginOption) (KVTx, error) {
	cfg := kvBeginConfig{
		mode: KVModeReadWrite,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	tx, err := c.inner.Begin(
		ctx,
		route,
		uint8(durability),
		internalkv.WithMode(uint8(cfg.mode)),
	)
	if err != nil {
		return nil, err
	}
	return &kvTx{inner: tx}, nil
}

func (t *kvTx) Get(ctx context.Context, key []byte) (KVGetResult, error) {
	value, found, err := t.inner.Get(ctx, key)
	if err != nil {
		return KVGetResult{}, err
	}
	return KVGetResult{
		Found: found,
		Value: value,
	}, nil
}

func (t *kvTx) Scan(ctx context.Context, query KVScanQuery) (Iterator[KVPair], bool, error) {
	iter, hasMore, err := t.inner.Scan(ctx, internalkv.ScanQuery{
		StartKey: query.StartKey,
		EndKey:   query.EndKey,
		Limit:    query.Limit,
		Reverse:  query.Reverse,
	})
	if err != nil {
		return nil, false, err
	}
	return &kvPairIterator{inner: iter}, hasMore, nil
}

func (t *kvTx) Put(ctx context.Context, key, value []byte) error {
	return t.inner.Put(ctx, key, value)
}

func (t *kvTx) Insert(ctx context.Context, key, value []byte) error {
	return t.inner.Insert(ctx, key, value)
}

func (t *kvTx) Delete(ctx context.Context, key []byte) error {
	return t.inner.Delete(ctx, key)
}

func (t *kvTx) DeleteRange(ctx context.Context, startKey, endKey []byte) error {
	return t.inner.DeleteRange(ctx, startKey, endKey)
}

func (t *kvTx) Commit(ctx context.Context) error {
	return t.inner.Commit(ctx)
}

func (t *kvTx) Rollback(ctx context.Context) error {
	return t.inner.Rollback(ctx)
}

func (it *kvPairIterator) Next() bool {
	if !it.inner.Next() {
		return false
	}
	value := it.inner.Value()
	it.current = KVPair{
		Key:   append([]byte(nil), value.Key...),
		Value: append([]byte(nil), value.Value...),
	}
	return true
}

func (it *kvPairIterator) Value() KVPair {
	return it.current
}

func (it *kvPairIterator) Err() error {
	return it.inner.Err()
}

func (it *kvPairIterator) Close() error {
	return it.inner.Close()
}

var (
	ErrKVNotFound            = internalkv.ErrNotFound
	ErrKVKeyExists           = internalkv.ErrKeyExists
	ErrKVConcurrencyConflict = internalkv.ErrConcurrencyConflict
	ErrKVInvalidRange        = internalkv.ErrInvalidRange
	ErrKVKeyTooLarge         = internalkv.ErrKeyTooLarge
	ErrKVValueTooLarge       = internalkv.ErrValueTooLarge
	ErrKVTransactionAborted  = internalkv.ErrTransactionAborted
	ErrKVReadOnly            = internalkv.ErrReadOnlyTransaction
)
