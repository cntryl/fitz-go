package fitz

import (
	"context"

	internalkv "github.com/cntryl/fitz-go/internal/domains/kv"
)

type KVPair = internalkv.KVPair
type KVScanQuery = internalkv.ScanQuery
type KVDurabilityMode = uint8
type KVBeginOption = internalkv.BeginOption

const (
	KVDurabilityBuffered KVDurabilityMode = internalkv.DurabilityBuffered
	KVDurabilitySync     KVDurabilityMode = internalkv.DurabilitySync
)

func WithKVDurability(mode KVDurabilityMode) KVBeginOption {
	return internalkv.WithDurability(uint8(mode))
}

type KVGetResult struct {
	Found bool
	Value []byte
}

type KVReadTx interface {
	Get(ctx context.Context, key []byte) (KVGetResult, error)
	Scan(ctx context.Context, query KVScanQuery) (Iterator[KVPair], bool, error)
}

type KVTx interface {
	KVReadTx
	Put(ctx context.Context, key, value []byte) error
	Insert(ctx context.Context, key, value []byte) error
	Delete(ctx context.Context, key []byte) error
	DeleteRange(ctx context.Context, startKey, endKey []byte) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type KVClient interface {
	Begin(ctx context.Context, route string, opts ...KVBeginOption) (KVTx, error)
	BeginReadOnly(ctx context.Context, route string) (KVReadTx, error)
}

type kvClient struct {
	inner internalkv.Client
}

type kvReadTx struct {
	inner internalkv.ReadTx
}

type kvTx struct {
	inner internalkv.Tx
}

func (c *kvClient) Begin(ctx context.Context, route string, opts ...KVBeginOption) (KVTx, error) {
	tx, err := c.inner.Begin(ctx, route, opts...)
	if err != nil {
		return nil, err
	}
	return &kvTx{inner: tx}, nil
}

func (c *kvClient) BeginReadOnly(ctx context.Context, route string) (KVReadTx, error) {
	tx, err := c.inner.BeginRead(ctx, route)
	if err != nil {
		return nil, err
	}
	return &kvReadTx{inner: tx}, nil
}

func (t *kvReadTx) Get(ctx context.Context, key []byte) (KVGetResult, error) {
	value, found, err := t.inner.Get(ctx, key)
	if err != nil {
		return KVGetResult{}, err
	}
	return KVGetResult{
		Found: found,
		Value: value,
	}, nil
}

func (t *kvReadTx) Scan(ctx context.Context, query KVScanQuery) (Iterator[KVPair], bool, error) {
	return t.inner.Scan(ctx, query)
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
	return t.inner.Scan(ctx, query)
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

var (
	ErrKVNotFound            = internalkv.ErrNotFound
	ErrKVKeyExists           = internalkv.ErrKeyExists
	ErrKVConcurrencyConflict = internalkv.ErrConcurrencyConflict
	ErrKVInvalidRange        = internalkv.ErrInvalidRange
	ErrKVKeyTooLarge         = internalkv.ErrKeyTooLarge
	ErrKVValueTooLarge       = internalkv.ErrValueTooLarge
	ErrKVTransactionAborted  = internalkv.ErrTransactionAborted
)
