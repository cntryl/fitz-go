package kv

import "errors"

// KV protocol constants and error codes.

// Standard error codes returned by the broker in KV operations.
var (
	ErrNotFound            = errors.New("key not found")
	ErrKeyExists           = errors.New("key already exists")
	ErrConcurrencyConflict = errors.New("concurrency conflict")
	ErrInvalidRange        = errors.New("invalid key range")
	ErrKeyTooLarge         = errors.New("key too large")
	ErrValueTooLarge       = errors.New("value too large")
	ErrTransactionAborted  = errors.New("transaction aborted")
)

// Channel constants (per CLIENT_SPEC.md).
const (
	ChannelControl  uint32 = 0
	ChannelPub      uint32 = 1
	ChannelSub      uint32 = 2
	ChannelRPC      uint32 = 3
	ChannelLease    uint32 = 4
	ChannelInternal uint32 = 5
	ChannelKV       uint32 = 6 // extended for KV domain
)

// Transport-agnostic wire codes for KV ops (per CLIENT_SPEC.md: 100+)
const (
	KVBegin       uint8 = 100
	KVCommit      uint8 = 101
	KVRollback    uint8 = 102
	KVGet         uint8 = 103
	KVPut         uint8 = 104
	KVInsert      uint8 = 105
	KVDelete      uint8 = 106
	KVDeleteRange uint8 = 107
	KVScan        uint8 = 108
)

// Frame type constants (per Fitz protocol). Use transport package shared Frame types.
const (
	FrameTypeAck uint8 = 2  // Acknowledgment
	FrameTypeErr uint8 = 3  // Error
	FrameTypeDAT uint8 = 12 // Data
)
