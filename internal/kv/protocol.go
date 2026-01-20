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

// Frame type constants (per Fitz protocol).
const (
	FrameTypeReq  uint8 = 10 // Request (RPC-like)
	FrameTypeResp uint8 = 11 // Response (RPC-like)
	FrameTypeAck  uint8 = 2  // Acknowledgment
	FrameTypeErr  uint8 = 3  // Error
	FrameTypeDAT  uint8 = 12 // Data
)
