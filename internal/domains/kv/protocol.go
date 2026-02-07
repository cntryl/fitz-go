package kv

import (
	"errors"
	"strings"
)

// Wire opcodes for KV domain (per CLIENT_SPEC.md: 100+).
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

// Domain-specific errors.
var (
	ErrNotFound            = errors.New("key not found")
	ErrKeyExists           = errors.New("key already exists")
	ErrConcurrencyConflict = errors.New("concurrency conflict")
	ErrInvalidRange        = errors.New("invalid key range")
	ErrKeyTooLarge         = errors.New("key too large")
	ErrValueTooLarge       = errors.New("value too large")
	ErrTransactionAborted  = errors.New("transaction aborted")
)

// mapKVError maps a broker error message to a domain-specific Go error.
func mapKVError(msg string) error {
	l := strings.ToLower(msg)
	switch {
	case strings.Contains(l, "not found"):
		return ErrNotFound
	case strings.Contains(l, "exists") || strings.Contains(l, "already"):
		return ErrKeyExists
	case strings.Contains(l, "conflict") || strings.Contains(l, "concurrency"):
		return ErrConcurrencyConflict
	case strings.Contains(l, "range"):
		return ErrInvalidRange
	case strings.Contains(l, "key too large"):
		return ErrKeyTooLarge
	case strings.Contains(l, "value too large"):
		return ErrValueTooLarge
	case strings.Contains(l, "abort"):
		return ErrTransactionAborted
	default:
		return errors.New(msg)
	}
}
