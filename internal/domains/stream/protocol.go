package stream

import (
	"errors"
	"strings"
)

// Wire opcodes for Stream domain (per CLIENT_SPEC.md). Values are low-byte uint8 equivalents.
const (
	StreamBegin       uint8 = 200
	StreamAppend      uint8 = 201
	StreamCommit      uint8 = 202
	StreamRollback    uint8 = 203
	StreamRead        uint8 = 204
	StreamLast        uint8 = 205
	StreamGetMetadata uint8 = 206
)

// Domain-specific errors.
var (
	ErrStreamNotFound  = errors.New("stream not found")
	ErrStreamConflict  = errors.New("stream conflict")
	ErrStreamReadError = errors.New("stream read error")
)

// mapStreamError maps a broker error message to a domain-specific Go error.
func mapStreamError(msg string) error {
	l := strings.ToLower(msg)
	switch {
	case strings.Contains(l, "not found"):
		return ErrStreamNotFound
	case strings.Contains(l, "conflict"):
		return ErrStreamConflict
	default:
		return errors.New(msg)
	}
}
