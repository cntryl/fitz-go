package rpc

import (
	"errors"
	"strings"
)

// Wire opcodes for RPC domain (per CLIENT_SPEC.md). Values are low-byte uint8 equivalents.
const (
	RPCSubscribeWorker   uint8 = 44
	RPCUnsubscribeWorker uint8 = 45
	RPCRequest           uint8 = 46
	RPCResponse          uint8 = 47
	RPCAck               uint8 = 48
)

// Domain-specific errors.
var (
	ErrNoWorkers  = errors.New("no workers available")
	ErrRPCTimeout = errors.New("rpc timeout")
)

// mapRPCError maps a broker error message to a domain-specific Go error.
func mapRPCError(msg string) error {
	l := strings.ToLower(msg)
	switch {
	case strings.Contains(l, "no workers"):
		return ErrNoWorkers
	default:
		return errors.New(msg)
	}
}
