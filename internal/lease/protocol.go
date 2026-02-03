package lease

import "errors"

// Domain-specific errors (mapped from broker error responses).
var (
	ErrLeaseHeld     = errors.New("lease held")
	ErrInvalidFence  = errors.New("invalid fencing token")
	ErrLeaseExpired  = errors.New("lease expired")
	ErrLeaseNotFound = errors.New("lease not found")
)

// Wire operation codes for Lease domain. The canonical spec uses 400–403,
// here we store the low-byte values as uint8 for use on the transport frame
// Type field (server and client must agree on the encoding).
const (
	LeaseAcquire uint8 = 400 % 256 // 144
	LeaseRenew   uint8 = 401 % 256 // 145
	LeaseRelease uint8 = 402 % 256 // 146
	LeaseQuery   uint8 = 403 % 256 // 147
)
