package lease

import (
	"bytes"
	"errors"
	"strings"

	"github.com/cntryl/fitz-go/v2/internal/core/encoding"
	coreerrors "github.com/cntryl/fitz-go/v2/internal/core/errors"
)

// Wire opcodes for Lease domain (per CLIENT_SPEC.md 400–403).
const (
	LeaseAcquire uint16 = 400
	LeaseRenew   uint16 = 401
	LeaseRelease uint16 = 402
	LeaseQuery   uint16 = 403
)

// Domain-specific errors (mapped from broker error responses).
//   - ErrLeaseHeld: acquire failed because the lease is held by another owner.
//   - ErrLeaseQueued: request queued behind the current holder.
//   - ErrInvalidFence: renew or release used an invalid or wrong fencing token.
//   - ErrLeaseExpired: the lease has already expired.
//   - ErrLeaseNotFound: no lease exists for the route.
var (
	ErrLeaseHeld     = errors.New("lease held")
	ErrLeaseQueued   = errors.New("lease queued")
	ErrInvalidFence  = errors.New("invalid fencing token")
	ErrLeaseExpired  = errors.New("lease expired")
	ErrLeaseNotFound = errors.New("lease not found")
)

// mapLeaseError maps a broker error to a domain-specific Go error.
func mapLeaseError(err error) error {
	if err == nil {
		return nil
	}

	var domainErr *coreerrors.DomainError
	if errors.As(err, &domainErr) {
		switch uint32(domainErr.Code) {
		case coreerrors.LeaseHeld:
			return ErrLeaseHeld
		case coreerrors.LeaseInvalidFence:
			return ErrInvalidFence
		case coreerrors.LeaseExpired:
			return ErrLeaseExpired
		case coreerrors.LeaseNotFound:
			return ErrLeaseNotFound
		default:
			return err
		}
	}

	lower := strings.ToLower(err.Error())
	switch {
	case strings.Contains(lower, "held"):
		return ErrLeaseHeld
	case strings.Contains(lower, "invalid") && (strings.Contains(lower, "fence") || strings.Contains(lower, "token")):
		return ErrInvalidFence
	case strings.Contains(lower, "expired"):
		return ErrLeaseExpired
	case strings.Contains(lower, "not found"):
		return ErrLeaseNotFound
	default:
		return err
	}
}

// encodeLeaseAcquire encodes a LEASE_ACQUIRE request per CLIENT_SPEC.md.
// Wire format: [string route][string client_id][u64 ttl_seconds]
// The client_id is optional (empty string for auto-assignment by server).
func encodeLeaseAcquire(route string, ttlSeconds uint64) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteRoute(buf, "") // client_id (empty = server assigns)
		encoding.WriteU64(buf, ttlSeconds)
	}), nil
}

// encodeLeaseRenew encodes a LEASE_RENEW request per CLIENT_SPEC.md.
// Wire format: [string resource][string client_id][u64 fence_token][u64 ttl_seconds]
func encodeLeaseRenew(resource string, fenceToken uint64, ttlSeconds uint64) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, resource)
		encoding.WriteRoute(buf, "") // client_id (empty = use existing)
		encoding.WriteU64(buf, fenceToken)
		encoding.WriteU64(buf, ttlSeconds)
	}), nil
}

// encodeLeaseRelease encodes a LEASE_RELEASE request per CLIENT_SPEC.md.
// Wire format: [string resource][string client_id][u64 fence_token]
func encodeLeaseRelease(resource string, fenceToken uint64) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, resource)
		encoding.WriteRoute(buf, "") // client_id (empty = use existing)
		encoding.WriteU64(buf, fenceToken)
	}), nil
}

// encodeLeaseQuery encodes a LEASE_QUERY request per CLIENT_SPEC.md.
// Wire format: [string route]
func encodeLeaseQuery(route string) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
	}), nil
}

// Payload writer helpers for zero-copy frame encoding

func leaseAcquirePayloadWriter(route string, ttlSeconds uint64) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteRoute(buf, "") // client_id (empty = server assigns)
		encoding.WriteU64(buf, ttlSeconds)
	}
}

func leaseRenewPayloadWriter(resource string, fenceToken uint64, ttlSeconds uint64) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, resource)
		encoding.WriteRoute(buf, "") // client_id (empty = use existing)
		encoding.WriteU64(buf, fenceToken)
		encoding.WriteU64(buf, ttlSeconds)
	}
}

func leaseReleasePayloadWriter(resource string, fenceToken uint64) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, resource)
		encoding.WriteRoute(buf, "") // client_id (empty = use existing)
		encoding.WriteU64(buf, fenceToken)
	}
}

func leaseQueryPayloadWriter(route string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
	}
}

// Subscription payload writers for SUBSCRIBE/UNSUBSCRIBE

func subscribePayloadWriter(route string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
	}
}

func unsubscribePayloadWriter(route string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
	}
}
