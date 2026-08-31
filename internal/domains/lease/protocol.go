package lease

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/cntryl/fitz-go/v2/internal/core/connection"
	"github.com/cntryl/fitz-go/v2/internal/core/encoding"
	coreerrors "github.com/cntryl/fitz-go/v2/internal/core/errors"
	"github.com/cntryl/fitz-go/v2/internal/core/types"
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
//   - ErrInvalidListCursor: LIST cursor is unknown, evicted, or reused with a
//     different pattern/RouteFamily than the scan it was issued for.
//   - ErrInvalidListPattern: LIST pattern fails the wildcard selector grammar.
var (
	ErrLeaseHeld          = errors.New("lease held")
	ErrLeaseQueued        = errors.New("lease queued")
	ErrInvalidFence       = errors.New("invalid fencing token")
	ErrLeaseExpired       = errors.New("lease expired")
	ErrLeaseNotFound      = errors.New("lease not found")
	ErrInvalidListCursor  = errors.New("invalid list cursor")
	ErrInvalidListPattern = errors.New("invalid list pattern")
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
		case coreerrors.LeaseInvalidListCursor:
			return ErrInvalidListCursor
		case coreerrors.LeaseInvalidListPattern:
			return ErrInvalidListPattern
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
// Wire format: [string route][string owner_id][u64 ttl_seconds][u32 wait_seconds].
func encodeLeaseAcquire(route, ownerID string, ttlSeconds uint64, waitSeconds uint32) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteRoute(buf, ownerID)
		encoding.WriteU64(buf, ttlSeconds)
		encoding.WriteU32(buf, waitSeconds)
	}), nil
}

// encodeLeaseRenew encodes a LEASE_RENEW request per CLIENT_SPEC.md.
// Wire format: [string resource][string client_id][u64 fence_token][u64 ttl_seconds]
func encodeLeaseRenew(resource, ownerID string, fenceToken uint64, ttlSeconds uint64) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, resource)
		encoding.WriteRoute(buf, ownerID)
		encoding.WriteU64(buf, fenceToken)
		encoding.WriteU64(buf, ttlSeconds)
	}), nil
}

// encodeLeaseRelease encodes a LEASE_RELEASE request per CLIENT_SPEC.md.
// Wire format: [string resource][string client_id][u64 fence_token]
func encodeLeaseRelease(resource, ownerID string, fenceToken uint64) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, resource)
		encoding.WriteRoute(buf, ownerID)
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

func leaseAcquirePayloadWriter(route, ownerID string, ttlSeconds uint64, waitSeconds uint32) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteRoute(buf, ownerID)
		encoding.WriteU64(buf, ttlSeconds)
		encoding.WriteU32(buf, waitSeconds)
	}
}

func leaseRenewPayloadWriter(resource, ownerID string, fenceToken uint64, ttlSeconds uint64) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, resource)
		encoding.WriteRoute(buf, ownerID)
		encoding.WriteU64(buf, fenceToken)
		encoding.WriteU64(buf, ttlSeconds)
	}
}

func leaseReleasePayloadWriter(resource, ownerID string, fenceToken uint64) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, resource)
		encoding.WriteRoute(buf, ownerID)
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

// validateLeaseSubscribeSelector validates the wildcard grammar accepted by
// SUBSCRIBE/UNSUBSCRIBE (407/408) and LIST (410) patterns: exact three-segment
// routes, a whole-segment "*" in any of the three positions, and one or more
// "**" segments, so long as no two "**" segments are directly adjacent to
// each other (e.g. "lease://**/renderers/**" is valid; "lease://acme/**/**"
// is not).
//
// types.ValidateRegistrationPattern enforces the shared registration-pattern
// shape (scheme, segment count, whole-segment wildcards) but does not by
// itself reject adjacent "**" segments; per the broker's matcher grammar
// (src/runtime/matcher.rs), only directly-adjacent "**" segments are
// rejected, not multiple non-adjacent occurrences, so that case is rejected
// here explicitly.
func validateLeaseSubscribeSelector(route string) error {
	if err := types.ValidateRegistrationPattern(route, "lease", 3); err != nil {
		return err
	}
	segments := strings.Split(strings.TrimPrefix(route, "lease://"), "/")
	for i := 1; i < len(segments); i++ {
		if segments[i-1] == "**" && segments[i] == "**" {
			return fmt.Errorf("%w: adjacent ** segments are not allowed", types.ErrInvalidRouteShape)
		}
	}
	return nil
}

// leaseListCursor is the opaque server-issued pagination cursor for LIST.
type leaseListCursor struct {
	snapshotID uint64
	offset     uint32
}

// leaseListPayloadWriter encodes a LEASE_LIST (410) request.
// Wire format: [string pattern][u8 has_cursor][if 1: u64 snapshot_id][u32 offset]][u32 limit]
func leaseListPayloadWriter(pattern string, cursor *leaseListCursor, limit uint32) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, pattern)
		if cursor == nil {
			buf.WriteByte(0)
		} else {
			buf.WriteByte(1)
			encoding.WriteU64(buf, cursor.snapshotID)
			encoding.WriteU32(buf, cursor.offset)
		}
		encoding.WriteU32(buf, limit)
	}
}

// encodeLeaseList encodes a LEASE_LIST (410) request per the wire format above.
func encodeLeaseList(pattern string, cursor *leaseListCursor, limit uint32) ([]byte, error) {
	return encoding.EncodeWithBuffer(leaseListPayloadWriter(pattern, cursor, limit)), nil
}

// parseLeaseListResponse parses the success body of a LEASE_LIST (410) response
// (the payload remaining after the [u8 status=0] prefix has been stripped).
//
// Wire format:
//
//	[u32 item_count]
//	repeated item_count times:
//	  [string route][string owner_id][u64 holder_incarnation][string acquired_at][u64 expires_in_secs][u32 renewals]
//	[u8 has_next]
//	  if has_next == 1: [u64 snapshot_id][u32 offset]
func parseLeaseListResponse(remaining []byte) ([]LeaseEntry, *leaseListCursor, error) {
	itemCount, offset, err := connection.ReadU32BE(remaining, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("LIST response missing item_count: %w", err)
	}

	items := make([]LeaseEntry, 0, itemCount)
	for i := range itemCount {
		route, newOffset, err := connection.ReadString(remaining, offset)
		if err != nil {
			return nil, nil, fmt.Errorf("LIST response invalid route at item %d: %w", i, err)
		}
		offset = newOffset

		ownerID, newOffset, err := connection.ReadString(remaining, offset)
		if err != nil {
			return nil, nil, fmt.Errorf("LIST response invalid owner_id at item %d: %w", i, err)
		}
		offset = newOffset

		holderIncarnation, newOffset, err := connection.ReadU64BE(remaining, offset)
		if err != nil {
			return nil, nil, fmt.Errorf("LIST response invalid holder_incarnation at item %d: %w", i, err)
		}
		offset = newOffset

		acquiredAt, newOffset, err := connection.ReadString(remaining, offset)
		if err != nil {
			return nil, nil, fmt.Errorf("LIST response invalid acquired_at at item %d: %w", i, err)
		}
		offset = newOffset

		expiresInSecs, newOffset, err := connection.ReadU64BE(remaining, offset)
		if err != nil {
			return nil, nil, fmt.Errorf("LIST response invalid expires_in_secs at item %d: %w", i, err)
		}
		offset = newOffset

		renewals, newOffset, err := connection.ReadU32BE(remaining, offset)
		if err != nil {
			return nil, nil, fmt.Errorf("LIST response invalid renewals at item %d: %w", i, err)
		}
		offset = newOffset

		items = append(items, LeaseEntry{
			Route:             route,
			OwnerID:           ownerID,
			HolderIncarnation: holderIncarnation,
			AcquiredAt:        acquiredAt,
			ExpiresInSecs:     expiresInSecs,
			Renewals:          renewals,
		})
	}

	hasNext, offset, err := connection.ReadU8(remaining, offset)
	if err != nil {
		return nil, nil, fmt.Errorf("LIST response missing has_next: %w", err)
	}
	switch hasNext {
	case 0:
		if offset != len(remaining) {
			return nil, nil, fmt.Errorf("LIST response has trailing bytes: %d", len(remaining)-offset)
		}
		return items, nil, nil
	case 1:
		snapshotID, newOffset, err := connection.ReadU64BE(remaining, offset)
		if err != nil {
			return nil, nil, fmt.Errorf("LIST response missing cursor snapshot_id: %w", err)
		}
		offset = newOffset
		cursorOffset, newOffset, err := connection.ReadU32BE(remaining, offset)
		if err != nil {
			return nil, nil, fmt.Errorf("LIST response missing cursor offset: %w", err)
		}
		offset = newOffset
		if offset != len(remaining) {
			return nil, nil, fmt.Errorf("LIST response has trailing bytes: %d", len(remaining)-offset)
		}
		return items, &leaseListCursor{snapshotID: snapshotID, offset: cursorOffset}, nil
	default:
		return nil, nil, fmt.Errorf("LIST response invalid has_next flag: %d", hasNext)
	}
}
