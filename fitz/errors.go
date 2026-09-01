package fitz

import (
	"errors"

	coreerrors "github.com/cntryl/fitz-go/v2/internal/core/errors"
	"github.com/cntryl/fitz-go/v2/internal/core/transport"
)

// Error code constants mirror the canonical Fitz server error registry.
// Use these with the DomainError.Code field for code-based error handling:
//
//	var de *DomainError
//	if errors.As(err, &de) && de.Code == ErrCodeKvKeyNotFound {
//	    // handle missing key
//	}
const (
	// KV domain (1000-1099)
	ErrCodeKvTransactionNotFound = uint32(coreerrors.KvTransactionNotFound)
	ErrCodeKvInvalidMode         = uint32(coreerrors.KvInvalidMode)
	ErrCodeKvKeyNotFound         = uint32(coreerrors.KvKeyNotFound)
	ErrCodeKvIsolationConflict   = uint32(coreerrors.KvIsolationConflict)
	ErrCodeKvWriteInReadonly     = uint32(coreerrors.KvWriteInReadonly)
	ErrCodeKvKeyExists           = uint32(coreerrors.KvKeyExists)
	ErrCodeKvInvalidRoute        = uint32(coreerrors.KvInvalidRoute)
	ErrCodeKvRealmMismatch       = uint32(coreerrors.KvRealmMismatch)
	ErrCodeKvBackendError        = uint32(coreerrors.KvBackendError)
	ErrCodeKvTransactionAborted  = uint32(coreerrors.KvTransactionAborted)
	ErrCodeKvUnauthorized        = uint32(coreerrors.KvUnauthorized)
	ErrCodeKvInvalidSubscription = uint32(coreerrors.KvInvalidSubscription)
	ErrCodeKvSubscriptionLimit   = uint32(coreerrors.KvSubscriptionLimit)

	// Stream domain (2000-2099)
	ErrCodeStreamConcurrencyConflict = uint32(coreerrors.StreamConcurrencyConflict)
	ErrCodeStreamOffsetTooFarAhead   = uint32(coreerrors.StreamOffsetTooFarAhead)
	ErrCodeStreamInvalidReadBound    = uint32(coreerrors.StreamInvalidReadBound)
	ErrCodeStreamReadBeyondWatermark = uint32(coreerrors.StreamReadBeyondWatermark)
	ErrCodeStreamResourceNotFound    = uint32(coreerrors.StreamResourceNotFound)
	ErrCodeStreamInvalidSubscription = uint32(coreerrors.StreamInvalidSubscription)
	ErrCodeStreamSubscriptionLimit   = uint32(coreerrors.StreamSubscriptionLimit)

	// Notice domain (3000-3099)
	ErrCodeNoticeInvalidRoute      = uint32(coreerrors.NoticeInvalidRoute)
	ErrCodeNoticeInvalidPattern    = uint32(coreerrors.NoticeInvalidPattern)
	ErrCodeNoticeSubscriptionLimit = uint32(coreerrors.NoticeSubscriptionLimit)
	ErrCodeNoticeTransportClosed   = uint32(coreerrors.NoticeTransportClosed)

	// Queue domain (4000-4099)
	ErrCodeQueueInvalidToken        = uint32(coreerrors.QueueInvalidToken)
	ErrCodeQueueLeaseExpired        = uint32(coreerrors.QueueLeaseExpired)
	ErrCodeQueueMessageNotFound     = uint32(coreerrors.QueueMessageNotFound)
	ErrCodeQueueNotFound            = uint32(coreerrors.QueueNotFound)
	ErrCodeQueueFull                = uint32(coreerrors.QueueFull)
	ErrCodeQueueInvalidSubscription = uint32(coreerrors.QueueInvalidSubscription)
	ErrCodeQueueSubscriptionLimit   = uint32(coreerrors.QueueSubscriptionLimit)

	// Lease domain (5000-5099)
	ErrCodeLeaseHeld                     = uint32(coreerrors.LeaseHeld)
	ErrCodeLeaseInvalidFence             = uint32(coreerrors.LeaseInvalidFence)
	ErrCodeLeaseExpired                  = uint32(coreerrors.LeaseExpired)
	ErrCodeLeaseNotFound                 = uint32(coreerrors.LeaseNotFound)
	ErrCodeLeaseBadRequest               = uint32(coreerrors.LeaseBadRequest)
	ErrCodeLeaseInvalidSubscriptionRoute = uint32(coreerrors.LeaseInvalidSubscriptionRoute)
	ErrCodeLeaseInvalidListCursor        = uint32(coreerrors.LeaseInvalidListCursor)
	ErrCodeLeaseInvalidListPattern       = uint32(coreerrors.LeaseInvalidListPattern)

	// RPC domain (6000-6099)
	ErrCodeRpcTimeout             = uint32(coreerrors.RpcTimeout)
	ErrCodeRpcWorkerNotFound      = uint32(coreerrors.RpcWorkerNotFound)
	ErrCodeRpcBackpressure        = uint32(coreerrors.RpcBackpressure)
	ErrCodeRpcRouteNotRegistered  = uint32(coreerrors.RpcRouteNotRegistered)
	ErrCodeRpcCorrelationNotFound = uint32(coreerrors.RpcCorrelationNotFound)
	ErrCodeRpcInvalidSubscription = uint32(coreerrors.RpcInvalidSubscription)
	ErrCodeRpcSubscriptionLimit   = uint32(coreerrors.RpcSubscriptionLimit)

	// Schedule domain (7000-7099)
	ErrCodeScheduleNotFound            = uint32(coreerrors.ScheduleNotFound)
	ErrCodeScheduleInvalidCron         = uint32(coreerrors.ScheduleInvalidCron)
	ErrCodeScheduleLimit               = uint32(coreerrors.ScheduleLimit)
	ErrCodeScheduleParseError          = uint32(coreerrors.ScheduleParseError)
	ErrCodeScheduleInvalidTarget       = uint32(coreerrors.ScheduleInvalidTarget)
	ErrCodeScheduleInvalidSubscription = uint32(coreerrors.ScheduleInvalidSubscription)
	ErrCodeScheduleSubscriptionLimit   = uint32(coreerrors.ScheduleSubscriptionLimit)
	ErrCodeScheduleInvalidDeliveryMode = uint32(coreerrors.ScheduleInvalidDeliveryMode)
	ErrCodeScheduleBackendError        = uint32(coreerrors.ScheduleBackendError)
)

// DomainError is a server-returned error carrying a numeric code and message.
// Unwrap errors returned by client methods with errors.As to access the code:
//
//	var de *DomainError
//	if errors.As(err, &de) {
//	    fmt.Printf("server error %d: %s\n", de.Code, de.Message)
//	}
type DomainError = coreerrors.DomainError

// TransportError is a client-side transport failure (dial, handshake, read, write).
// Use errors.As(err, &te) to detect network/transport-layer failures distinct
// from server-returned domain errors.
type TransportError = transport.TransportError

// AsyncHandlerOverflowError terminates a callback subscription when its
// configured local async-handler queue cannot accept another notification.
type AsyncHandlerOverflowError = coreerrors.AsyncHandlerOverflowError

// ErrAsyncHandlerOverflow supports errors.Is checks for callback-subscription
// queue saturation.
var ErrAsyncHandlerOverflow = coreerrors.ErrAsyncHandlerOverflow

// IsRetryable reports whether err indicates a transient, retryable condition.
// The following server-signaled situations are considered retryable:
//   - KV isolation conflict (concurrent transaction collision)      [1004]
//   - Stream read beyond watermark (catchup not yet available)      [2004]
//   - Queue full (backpressure)                                     [4005]
//   - Lease held (contention, retry after backoff)                  [5001]
//   - RPC timeout (worker temporarily overloaded)                   [6001]
//   - RPC worker not found (route may not yet be registered)        [6002]
//   - RPC backpressure                                              [6003]
//   - RPC route not registered (transient routing gap)              [6004]
//   - Schedule backend unavailable or saturated                    [7010]
func IsRetryable(err error) bool {
	var de *coreerrors.DomainError
	if !errors.As(err, &de) {
		return false
	}
	switch uint32(de.Code) {
	case ErrCodeKvIsolationConflict,
		ErrCodeStreamReadBeyondWatermark,
		ErrCodeQueueFull,
		ErrCodeLeaseHeld,
		ErrCodeRpcTimeout,
		ErrCodeRpcWorkerNotFound,
		ErrCodeRpcBackpressure,
		ErrCodeRpcRouteNotRegistered,
		ErrCodeScheduleBackendError:
		return true
	}
	return false
}
