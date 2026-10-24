// Package errors provides domain-agnostic error handling for the Fitz client.
//
// Domain packages (kv, lease, notice, queue, rpc, schedule, stream) expose
// sentinel errors (e.g. ErrLeaseHeld, ErrQueueFull) and map server messages
// to them so callers can use errors.Is for handling. ErrorCode and DomainError
// in this package are for optional code-based handling when the server sends
// numeric error codes on the wire; testkit.AssertDomainErrorCode uses them.
package errors

import (
	"errors"
	"fmt"
)

var ErrAsyncHandlerOverflow = errors.New("async handler queue overflow")

type AsyncHandlerOverflowError struct {
	Domain         string
	SubscriptionID uint64
}

func (e *AsyncHandlerOverflowError) Error() string {
	return fmt.Sprintf("%s subscription %d: %s", e.Domain, e.SubscriptionID, ErrAsyncHandlerOverflow)
}

func (e *AsyncHandlerOverflowError) Unwrap() error {
	return ErrAsyncHandlerOverflow
}

// Error code ranges by domain (from Fitz server)
const (
	// KV Domain (1000-1099)
	KvUnauthorized        = 1001
	KvInvalidRoute        = 1002
	KvInvalidSubscription = 1003
	KvSubscriptionLimit   = 1004
	KvBusy                = 1005
	KvBackendError        = 1006
	KvTimeout             = 1007
	KvBadRequest          = 1008
	KvNotFound            = 1009
	KvUnsupported         = 1010
	KvTransactionNotFound = 1050
	KvInvalidMode         = 1051
	KvKeyNotFound         = 1052
	KvIsolationConflict   = 1053
	KvWriteInReadonly     = 1054
	KvKeyExists           = 1055
	KvRealmMismatch       = 1056
	KvTransactionAborted  = 1057

	// Stream Domain (2000-2099)
	StreamUnauthorized         = 2001
	StreamInvalidRoute         = 2002
	StreamInvalidSubscription  = 2003
	StreamSubscriptionLimit    = 2004
	StreamBusy                 = 2005
	StreamBackendError         = 2006
	StreamTimeout              = 2007
	StreamBadRequest           = 2008
	StreamNotFound             = 2009
	StreamUnsupported          = 2010
	StreamConcurrencyConflict  = 2050
	StreamSessionAlreadyActive = 2051
	StreamSessionNotFound      = 2052
	StreamInvalidReadBound     = 2053
	StreamResourceNotFound     = 2054
	StreamFilterUnsupported    = 2055
	StreamFilterInvalidPayload = 2056
	StreamReadResponseTooLarge = 2057
	// Deprecated: the former names describe different broker errors. Use the corrected constants.
	StreamOffsetTooFarAhead   = StreamSessionAlreadyActive
	StreamReadBeyondWatermark = StreamInvalidReadBound

	// Notice Domain (3000-3099)
	NoticeUnauthorized      = 3001
	NoticeInvalidRoute      = 3002
	NoticeInvalidPattern    = 3003
	NoticeSubscriptionLimit = 3004
	NoticeBusy              = 3005
	NoticeBackendError      = 3006
	NoticeTimeout           = 3007
	NoticeBadRequest        = 3008
	NoticeNotFound          = 3009
	NoticeUnsupported       = 3010
	NoticeTransportClosed   = 3050

	// Queue Domain (4000-4099)
	QueueUnauthorized        = 4001
	QueueInvalidRoute        = 4002
	QueueInvalidSubscription = 4003
	QueueSubscriptionLimit   = 4004
	QueueFull                = 4005 // BUSY / backpressure signal
	QueueBackendError        = 4006
	QueueTimeout             = 4007
	QueueBadRequest          = 4008
	QueueGenericNotFound     = 4009
	QueueUnsupported         = 4010
	QueueInvalidToken        = 4050
	QueueLeaseExpired        = 4051
	QueueMessageNotFound     = 4052
	QueueNotFound            = 4053

	// Lease Domain (5000-5099)
	LeaseUnauthorized             = 5001
	LeaseInvalidRoute             = 5002
	LeaseInvalidSubscriptionRoute = 5003
	LeaseSubscriptionLimit        = 5004
	LeaseQueueFull                = 5005
	LeaseBackendError             = 5006
	LeaseTimeout                  = 5007
	LeaseBadRequest               = 5008
	LeaseGenericNotFound          = 5009
	LeaseUnsupported              = 5010
	LeaseHeld                     = 5050
	LeaseInvalidFence             = 5051
	LeaseExpired                  = 5052
	LeaseNotFound                 = 5053
	LeaseInvalidToken             = 5054
	LeaseAcquireWaitExpired       = 5055
	LeaseWaitersLimit             = 5056
	LeaseInvalidListCursor        = 5057
	LeaseInvalidListPattern       = LeaseInvalidSubscriptionRoute

	// RPC Domain (6000-6099). 6004 = no workers for route or timeout before any reply (per CLIENT_ACCEPTANCE_CRITERIA).
	RpcUnauthorized         = 6001
	RpcInvalidRoute         = 6002
	RpcInvalidSubscription  = 6003
	RpcSubscriptionLimit    = 6004
	RpcBackpressure         = 6005 // BUSY / backpressure signal
	RpcBackendError         = 6006
	RpcTimeout              = 6007
	RpcBadRequest           = 6008
	RpcNotFound             = 6009
	RpcUnsupported          = 6010
	RpcWorkerNotFound       = 6050
	RpcRouteNotRegistered   = 6051
	RpcCorrelationNotFound  = 6052
	RpcInvalidSequence      = 6053
	RpcDuplicateCorrelation = 6054
	RpcWrongWorker          = 6055

	// Schedule Domain (7000-7099)
	ScheduleUnauthorized        = 7001
	ScheduleInvalidRoute        = 7002
	ScheduleInvalidSubscription = 7003
	ScheduleSubscriptionLimit   = 7004
	ScheduleBusy                = 7005
	ScheduleBackendError        = 7006
	ScheduleTimeout             = 7007
	ScheduleParseError          = 7008
	ScheduleNotFound            = 7050
	ScheduleInvalidCron         = 7051
	ScheduleLimit               = 7052
	ScheduleInvalidTarget       = 7053
	ScheduleInvalidDeliveryMode = 7054
)

// IsBackpressure returns true if the error code indicates backpressure
func IsBackpressure(code uint32) bool {
	return code == KvBusy || code == StreamBusy || code == NoticeBusy || code == QueueFull || code == LeaseQueueFull || code == RpcBackpressure || code == ScheduleBusy
}

// ErrorCode represents a Fitz protocol error
type ErrorCode uint32

func (e ErrorCode) String() string {
	switch e {
	// KV errors
	case KvTransactionNotFound:
		return "transaction_not_found"
	case KvInvalidMode:
		return "invalid_mode"
	case KvKeyNotFound:
		return "key_not_found"
	case KvIsolationConflict:
		return "isolation_conflict"
	case KvWriteInReadonly:
		return "write_in_readonly"
	case KvKeyExists:
		return "key_exists"
	case KvInvalidRoute:
		return "invalid_route"
	case KvRealmMismatch:
		return "realm_mismatch"
	case KvBackendError:
		return "backend_error"
	case KvTransactionAborted:
		return "transaction_aborted"
	case KvUnauthorized:
		return "kv_unauthorized"
	case KvInvalidSubscription:
		return "kv_invalid_subscription"
	case KvSubscriptionLimit:
		return "kv_subscription_limit"
	case KvBusy:
		return "kv_busy"

	// Stream errors
	case StreamConcurrencyConflict:
		return "concurrency_conflict"
	case StreamSessionAlreadyActive:
		return "session_already_active"
	case StreamSessionNotFound:
		return "session_not_found"
	case StreamInvalidReadBound:
		return "invalid_read_bound"
	case StreamResourceNotFound:
		return "stream_resource_not_found"
	case StreamInvalidSubscription:
		return "stream_invalid_subscription"
	case StreamSubscriptionLimit:
		return "stream_subscription_limit"
	case StreamBusy:
		return "stream_busy"

	// Notice errors
	case NoticeInvalidRoute:
		return "notice_invalid_route"
	case NoticeInvalidPattern:
		return "notice_invalid_pattern"
	case NoticeSubscriptionLimit:
		return "notice_subscription_limit"
	case NoticeTransportClosed:
		return "notice_transport_closed"
	case NoticeBusy:
		return "notice_busy"

	// Queue errors
	case QueueInvalidToken:
		return "queue_invalid_token"
	case QueueLeaseExpired:
		return "queue_lease_expired"
	case QueueMessageNotFound:
		return "queue_message_not_found"
	case QueueNotFound:
		return "queue_not_found"
	case QueueFull:
		return "queue_full"
	case QueueInvalidSubscription:
		return "queue_invalid_subscription"
	case QueueSubscriptionLimit:
		return "queue_subscription_limit"

	// Lease errors
	case LeaseHeld:
		return "lease_held"
	case LeaseInvalidFence:
		return "lease_invalid_fence"
	case LeaseExpired:
		return "lease_expired"
	case LeaseNotFound:
		return "lease_not_found"
	case LeaseBadRequest:
		return "lease_bad_request"
	case LeaseQueueFull:
		return "lease_queue_full"
	case LeaseInvalidSubscriptionRoute:
		return "lease_invalid_subscription_route"
	case LeaseInvalidListCursor:
		return "lease_invalid_list_cursor"

	// RPC errors
	case RpcTimeout:
		return "rpc_timeout"
	case RpcWorkerNotFound:
		return "rpc_worker_not_found"
	case RpcBackpressure:
		return "rpc_backpressure"
	case RpcRouteNotRegistered:
		return "rpc_route_not_registered"
	case RpcCorrelationNotFound:
		return "rpc_correlation_not_found"
	case RpcBackendError:
		return "rpc_backend_error"
	case RpcInvalidSubscription:
		return "rpc_invalid_subscription"
	case RpcSubscriptionLimit:
		return "rpc_subscription_limit"

	// Schedule errors
	case ScheduleNotFound:
		return "schedule_not_found"
	case ScheduleInvalidCron:
		return "schedule_invalid_cron"
	case ScheduleLimit:
		return "schedule_limit"
	case ScheduleParseError:
		return "schedule_parse_error"
	case ScheduleInvalidTarget:
		return "schedule_invalid_target"
	case ScheduleInvalidSubscription:
		return "schedule_invalid_subscription"
	case ScheduleSubscriptionLimit:
		return "schedule_subscription_limit"
	case ScheduleInvalidDeliveryMode:
		return "schedule_invalid_delivery_mode"
	case ScheduleBackendError:
		return "schedule_backend_error"

	default:
		return fmt.Sprintf("unknown_error_%d", e)
	}
}

// Error represents a domain error with numeric code
type DomainError struct {
	Code    ErrorCode
	Class   uint8
	Effect  uint8
	Message string
}

func (e *DomainError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// NewDomainError creates a new domain error
func NewDomainError(code uint32, message string) *DomainError {
	return &DomainError{
		Code:    ErrorCode(code),
		Message: message,
	}
}

// NewDomainErrorWithDisposition preserves the common wire envelope metadata.
func NewDomainErrorWithDisposition(code uint16, class, effect uint8, message string) *DomainError {
	return &DomainError{Code: ErrorCode(code), Class: class, Effect: effect, Message: message}
}
