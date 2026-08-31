package errors

import (
	"testing"
)

// TestErrorCodeRegistry asserts every error-code constant matches the canonical
// numeric value from the Fitz server error registry.  A drift here (e.g. after
// a copy-paste mistake) would silently break DomainError.Code comparisons.
func TestErrorCodeRegistry(t *testing.T) {
	cases := []struct {
		name string
		got  uint32
		want uint32
	}{
		// KV Domain (1000-1099)
		{"KvTransactionNotFound", KvTransactionNotFound, 1001},
		{"KvInvalidMode", KvInvalidMode, 1002},
		{"KvKeyNotFound", KvKeyNotFound, 1003},
		{"KvIsolationConflict", KvIsolationConflict, 1004},
		{"KvWriteInReadonly", KvWriteInReadonly, 1005},
		{"KvKeyExists", KvKeyExists, 1006},
		{"KvInvalidRoute", KvInvalidRoute, 1007},
		{"KvRealmMismatch", KvRealmMismatch, 1008},
		{"KvBackendError", KvBackendError, 1009},
		{"KvTransactionAborted", KvTransactionAborted, 1010},

		// Stream Domain (2000-2099)
		{"StreamConcurrencyConflict", StreamConcurrencyConflict, 2001},
		{"StreamOffsetTooFarAhead", StreamOffsetTooFarAhead, 2002},
		{"StreamInvalidReadBound", StreamInvalidReadBound, 2003},
		{"StreamReadBeyondWatermark", StreamReadBeyondWatermark, 2004},
		{"StreamResourceNotFound", StreamResourceNotFound, 2005},
		{"StreamInvalidSubscription", StreamInvalidSubscription, 2010},
		{"StreamSubscriptionLimit", StreamSubscriptionLimit, 2011},

		// Notice Domain (3000-3099)
		{"NoticeInvalidRoute", NoticeInvalidRoute, 3001},
		{"NoticeInvalidPattern", NoticeInvalidPattern, 3002},
		{"NoticeSubscriptionLimit", NoticeSubscriptionLimit, 3003},
		{"NoticeTransportClosed", NoticeTransportClosed, 3004},

		// Queue Domain (4000-4099)
		{"QueueInvalidToken", QueueInvalidToken, 4001},
		{"QueueLeaseExpired", QueueLeaseExpired, 4002},
		{"QueueMessageNotFound", QueueMessageNotFound, 4003},
		{"QueueNotFound", QueueNotFound, 4004},
		{"QueueFull", QueueFull, 4005},

		// Lease Domain (5000-5099)
		{"LeaseHeld", LeaseHeld, 5001},
		{"LeaseInvalidFence", LeaseInvalidFence, 5002},
		{"LeaseExpired", LeaseExpired, 5003},
		{"LeaseNotFound", LeaseNotFound, 5004},
		{"LeaseBadRequest", LeaseBadRequest, 5008},
		{"LeaseInvalidSubscriptionRoute", LeaseInvalidSubscriptionRoute, 5010},
		{"LeaseInvalidListCursor", LeaseInvalidListCursor, 5011},
		{"LeaseInvalidListPattern", LeaseInvalidListPattern, 5012},

		// RPC Domain (6000-6099)
		{"RpcTimeout", RpcTimeout, 6001},
		{"RpcWorkerNotFound", RpcWorkerNotFound, 6002},
		{"RpcBackpressure", RpcBackpressure, 6003},
		{"RpcRouteNotRegistered", RpcRouteNotRegistered, 6004},
		{"RpcCorrelationNotFound", RpcCorrelationNotFound, 6005},

		// Schedule Domain (7000-7099)
		{"ScheduleNotFound", ScheduleNotFound, 7001},
		{"ScheduleInvalidCron", ScheduleInvalidCron, 7002},
		{"ScheduleLimit", ScheduleLimit, 7003},
		{"ScheduleParseError", ScheduleParseError, 7004},
		{"ScheduleInvalidTarget", ScheduleInvalidTarget, 7005},
		{"ScheduleInvalidSubscription", ScheduleInvalidSubscription, 7006},
		{"ScheduleSubscriptionLimit", ScheduleSubscriptionLimit, 7007},
		{"ScheduleInvalidDeliveryMode", ScheduleInvalidDeliveryMode, 7008},
		{"ScheduleBackendError", ScheduleBackendError, 7010},
	}

	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("error code %s: got %d, want %d", tc.name, tc.got, tc.want)
		}
	}
}
