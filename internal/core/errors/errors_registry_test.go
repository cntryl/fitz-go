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
		{"KvTransactionNotFound", KvTransactionNotFound, 1050},
		{"KvInvalidMode", KvInvalidMode, 1051},
		{"KvKeyNotFound", KvKeyNotFound, 1052},
		{"KvIsolationConflict", KvIsolationConflict, 1053},
		{"KvWriteInReadonly", KvWriteInReadonly, 1054},
		{"KvKeyExists", KvKeyExists, 1055},
		{"KvInvalidRoute", KvInvalidRoute, 1002},
		{"KvRealmMismatch", KvRealmMismatch, 1056},
		{"KvBackendError", KvBackendError, 1006},
		{"KvTransactionAborted", KvTransactionAborted, 1057},

		// Stream Domain (2000-2099)
		{"StreamConcurrencyConflict", StreamConcurrencyConflict, 2050},
		{"StreamSessionAlreadyActive", StreamSessionAlreadyActive, 2051},
		{"StreamSessionNotFound", StreamSessionNotFound, 2052},
		{"StreamInvalidReadBound", StreamInvalidReadBound, 2053},
		{"StreamBusy", StreamBusy, 2005},
		{"StreamResourceNotFound", StreamResourceNotFound, 2054},
		{"StreamInvalidSubscription", StreamInvalidSubscription, 2003},
		{"StreamSubscriptionLimit", StreamSubscriptionLimit, 2004},

		// Notice Domain (3000-3099)
		{"NoticeInvalidRoute", NoticeInvalidRoute, 3002},
		{"NoticeInvalidPattern", NoticeInvalidPattern, 3003},
		{"NoticeSubscriptionLimit", NoticeSubscriptionLimit, 3004},
		{"NoticeTransportClosed", NoticeTransportClosed, 3050},

		// Queue Domain (4000-4099)
		{"QueueInvalidToken", QueueInvalidToken, 4050},
		{"QueueLeaseExpired", QueueLeaseExpired, 4051},
		{"QueueMessageNotFound", QueueMessageNotFound, 4052},
		{"QueueNotFound", QueueNotFound, 4053},
		{"QueueFull", QueueFull, 4005},

		// Lease Domain (5000-5099)
		{"LeaseHeld", LeaseHeld, 5050},
		{"LeaseInvalidFence", LeaseInvalidFence, 5051},
		{"LeaseExpired", LeaseExpired, 5052},
		{"LeaseNotFound", LeaseNotFound, 5053},
		{"LeaseBadRequest", LeaseBadRequest, 5008},
		{"LeaseInvalidSubscriptionRoute", LeaseInvalidSubscriptionRoute, 5003},
		{"LeaseInvalidListCursor", LeaseInvalidListCursor, 5057},
		{"LeaseInvalidListPattern", LeaseInvalidListPattern, 5003},

		// RPC Domain (6000-6099)
		{"RpcTimeout", RpcTimeout, 6007},
		{"RpcWorkerNotFound", RpcWorkerNotFound, 6050},
		{"RpcBackpressure", RpcBackpressure, 6005},
		{"RpcRouteNotRegistered", RpcRouteNotRegistered, 6051},
		{"RpcCorrelationNotFound", RpcCorrelationNotFound, 6052},

		// Schedule Domain (7000-7099)
		{"ScheduleNotFound", ScheduleNotFound, 7050},
		{"ScheduleInvalidCron", ScheduleInvalidCron, 7051},
		{"ScheduleLimit", ScheduleLimit, 7052},
		{"ScheduleParseError", ScheduleParseError, 7008},
		{"ScheduleInvalidTarget", ScheduleInvalidTarget, 7053},
		{"ScheduleInvalidSubscription", ScheduleInvalidSubscription, 7003},
		{"ScheduleSubscriptionLimit", ScheduleSubscriptionLimit, 7004},
		{"ScheduleInvalidDeliveryMode", ScheduleInvalidDeliveryMode, 7054},
		{"ScheduleBackendError", ScheduleBackendError, 7006},
	}

	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("error code %s: got %d, want %d", tc.name, tc.got, tc.want)
		}
	}
}
