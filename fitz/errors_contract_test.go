package fitz

import (
	"testing"

	coreerrors "github.com/cntryl/fitz-go/internal/core/errors"
)

func TestShouldMatchBrokerErrorRegistryGivenStreamCodes(t *testing.T) {
	cases := []struct {
		code uint32
		name string
	}{
		{ErrCodeStreamSessionAlreadyActive, "session_already_active"},
		{ErrCodeStreamSessionNotFound, "session_not_found"},
		{ErrCodeStreamInvalidReadBound, "invalid_read_bound"},
	}
	for _, tc := range cases {
		if got := coreerrors.ErrorCode(tc.code).String(); got != tc.name {
			t.Errorf("code %d: got %q, want %q", tc.code, got, tc.name)
		}
	}
	if ErrCodeStreamSessionAlreadyActive != 2051 || ErrCodeStreamSessionNotFound != 2052 || ErrCodeStreamInvalidReadBound != 2053 {
		t.Fatal("Stream code values differ from the broker")
	}
}

func TestShouldClassifyBrokerDomainCodesGivenRetryabilityTable(t *testing.T) {
	// Arrange
	cases := []struct {
		code          uint16
		class, effect uint8
		want          bool
	}{{1005, 5, 0, true}, {2005, 5, 0, true}, {4005, 5, 0, true}, {5050, 0, 0, true}, {6051, 0, 0, true}, {7006, 6, 1, false}, {6007, 7, 1, false}, {4007, 7, 1, false}, {4005, 5, 1, false}, {6001, 1, 0, false}}
	// Act
	for _, tc := range cases {
		got := IsRetryable(coreerrors.NewDomainErrorWithDisposition(tc.code, tc.class, tc.effect, "test"))
		// Assert
		if got != tc.want {
			t.Errorf("code %d: got %t, want %t", tc.code, got, tc.want)
		}
	}
}

func TestShouldNameRPCBackendErrorGivenServerCode(t *testing.T) {
	if ErrCodeRpcBackendError != 6006 {
		t.Fatalf("RPC backend error code: got %d, want 6006", ErrCodeRpcBackendError)
	}
	if got := coreerrors.ErrorCode(ErrCodeRpcBackendError).String(); got != "rpc_backend_error" {
		t.Fatalf("RPC backend error name: got %q", got)
	}
}
