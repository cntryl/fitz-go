package fitz

import (
	"testing"

	coreerrors "github.com/cntryl/fitz-go/v2/internal/core/errors"
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
	if ErrCodeStreamSessionAlreadyActive != 2002 || ErrCodeStreamSessionNotFound != 2003 || ErrCodeStreamInvalidReadBound != 2004 {
		t.Fatal("Stream code values differ from the broker")
	}
}

func TestShouldClassifyBrokerDomainCodesGivenRetryabilityTable(t *testing.T) {
	cases := []struct {
		code uint32
		want bool
	}{
		{1004, true}, {1014, true}, {2014, true}, {3006, true},
		{4005, true}, {5001, true}, {5007, true}, {6001, true},
		{6002, true}, {6003, true}, {6004, true}, {7010, true},
		{1009, false}, {2004, false}, {5006, false}, {1011, false},
	}
	for _, tc := range cases {
		if got := IsRetryable(coreerrors.NewDomainError(tc.code, "test")); got != tc.want {
			t.Errorf("code %d: got %t, want %t", tc.code, got, tc.want)
		}
	}
}
