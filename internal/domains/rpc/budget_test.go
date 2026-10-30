package rpc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShouldDecodeBudgetGivenWire2PresenceMarker(t *testing.T) {
	// Arrange
	pointer := func(value uint32) *uint32 { return &value }
	cases := []struct {
		wire   []byte
		budget *uint32
		valid  bool
	}{{[]byte{0}, nil, true}, {[]byte{1, 0, 0, 0, 0}, pointer(0), true}, {[]byte{1, 0, 0, 0, 100}, pointer(100), true}, {nil, nil, false}, {[]byte{0, 0}, nil, false}, {[]byte{2}, nil, false}, {[]byte{1, 255, 255, 255, 255}, nil, false}}
	// Act
	for _, tc := range cases {
		got, err := decodeRPCRequestBudget(tc.wire)
		// Assert
		if tc.valid {
			require.NoError(t, err)
			require.Equal(t, tc.budget, got)
		} else {
			require.Error(t, err)
		}
	}
}
