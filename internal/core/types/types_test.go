package types

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShouldForwardOpaqueRoutesGivenAllRouteHelpers(t *testing.T) {
	routes := []string{
		"",
		"not-a-uri",
		"queue://realm//resource",
		"wrong://realm/area/resource",
		"schedule://*/**/unusual*selector",
	}
	helpers := []func(string) error{
		func(route string) error { return ValidateRoute(route, "queue") },
		func(route string) error { return ValidateConcreteRoute(route, "rpc") },
		func(route string) error { return ValidateFixedRoute(route, "kv", 3) },
		func(route string) error { return ValidateSelectorRoute(route, "notice", 3, false) },
		ValidateScheduleRoute,
		ValidateScheduleSelector,
	}

	for _, route := range routes {
		for _, helper := range helpers {
			require.NoError(t, helper(route), route)
		}
	}
}

func TestShouldRejectRouteGivenInvalidUTF8(t *testing.T) {
	route := string([]byte{0xff})

	require.ErrorIs(t, ValidateRoute(route, "ignored"), ErrInvalidRouteShape)
}

func TestShouldRejectRouteGivenWireLimitExceeded(t *testing.T) {
	route := strings.Repeat("r", 65_536)

	require.ErrorIs(t, ValidateRoute(route, "ignored"), ErrInvalidRouteShape)
}

func TestShouldAcceptRouteGivenWireLimitBoundary(t *testing.T) {
	route := strings.Repeat("r", 65_535)

	require.NoError(t, ValidateRoute(route, "ignored"))
}
