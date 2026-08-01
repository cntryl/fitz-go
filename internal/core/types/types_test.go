package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShouldValidateConcreteRouteSyntaxGivenValidateRouteCalled(t *testing.T) {
	accepted := []string{
		"queue://realm/area",
		"queue://realm/area/resource",
	}
	for _, route := range accepted {
		require.NoError(t, ValidateRoute(route, "queue"), route)
	}

	rejected := []string{
		"",
		"notice://realm/area/resource",
		"queue://",
		"queue://realm//resource",
		"queue://realm/area/foo*",
	}
	for _, route := range rejected {
		require.ErrorIs(t, ValidateRoute(route, "queue"), ErrInvalidRouteShape, route)
	}
}

func TestShouldValidateKVRouteShapesGivenMethodSpecificHelpers(t *testing.T) {
	require.NoError(t, ValidateFixedRoute("kv://realm/area/resource", "kv", 3))
	require.NoError(t, ValidateSelectorRoute("kv://realm/area/resource", "kv", 3, true))
	require.NoError(t, ValidateSelectorRoute("kv://realm/area/*", "kv", 3, true))
	require.NoError(t, ValidateSelectorRoute("kv://realm/*/*", "kv", 3, true))

	rejected := []string{
		"kv://realm/area",
		"kv://realm/area/resource/extra",
		"kv://realm//resource",
		"kv://*/area/resource",
		"kv://realm/*/resource",
		"kv://realm/**",
	}
	for _, route := range rejected {
		require.ErrorIs(t, ValidateFixedRoute(route, "kv", 3), ErrInvalidRouteShape, route)
	}
}

func TestShouldValidateQueueRouteShapesGivenMethodSpecificHelpers(t *testing.T) {
	require.NoError(t, ValidateFixedRoute("queue://realm/area/resource", "queue", 3))
	require.NoError(t, ValidateSelectorRoute("queue://realm/area/resource", "queue", 3, false))
	require.NoError(t, ValidateSelectorRoute("queue://realm/area/*", "queue", 3, false))
	require.NoError(t, ValidateSelectorRoute("queue://realm/*/*", "queue", 3, true))

	require.ErrorIs(t, ValidateFixedRoute("queue://realm/area/*", "queue", 3), ErrInvalidRouteShape)
	require.ErrorIs(t, ValidateSelectorRoute("queue://realm/*/*", "queue", 3, false), ErrInvalidRouteShape)
	require.ErrorIs(t, ValidateSelectorRoute("queue://realm/**", "queue", 3, true), ErrInvalidRouteShape)
	require.ErrorIs(t, ValidateSelectorRoute("queue://*/area/resource", "queue", 3, true), ErrInvalidRouteShape)
}

func TestShouldValidateNoticeRouteShapesGivenPublishAndSubscribe(t *testing.T) {
	require.NoError(t, ValidateFixedRoute("notice://realm/area/resource", "notice", 3))
	require.NoError(t, ValidateNoticeSelector("notice://realm/area/resource"))
	require.NoError(t, ValidateNoticeSelector("notice://realm/area/*"))
	require.NoError(t, ValidateNoticeSelector("notice://realm/*/*"))
	require.NoError(t, ValidateNoticeSelector("notice://realm/**"))

	require.ErrorIs(t, ValidateFixedRoute("notice://realm/area/*", "notice", 3), ErrInvalidRouteShape)
	require.ErrorIs(t, ValidateNoticeSelector("notice://realm/*/resource"), ErrInvalidRouteShape)
	require.ErrorIs(t, ValidateNoticeSelector("notice://*/**"), ErrInvalidRouteShape)
}

func TestShouldValidateStreamRouteShapesGivenMethodSpecificHelpers(t *testing.T) {
	require.NoError(t, ValidateFixedRoute("stream://realm/area/resource", "stream", 3))
	require.NoError(t, ValidateSelectorRoute("stream://realm/area/resource", "stream", 3, true))
	require.NoError(t, ValidateSelectorRoute("stream://realm/area/*", "stream", 3, true))
	require.NoError(t, ValidateSelectorRoute("stream://realm/*/*", "stream", 3, true))

	require.ErrorIs(t, ValidateFixedRoute("stream://realm/area/*", "stream", 3), ErrInvalidRouteShape)
	require.ErrorIs(t, ValidateSelectorRoute("stream://realm/**", "stream", 3, true), ErrInvalidRouteShape)
	require.ErrorIs(t, ValidateSelectorRoute("stream://realm/*/resource", "stream", 3, true), ErrInvalidRouteShape)
}

func TestShouldValidateLeaseRouteShapesGivenConcreteOnlyContract(t *testing.T) {
	require.NoError(t, ValidateFixedRoute("lease://realm/area/resource", "lease", 3))

	rejected := []string{
		"lease://realm/area",
		"lease://realm/area/*",
		"lease://realm/*/*",
		"lease://*/area/resource",
		"queue://realm/area/resource",
	}
	for _, route := range rejected {
		require.ErrorIs(t, ValidateFixedRoute(route, "lease", 3), ErrInvalidRouteShape, route)
	}
}

func TestShouldValidateRPCRouteShapesGivenExactOnlyContract(t *testing.T) {
	require.NoError(t, ValidateConcreteRoute("rpc://realm/area/resource", "rpc"))
	require.NoError(t, ValidateConcreteRoute("rpc://realm/area/resource/operation", "rpc"))

	rejected := []string{
		"rpc://",
		"rpc://realm//resource",
		"rpc://realm/area/*",
		"rpc://realm/*/*",
		"queue://realm/area/resource",
	}
	for _, route := range rejected {
		require.ErrorIs(t, ValidateConcreteRoute(route, "rpc"), ErrInvalidRouteShape, route)
	}
}

func TestShouldValidateScheduleRouteShapesGivenConcreteAndSelectorContracts(t *testing.T) {
	require.NoError(t, ValidateScheduleRoute("schedule://realm/area/resource/run"))
	require.NoError(t, ValidateScheduleSelector("schedule://realm/area/resource/run"))
	require.NoError(t, ValidateScheduleSelector("schedule://realm/area/resource/*"))
	require.NoError(t, ValidateScheduleSelector("schedule://realm/area/*"))
	require.NoError(t, ValidateScheduleSelector("schedule://realm/**"))

	routeRejects := []string{
		"",
		"schedule://realm/area/resource",
		"schedule://realm/area/resource/run/extra",
		"schedule://realm/area/resource/*",
		"schedule://*/area/resource/run",
		"queue://realm/area/resource/run",
	}
	for _, route := range routeRejects {
		require.ErrorIs(t, ValidateScheduleRoute(route), ErrInvalidRouteShape, route)
	}

	selectorRejects := []string{
		"schedule://realm/area",
		"schedule://realm/*/resource/*",
		"schedule://*/area/resource/*",
		"schedule://realm/**/extra",
	}
	for _, selector := range selectorRejects {
		require.ErrorIs(t, ValidateScheduleSelector(selector), ErrInvalidRouteShape, selector)
	}
}

func TestShouldValidateRegistrationPatternsGivenSharedWildcardContract(t *testing.T) {
	// Arrange
	accepted := []string{
		"queue://realm/area/resource",
		"queue://realm/area/*",
		"queue://realm/**",
		"queue://*/area/resource",
		"queue://**/resource",
		"queue://realm/**/**",
	}
	rejected := []string{
		"stream://realm/area/resource",
		"queue://realm//resource",
		"queue://realm/area/res*",
		"queue://realm/area",
		"queue://realm/area/resource/extra/**",
	}

	// Act and Assert
	for _, pattern := range accepted {
		require.NoError(t, ValidateRegistrationPattern(pattern, "queue", 3), pattern)
	}
	for _, pattern := range rejected {
		require.ErrorIs(t, ValidateRegistrationPattern(pattern, "queue", 3), ErrInvalidRouteShape, pattern)
	}
}

func TestShouldMatchConcreteRoutesGivenSharedWildcardContract(t *testing.T) {
	// Arrange
	cases := []struct {
		route    string
		pattern  string
		expected bool
	}{
		{"rpc://acme/orders/v1/create", "rpc://*/orders/**", true},
		{"rpc://acme/orders/create", "rpc://acme/**/**", true},
		{"rpc://acme/create", "rpc://acme/**/orders", false},
		{"queue://acme/app/jobs", "stream://**", false},
	}

	// Act and Assert
	for _, testCase := range cases {
		require.Equal(t, testCase.expected, RouteMatchesPattern(testCase.route, testCase.pattern))
	}
}
