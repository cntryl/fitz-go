package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShouldAcceptRouteGivenServerSupportedTwoSegmentShapeWhenValidateRouteCalled(t *testing.T) {
	require.NoError(t, ValidateRoute("queue://realm/resource", "queue"))
}

func TestShouldAcceptRouteGivenServerSupportedFourSegmentShapeWhenValidateRouteCalled(t *testing.T) {
	require.NoError(t, ValidateRoute("rpc://acme/auth/users/authenticate", "rpc"))
}

func TestShouldAcceptRouteGivenDifferentSchemeWhenValidateRouteCalled(t *testing.T) {
	require.ErrorContains(t, ValidateRoute("notice://realm/area/resource", "queue"), "must start with queue://")
}

func TestShouldRejectRouteGivenEmptyStringWhenValidateRouteCalled(t *testing.T) {
	require.ErrorContains(t, ValidateRoute("", "stream"), "non-empty")
}

func TestShouldRejectConcreteRouteGivenWildcardWhenValidateConcreteRouteCalled(t *testing.T) {
	require.ErrorContains(t, ValidateConcreteRoute("rpc://acme/*/users", "rpc"), "must not contain wildcards")
}

func TestShouldAcceptFixedRouteGivenExactThreeSegmentsWhenValidateFixedRouteCalled(t *testing.T) {
	require.NoError(t, ValidateFixedRoute("stream://realm/area/resource", "stream", 3))
}

func TestShouldRejectFixedRouteGivenWrongShapeWhenValidateFixedRouteCalled(t *testing.T) {
	require.ErrorContains(t, ValidateFixedRoute("stream://realm/area/*", "stream", 3), "must not contain wildcards")
}

func TestShouldAcceptSelectorRouteGivenRealmWildcardWhenValidateSelectorRouteCalled(t *testing.T) {
	require.NoError(t, ValidateSelectorRoute("notice://realm/**", "notice", 3, true))
}

func TestShouldRejectSelectorRouteGivenUnsupportedWildcardWhenValidateSelectorRouteCalled(t *testing.T) {
	require.ErrorContains(t, ValidateSelectorRoute("queue://realm/**", "queue", 3, false), "must be one of")
}

func TestShouldAcceptScheduleRouteGivenServerSupportedFourSegmentShapeWhenValidateScheduleRouteCalled(t *testing.T) {
	require.NoError(t, ValidateScheduleRoute("schedule://realm/area/resource/run"))
}

func TestShouldRejectScheduleRouteGivenEmptyStringWhenValidateScheduleRouteCalled(t *testing.T) {
	require.ErrorContains(t, ValidateScheduleRoute(""), "non-empty")
}

func TestShouldRejectScheduleRouteGivenLegacyThreeSegmentShapeWhenValidateScheduleRouteCalled(t *testing.T) {
	require.ErrorContains(t, ValidateScheduleRoute("schedule://realm/area/resource"), "schedule://{realm}/{area}/{resource}/{operation}")
}

func TestShouldRejectScheduleRouteGivenWildcardWhenValidateScheduleRouteCalled(t *testing.T) {
	require.ErrorContains(t, ValidateScheduleRoute("schedule://realm/area/resource/*"), "must not contain wildcards")
}

func TestShouldRejectScheduleRouteGivenWrongSchemeWhenValidateScheduleRouteCalled(t *testing.T) {
	require.ErrorContains(t, ValidateScheduleRoute("queue://realm/area/resource/run"), "must start with schedule://")
}

func TestShouldRejectScheduleRouteGivenEmptySegmentWhenValidateScheduleRouteCalled(t *testing.T) {
	require.ErrorContains(t, ValidateScheduleRoute("schedule://realm//resource/run"), "segments must be non-empty")
}

func TestShouldAcceptScheduleSelectorGivenWildcardPatternWhenValidateScheduleSelectorCalled(t *testing.T) {
	require.NoError(t, ValidateScheduleSelector("schedule://realm/**"))
}

func TestShouldAcceptScheduleSelectorGivenAreaWildcardWhenValidateScheduleSelectorCalled(t *testing.T) {
	require.NoError(t, ValidateScheduleSelector("schedule://realm/area/*"))
}

func TestShouldAcceptScheduleSelectorGivenResourceWildcardWhenValidateScheduleSelectorCalled(t *testing.T) {
	require.NoError(t, ValidateScheduleSelector("schedule://realm/area/resource/*"))
}

func TestShouldRejectScheduleSelectorGivenEmptyStringWhenValidateScheduleSelectorCalled(t *testing.T) {
	require.ErrorContains(t, ValidateScheduleSelector(""), "non-empty")
}

func TestShouldRejectScheduleSelectorGivenLegacyPrefixWhenValidateScheduleSelectorCalled(t *testing.T) {
	require.ErrorContains(t, ValidateScheduleSelector("schedule://realm/area"), "exact 4-part route or an explicit wildcard selector")
}

func TestShouldRejectScheduleSelectorGivenWildcardInRealmWhenValidateScheduleSelectorCalled(t *testing.T) {
	require.ErrorContains(t, ValidateScheduleSelector("schedule://*/area/resource/*"), "explicit wildcard forms")
}
