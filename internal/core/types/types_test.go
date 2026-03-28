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
	require.NoError(t, ValidateRoute("notice://realm/area/resource", "queue"))
}

func TestShouldRejectRouteGivenEmptyStringWhenValidateRouteCalled(t *testing.T) {
	require.ErrorContains(t, ValidateRoute("", "stream"), "non-empty")
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
	require.ErrorContains(t, ValidateScheduleRoute("schedule://realm/area/resource/*"), "wildcards")
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
