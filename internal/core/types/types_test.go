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

func TestShouldTreatDifferentSchemeRouteAsOpaqueWhenValidateRouteCalled(t *testing.T) {
	require.NoError(t, ValidateRoute("notice://realm/area/resource", "queue"))
}

func TestShouldTreatEmptyRouteAsOpaqueWhenValidateRouteCalled(t *testing.T) {
	require.NoError(t, ValidateRoute("", "stream"))
}

func TestShouldTreatWildcardConcreteRouteAsOpaqueWhenValidateConcreteRouteCalled(t *testing.T) {
	require.NoError(t, ValidateConcreteRoute("rpc://acme/*/users", "rpc"))
}

func TestShouldAcceptFixedRouteGivenExactThreeSegmentsWhenValidateFixedRouteCalled(t *testing.T) {
	require.NoError(t, ValidateFixedRoute("stream://realm/area/resource", "stream", 3))
}

func TestShouldTreatWildcardFixedRouteAsOpaqueWhenValidateFixedRouteCalled(t *testing.T) {
	require.NoError(t, ValidateFixedRoute("stream://realm/area/*", "stream", 3))
}

func TestShouldAcceptSelectorRouteGivenRealmWildcardWhenValidateSelectorRouteCalled(t *testing.T) {
	require.NoError(t, ValidateSelectorRoute("notice://realm/**", "notice", 3, true))
}

func TestShouldTreatSelectorWildcardAsOpaqueWhenValidateSelectorRouteCalled(t *testing.T) {
	require.NoError(t, ValidateSelectorRoute("queue://realm/**", "queue", 3, false))
}

func TestShouldAcceptScheduleRouteGivenServerSupportedFourSegmentShapeWhenValidateScheduleRouteCalled(t *testing.T) {
	require.NoError(t, ValidateScheduleRoute("schedule://realm/area/resource/run"))
}

func TestShouldTreatEmptyScheduleRouteAsOpaqueWhenValidateScheduleRouteCalled(t *testing.T) {
	require.NoError(t, ValidateScheduleRoute(""))
}

func TestShouldTreatLegacyScheduleRouteAsOpaqueWhenValidateScheduleRouteCalled(t *testing.T) {
	require.NoError(t, ValidateScheduleRoute("schedule://realm/area/resource"))
}

func TestShouldTreatWildcardScheduleRouteAsOpaqueWhenValidateScheduleRouteCalled(t *testing.T) {
	require.NoError(t, ValidateScheduleRoute("schedule://realm/area/resource/*"))
}

func TestShouldTreatWrongSchemeScheduleRouteAsOpaqueWhenValidateScheduleRouteCalled(t *testing.T) {
	require.NoError(t, ValidateScheduleRoute("queue://realm/area/resource/run"))
}

func TestShouldTreatEmptySegmentScheduleRouteAsOpaqueWhenValidateScheduleRouteCalled(t *testing.T) {
	require.NoError(t, ValidateScheduleRoute("schedule://realm//resource/run"))
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

func TestShouldTreatEmptyScheduleSelectorAsOpaqueWhenValidateScheduleSelectorCalled(t *testing.T) {
	require.NoError(t, ValidateScheduleSelector(""))
}

func TestShouldTreatLegacyScheduleSelectorAsOpaqueWhenValidateScheduleSelectorCalled(t *testing.T) {
	require.NoError(t, ValidateScheduleSelector("schedule://realm/area"))
}

func TestShouldTreatRealmWildcardScheduleSelectorAsOpaqueWhenValidateScheduleSelectorCalled(t *testing.T) {
	require.NoError(t, ValidateScheduleSelector("schedule://*/area/resource/*"))
}
