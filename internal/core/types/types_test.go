package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldAcceptStandardRouteGivenThreeSegmentsWhenValidateRouteCalled(t *testing.T) {
	// Arrange
	route := "rpc://realm/area/resource"

	// Act
	err := ValidateRoute(route, "rpc")

	// Assert
	require.NoError(t, err)
}

func TestShouldAcceptScheduleRouteGivenThreeSegmentsWhenValidateScheduleRouteCalled(t *testing.T) {
	// Arrange
	route := "schedule://realm/area/resource"

	// Act
	err := ValidateScheduleRoute(route)

	// Assert
	require.NoError(t, err)
}

func TestShouldRejectRouteGivenWrongSchemeWhenValidateRouteCalled(t *testing.T) {
	// Arrange
	route := "queue://realm/area/resource"

	// Act
	err := ValidateRoute(route, "rpc")

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rpc://")
}

func TestShouldRejectRouteGivenMissingSegmentsWhenValidateRouteCalled(t *testing.T) {
	// Arrange
	route := "kv://realm/area"

	// Act
	err := ValidateRoute(route, "kv")

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly 3 segments")
}

func TestShouldRejectScheduleRouteGivenMissingResourceWhenValidateScheduleRouteCalled(t *testing.T) {
	// Arrange
	route := "schedule://realm/area"

	// Act
	err := ValidateScheduleRoute(route)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly 3 segments")
}

func TestShouldAcceptScheduleSelectorGivenAreaScopeWhenValidateScheduleSelectorCalled(t *testing.T) {
	err := ValidateScheduleSelector("schedule://realm/area")
	require.NoError(t, err)
}

func TestShouldAcceptScheduleSelectorGivenExactResourceWhenValidateScheduleSelectorCalled(t *testing.T) {
	err := ValidateScheduleSelector("schedule://realm/area/resource")
	require.NoError(t, err)
}

func TestShouldAcceptScheduleSelectorGivenAreaWildcardWhenValidateScheduleSelectorCalled(t *testing.T) {
	err := ValidateScheduleSelector("schedule://realm/area/*")
	require.NoError(t, err)
}

func TestShouldRejectScheduleSelectorGivenWildcardOutsideResourceWhenValidateScheduleSelectorCalled(t *testing.T) {
	err := ValidateScheduleSelector("schedule://*/area/resource")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "third segment")
}

func TestShouldRejectRouteGivenEmptySegmentWhenValidateRouteCalled(t *testing.T) {
	// Arrange
	route := "stream://realm//resource"

	// Act
	err := ValidateRoute(route, "stream")

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-empty")
}

func TestShouldRejectRouteGivenExtraSegmentsWhenValidateRouteCalled(t *testing.T) {
	// Arrange
	route := "lease://realm/area/resource/extra"

	// Act
	err := ValidateRoute(route, "lease")

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly 3 segments")
}
