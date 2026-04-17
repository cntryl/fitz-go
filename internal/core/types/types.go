// Package types defines shared types used across the fitz SDK.
// This package has no dependencies on other internal packages to ensure
// it can be imported anywhere without creating cycles.
package types

import (
	"context"
	"fmt"
	"strings"
)

// TokenProvider is a function that returns a JWT token for authentication.
// It is called during connection establishment and reconnection attempts,
// allowing for token renewal and refresh logic. Return an empty string for
// unauthenticated connections.
type TokenProvider func(ctx context.Context) (string, error)

// ValidateRoute checks that a route string is a concrete route for the expected scheme.
//
// This validates the scheme prefix and rejects empty segments and wildcards.
// Domain-specific helpers apply stricter segment-count or selector rules.
func ValidateRoute(route string, expectedScheme string) error {
	return ValidateConcreteRoute(route, expectedScheme)
}

// ValidateConcreteRoute validates a concrete route with the expected scheme.
// It allows any non-empty segment count but rejects wildcards.
func ValidateConcreteRoute(route string, expectedScheme string) error {
	segments, err := parseRoutePath(route, expectedScheme)
	if err != nil {
		return err
	}
	if hasWildcardSegment(segments) {
		return fmt.Errorf("%s route %q must not contain wildcards", expectedScheme, route)
	}
	return nil
}

// ValidateFixedRoute validates an exact route with a required segment count.
func ValidateFixedRoute(route string, expectedScheme string, segmentCount int) error {
	segments, err := parseRoutePath(route, expectedScheme)
	if err != nil {
		return err
	}
	if len(segments) != segmentCount {
		return fmt.Errorf("%s route %q must be %s", expectedScheme, route, routeShape(expectedScheme, segmentCount))
	}
	if hasWildcardSegment(segments) {
		return fmt.Errorf("%s route %q must not contain wildcards", expectedScheme, route)
	}
	return nil
}

// ValidateSelectorRoute validates exact-or-wildcard selector forms for a route.
func ValidateSelectorRoute(route string, expectedScheme string, segmentCount int, allowRealmWildcard bool) error {
	segments, err := parseRoutePath(route, expectedScheme)
	if err != nil {
		return err
	}

	if len(segments) == segmentCount {
		if segmentsAreConcrete(segments) {
			return nil
		}

		if segments[segmentCount-1] == "*" && segmentsAreConcrete(segments[:segmentCount-1]) {
			return nil
		}
	}

	if allowRealmWildcard && len(segments) == 2 && segments[0] != "*" && segments[0] != "**" && segments[1] == "**" {
		return nil
	}

	return fmt.Errorf("%s route %q must be one of %s", expectedScheme, route, selectorRouteShapes(expectedScheme, segmentCount, allowRealmWildcard))
}

func parseRoutePath(route string, expectedScheme string) ([]string, error) {
	if route == "" {
		return nil, fmt.Errorf("%s route must be non-empty", expectedScheme)
	}

	prefix := expectedScheme + "://"
	if !strings.HasPrefix(route, prefix) {
		return nil, fmt.Errorf("%s route %q must start with %s", expectedScheme, route, prefix)
	}

	path := strings.TrimPrefix(route, prefix)
	rawSegments := strings.Split(path, "/")
	segments := make([]string, 0, len(rawSegments))
	for _, segment := range rawSegments {
		if segment == "" {
			return nil, fmt.Errorf("%s route %q segments must be non-empty", expectedScheme, route)
		}
		segments = append(segments, segment)
	}

	return segments, nil
}

func hasWildcardSegment(segments []string) bool {
	for _, segment := range segments {
		if segment == "*" || segment == "**" {
			return true
		}
	}
	return false
}

func segmentsAreConcrete(segments []string) bool {
	for _, segment := range segments {
		if segment == "*" || segment == "**" {
			return false
		}
	}
	return true
}

func routeShape(expectedScheme string, segmentCount int) string {
	parts := make([]string, 0, segmentCount)
	for i := 0; i < segmentCount; i++ {
		parts = append(parts, placeholderForIndex(i))
	}
	return fmt.Sprintf("%s://%s", expectedScheme, strings.Join(parts, "/"))
}

func selectorRouteShapes(expectedScheme string, segmentCount int, allowRealmWildcard bool) string {
	exact := routeShape(expectedScheme, segmentCount)
	if segmentCount == 3 {
		if allowRealmWildcard {
			return fmt.Sprintf("%s, %s://{realm}/{area}/*, or %s://{realm}/**", exact, expectedScheme, expectedScheme)
		}
		return fmt.Sprintf("%s or %s://{realm}/{area}/*", exact, expectedScheme)
	}

	if allowRealmWildcard {
		return fmt.Sprintf("%s or %s://{realm}/**", exact, expectedScheme)
	}

	return exact
}

func placeholderForIndex(index int) string {
	switch index {
	case 0:
		return "{realm}"
	case 1:
		return "{area}"
	case 2:
		return "{resource}"
	case 3:
		return "{operation}"
	default:
		return fmt.Sprintf("{segment%d}", index+1)
	}
}

// ValidateScheduleRoute validates that a schedule route is an exact
// schedule://{realm}/{area}/{resource}/{operation} identifier.
func ValidateScheduleRoute(route string) error {
	segments, err := parseSchedulePath(route)
	if err != nil {
		return err
	}
	if len(segments) != 4 {
		return fmt.Errorf("schedule route %q must be schedule://{realm}/{area}/{resource}/{operation}", route)
	}
	for _, segment := range segments {
		if segment == "*" || segment == "**" {
			return fmt.Errorf("schedule route %q must not contain wildcards", route)
		}
	}
	return nil
}

// ValidateScheduleSelector validates the explicit schedule list-selector forms:
// - schedule://realm/area/resource/operation
// - schedule://realm/area/resource/*
// - schedule://realm/area/*
// - schedule://realm/**
func ValidateScheduleSelector(selector string) error {
	segments, err := parseSchedulePath(selector)
	if err != nil {
		return err
	}

	switch len(segments) {
	case 2:
		if segments[1] == "**" && segments[0] != "*" && segments[0] != "**" {
			return nil
		}
	case 3:
		if segments[2] == "*" && segments[0] != "*" && segments[0] != "**" && segments[1] != "*" && segments[1] != "**" {
			return nil
		}
	case 4:
		if segments[0] == "*" || segments[0] == "**" || segments[1] == "*" || segments[1] == "**" || segments[2] == "*" || segments[2] == "**" {
			return fmt.Errorf("schedule selector must use explicit wildcard forms only")
		}
		if segments[3] == "*" {
			return nil
		}
		if segments[3] != "**" {
			return ValidateScheduleRoute(selector)
		}
	}

	return fmt.Errorf("schedule selector must be an exact 4-part route or an explicit wildcard selector")
}

func parseSchedulePath(route string) ([]string, error) {
	if route == "" {
		return nil, fmt.Errorf("schedule route must be non-empty")
	}
	if !strings.HasPrefix(route, "schedule://") {
		return nil, fmt.Errorf("schedule route %q must start with schedule://", route)
	}

	path := strings.TrimPrefix(route, "schedule://")
	rawSegments := strings.Split(path, "/")
	segments := make([]string, 0, len(rawSegments))
	for _, segment := range rawSegments {
		if segment == "" {
			return nil, fmt.Errorf("schedule route %q segments must be non-empty", route)
		}
		segments = append(segments, segment)
	}
	return segments, nil
}
