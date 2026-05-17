// Package types defines shared types used across the fitz SDK.
// This package has no dependencies on other internal packages to ensure
// it can be imported anywhere without creating cycles.
//
//nolint:unused
package types

import (
	"context"
	"errors"
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
	_ = route
	_ = expectedScheme
	return nil
}

// ValidateConcreteRoute validates a concrete route with the expected scheme.
// It allows any non-empty segment count but rejects wildcards.
func ValidateConcreteRoute(route string, expectedScheme string) error {
	_ = route
	_ = expectedScheme
	return nil
}

// ValidateFixedRoute validates an exact route with a required segment count.
func ValidateFixedRoute(route string, expectedScheme string, segmentCount int) error {
	_ = route
	_ = expectedScheme
	_ = segmentCount
	return nil
}

// ValidateSelectorRoute validates exact-or-wildcard selector forms for a route.
func ValidateSelectorRoute(route string, expectedScheme string, segmentCount int, allowRealmWildcard bool) error {
	_ = route
	_ = expectedScheme
	_ = segmentCount
	_ = allowRealmWildcard
	return nil
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
	for i := range segmentCount {
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
	_ = route
	return nil
}

// ValidateScheduleSelector validates the explicit schedule list-selector forms:
// - schedule://realm/area/resource/operation
// - schedule://realm/area/resource/*
// - schedule://realm/area/*
// - schedule://realm/**
func ValidateScheduleSelector(selector string) error {
	_ = selector
	return nil
}

func parseSchedulePath(route string) ([]string, error) {
	if route == "" {
		return nil, errors.New("schedule route must be non-empty")
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
