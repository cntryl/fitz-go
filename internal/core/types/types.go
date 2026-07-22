// Package types defines shared types used across the fitz SDK.
// This package has no dependencies on other internal packages to ensure
// it can be imported anywhere without creating cycles.
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

// ErrInvalidRouteShape indicates a client-side route syntax/shape rejection.
//
// It is intentionally narrow: validators check only the URI-like scheme,
// path segment count, empty segments, and wildcard placement allowed by the
// calling method. Route existence, permissions, realm semantics, and resource
// naming are broker-owned concerns.
var ErrInvalidRouteShape = errors.New("invalid route shape")

// ValidateRoute checks that a route string is a concrete route for the expected scheme.
//
// This validates the scheme prefix and rejects empty segments and wildcards.
// Domain-specific helpers apply stricter segment-count or selector rules.
func ValidateRoute(route string, expectedScheme string) error {
	_, err := parseRouteSegments(route, expectedScheme)
	return err
}

// ValidateConcreteRoute validates a concrete route with the expected scheme.
// It allows any non-empty segment count but rejects wildcards.
func ValidateConcreteRoute(route string, expectedScheme string) error {
	segments, err := parseRouteSegments(route, expectedScheme)
	if err != nil {
		return err
	}
	if len(segments) == 0 {
		return invalidRoute("missing route path")
	}
	for _, segment := range segments {
		if isWildcardSegment(segment) {
			return invalidRoute("wildcard not allowed in concrete route")
		}
	}
	return nil
}

// ValidateFixedRoute validates an exact route with a required segment count.
func ValidateFixedRoute(route string, expectedScheme string, segmentCount int) error {
	segments, err := parseRouteSegments(route, expectedScheme)
	if err != nil {
		return err
	}
	if len(segments) != segmentCount {
		return invalidRoute("expected %d route segments, got %d", segmentCount, len(segments))
	}
	for _, segment := range segments {
		if isWildcardSegment(segment) {
			return invalidRoute("wildcard not allowed in fixed route")
		}
	}
	return nil
}

// ValidateSelectorRoute validates exact-or-wildcard selector forms for a route.
func ValidateSelectorRoute(route string, expectedScheme string, segmentCount int, allowRealmWildcard bool) error {
	segments, err := parseRouteSegments(route, expectedScheme)
	if err != nil {
		return err
	}
	if len(segments) != segmentCount {
		return invalidRoute("expected %d selector segments, got %d", segmentCount, len(segments))
	}
	if segmentCount == 0 {
		return invalidRoute("selector segment count must be positive")
	}
	if isWildcardSegment(segments[0]) {
		return invalidRoute("realm wildcard is not allowed")
	}
	wildcardIndex := -1
	for index, segment := range segments {
		if !isWildcardSegment(segment) {
			continue
		}
		if segment != "*" {
			return invalidRoute("only single-segment wildcards are allowed here")
		}
		wildcardIndex = index
		break
	}
	if wildcardIndex == -1 {
		return nil
	}
	for _, segment := range segments[wildcardIndex:] {
		if segment != "*" {
			return invalidRoute("wildcards must form a terminal suffix")
		}
	}
	if wildcardIndex == segmentCount-1 {
		return nil
	}
	if allowRealmWildcard && wildcardIndex == 1 {
		return nil
	}
	return invalidRoute("wildcard placement not allowed for this method")
}

// ValidateScheduleRoute validates that a schedule route is an exact
// schedule://{realm}/{area}/{resource}/{operation} identifier.
func ValidateScheduleRoute(route string) error {
	return ValidateFixedRoute(route, "schedule", 4)
}

// ValidateScheduleSelector validates the explicit schedule list-selector forms:
// - schedule://realm/area/resource/operation
// - schedule://realm/area/resource/*
// - schedule://realm/area/*
// - schedule://realm/**
func ValidateScheduleSelector(selector string) error {
	segments, err := parseRouteSegments(selector, "schedule")
	if err != nil {
		return err
	}
	if len(segments) == 0 {
		return invalidRoute("missing schedule selector path")
	}
	if isWildcardSegment(segments[0]) {
		return invalidRoute("realm wildcard is not allowed")
	}

	switch len(segments) {
	case 2:
		if segments[1] == "**" {
			return nil
		}
	case 3:
		if segments[2] == "*" && noWildcardPrefix(segments[:2]) {
			return nil
		}
	case 4:
		if noWildcardPrefix(segments) {
			return nil
		}
		if segments[3] == "*" && noWildcardPrefix(segments[:3]) {
			return nil
		}
	}
	return invalidRoute("schedule selector wildcard placement not allowed")
}

func parseRouteSegments(route string, expectedScheme string) ([]string, error) {
	if expectedScheme == "" {
		return nil, invalidRoute("expected scheme is empty")
	}
	prefix := expectedScheme + "://"
	if !strings.HasPrefix(route, prefix) {
		return nil, invalidRoute("expected %q scheme", expectedScheme)
	}
	path := strings.TrimPrefix(route, prefix)
	if path == "" {
		return nil, invalidRoute("missing route path")
	}
	segments := strings.Split(path, "/")
	for _, segment := range segments {
		if segment == "" {
			return nil, invalidRoute("empty route segment")
		}
		if strings.Contains(segment, "*") && !isWildcardSegment(segment) {
			return nil, invalidRoute("wildcards must occupy a full path segment")
		}
	}
	return segments, nil
}

func invalidRoute(format string, args ...any) error {
	if format == "" {
		return ErrInvalidRouteShape
	}
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalidRouteShape}, args...)...)
}

func isWildcardSegment(segment string) bool {
	return segment == "*" || segment == "**"
}

func noWildcardPrefix(segments []string) bool {
	for _, segment := range segments {
		if isWildcardSegment(segment) {
			return false
		}
	}
	return true
}
