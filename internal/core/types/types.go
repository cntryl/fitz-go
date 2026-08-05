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
	shape, err := scanRoute(route, expectedScheme)
	if err != nil {
		return err
	}
	if shape.wildcardIndex >= 0 {
		return invalidRoute("wildcard not allowed in concrete route")
	}
	return nil
}

// ValidateConcreteRoute validates a concrete route with the expected scheme.
// It allows any non-empty segment count but rejects wildcards.
func ValidateConcreteRoute(route string, expectedScheme string) error {
	shape, err := scanRoute(route, expectedScheme)
	if err != nil {
		return err
	}
	if shape.wildcardIndex >= 0 {
		return invalidRoute("wildcard not allowed in concrete route")
	}
	return nil
}

// ValidateFixedRoute validates an exact route with a required segment count.
func ValidateFixedRoute(route string, expectedScheme string, segmentCount int) error {
	shape, err := scanRoute(route, expectedScheme)
	if err != nil {
		return err
	}
	if shape.segmentCount != segmentCount {
		return invalidRoute("expected %d route segments, got %d", segmentCount, shape.segmentCount)
	}
	if shape.wildcardIndex >= 0 {
		return invalidRoute("wildcard not allowed in fixed route")
	}
	return nil
}

// ValidateSelectorRoute validates exact-or-wildcard selector forms for a route.
func ValidateSelectorRoute(route string, expectedScheme string, segmentCount int, allowRealmWildcard bool) error {
	shape, err := scanRoute(route, expectedScheme)
	if err != nil {
		return err
	}
	if shape.segmentCount != segmentCount {
		return invalidRoute("expected %d selector segments, got %d", segmentCount, shape.segmentCount)
	}
	if segmentCount == 0 {
		return invalidRoute("selector segment count must be positive")
	}
	if shape.wildcardIndex == 0 {
		return invalidRoute("realm wildcard is not allowed")
	}
	if shape.wildcardIndex == -1 {
		return nil
	}
	if shape.hasDoubleWildcard {
		return invalidRoute("only single-segment wildcards are allowed here")
	}
	if !shape.wildcardsAreSuffix {
		return invalidRoute("wildcards must form a terminal suffix")
	}
	if shape.wildcardIndex == segmentCount-1 {
		return nil
	}
	if allowRealmWildcard && shape.wildcardIndex == 1 {
		return nil
	}
	return invalidRoute("wildcard placement not allowed for this method")
}

// ValidateNoticeSelector validates exact and wildcard Notice subscription forms,
// including the canonical recursive notice://realm/** selector.
func ValidateNoticeSelector(route string) error {
	shape, err := scanRoute(route, "notice")
	if err != nil {
		return err
	}
	if shape.segmentCount == 2 && shape.wildcardIndex == 1 && shape.hasDoubleWildcard {
		return nil
	}
	return ValidateSelectorRoute(route, "notice", 3, true)
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
	shape, err := scanRoute(selector, "schedule")
	if err != nil {
		return err
	}
	if shape.wildcardIndex == 0 {
		return invalidRoute("realm wildcard is not allowed")
	}

	switch shape.segmentCount {
	case 2:
		if shape.wildcardIndex == 1 && shape.hasDoubleWildcard {
			return nil
		}
	case 3:
		if shape.wildcardIndex == 2 && !shape.hasDoubleWildcard {
			return nil
		}
	case 4:
		if shape.wildcardIndex == -1 {
			return nil
		}
		if shape.wildcardIndex == 3 && !shape.hasDoubleWildcard {
			return nil
		}
	}
	return invalidRoute("schedule selector wildcard placement not allowed")
}

type scannedRoute struct {
	segmentCount        int
	wildcardIndex       int
	hasDoubleWildcard   bool
	doubleWildcardCount int
	wildcardsAreSuffix  bool
}

// ValidateRegistrationPattern validates the shared live-registration grammar.
// A required segment count of zero keeps route depth flexible (Notice and RPC).
func ValidateRegistrationPattern(route string, expectedScheme string, requiredSegments int) error {
	shape, err := scanRoute(route, expectedScheme)
	if err != nil {
		return err
	}
	if requiredSegments < 0 {
		return invalidRoute("required segment count cannot be negative")
	}
	if requiredSegments == 0 {
		return nil
	}
	if shape.doubleWildcardCount == 0 {
		if shape.segmentCount != requiredSegments {
			return invalidRoute("pattern cannot match a %d-segment route", requiredSegments)
		}
		return nil
	}
	minimumSegments := shape.segmentCount - shape.doubleWildcardCount
	if minimumSegments > requiredSegments {
		return invalidRoute("pattern cannot match a %d-segment route", requiredSegments)
	}
	return nil
}

// ValidateStreamSelector validates the four route shapes shared by Stream READ and SUBSCRIBE.
func ValidateStreamSelector(route string) error {
	if route == "stream://**" {
		return nil
	}
	shape, err := scanRoute(route, "stream")
	if err != nil || shape.segmentCount != 3 {
		return invalidRoute("stream selector must be realm/area/resource, realm/area/*, realm/*/*, or stream://**")
	}
	segments := splitRouteSegments(route)
	if len(segments) != 3 {
		return invalidRoute("stream selector must be realm/area/resource, realm/area/*, realm/*/*, or stream://**")
	}
	if strings.Contains(segments[0], "*") || strings.Contains(segments[1], "*") && segments[1] != "*" || strings.Contains(segments[2], "*") && segments[2] != "*" {
		return invalidRoute("stream selector must use whole-segment wildcards")
	}
	if (segments[1] == "*" && segments[2] == "*") || (segments[1] != "*" && segments[2] == "*") || (segments[1] != "*" && segments[2] != "*") {
		return nil
	}
	return invalidRoute("stream selector must use whole-segment wildcards")
}

// RouteMatchesPattern reports whether a validated concrete route matches a
// validated registration pattern using whole-segment * and ** semantics.
func RouteMatchesPattern(route string, pattern string) bool {
	routeScheme, _, routeOK := strings.Cut(route, "://")
	patternScheme, _, patternOK := strings.Cut(pattern, "://")
	if !routeOK || !patternOK || routeScheme != patternScheme {
		return false
	}
	routeSegments := splitRouteSegments(route)
	patternSegments := splitRouteSegments(pattern)
	if len(routeSegments) == 0 || len(patternSegments) == 0 {
		return false
	}

	routeIndex, patternIndex := 0, 0
	lastDoubleWildcard := -1
	lastDoubleMatch := 0
	for routeIndex < len(routeSegments) {
		if patternIndex < len(patternSegments) && (patternSegments[patternIndex] == "*" || patternSegments[patternIndex] == routeSegments[routeIndex]) {
			patternIndex++
			routeIndex++
			continue
		}
		if patternIndex < len(patternSegments) && patternSegments[patternIndex] == "**" {
			lastDoubleWildcard = patternIndex
			lastDoubleMatch = routeIndex
			patternIndex++
			continue
		}
		if lastDoubleWildcard < 0 {
			return false
		}
		lastDoubleMatch++
		routeIndex = lastDoubleMatch
		patternIndex = lastDoubleWildcard + 1
	}
	for patternIndex < len(patternSegments) && patternSegments[patternIndex] == "**" {
		patternIndex++
	}
	return patternIndex == len(patternSegments)
}

func splitRouteSegments(route string) []string {
	for index := 0; index+2 < len(route); index++ {
		if route[index:index+3] == "://" {
			return splitSegments(route[index+3:])
		}
	}
	return nil
}

func splitSegments(path string) []string {
	segments := make([]string, 0, 4)
	start := 0
	for index := 0; index <= len(path); index++ {
		if index == len(path) || path[index] == '/' {
			segments = append(segments, path[start:index])
			start = index + 1
		}
	}
	return segments
}

// scanRoute validates a route in one pass without regular expressions,
// split strings, or success-path heap allocation.
func scanRoute(route string, expectedScheme string) (scannedRoute, error) {
	shape := scannedRoute{wildcardIndex: -1, wildcardsAreSuffix: true}
	if len(route) > 65_535 {
		return shape, invalidRoute("route exceeds the 65,535-byte TLV value limit")
	}
	if expectedScheme == "" {
		return shape, invalidRoute("expected scheme is empty")
	}
	prefixLen := len(expectedScheme)
	if len(route) < prefixLen+3 || route[:prefixLen] != expectedScheme || route[prefixLen:prefixLen+3] != "://" {
		return shape, invalidRoute("expected %q scheme", expectedScheme)
	}
	start := prefixLen + 3
	if start == len(route) {
		return shape, invalidRoute("missing route path")
	}
	segmentStart := start
	for index := start; index <= len(route); index++ {
		if index != len(route) && route[index] != '/' {
			continue
		}
		if index == segmentStart {
			return shape, invalidRoute("empty route segment")
		}
		segmentLen := index - segmentStart
		wildcard := segmentLen == 1 && route[segmentStart] == '*'
		doubleWildcard := segmentLen == 2 && route[segmentStart] == '*' && route[segmentStart+1] == '*'
		if !wildcard && !doubleWildcard {
			for cursor := segmentStart; cursor < index; cursor++ {
				if route[cursor] == '*' {
					return shape, invalidRoute("wildcards must occupy a full path segment")
				}
			}
			if shape.wildcardIndex >= 0 {
				shape.wildcardsAreSuffix = false
			}
		} else {
			if shape.wildcardIndex == -1 {
				shape.wildcardIndex = shape.segmentCount
			}
			shape.hasDoubleWildcard = shape.hasDoubleWildcard || doubleWildcard
			if doubleWildcard {
				shape.doubleWildcardCount++
			}
		}
		shape.segmentCount++
		segmentStart = index + 1
	}
	return shape, nil
}

func invalidRoute(format string, args ...any) error {
	if format == "" {
		return ErrInvalidRouteShape
	}
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalidRouteShape}, args...)...)
}
