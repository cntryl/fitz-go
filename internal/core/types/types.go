// Package types defines shared types used across the fitz SDK.
// This package has no dependencies on other internal packages to ensure
// it can be imported anywhere without creating cycles.
package types

import (
	"context"
	"errors"
	"fmt"
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
	segmentCount       int
	wildcardIndex      int
	hasDoubleWildcard  bool
	wildcardsAreSuffix bool
}

// scanRoute validates a route in one pass without regular expressions,
// split strings, or success-path heap allocation.
func scanRoute(route string, expectedScheme string) (scannedRoute, error) {
	shape := scannedRoute{wildcardIndex: -1, wildcardsAreSuffix: true}
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
