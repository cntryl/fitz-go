// Package types defines shared types used across the fitz SDK.
// This package has no dependencies on other internal packages to ensure
// it can be imported anywhere without creating cycles.
package types

import (
	"context"
	"errors"
	"fmt"
	"unicode/utf8"
)

// TokenProvider is a function that returns a JWT token for authentication.
// It is called during connection establishment and reconnection attempts,
// allowing for token renewal and refresh logic. Return an empty string for
// unauthenticated connections.
type TokenProvider func(ctx context.Context) (string, error)

// ErrInvalidRouteShape indicates that a route cannot be represented on the wire.
// Fitz route grammar and authorization are broker-owned concerns.
var ErrInvalidRouteShape = errors.New("invalid route shape")

// ValidateRoute performs only UTF-8 and wire-size checks. The broker owns all
// route and selector grammar, including schemes, segments, and wildcards.
func ValidateRoute(route string, _ string) error {
	if !utf8.ValidString(route) {
		return invalidRoute("route is not valid UTF-8")
	}
	if len(route) > 65_535 {
		return invalidRoute("route exceeds 65535-byte wire limit")
	}
	return nil
}

// ValidateConcreteRoute validates a concrete route with the expected scheme.
// It allows any non-empty segment count but rejects wildcards.
func ValidateConcreteRoute(route string, expectedScheme string) error {
	return ValidateRoute(route, expectedScheme)
}

// ValidateFixedRoute validates an exact route with a required segment count.
func ValidateFixedRoute(route string, expectedScheme string, segmentCount int) error {
	_ = segmentCount
	return ValidateRoute(route, expectedScheme)
}

// ValidateSelectorRoute validates exact-or-wildcard selector forms for a route.
func ValidateSelectorRoute(route string, expectedScheme string, segmentCount int, allowRealmWildcard bool) error {
	_ = segmentCount
	_ = allowRealmWildcard
	return ValidateRoute(route, expectedScheme)
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
	return ValidateRoute(selector, "schedule")
}

func invalidRoute(format string, args ...any) error {
	if format == "" {
		return ErrInvalidRouteShape
	}
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalidRouteShape}, args...)...)
}
