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

// ValidateRoute checks that a route string is usable by the client.
//
// The Fitz server treats routes as opaque strings on the wire, so client-side
// validation intentionally avoids imposing segment-count or scheme rules that
// the server does not enforce.
func ValidateRoute(route string, expectedScheme string) error {
	if route == "" {
		return fmt.Errorf("route must be non-empty")
	}
	return nil
}

// ValidateScheduleRoute validates that a schedule route is an exact
// schedule://{realm}/{area}/{resource}/{operation} identifier.
func ValidateScheduleRoute(route string) error {
	segments, err := parseSchedulePath(route)
	if err != nil {
		return err
	}
	if len(segments) != 4 {
		return fmt.Errorf("schedule route must be schedule://{realm}/{area}/{resource}/{operation}")
	}
	for _, segment := range segments {
		if segment == "*" || segment == "**" {
			return fmt.Errorf("schedule route must not contain wildcards")
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
		return nil, fmt.Errorf("schedule route scheme must be schedule")
	}

	path := strings.TrimPrefix(route, "schedule://")
	rawSegments := strings.Split(path, "/")
	segments := make([]string, 0, len(rawSegments))
	for _, segment := range rawSegments {
		if segment == "" {
			return nil, fmt.Errorf("schedule route segments must be non-empty")
		}
		segments = append(segments, segment)
	}
	return segments, nil
}
