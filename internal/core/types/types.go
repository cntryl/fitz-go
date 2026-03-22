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

// ValidateRoute checks that a route string matches the default domain format:
//
//	<expectedScheme>://realm/area/resource
//
// All path segments must be non-empty. The scheme prefix is validated
// against expectedScheme (e.g., "queue", "stream", "rpc", "lease").
func ValidateRoute(route string, expectedScheme string) error {
	segs, err := routeSegments(route, expectedScheme)
	if err != nil {
		return err
	}
	if len(segs) != 3 {
		return fmt.Errorf("%s route must have exactly 3 segments: realm/area/resource", expectedScheme)
	}
	return nil
}

// ValidateScheduleRoute validates exact schedule routes accepted by CREATE and CANCEL:
//
//	schedule://realm/area/resource
func ValidateScheduleRoute(route string) error {
	segs, err := routeSegments(route, "schedule")
	if err != nil {
		return err
	}
	if len(segs) != 3 {
		return fmt.Errorf("schedule route must have exactly 3 segments: realm/area/resource")
	}
	for _, seg := range segs {
		if seg == "*" {
			return fmt.Errorf("schedule route does not support wildcards")
		}
	}
	return nil
}

// ValidateScheduleSelector validates route selectors accepted by schedule LIST and SUBSCRIBE:
//
//	schedule://realm/area
//	schedule://realm/area/resource
//	schedule://realm/area/*
func ValidateScheduleSelector(selector string) error {
	segs, err := routeSegments(selector, "schedule")
	if err != nil {
		return err
	}
	if len(segs) < 2 {
		return fmt.Errorf("schedule selector must have 2 or 3 segments: realm/area or realm/area/resource or realm/area/*")
	}
	if segs[0] == "*" || segs[1] == "*" {
		return fmt.Errorf("schedule selector wildcard is only allowed as the third segment")
	}
	switch len(segs) {
	case 2:
		return nil
	case 3:
		return nil
	default:
		return fmt.Errorf("schedule selector must have 2 or 3 segments: realm/area or realm/area/resource or realm/area/*")
	}
}

func routeSegments(route string, expectedScheme string) ([]string, error) {
	prefix := expectedScheme + "://"
	if !strings.HasPrefix(route, prefix) {
		return nil, fmt.Errorf("%s route must start with %s", expectedScheme, prefix)
	}

	path := route[len(prefix):]
	segs := strings.Split(path, "/")
	if len(segs) == 0 {
		return nil, fmt.Errorf("%s route segments must be non-empty", expectedScheme)
	}
	for _, seg := range segs {
		if seg == "" {
			return nil, fmt.Errorf("%s route segments must be non-empty", expectedScheme)
		}
	}
	return segs, nil
}
