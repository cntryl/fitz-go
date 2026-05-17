package kv

import (
	"errors"
	"fmt"
)

// Route represents a KV domain route.
// Format: "kv://<realm>/<area>/<resource>"
// Per CLIENT_SPEC.md: realm, area, resource (never tenant/namespace/endpoint).
type Route struct {
	Realm    string // top-level partition (e.g., "prod", "staging")
	Area     string // logical grouping within realm (e.g., "users", "orders")
	Resource string // specific resource (e.g., "profiles", "settings")
}

// String returns the canonical route string representation.
func (r Route) String() string {
	return fmt.Sprintf("kv://%s/%s/%s", r.Realm, r.Area, r.Resource)
}

// NewRoute creates a new KV route.
func NewRoute(realm, area, resource string) Route {
	return Route{
		Realm:    realm,
		Area:     area,
		Resource: resource,
	}
}

// Validate checks that the route has all required fields populated.
func (r Route) Validate() error {
	if r.Realm == "" {
		return errors.New("kv route realm is required")
	}
	if r.Area == "" {
		return errors.New("kv route area is required")
	}
	if r.Resource == "" {
		return errors.New("kv route resource is required")
	}
	return nil
}
