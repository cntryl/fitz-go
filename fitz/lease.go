package fitz

import (
	"context"

	internallease "github.com/cntryl/fitz-go/internal/domains/lease"
)

type Lease struct {
	token     []byte
	ExpiresAt int64

	inner *internallease.Lease
}

// Extend renews this lease using its current token.
func (l *Lease) Extend(ctx context.Context, ttlSecs uint64) (int64, error) {
	newExpiry, err := l.inner.Extend(ctx, ttlSecs)
	if err != nil {
		return 0, err
	}
	l.syncFromInner()
	return newExpiry, nil
}

// ExtendWithToken renews this lease using an explicit token.
func (l *Lease) ExtendWithToken(ctx context.Context, token []byte, ttlSecs uint64) (int64, error) {
	newExpiry, err := l.inner.ExtendWithToken(ctx, token, ttlSecs)
	if err != nil {
		return 0, err
	}
	l.syncFromInner()
	return newExpiry, nil
}

// Release relinquishes this lease using its current token.
func (l *Lease) Release(ctx context.Context) error {
	return l.inner.Release(ctx)
}

// ReleaseWithToken relinquishes this lease using an explicit token.
func (l *Lease) ReleaseWithToken(ctx context.Context, token []byte) error {
	return l.inner.ReleaseWithToken(ctx, token)
}

func (l *Lease) syncFromInner() {
	l.token = append(l.token[:0], l.inner.Token...)
	l.ExpiresAt = l.inner.ExpiresAt
}

type LeaseChangeNotification struct {
	Route string
}

type LeaseChangeHandler func(context.Context, LeaseChangeNotification) error

type LeaseSubscription struct {
	inner *internallease.Subscription
}

// Unsubscribe stops receiving lease change notifications.
func (s *LeaseSubscription) Unsubscribe() {
	if s != nil && s.inner != nil {
		s.inner.Unsubscribe()
	}
}

type LeaseInfo struct {
	Held             bool
	OwnerID          string
	TTLRemainingSecs uint64
	PendingWaiters   uint32
}

type LeaseClient interface {
	Acquire(ctx context.Context, route string, ttlSecs uint64) (*Lease, error)
	Query(ctx context.Context, route string) (*LeaseInfo, error)
	Subscribe(ctx context.Context, pattern string, handler LeaseChangeHandler) (*LeaseSubscription, error)
}

type leaseClient struct {
	inner internallease.Client
}

// Acquire attempts to acquire a lease for the route.
func (c *leaseClient) Acquire(ctx context.Context, route string, ttlSecs uint64) (*Lease, error) {
	lease, err := c.inner.Acquire(ctx, route, ttlSecs)
	if err != nil {
		return nil, err
	}
	publicLease := &Lease{inner: lease}
	publicLease.syncFromInner()
	return publicLease, nil
}

// Query returns the current lease state for the route.
func (c *leaseClient) Query(ctx context.Context, route string) (*LeaseInfo, error) {
	info, err := c.inner.Query(ctx, route)
	if err != nil {
		return nil, err
	}
	return &LeaseInfo{
		Held:             info.Held,
		OwnerID:          info.OwnerID,
		TTLRemainingSecs: info.TTLRemainingSecs,
		PendingWaiters:   info.PendingWaiters,
	}, nil
}

// Subscribe registers a lease change handler for the route pattern.
func (c *leaseClient) Subscribe(ctx context.Context, pattern string, handler LeaseChangeHandler) (*LeaseSubscription, error) {
	subscription, err := c.inner.Subscribe(ctx, pattern, func(ctx context.Context, notif internallease.ChangeNotification) error {
		return handler(ctx, LeaseChangeNotification{Route: notif.Route})
	})
	if err != nil {
		return nil, err
	}
	return &LeaseSubscription{inner: subscription}, nil
}

var (
	ErrLeaseHeld   = internallease.ErrLeaseHeld
	ErrLeaseQueued = internallease.ErrLeaseQueued
)
