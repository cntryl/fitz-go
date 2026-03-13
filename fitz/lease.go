package fitz

import (
	"context"

	internallease "github.com/cntryl/fitz-go/internal/domains/lease"
)

type Lease = internallease.Lease
type LeaseChangeNotification = internallease.ChangeNotification
type LeaseChangeHandler = internallease.ChangeHandler
type LeaseSubscription = internallease.Subscription

type LeaseInfo struct {
	Held             bool
	Token            []byte
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

func (c *leaseClient) Acquire(ctx context.Context, route string, ttlSecs uint64) (*Lease, error) {
	return c.inner.Acquire(ctx, route, ttlSecs)
}

func (c *leaseClient) Query(ctx context.Context, route string) (*LeaseInfo, error) {
	info, err := c.inner.Query(ctx, route)
	if err != nil {
		return nil, err
	}
	return &LeaseInfo{
		Held:             info.Held,
		Token:            info.Token,
		OwnerID:          info.OwnerID,
		TTLRemainingSecs: info.TTLRemainingSecs,
		PendingWaiters:   info.PendingWaiters,
	}, nil
}

func (c *leaseClient) Subscribe(ctx context.Context, pattern string, handler LeaseChangeHandler) (*LeaseSubscription, error) {
	return c.inner.Subscribe(ctx, pattern, handler)
}

var (
	ErrLeaseHeld   = internallease.ErrLeaseHeld
	ErrLeaseQueued = internallease.ErrLeaseQueued
)
