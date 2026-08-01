package fitz

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	internallease "github.com/cntryl/fitz-go/internal/domains/lease"
)

type Lease struct {
	token     []byte
	ExpiresAt int64

	inner *internallease.Lease
	mu    sync.Mutex
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
	l.mu.Lock()
	defer l.mu.Unlock()
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
	WithLease(ctx context.Context, route string, ttlSecs uint64, callback func(context.Context) error, opts ...WithLeaseOption) error
	Query(ctx context.Context, route string) (*LeaseInfo, error)
	Subscribe(ctx context.Context, route string, handler LeaseChangeHandler) (*LeaseSubscription, error)
}

type leaseExecutionOptions struct {
	wait bool
}

// WithLeaseOption configures a managed lease execution.
type WithLeaseOption func(*leaseExecutionOptions)

// WithLeaseWait retries typed contention outcomes until acquisition or cancellation.
func WithLeaseWait() WithLeaseOption {
	return func(options *leaseExecutionOptions) { options.wait = true }
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

// WithLease owns acquisition, renewal, callback cancellation, and release.
func (c *leaseClient) WithLease(ctx context.Context, route string, ttlSecs uint64, callback func(context.Context) error, opts ...WithLeaseOption) (result error) {
	if callback == nil {
		return errors.New("lease callback must not be nil")
	}
	if ttlSecs == 0 || ttlSecs > uint64(math.MaxInt64/int64(time.Second)) {
		return errors.New("lease TTL must be positive and schedulable")
	}
	options := leaseExecutionOptions{}
	for _, option := range opts {
		option(&options)
	}

	var lease *Lease
	delay := 50 * time.Millisecond
	for {
		var err error
		lease, err = c.Acquire(ctx, route, ttlSecs)
		if err == nil {
			break
		}
		if !options.wait || (!errors.Is(err, ErrLeaseHeld) && !errors.Is(err, ErrLeaseQueued)) {
			return err
		}
		timer := time.NewTimer(time.Duration(rand.Int64N(int64(delay) + 1)))
		select {
		case <-ctx.Done():
			stopAndDrain(timer)
			return context.Cause(ctx)
		case <-timer.C:
		}
		delay = min(delay*2, time.Second)
	}

	callbackCtx, cancelCallback := context.WithCancelCause(ctx)
	defer cancelCallback(nil)
	type callbackOutcome struct {
		err      error
		panicVal any
	}
	callbackResult := make(chan callbackOutcome, 1)
	go func() {
		outcome := callbackOutcome{}
		defer func() {
			outcome.panicVal = recover()
			callbackResult <- outcome
		}()
		outcome.err = callback(callbackCtx)
	}()

	renewEvery := time.Duration(ttlSecs) * time.Second / 3
	timer := time.NewTimer(renewEvery)
	defer stopAndDrain(timer)
	var callbackErr error
	var callbackPanic any
	var leaseLoss error
	parentDone := ctx.Done()
	for callbackErr == nil && leaseLoss == nil {
		select {
		case outcome := <-callbackResult:
			callbackErr, callbackPanic = outcome.err, outcome.panicVal
			goto completed
		case <-parentDone:
			cancelCallback(context.Cause(ctx))
			parentDone = nil
		case <-timer.C:
			if _, err := lease.Extend(context.WithoutCancel(ctx), ttlSecs); err != nil {
				leaseLoss = errors.Join(ErrLeaseLost, err)
				cancelCallback(leaseLoss)
				outcome := <-callbackResult
				callbackErr, callbackPanic = outcome.err, outcome.panicVal
				goto completed
			}
			timer.Reset(renewEvery)
		}
	}

completed:
	stopAndDrain(timer)
	if leaseLoss != nil {
		if callbackPanic != nil {
			panic(callbackPanic)
		}
		if callbackErr != nil && !errors.Is(callbackErr, context.Canceled) {
			return errors.Join(leaseLoss, callbackErr)
		}
		return leaseLoss
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	releaseErr := lease.Release(cleanupCtx)
	cleanupCancel()
	if callbackPanic != nil {
		panic(callbackPanic)
	}
	if ctx.Err() != nil {
		return errors.Join(context.Cause(ctx), callbackErr, releaseErr)
	}
	return errors.Join(callbackErr, releaseErr)
}

func stopAndDrain(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
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

// Subscribe registers a lease change handler for one exact route.
func (c *leaseClient) Subscribe(ctx context.Context, route string, handler LeaseChangeHandler) (*LeaseSubscription, error) {
	subscription, err := c.inner.Subscribe(ctx, route, func(ctx context.Context, notif internallease.ChangeNotification) error {
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
	// ErrLeaseLost marks an uncertain or rejected managed renewal.
	ErrLeaseLost = errors.New("lease ownership lost")
)
