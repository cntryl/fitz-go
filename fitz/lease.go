package fitz

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"time"

	internallease "github.com/cntryl/fitz-go/v2/internal/domains/lease"
)

type Lease struct {
	token     []byte
	ExpiresAt int64

	inner *internallease.Lease
	mu    sync.Mutex
}

// LeaseAuthority is the immutable admission authority for one managed lease
// callback invocation. FencingToken is the token returned by the ACQUIRE that
// admitted the callback, not the live credential used by later renewals.
type LeaseAuthority struct {
	FencingToken uint64
}

type leaseAuthorityContextKey struct{}

// LeaseAuthorityFromContext returns the managed lease admission authority
// attached to a WithLease callback context.
func LeaseAuthorityFromContext(ctx context.Context) (LeaseAuthority, bool) {
	if ctx == nil {
		return LeaseAuthority{}, false
	}
	authority, ok := ctx.Value(leaseAuthorityContextKey{}).(LeaseAuthority)
	return authority, ok
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

func (l *Lease) admissionAuthority() LeaseAuthority {
	l.mu.Lock()
	defer l.mu.Unlock()
	return LeaseAuthority{FencingToken: binary.BigEndian.Uint64(l.token)}
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
	Acquire(ctx context.Context, route string, ttlSecs uint64, options LeaseAcquireOptions) (*Lease, error)
	WithLease(ctx context.Context, route string, ttlSecs uint64, callback func(context.Context) error, opts ...WithLeaseOption) error
	Query(ctx context.Context, route string) (*LeaseInfo, error)
	Subscribe(ctx context.Context, route string, handler LeaseChangeHandler) (*LeaseSubscription, error)
}

type leaseExecutionOptions struct {
	ownerID     string
	waitSeconds uint32
}

type LeaseAcquireOptions struct {
	OwnerID     string
	WaitSeconds uint32
}

// WithLeaseOption configures a managed lease execution.
type WithLeaseOption func(*leaseExecutionOptions)

func WithLeaseOwnerID(ownerID string) WithLeaseOption {
	return func(options *leaseExecutionOptions) { options.ownerID = ownerID }
}

// WithLeaseWaitSeconds uses the broker's FIFO acquisition queue.
func WithLeaseWaitSeconds(waitSeconds uint32) WithLeaseOption {
	return func(options *leaseExecutionOptions) { options.waitSeconds = waitSeconds }
}

type leaseClient struct {
	inner internallease.Client
}

type callbackOutcome struct {
	err      error
	panicVal any
}

type managedLeaseOutcome struct {
	callbackErr   error
	callbackPanic any
	leaseLoss     error
}

// Acquire attempts to acquire a lease for the route.
func (c *leaseClient) Acquire(ctx context.Context, route string, ttlSecs uint64, options LeaseAcquireOptions) (*Lease, error) {
	lease, err := c.inner.Acquire(ctx, route, ttlSecs, internallease.AcquireOptions{OwnerID: options.OwnerID, WaitSeconds: options.WaitSeconds})
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

	lease, err := c.Acquire(ctx, route, ttlSecs, LeaseAcquireOptions{OwnerID: options.ownerID, WaitSeconds: options.waitSeconds})
	if err != nil {
		return err
	}
	outcome := c.superviseManagedLease(ctx, lease, ttlSecs, callback)
	return finalizeManagedLease(ctx, lease, outcome)
}

func (c *leaseClient) superviseManagedLease(ctx context.Context, lease *Lease, ttlSecs uint64, callback func(context.Context) error) managedLeaseOutcome {
	authorityCtx := context.WithValue(ctx, leaseAuthorityContextKey{}, lease.admissionAuthority())
	callbackCtx, cancelCallback := context.WithCancelCause(authorityCtx)
	defer cancelCallback(nil)
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
	outcome := managedLeaseOutcome{}
	parentDone := ctx.Done()
	for {
		select {
		case callback := <-callbackResult:
			outcome.callbackErr, outcome.callbackPanic = callback.err, callback.panicVal
			return outcome
		case <-parentDone:
			cancelCallback(context.Cause(ctx))
			parentDone = nil
		case <-timer.C:
			if _, err := lease.Extend(context.WithoutCancel(ctx), ttlSecs); err != nil {
				outcome.leaseLoss = errors.Join(ErrLeaseLost, err)
				cancelCallback(outcome.leaseLoss)
				select {
				case callback := <-callbackResult:
					outcome.callbackErr, outcome.callbackPanic = callback.err, callback.panicVal
				default:
				}
				return outcome
			}
			timer.Reset(renewEvery)
		}
	}
}

func finalizeManagedLease(ctx context.Context, lease *Lease, outcome managedLeaseOutcome) error {
	if outcome.leaseLoss != nil {
		if outcome.callbackPanic != nil {
			panic(outcome.callbackPanic)
		}
		if outcome.callbackErr != nil && !errors.Is(outcome.callbackErr, context.Canceled) {
			return errors.Join(outcome.leaseLoss, outcome.callbackErr)
		}
		return outcome.leaseLoss
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	releaseErr := lease.Release(cleanupCtx)
	cleanupCancel()
	if outcome.callbackPanic != nil {
		panic(outcome.callbackPanic)
	}
	if ctx.Err() != nil {
		return errors.Join(context.Cause(ctx), outcome.callbackErr, releaseErr)
	}
	return errors.Join(outcome.callbackErr, releaseErr)
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
