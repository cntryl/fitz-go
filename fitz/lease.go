package fitz

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"time"

	internaliter "github.com/cntryl/fitz-go/v2/internal/core/iter"
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

func (s *LeaseSubscription) Completion() <-chan error {
	if s == nil || s.inner == nil {
		return nil
	}
	return s.inner.Completion()
}

type LeaseInfo struct {
	Held             bool
	OwnerID          string
	TTLRemainingSecs uint64
	PendingWaiters   uint32
}

// LeaseEntry describes one lease returned by List.
type LeaseEntry struct {
	Route             string
	OwnerID           string // The logical owner_id passed to ACQUIRE (never a raw session id).
	HolderIncarnation uint64 // Opaque; stable for one live session, distinct per session/reconnect.
	AcquiredAt        string // RFC3339 timestamp string.
	ExpiresInSecs     uint64
	Renewals          uint32
}

// ListOptions configures a List call.
type ListOptions struct {
	// Limit caps the page size fetched per wire round trip. 0 uses the server
	// default page size; the server clamps any requested limit to its maximum.
	Limit uint32
}

// ListOption configures a List call.
type ListOption func(*ListOptions)

// WithListLimit sets the page size requested per wire round trip.
func WithListLimit(limit uint32) ListOption {
	return func(o *ListOptions) { o.Limit = limit }
}

type LeaseClient interface {
	Acquire(ctx context.Context, route string, ttlSecs uint64, options LeaseAcquireOptions) (*Lease, error)
	WithLease(ctx context.Context, route string, ttlSecs uint64, callback func(context.Context) error, opts ...WithLeaseOption) error
	Query(ctx context.Context, route string) (*LeaseInfo, error)
	Subscribe(ctx context.Context, route string, handler LeaseChangeHandler) (*LeaseSubscription, error)

	// List returns an iterator over lease entries matching pattern (wildcards
	// allowed, per the same grammar as Subscribe). The iterator transparently
	// pages through the broker's cursor-based LIST protocol.
	List(ctx context.Context, pattern string, opts ...ListOption) (Iterator[LeaseEntry], error)

	// Observe establishes a race-safe, continuously reconciled inventory
	// observer for lease entries matching pattern (wildcards allowed, per the
	// same grammar as Subscribe). It owns the subscribe-then-list bootstrap
	// sequence, full LIST reconciliation after invalidations, periodic reconciliation,
	// and reconnect recovery, so callers never need to hand-roll any of it.
	// Call Close on the returned observer when done.
	Observe(ctx context.Context, pattern string, opts ...ObserveOption) (*InventoryObserver, error)
}

// ObserveOptions configures an Observe call.
type ObserveOptions struct {
	// ReconcileInterval is the base interval between full backstop relists.
	// Defaults to 60s. The actual delay is jittered by +/- ReconcileJitter.
	ReconcileInterval time.Duration
	// ReconcileJitter is the fractional jitter applied to ReconcileInterval
	// (e.g. 0.2 for +/- 20%). Defaults to 0.2. A value <= 0 disables jitter.
	ReconcileJitter float64
}

// ObserveOption configures an Observe call.
type ObserveOption func(*ObserveOptions)

// WithObserveReconcileInterval sets the base periodic full-relist interval.
func WithObserveReconcileInterval(interval time.Duration) ObserveOption {
	return func(o *ObserveOptions) { o.ReconcileInterval = interval }
}

// WithObserveReconcileJitter sets the fractional jitter applied to the
// reconcile interval. A value <= 0 disables jitter (fixed interval).
func WithObserveReconcileJitter(fraction float64) ObserveOption {
	return func(o *ObserveOptions) { o.ReconcileJitter = fraction }
}

// InventoryObserver maintains a race-safe, continuously reconciled view of
// lease entries matching a selector pattern. Construct one via
// LeaseClient.Observe and call Close when done.
type InventoryObserver struct {
	inner *internallease.InventoryObserver
}

// View returns a snapshot of the currently observed lease entries.
func (o *InventoryObserver) View() []LeaseEntry {
	internalView := o.inner.View()
	view := make([]LeaseEntry, 0, len(internalView))
	for _, entry := range internalView {
		view = append(view, LeaseEntry{
			Route:             entry.Route,
			OwnerID:           entry.OwnerID,
			HolderIncarnation: entry.HolderIncarnation,
			AcquiredAt:        entry.AcquiredAt,
			ExpiresInSecs:     entry.ExpiresInSecs,
			Renewals:          entry.Renewals,
		})
	}
	return view
}

// Ready reports whether the observer has completed its initial (or
// post-reconnect) bootstrap and holds a current view.
func (o *InventoryObserver) Ready() bool {
	return o.inner.Ready()
}

// Changed returns a channel that receives a value whenever the observed view
// changes. Sends are coalesced (buffered 1, non-blocking): a burst of
// changes may be represented by a single signal, so callers should always
// re-read View() rather than assume one signal means one change.
func (o *InventoryObserver) Changed() <-chan struct{} {
	return o.inner.Changed()
}

// Close unsubscribes and stops the observer's background goroutine. It is
// safe to call more than once; subsequent calls are no-ops.
func (o *InventoryObserver) Close() error {
	return o.inner.Close()
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

// List returns an iterator over lease entries matching pattern (wildcards
// allowed, per the same grammar as Subscribe). The iterator transparently
// pages through the broker's cursor-based LIST protocol.
func (c *leaseClient) List(ctx context.Context, pattern string, opts ...ListOption) (Iterator[LeaseEntry], error) {
	options := ListOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	it, err := c.inner.List(ctx, pattern, internallease.WithListLimit(options.Limit))
	if err != nil {
		return nil, err
	}
	return &leaseEntryIterator{inner: it}, nil
}

type leaseEntryIterator struct {
	inner   internaliter.Iterator[internallease.LeaseEntry]
	current LeaseEntry
}

func (it *leaseEntryIterator) Next() bool {
	if !it.inner.Next() {
		return false
	}
	value := it.inner.Value()
	it.current = LeaseEntry{
		Route:             value.Route,
		OwnerID:           value.OwnerID,
		HolderIncarnation: value.HolderIncarnation,
		AcquiredAt:        value.AcquiredAt,
		ExpiresInSecs:     value.ExpiresInSecs,
		Renewals:          value.Renewals,
	}
	return true
}

func (it *leaseEntryIterator) Value() LeaseEntry {
	return it.current
}

func (it *leaseEntryIterator) Err() error {
	return it.inner.Err()
}

func (it *leaseEntryIterator) Close() error {
	return it.inner.Close()
}

// Observe establishes a race-safe, continuously reconciled inventory
// observer for lease entries matching pattern (wildcards allowed, per the
// same grammar as Subscribe).
func (c *leaseClient) Observe(ctx context.Context, pattern string, opts ...ObserveOption) (*InventoryObserver, error) {
	// Defaults mirror the internal package's own defaults (60s +/- 20%) so an
	// unset field here behaves identically to an unset field there, rather
	// than a public zero-value silently disabling jitter.
	options := ObserveOptions{
		ReconcileInterval: 60 * time.Second,
		ReconcileJitter:   0.2,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}

	inner, err := c.inner.Observe(ctx, pattern,
		internallease.WithObserveReconcileInterval(options.ReconcileInterval),
		internallease.WithObserveReconcileJitter(options.ReconcileJitter),
	)
	if err != nil {
		return nil, err
	}
	return &InventoryObserver{inner: inner}, nil
}

var (
	ErrLeaseHeld   = internallease.ErrLeaseHeld
	ErrLeaseQueued = internallease.ErrLeaseQueued
	// ErrLeaseLost marks an uncertain or rejected managed renewal.
	ErrLeaseLost = errors.New("lease ownership lost")
	// ErrLeaseInvalidListCursor: LIST cursor is unknown, evicted, or reused
	// with a different pattern than the scan it was issued for.
	ErrLeaseInvalidListCursor = internallease.ErrInvalidListCursor
	// ErrLeaseInvalidListPattern: LIST pattern fails the wildcard selector grammar.
	ErrLeaseInvalidListPattern = internallease.ErrInvalidListPattern
)
