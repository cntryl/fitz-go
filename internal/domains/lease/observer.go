package lease

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/cntryl/fitz-go/v2/internal/core/iter"
	"github.com/cntryl/fitz-go/v2/internal/core/retry"
)

// Default periodic reconciliation cadence for an InventoryObserver, per
// docs/clients/client-requirements.md REQ-API-004C: a full relist backstop
// against a missed or dropped LEASE_NOTIFY, with jitter so a fleet of
// observers does not reconcile in lockstep.
const (
	defaultReconcileInterval = 60 * time.Second
	defaultReconcileJitter   = 0.2 // +/- 20%
)

// ObserveOptions configures an InventoryObserver.
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
// lease entries matching a selector pattern. It owns the full bootstrap
// sequence (subscribe, then list to completion, buffering and draining any
// notifications that race the bootstrap), complete LIST reconciliation on
// each notification, periodic full-relist reconciliation
// as a backstop, and reconnect recovery. Callers never need to hand-roll any
// of this; construct one via Client.Observe and call Close when done.
type InventoryObserver struct {
	client  *client
	pattern string
	opts    ObserveOptions

	mu    sync.RWMutex
	view  map[string]LeaseEntry
	ready bool
	sub   *Subscription

	// bootstrapMu guards bootstrapping/pendingInvalidation, which track
	// whether the observer is currently in a subscribe-then-list bootstrap
	// window and whether any notification arrived (but was not yet applied)
	// during that window.
	bootstrapMu            sync.Mutex
	bootstrapping          bool
	pendingInvalidation    bool
	reconcileRequested     bool
	subscriptionGeneration uint64
	// refreshMu serializes bootstrap, notification-triggered relists, and
	// periodic reconciliation so an older pass can never overwrite a newer
	// broker-lifetime view.
	refreshMu sync.Mutex

	changed chan struct{} // signaled (non-blocking, coalesced) on any view change

	unregisterReconnectHook func()

	// lifecycleMu serializes wg.Add (for the background reconnect-bootstrap
	// goroutine) against Close so a reconnect racing Close can never call
	// wg.Add concurrently with wg.Wait.
	lifecycleMu            sync.Mutex
	closing                bool
	recoveryRunning        bool
	recoveryRequested      bool
	recoveryNeedsSubscribe bool

	closed    chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup

	// bgCtx is the context passed to network calls made by the observer's
	// background goroutines (reconnect-triggered bootstrap, periodic
	// reconcile). bgCancel is invoked by Close before wg.Wait, so an in-flight
	// List call started by a background goroutine is canceled promptly
	// instead of leaving Close blocked on it indefinitely.
	bgCtx    context.Context
	bgCancel context.CancelFunc
}

// tryStartBackgroundWork registers one more background goroutine with wg,
// unless the observer is already closing. Callers that get false must not
// start their goroutine.
func (o *InventoryObserver) tryStartBackgroundWork() bool {
	o.lifecycleMu.Lock()
	defer o.lifecycleMu.Unlock()
	if o.closing {
		return false
	}
	o.wg.Add(1)
	return true
}

// Observe establishes an InventoryObserver for pattern (wildcards allowed,
// per the same grammar as Subscribe). It blocks until the initial bootstrap
// (subscribe + full list) completes, so the returned observer is
// immediately Ready.
func (c *client) Observe(ctx context.Context, pattern string, opts ...ObserveOption) (*InventoryObserver, error) {
	if err := validateLeaseSubscribeSelector(pattern); err != nil {
		return nil, err
	}

	options := ObserveOptions{
		ReconcileInterval: defaultReconcileInterval,
		ReconcileJitter:   defaultReconcileJitter,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	if options.ReconcileInterval <= 0 {
		options.ReconcileInterval = defaultReconcileInterval
	}

	bgCtx, bgCancel := context.WithCancel(context.Background())
	obs := &InventoryObserver{
		client:   c,
		pattern:  pattern,
		opts:     options,
		view:     make(map[string]LeaseEntry),
		changed:  make(chan struct{}, 1),
		closed:   make(chan struct{}),
		bgCtx:    bgCtx,
		bgCancel: bgCancel,
	}

	if err := obs.bootstrap(ctx, true); err != nil {
		return nil, err
	}

	obs.unregisterReconnectHook = c.registerReconnectHook(obs.onReconnect)

	obs.tryStartBackgroundWork() // always succeeds here: nothing can close obs before it is returned
	go obs.run()

	return obs, nil
}

// bootstrap runs the subscribe-then-list sequence:
//  1. (if subscribe is true) establish the patterned Subscribe and wait for
//     its acknowledgement.
//  2. begin buffering incoming notifications for the observer's pattern
//     instead of applying them (bootstrapping = true, set before either step
//     1 or 2 can race a notification).
//  3. List to completion, building a local route -> LeaseEntry map.
//  4. install that map as the current view and mark ready.
//  5. drain whatever notifications were buffered during 1-4: if any arrived,
//     do one more full List pass and replace the view.
func (o *InventoryObserver) bootstrap(ctx context.Context, subscribe bool) error {
	o.refreshMu.Lock()
	defer o.refreshMu.Unlock()

	o.bootstrapMu.Lock()
	if !o.bootstrapping {
		o.bootstrapping = true
		o.pendingInvalidation = false
	}
	o.bootstrapMu.Unlock()

	var createdSub *Subscription
	if subscribe {
		o.client.initNotifyHandler()
		sub, err := o.client.subscribe(ctx, o.pattern, o.handleNotify, o.invalidate, o.onOverflow)
		if err != nil {
			o.bootstrapMu.Lock()
			o.bootstrapping = false
			o.bootstrapMu.Unlock()
			return err
		}
		o.mu.Lock()
		o.sub = sub
		o.mu.Unlock()
		createdSub = sub
		o.bootstrapMu.Lock()
		o.subscriptionGeneration++
		o.bootstrapMu.Unlock()
	}

	for attempt := 0; ; attempt++ {
		o.bootstrapMu.Lock()
		passSubscriptionGeneration := o.subscriptionGeneration
		o.bootstrapMu.Unlock()
		view, err := o.listAll(ctx)
		if err != nil {
			o.bootstrapMu.Lock()
			o.bootstrapping = false
			o.bootstrapMu.Unlock()
			if createdSub != nil {
				createdSub.Unsubscribe()
			}
			return err
		}

		o.bootstrapMu.Lock()
		if o.subscriptionGeneration != passSubscriptionGeneration {
			o.bootstrapMu.Unlock()
			if createdSub != nil {
				createdSub.Unsubscribe()
			}
			return errObserverSubscriptionChanged
		}
		if o.pendingInvalidation {
			// At least one invalidation raced this complete pass. Clear the
			// coalesced flag and repeat while still buffering; never expose
			// the possibly stale pass as ready in between. A bounded backoff
			// between attempts avoids a livelock spinning at wire speed
			// against a steady stream of invalidations.
			o.pendingInvalidation = false
			o.bootstrapMu.Unlock()
			waitInvalidationBackoff(ctx, attempt)
			continue
		}

		o.mu.Lock()
		o.view = view
		o.ready = true
		o.mu.Unlock()
		o.bootstrapping = false
		o.bootstrapMu.Unlock()
		o.signalChanged()
		return nil
	}
}

// invalidate runs synchronously in the wire notification dispatcher,
// before the observer's bounded async handler is scheduled. This closes the
// acknowledgement/LIST handoff race where an earlier notification could
// otherwise be processed only after a stale first pass was marked ready, and
// coalesces a steady-state burst into as few full relists as needed.
func (o *InventoryObserver) invalidate(notif ChangeNotification) {
	o.bootstrapMu.Lock()
	defer o.bootstrapMu.Unlock()
	if o.bootstrapping {
		o.pendingInvalidation = true
	} else {
		o.reconcileRequested = true
	}
}

// handleNotify is the ChangeHandler registered on the observer's Subscribe.
// During a bootstrap window it buffers the route for the post-bootstrap
// drain; in steady state it relists the selector. QUERY cannot reconstruct
// holder incarnation, acquisition time, or renewal count, so using it here
// would silently replace complete LIST items with partial placeholders.
func (o *InventoryObserver) handleNotify(ctx context.Context, _ ChangeNotification) error {
	o.bootstrapMu.Lock()
	if o.bootstrapping {
		o.bootstrapMu.Unlock()
		return nil
	}
	o.bootstrapMu.Unlock()

	return o.reconcileView(ctx)
}

func (o *InventoryObserver) reconcileView(ctx context.Context) error {
	o.refreshMu.Lock()
	defer o.refreshMu.Unlock()

	for attempt := 0; ; attempt++ {
		o.bootstrapMu.Lock()
		if o.bootstrapping || !o.reconcileRequested {
			o.bootstrapMu.Unlock()
			return nil
		}
		o.reconcileRequested = false
		o.bootstrapMu.Unlock()

		view, err := o.listAll(ctx)
		if err != nil {
			return err
		}
		o.bootstrapMu.Lock()
		if o.bootstrapping {
			o.bootstrapMu.Unlock()
			return nil
		}
		if o.reconcileRequested {
			// Another invalidation raced this pass; retry with a bounded
			// backoff so a steady stream of invalidations cannot livelock
			// this loop spinning at wire speed.
			o.bootstrapMu.Unlock()
			waitInvalidationBackoff(ctx, attempt)
			continue
		}
		o.mu.Lock()
		o.view = view
		// A successful reconcile pass is itself a sufficient correctness
		// signal for readiness: restore ready here so that if a post-reconnect
		// background bootstrap (see onReconnect) fails and gives up, a
		// subsequent notify-triggered reconcile still recovers Ready() rather
		// than leaving it permanently false despite a live, correct view.
		o.ready = true
		o.mu.Unlock()
		o.bootstrapMu.Unlock()
		o.signalChanged()
		return nil
	}
}

// listAll pages through List(o.pattern) to completion and returns the
// resulting route -> LeaseEntry map.
func (o *InventoryObserver) listAll(ctx context.Context) (map[string]LeaseEntry, error) {
	it, err := o.client.List(ctx, o.pattern)
	if err != nil {
		return nil, err
	}

	view := make(map[string]LeaseEntry)
	if err := iter.ForEach(it, func(entry LeaseEntry) error {
		view[entry.Route] = entry
		return nil
	}); err != nil {
		return nil, err
	}
	return view, nil
}

func (o *InventoryObserver) signalChanged() {
	select {
	case o.changed <- struct{}{}:
	default:
	}
}

// triggerBackgroundBootstrap marks the view not-ready and re-runs the
// list-to-completion bootstrap (subscribe-then-list steps, or just steps
// 3-5 if the subscription itself is still valid) in the background, so the
// caller (a reconnect hook or an overflow hook) is never blocked on it. It
// is the shared recovery path for both onReconnect and onOverflow.
func (o *InventoryObserver) triggerBackgroundBootstrap(subscribe bool) {
	o.mu.Lock()
	o.ready = false
	o.mu.Unlock()
	o.bootstrapMu.Lock()
	o.bootstrapping = true
	o.pendingInvalidation = false
	o.reconcileRequested = false
	o.subscriptionGeneration++
	o.bootstrapMu.Unlock()
	o.signalChanged()

	o.lifecycleMu.Lock()
	if o.closing {
		o.lifecycleMu.Unlock()
		return
	}
	o.recoveryRequested = true
	o.recoveryNeedsSubscribe = o.recoveryNeedsSubscribe || subscribe
	if o.recoveryRunning {
		o.lifecycleMu.Unlock()
		return
	}
	o.recoveryRunning = true
	o.wg.Add(1)
	o.lifecycleMu.Unlock()

	go func() {
		defer o.wg.Done()
		attempt := 0
		for {
			select {
			case <-o.closed:
				o.lifecycleMu.Lock()
				o.recoveryRunning = false
				o.lifecycleMu.Unlock()
				return
			default:
			}

			o.lifecycleMu.Lock()
			needsSubscribe := o.recoveryNeedsSubscribe
			o.recoveryNeedsSubscribe = false
			o.recoveryRequested = false
			o.lifecycleMu.Unlock()

			// bgCtx is canceled by Close before wg.Wait, so a List (and, for
			// onOverflow, Subscribe) call in flight when Close is invoked is
			// canceled promptly instead of blocking Close indefinitely
			// (finding: Close() can hang).
			err := o.bootstrap(o.bgCtx, needsSubscribe)
			if o.bgCtx.Err() != nil {
				o.lifecycleMu.Lock()
				o.recoveryRunning = false
				o.lifecycleMu.Unlock()
				return
			}

			o.lifecycleMu.Lock()
			requested := o.recoveryRequested
			if err != nil && needsSubscribe {
				o.recoveryRequested = true
				o.recoveryNeedsSubscribe = true
				requested = true
			}
			if err == nil && !requested {
				o.recoveryRunning = false
				o.lifecycleMu.Unlock()
				return
			}
			if err != nil && !needsSubscribe && !errors.Is(err, errObserverSubscriptionChanged) {
				o.recoveryRunning = false
				o.lifecycleMu.Unlock()
				return
			}
			o.lifecycleMu.Unlock()

			// Overflow removes the wire registration, so a transient failure
			// while replacing it cannot be left for the periodic LIST-only
			// reconciler: without a subscription the observer would miss every
			// subsequent live invalidation. Retry the complete subscribe+LIST
			// bootstrap with the same bounded backoff used for raced passes.
			waitInvalidationBackoff(o.bgCtx, attempt)
			attempt++
		}
	}()
}

// onReconnect is invoked (via the lease client's reconnect hook, itself
// driven by RestoreSubscriptions) after the underlying connection
// reconnects and the observer's Subscribe has already been re-established
// by the normal subscription-restore path. subscribe=false: only the
// list-to-completion steps need to re-run.
func (o *InventoryObserver) onReconnect(ctx context.Context) {
	o.triggerBackgroundBootstrap(false)
}

// onOverflow is registered as the observer's underlying subscription's
// overflow hook and invoked when the lease client's bounded async
// notify-handler queue was full, causing handleNotify to tear down the
// subscription entirely (see lease.go handleNotify). Without this hook,
// InventoryObserver would have no way to detect the teardown (unlike
// onReconnect, there was previously no signal at all) and would silently
// degrade to relying solely on the periodic backstop reconciler, with
// Ready() still reporting true on an increasingly stale view.
//
// subscribe=true here (unlike onReconnect) because the old subscription no
// longer exists on the broker at all and must be re-established, not just
// relisted.
func (o *InventoryObserver) onOverflow() {
	o.triggerBackgroundBootstrap(true)
}

// run drives periodic full-relist reconciliation, the backstop against a
// missed or dropped LEASE_NOTIFY.
func (o *InventoryObserver) run() {
	defer o.wg.Done()
	for {
		timer := time.NewTimer(jitteredInterval(o.opts.ReconcileInterval, o.opts.ReconcileJitter))
		select {
		case <-o.closed:
			timer.Stop()
			return
		case <-timer.C:
			o.bootstrapMu.Lock()
			o.reconcileRequested = true
			o.bootstrapMu.Unlock()
			_ = o.reconcileView(o.bgCtx)
		}
	}
}

// invalidationRetryBackoff bounds the delay between consecutive
// invalidation-raced retries in bootstrap/reconcileView so a steady stream
// of invalidations cannot livelock either loop into relisting at wire speed.
var invalidationRetryBackoff = retry.BackoffConfig{
	InitialDelay: 10 * time.Millisecond,
	MaxDelay:     2 * time.Second,
	Multiplier:   2,
	JitterFactor: 0.2,
}

var errObserverSubscriptionChanged = errors.New("lease observer subscription changed during bootstrap")

// waitInvalidationBackoff sleeps for the backoff delay corresponding to
// attempt (0-based), or returns early if ctx is done.
func waitInvalidationBackoff(ctx context.Context, attempt int) {
	delay := retry.CalculateDelay(invalidationRetryBackoff, attempt)
	if delay <= 0 {
		return
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

func jitteredInterval(base time.Duration, fraction float64) time.Duration {
	if fraction <= 0 || base <= 0 {
		return base
	}
	// Reuse the shared retry package's jitter math rather than
	// reimplementing it: a fixed (non-exponential) delay with +/- fraction
	// jitter is exactly BackoffConfig{InitialDelay: MaxDelay: base,
	// Multiplier: 1, JitterFactor: fraction} evaluated at attempt 0.
	result := retry.CalculateDelay(retry.BackoffConfig{
		InitialDelay: base,
		MaxDelay:     base,
		Multiplier:   1,
		JitterFactor: fraction,
	}, 0)
	if result <= 0 {
		return base
	}
	return result
}

// View returns a snapshot of the currently observed lease entries.
func (o *InventoryObserver) View() []LeaseEntry {
	o.mu.RLock()
	defer o.mu.RUnlock()
	out := make([]LeaseEntry, 0, len(o.view))
	for _, entry := range o.view {
		out = append(out, entry)
	}
	return out
}

// Ready reports whether the observer has completed its initial (or
// post-reconnect) bootstrap and holds a current view.
func (o *InventoryObserver) Ready() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.ready
}

// Changed returns a channel that receives a value whenever the observed view
// changes. Sends are coalesced (buffered 1, non-blocking): a burst of
// changes may be represented by a single signal, so callers should always
// re-read View() rather than assume one signal means one change.
func (o *InventoryObserver) Changed() <-chan struct{} {
	return o.changed
}

// Close unsubscribes and stops the observer's background goroutine. It is
// safe to call more than once; subsequent calls are no-ops.
func (o *InventoryObserver) Close() error {
	o.closeOnce.Do(func() {
		o.lifecycleMu.Lock()
		o.closing = true
		o.lifecycleMu.Unlock()

		close(o.closed)
		o.bgCancel()
		if o.unregisterReconnectHook != nil {
			o.unregisterReconnectHook()
		}
		o.mu.RLock()
		sub := o.sub
		o.mu.RUnlock()
		if sub != nil {
			sub.Unsubscribe()
		}
	})
	o.wg.Wait()
	return nil
}
