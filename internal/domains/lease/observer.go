package lease

import (
	"context"
	"math/rand"
	"sync"
	"time"
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

	// bootstrapMu guards bootstrapping/pendingRoutes, which track whether the
	// observer is currently in a subscribe-then-list bootstrap window and
	// which routes were notified (but not yet applied) during that window.
	bootstrapMu        sync.Mutex
	bootstrapping      bool
	pendingRoutes      map[string]struct{}
	reconcileRequested bool
	// refreshMu serializes bootstrap, notification-triggered relists, and
	// periodic reconciliation so an older pass can never overwrite a newer
	// broker-lifetime view.
	refreshMu sync.Mutex

	changed chan struct{} // signaled (non-blocking, coalesced) on any view change

	unregisterReconnectHook func()

	// lifecycleMu serializes wg.Add (for the background reconnect-bootstrap
	// goroutine) against Close so a reconnect racing Close can never call
	// wg.Add concurrently with wg.Wait.
	lifecycleMu sync.Mutex
	closing     bool

	closed    chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
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

	obs := &InventoryObserver{
		client:        c,
		pattern:       pattern,
		opts:          options,
		view:          make(map[string]LeaseEntry),
		pendingRoutes: make(map[string]struct{}),
		changed:       make(chan struct{}, 1),
		closed:        make(chan struct{}),
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
		o.pendingRoutes = make(map[string]struct{})
	}
	o.bootstrapMu.Unlock()

	var createdSub *Subscription
	if subscribe {
		o.client.initNotifyHandler()
		sub, err := o.client.subscribe(ctx, o.pattern, o.handleNotify, o.invalidate)
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
	}

	for {
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
		if len(o.pendingRoutes) > 0 {
			// At least one invalidation raced this complete pass. Clear the
			// coalesced set and repeat while still buffering; never expose the
			// possibly stale pass as ready in between.
			o.pendingRoutes = make(map[string]struct{})
			o.bootstrapMu.Unlock()
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
		o.pendingRoutes[notif.Route] = struct{}{}
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

	for {
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
			o.bootstrapMu.Unlock()
			continue
		}
		o.mu.Lock()
		o.view = view
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
	defer func() { _ = it.Close() }()

	view := make(map[string]LeaseEntry)
	for it.Next() {
		entry := it.Value()
		view[entry.Route] = entry
	}
	if err := it.Err(); err != nil {
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

// onReconnect is invoked (via the lease client's reconnect hook, itself
// driven by RestoreSubscriptions) after the underlying connection
// reconnects and the observer's Subscribe has already been re-established
// by the normal subscription-restore path. It immediately marks the view
// not-ready, then re-runs the list-to-completion bootstrap (steps 3-5) in
// the background so reconnect completion for other domains is not blocked.
func (o *InventoryObserver) onReconnect(ctx context.Context) {
	o.mu.Lock()
	o.ready = false
	o.mu.Unlock()
	o.bootstrapMu.Lock()
	o.bootstrapping = true
	o.pendingRoutes = make(map[string]struct{})
	o.reconcileRequested = false
	o.bootstrapMu.Unlock()
	o.signalChanged()

	if !o.tryStartBackgroundWork() {
		return
	}
	go func() {
		defer o.wg.Done()
		select {
		case <-o.closed:
			return
		default:
		}
		_ = o.bootstrap(o.client.conn.LifecycleContext(), false)
	}()
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
			bgCtx := o.client.conn.LifecycleContext()
			o.bootstrapMu.Lock()
			o.reconcileRequested = true
			o.bootstrapMu.Unlock()
			_ = o.reconcileView(bgCtx)
		}
	}
}

func jitteredInterval(base time.Duration, fraction float64) time.Duration {
	if fraction <= 0 || base <= 0 {
		return base
	}
	delta := float64(base) * fraction
	offset := (rand.Float64()*2 - 1) * delta
	result := time.Duration(float64(base) + offset)
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
