// Package lease implements the Fitz Lease domain client.
// Per CLIENT_SPEC.md: Distributed lease acquisition with fencing tokens.
package lease

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cntryl/fitz-go/v2/internal/core/connection"
	coreerrors "github.com/cntryl/fitz-go/v2/internal/core/errors"
	"github.com/cntryl/fitz-go/v2/internal/core/iter"
	"github.com/cntryl/fitz-go/v2/internal/core/reconnect"
	"github.com/cntryl/fitz-go/v2/internal/core/subscriptions"
	"github.com/cntryl/fitz-go/v2/internal/core/types"
	"github.com/cntryl/fitz-go/v2/internal/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Lease is a handle representing an acquired lease. Extend and Release are called on it.
type Lease struct {
	Token     []byte
	ExpiresAt int64

	route   string
	ownerID string
	conn    *connection.Connection
	opMu    sync.Mutex
	closed  bool
}

// Extend extends the lease TTL. Returns the new expiry timestamp.
func (l *Lease) Extend(ctx context.Context, ttlSecs uint64) (int64, error) {
	return l.extendWithToken(ctx, l.Token, ttlSecs)
}

// ExtendWithToken extends using an explicit token (e.g. for testing invalid token).
func (l *Lease) ExtendWithToken(ctx context.Context, token []byte, ttlSecs uint64) (int64, error) {
	return l.extendWithToken(ctx, token, ttlSecs)
}

func (l *Lease) extendWithToken(ctx context.Context, token []byte, ttlSecs uint64) (int64, error) {
	l.opMu.Lock()
	defer l.opMu.Unlock()
	if l.closed {
		return 0, connection.ErrStaleHandle
	}
	ctx, span := l.conn.Tracer().Start(ctx, "fitz.lease.Extend", trace.WithAttributes(
		attribute.String("fitz.route", l.route),
		attribute.Int("fitz.ttl_secs", int(ttlSecs)),
	))
	defer span.End()
	if err := l.conn.CheckLiveHandle(); err != nil {
		l.closed = true
		return 0, err
	}
	resp, err := l.conn.SendRequestWithWriter(ctx, protocol.MessageTypeLeaseRenew, leaseRenewPayloadWriter(l.route, l.ownerID, tokenToU64(token), ttlSecs))
	if err != nil {
		l.closed = true
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, fmt.Errorf("EXTEND request failed: %w", err)
	}
	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		l.closed = true
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, fmt.Errorf("EXTEND failed: %w", mapLeaseError(err))
	}
	if !success {
		l.closed = true
		recordErr := errors.New("EXTEND failed: unexpected status")
		span.RecordError(recordErr)
		span.SetStatus(codes.Error, recordErr.Error())
		return 0, recordErr
	}
	// Per CLIENT_SPEC: success = [u8 status=0][u64 BE new_fencing_token]
	if len(remaining) >= 8 {
		newToken := binary.BigEndian.Uint64(remaining[0:8])
		l.Token = make([]byte, 8)
		binary.BigEndian.PutUint64(l.Token, newToken)
	}
	if len(remaining) < 8 {
		l.closed = true
		return 0, errors.New("EXTEND failed: response missing fencing token")
	}
	newExpiry := time.Now().Unix() + int64(ttlSecs)
	l.ExpiresAt = newExpiry
	return newExpiry, nil
}

// Release frees the lease.
func (l *Lease) Release(ctx context.Context) error {
	return l.releaseWithToken(ctx, l.Token)
}

// ReleaseWithToken releases using an explicit token (e.g. for testing invalid token).
func (l *Lease) ReleaseWithToken(ctx context.Context, token []byte) error {
	return l.releaseWithToken(ctx, token)
}

func (l *Lease) releaseWithToken(ctx context.Context, token []byte) error {
	l.opMu.Lock()
	defer l.opMu.Unlock()
	if l.closed {
		return connection.ErrStaleHandle
	}
	l.closed = true
	ctx, span := l.conn.Tracer().Start(ctx, "fitz.lease.Release", trace.WithAttributes(attribute.String("fitz.route", l.route)))
	defer span.End()
	if err := l.conn.CheckLiveHandle(); err != nil {
		return err
	}
	resp, err := l.conn.SendRequestWithWriter(ctx, protocol.MessageTypeLeaseRelease, leaseReleasePayloadWriter(l.route, l.ownerID, tokenToU64(token)))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("RELEASE request failed: %w", err)
	}
	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("RELEASE failed: %w", mapLeaseError(err))
	}
	if !success {
		recordErr := errors.New("RELEASE failed: unexpected status")
		span.RecordError(recordErr)
		span.SetStatus(codes.Error, recordErr.Error())
		return recordErr
	}
	return nil
}

// ChangeNotification represents a change notification from the lease domain.
type ChangeNotification struct {
	Route string
}

// ChangeHandler is called when a lease change notification arrives (released or expired).
type ChangeHandler func(ctx context.Context, notif ChangeNotification) error

// Subscription represents an active lease change subscription.
// Call Unsubscribe to stop receiving and release the subscription.
type Subscription struct {
	subID      uint64
	route      string
	client     *client
	handler    ChangeHandler
	preNotify  func(ChangeNotification)
	completion *subscriptions.Completion
	// onOverflow, if set, is invoked (in a background goroutine, after the
	// subscription has been torn down) when the async notify-handler queue
	// was full and handleNotify tore down this subscription itself, rather
	// than merely dropping one notification. This is distinct from a
	// caller-initiated Unsubscribe/Close and lets an InventoryObserver detect
	// the unexpected teardown and recover, instead of silently degrading to
	// its periodic backstop reconciler with no signal.
	onOverflow func()
}

// Unsubscribe removes this subscription.
func (s *Subscription) Unsubscribe() {
	if s.client != nil {
		s.client.unsubscribe(s)
	}
}

func (s *Subscription) Completion() <-chan error {
	if s == nil || s.completion == nil {
		return nil
	}
	return s.completion.Done()
}

// Client is the Lease domain client interface.
type Client interface {
	// Acquire attempts to acquire a lease on the given route.
	// Returns a Lease handle on success; use Extend and Release on it.
	// Returns ErrLeaseHeld when the lease is already held by another owner.
	Acquire(ctx context.Context, route string, ttlSecs uint64, options AcquireOptions) (*Lease, error)

	// Query returns current lease status for the route.
	Query(ctx context.Context, route string) (*LeaseInfo, error)

	// Subscribe registers a handler for lease change notifications (released or expired).
	// Route accepts a wildcard selector (whole-segment "*" in any position, or a single
	// trailing "**" alias) in addition to an exact lease://realm/area/resource route.
	// Returns a Subscription that can be used to unsubscribe.
	Subscribe(ctx context.Context, route string, handler ChangeHandler) (*Subscription, error)

	// List returns an iterator over lease entries matching pattern (wildcards allowed,
	// per the same grammar as Subscribe). The iterator transparently pages through the
	// broker's cursor-based LIST protocol.
	List(ctx context.Context, pattern string, opts ...ListOption) (iter.Iterator[LeaseEntry], error)

	// Observe establishes a race-safe, continuously reconciled inventory
	// observer for lease entries matching pattern (wildcards allowed, per the
	// same grammar as Subscribe). The returned InventoryObserver owns the
	// subscribe-then-list bootstrap sequence, full LIST reconciliation after
	// invalidations, periodic reconciliation, and reconnect recovery; callers never
	// need to hand-roll this. Call Close on the observer when done.
	Observe(ctx context.Context, pattern string, opts ...ObserveOption) (*InventoryObserver, error)
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

type AcquireOptions struct {
	OwnerID     string
	WaitSeconds uint32
}

// LeaseInfo holds lease query results per CLIENT_SPEC.md QUERY response.
type LeaseInfo struct {
	Held             bool
	Token            []byte // Not set from QUERY (server returns owner_id, not token)
	OwnerID          string // Set when Held (owner_id from server)
	TTLRemainingSecs uint64 // Seconds until expiry when Held
	PendingWaiters   uint32 // Count of clients waiting in queue
}

type client struct {
	conn *connection.Connection

	mu               sync.RWMutex
	subscriptions    map[uint64]*Subscription // subID -> subscription
	initialized      bool
	acquireOnce      sync.Once
	acquireGateOnce  sync.Once
	acquireGate      chan struct{}
	acquireWaitersMu sync.Mutex
	acquireWaiters   []*acquireWaiter

	// reconnectHooks are invoked after RestoreSubscriptions successfully
	// re-establishes subscriptions on a replacement connection. Used by
	// InventoryObserver to invalidate and re-bootstrap its view on reconnect
	// without inventing a separate reconnect-signal mechanism.
	reconnectHooks map[uint64]func(ctx context.Context)
	nextHookID     uint64
}

type acquireWaiter struct {
	response chan []byte
	done     bool
}

// NewClient creates a new Lease domain client.
func NewClient(conn *connection.Connection) Client {
	return &client{
		conn:          conn,
		subscriptions: make(map[uint64]*Subscription),
	}
}

var _ reconnect.DomainRestorer = (*client)(nil)

// Acquire per CLIENT_SPEC.md:
// Request: [route_len][route][owner_id_len][owner_id][ttl_secs][optional wait_seconds]
// Response (status=0): [u8 response_type (0=Acquired, 1=AlreadyHeld, 2=Queued, 3=AlreadyQueued)][u64 BE fencing_token]
func (c *client) Acquire(ctx context.Context, route string, ttlSecs uint64, options AcquireOptions) (*Lease, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.lease.Acquire", trace.WithAttributes(
		attribute.String("fitz.route", route),
		attribute.Int("fitz.ttl_secs", int(ttlSecs)),
	))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.DebugContext(ctx, "lease.Acquire", "route", route, "ttl_secs", ttlSecs)
	}

	// Validate route format
	if err := types.ValidateFixedRoute(route, "lease", 3); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("invalid route: %w", err)
	}
	if options.OwnerID == "" {
		return nil, errors.New("lease owner ID must not be empty")
	}
	c.acquireGateOnce.Do(func() { c.acquireGate = make(chan struct{}, 1) })
	select {
	case c.acquireGate <- struct{}{}:
		defer func() { <-c.acquireGate }()
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
	c.acquireOnce.Do(func() {
		c.conn.RegisterRawPushHandler(protocol.MessageTypeLeaseAcquire, c.handleDeferredAcquire)
	})
	var waiter *acquireWaiter
	if options.WaitSeconds > 0 {
		waiter = &acquireWaiter{response: make(chan []byte, 1)}
		c.acquireWaitersMu.Lock()
		c.acquireWaiters = append(c.acquireWaiters, waiter)
		c.acquireWaitersMu.Unlock()
		defer c.markAcquireWaiterDone(waiter)
	}

	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeLeaseAcquire, leaseAcquirePayloadWriter(route, options.OwnerID, ttlSecs, options.WaitSeconds))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("ACQUIRE request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		if isLeaseHeldError(err) {
			// Don't record as span error, this is an expected condition
			return nil, ErrLeaseHeld
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("ACQUIRE failed: %w", mapLeaseError(err))
	}
	if !success {
		return nil, ErrLeaseHeld
	}

	if len(remaining) < 9 {
		recordErr := fmt.Errorf("ACQUIRE response too short: got %d bytes", len(remaining))
		span.RecordError(recordErr)
		span.SetStatus(codes.Error, recordErr.Error())
		return nil, recordErr
	}
	responseType := remaining[0]
	fencingToken := binary.BigEndian.Uint64(remaining[1:9])

	switch responseType {
	case 0: // Acquired
	case 1: // AlreadyHeld (we already hold it, idempotent)
	case 2, 3:
		if waiter == nil {
			return nil, ErrLeaseQueued
		}
		select {
		case deferred := <-waiter.response:
			success, remaining, err = connection.ParseStandardResponse(deferred)
			if err != nil {
				return nil, fmt.Errorf("ACQUIRE deferred failed: %w", mapLeaseError(err))
			}
			if !success || len(remaining) < 9 || (remaining[0] != 0 && remaining[0] != 1) {
				return nil, errors.New("invalid deferred ACQUIRE response")
			}
			fencingToken = binary.BigEndian.Uint64(remaining[1:9])
		case <-ctx.Done():
			c.markAcquireWaiterDone(waiter)
			return nil, context.Cause(ctx)
		}
	default:
		return nil, fmt.Errorf("unknown ACQUIRE response type %d", responseType)
	}
	if waiter != nil {
		c.markAcquireWaiterDone(waiter)
	}

	tokenBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tokenBytes, fencingToken)
	expiresAt := time.Now().Unix() + int64(ttlSecs)

	return &Lease{
		Token:     tokenBytes,
		ExpiresAt: expiresAt,
		route:     route,
		ownerID:   options.OwnerID,
		conn:      c.conn,
	}, nil
}

func (c *client) markAcquireWaiterDone(waiter *acquireWaiter) {
	c.acquireWaitersMu.Lock()
	for index, candidate := range c.acquireWaiters {
		if candidate != waiter {
			continue
		}
		waiter.done = true
		copy(c.acquireWaiters[index:], c.acquireWaiters[index+1:])
		c.acquireWaiters[len(c.acquireWaiters)-1] = nil
		c.acquireWaiters = c.acquireWaiters[:len(c.acquireWaiters)-1]
		break
	}
	c.acquireWaitersMu.Unlock()
}

func (c *client) handleDeferredAcquire(payload []byte) {
	c.acquireWaitersMu.Lock()
	defer c.acquireWaitersMu.Unlock()
	for len(c.acquireWaiters) > 0 {
		waiter := c.acquireWaiters[0]
		c.acquireWaiters = c.acquireWaiters[1:]
		if waiter.done {
			continue
		}
		waiter.done = true
		waiter.response <- append([]byte(nil), payload...)
		return
	}
}

// Query per CLIENT_SPEC.md:
// Request: [route_len][route]
// Response (free): [status][u8 has_holder=0][u32 pending_waiters]
// Response (held): [status][u8 has_holder=1][owner_id_len][owner_id][u64 ttl_remaining_secs][u32 pending_waiters]
func (c *client) Query(ctx context.Context, route string) (*LeaseInfo, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.lease.Query", trace.WithAttributes(attribute.String("fitz.route", route)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.DebugContext(ctx, "lease.Query", "route", route)
	}
	if err := types.ValidateFixedRoute(route, "lease", 3); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("invalid route: %w", err)
	}
	var info *LeaseInfo
	err := c.conn.RunWithRetry(ctx, func() error {
		resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeLeaseQuery, leaseQueryPayloadWriter(route))
		if err != nil {
			return fmt.Errorf("QUERY request failed: %w", err)
		}

		success, remaining, err := connection.ParseStandardResponse(resp)
		if err != nil {
			return fmt.Errorf("QUERY failed: %w", mapLeaseError(err))
		}
		if !success {
			return errors.New("QUERY failed: unexpected status")
		}

		info, err = parseLeaseQueryResponse(remaining)
		if err != nil {
			return fmt.Errorf("QUERY failed: %w", err)
		}
		return nil
	}, func(err error) bool {
		return connection.IsTransientRetryable(err) || errors.Is(err, ErrLeaseHeld)
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return info, nil
}

func parseLeaseQueryResponse(remaining []byte) (*LeaseInfo, error) {
	info := &LeaseInfo{}
	if len(remaining) < 1 {
		return nil, fmt.Errorf("QUERY response too short: got %d bytes", len(remaining))
	}

	hasHolder := remaining[0]
	if hasHolder == 0 {
		// Lease free: [u32 pending_waiters]
		if len(remaining) != 5 {
			return nil, fmt.Errorf("QUERY free response malformed: expected 5 bytes, got %d", len(remaining))
		}
		info.PendingWaiters = binary.BigEndian.Uint32(remaining[1:5])
		return info, nil
	}
	if hasHolder != 1 {
		return nil, fmt.Errorf("QUERY response invalid has_holder flag: %d", hasHolder)
	}

	info.Held = true
	offset := 1
	// owner_id (string = u32 len + bytes)
	if offset+4 > len(remaining) {
		return nil, errors.New("QUERY held response missing owner_id length")
	}
	ownerIDLen := binary.BigEndian.Uint32(remaining[offset : offset+4])
	offset += 4
	if offset+int(ownerIDLen) > len(remaining) {
		return nil, errors.New("QUERY held response truncated owner_id")
	}
	info.OwnerID = string(remaining[offset : offset+int(ownerIDLen)])
	offset += int(ownerIDLen)
	// ttl_remaining_secs (u64)
	if offset+8 > len(remaining) {
		return nil, errors.New("QUERY held response missing ttl_remaining_secs")
	}
	info.TTLRemainingSecs = binary.BigEndian.Uint64(remaining[offset : offset+8])
	offset += 8
	// pending_waiters (u32)
	if offset+4 > len(remaining) {
		return nil, errors.New("QUERY held response missing pending_waiters")
	}
	info.PendingWaiters = binary.BigEndian.Uint32(remaining[offset : offset+4])
	offset += 4
	if offset != len(remaining) {
		return nil, fmt.Errorf("QUERY held response has trailing bytes: %d", len(remaining)-offset)
	}
	return info, nil
}

func tokenToU64(token []byte) uint64 {
	if len(token) >= 8 {
		return binary.BigEndian.Uint64(token[:8])
	}
	var padded [8]byte
	copy(padded[8-len(token):], token)
	return binary.BigEndian.Uint64(padded[:])
}

func isLeaseHeldError(err error) bool {
	if err == nil {
		return false
	}
	var domainErr *coreerrors.DomainError
	if errors.As(err, &domainErr) {
		return uint32(domainErr.Code) == coreerrors.LeaseHeld
	}
	msg := err.Error()
	return strings.Contains(msg, "held") || strings.Contains(msg, "already")
}

// initNotifyHandler registers the NOTIFY handler on first use.
func (c *client) initNotifyHandler() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialized {
		return
	}
	c.initialized = true
	c.conn.RegisterNotifyHandler(protocol.MessageTypeLeaseNotify, c.handleNotify)
}

// handleNotify is called by the mux when a NOTIFY (409) frame arrives.
func (c *client) handleNotify(subID uint64, route string, payload []byte) {
	c.mu.RLock()
	sub, ok := c.subscriptions[subID]
	var preNotify func(ChangeNotification)
	if ok {
		preNotify = sub.preNotify
	}
	c.mu.RUnlock()

	if !ok {
		return // Unknown subscription
	}

	notif := ChangeNotification{
		Route: route,
	}
	if preNotify != nil {
		// Inventory observers must record an invalidation synchronously at
		// dispatch time. Their user-facing handler still runs through the
		// bounded async lane below, but cannot then lag behind a LIST response
		// and briefly expose a stale bootstrap as ready.
		preNotify(notif)
	}
	lifecycleCtx := c.conn.LifecycleContext()

	if !c.conn.LaunchAsyncHandler(lifecycleCtx, "fitz.lease.handler", c.conn.AsyncHandlerTimeout(), func(handlerCtx context.Context, span trace.Span) {
		if err := sub.handler(handlerCtx, notif); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			if log := c.conn.Logger(); log != nil {
				log.Warn("lease notify handler failed", "route", route, "error", err)
			}
		}
	}, trace.WithAttributes(
		attribute.Int64("fitz.subscription_id", int64(subID)),
		attribute.String("fitz.route", route),
	)) {
		sub.completion.Complete(&coreerrors.AsyncHandlerOverflowError{Domain: "lease", SubscriptionID: subID})
		go func() {
			c.unsubscribe(sub)
			if sub.onOverflow != nil {
				sub.onOverflow()
			}
		}()
		if log := c.conn.Logger(); log != nil {
			log.Warn("lease notify handler dropped", "route", route, "sub_id", subID, "reason", "async handler queue full")
		}
	}
}

// Subscribe registers a handler for lease change notifications.
// Route accepts an exact lease route (lease://realm/area/resource) or a
// wildcard selector using whole-segment "*" and non-adjacent "**" segments
// whose language contains depth-three Lease routes.
func (c *client) Subscribe(ctx context.Context, route string, handler ChangeHandler) (*Subscription, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.lease.Subscribe", trace.WithAttributes(attribute.String("fitz.route", route)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.DebugContext(ctx, "lease.Subscribe", "route", route)
	}
	if err := validateLeaseSubscribeSelector(route); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("invalid route: %w", err)
	}
	c.initNotifyHandler()

	sub, err := c.subscribe(ctx, route, handler, nil, nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return sub, nil
}

// unsubscribe removes a subscription.
func (c *client) unsubscribe(sub *Subscription) {
	c.mu.Lock()
	if _, exists := c.subscriptions[sub.subID]; !exists {
		c.mu.Unlock()
		return
	}
	delete(c.subscriptions, sub.subID)
	c.mu.Unlock()
	sub.completion.Complete(nil)
	c.conn.AddSubscriptions(-1)

	// Send UNSUBSCRIBE to server (best-effort, ignore errors).
	ctx := c.conn.LifecycleContext()
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeLeaseUnsubscribe, unsubscribePayloadWriter(sub.route))
	if err != nil {
		return
	}
	if _, _, err := connection.ParseStandardResponse(resp); err != nil {
		return
	}
}

func (c *client) ReplaceConnection(conn *connection.Connection) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn = conn
	if c.initialized {
		c.conn.RegisterNotifyHandler(protocol.MessageTypeLeaseNotify, c.handleNotify)
	}
}

func (c *client) RestoreSubscriptions(ctx context.Context) error {
	c.mu.RLock()
	snapshot := make([]*Subscription, 0, len(c.subscriptions))
	for _, sub := range c.subscriptions {
		snapshot = append(snapshot, sub)
	}
	c.mu.RUnlock()

	type restoredEntry struct {
		sub   *Subscription
		subID uint64
	}
	restored := make([]restoredEntry, 0, len(snapshot))
	for _, sub := range snapshot {
		newSubID, err := c.restoreSubscribe(ctx, sub.route)
		if err != nil {
			for _, entry := range restored {
				c.rollbackRestoredSubscription(entry.sub.route)
			}
			return err
		}
		restored = append(restored, restoredEntry{sub: sub, subID: newSubID})
	}

	c.mu.Lock()
	newMap := make(map[uint64]*Subscription, len(restored))
	for _, entry := range restored {
		// Mutate the existing *Subscription in place (rather than allocating a
		// replacement) so any handle a caller is holding (e.g. to call
		// Unsubscribe, or an InventoryObserver tracking its own subscription)
		// remains valid across a reconnect instead of silently going stale.
		entry.sub.subID = entry.subID
		newMap[entry.subID] = entry.sub
	}
	c.subscriptions = newMap
	hooks := make([]func(context.Context), 0, len(c.reconnectHooks))
	for _, hook := range c.reconnectHooks {
		hooks = append(hooks, hook)
	}
	c.mu.Unlock()

	for _, hook := range hooks {
		hook(ctx)
	}
	return nil
}

// restoreSubscribe re-issues SUBSCRIBE for route on the (already replaced)
// connection and returns the new subscription_id.
func (c *client) restoreSubscribe(ctx context.Context, route string) (uint64, error) {
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeLeaseSubscribe, subscribePayloadWriter(route))
	if err != nil {
		return 0, fmt.Errorf("SUBSCRIBE request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return 0, fmt.Errorf("SUBSCRIBE failed: %w", mapLeaseError(err))
	}
	if !success {
		return 0, errors.New("SUBSCRIBE failed: unexpected status")
	}

	if len(remaining) < 8 {
		return 0, fmt.Errorf("SUBSCRIBE response too short for subscription_id: got %d bytes", len(remaining))
	}
	subID, _, err := connection.ReadU64BE(remaining, 0)
	if err != nil {
		return 0, fmt.Errorf("parse subscription_id: %w", err)
	}
	c.conn.AddSubscriptions(1)
	return subID, nil
}

func (c *client) rollbackRestoredSubscription(route string) {
	c.conn.AddSubscriptions(-1)

	ctx := c.conn.LifecycleContext()
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeLeaseUnsubscribe, unsubscribePayloadWriter(route))
	if err != nil {
		return
	}
	if _, _, err := connection.ParseStandardResponse(resp); err != nil {
		return
	}
}

// registerReconnectHook registers a callback invoked after RestoreSubscriptions
// successfully re-establishes subscriptions on a replacement connection. The
// returned function unregisters the hook. Used by InventoryObserver to
// invalidate and re-bootstrap its view on reconnect.
func (c *client) registerReconnectHook(hook func(ctx context.Context)) func() {
	c.mu.Lock()
	if c.reconnectHooks == nil {
		c.reconnectHooks = make(map[uint64]func(context.Context))
	}
	id := c.nextHookID
	c.nextHookID++
	c.reconnectHooks[id] = hook
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		delete(c.reconnectHooks, id)
		c.mu.Unlock()
	}
}

func (c *client) subscribe(
	ctx context.Context,
	route string,
	handler ChangeHandler,
	preNotify func(ChangeNotification),
	onOverflow func(),
) (*Subscription, error) {
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeLeaseSubscribe, subscribePayloadWriter(route))
	if err != nil {
		return nil, fmt.Errorf("SUBSCRIBE request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("SUBSCRIBE failed: %w", mapLeaseError(err))
	}
	if !success {
		return nil, errors.New("SUBSCRIBE failed: unexpected status")
	}

	if len(remaining) < 8 {
		return nil, fmt.Errorf("SUBSCRIBE response too short for subscription_id: got %d bytes", len(remaining))
	}
	subID, _, err := connection.ReadU64BE(remaining, 0)
	if err != nil {
		return nil, fmt.Errorf("parse subscription_id: %w", err)
	}
	c.conn.AddSubscriptions(1)

	sub := &Subscription{
		subID:      subID,
		route:      route,
		client:     c,
		handler:    handler,
		preNotify:  preNotify,
		onOverflow: onOverflow,
		completion: subscriptions.NewCompletion(),
	}
	c.mu.Lock()
	c.subscriptions[subID] = sub
	c.mu.Unlock()
	return sub, nil
}

// List returns an iterator over lease entries matching pattern (msg_type 410).
// pattern accepts the same wildcard grammar as Subscribe. The returned
// iterator lazily pages through the broker's cursor-based LIST protocol: each
// Next() call may issue a wire round trip when the current page is exhausted.
func (c *client) List(ctx context.Context, pattern string, opts ...ListOption) (iter.Iterator[LeaseEntry], error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.lease.List", trace.WithAttributes(attribute.String("fitz.pattern", pattern)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.DebugContext(ctx, "lease.List", "pattern", pattern)
	}
	if err := validateLeaseSubscribeSelector(pattern); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("invalid pattern: %w", err)
	}

	options := ListOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	return &leaseListIterator{
		ctx:     ctx,
		conn:    c.conn,
		pattern: pattern,
		limit:   options.Limit,
		index:   -1,
	}, nil
}

// leaseListIterator is a pull-based iter.Iterator[LeaseEntry] that pages
// through LEASE_LIST (410) responses on demand, one wire round trip per
// exhausted page.
type leaseListIterator struct {
	ctx     context.Context
	conn    *connection.Connection
	pattern string
	limit   uint32

	items   []LeaseEntry
	index   int
	cursor  *leaseListCursor
	started bool
	done    bool
	err     error
}

func (it *leaseListIterator) Next() bool {
	if it.err != nil {
		return false
	}
	for {
		if it.index+1 < len(it.items) {
			it.index++
			return true
		}
		if it.started && it.done {
			return false
		}
		if err := it.fetchPage(); err != nil {
			it.err = err
			return false
		}
	}
}

func (it *leaseListIterator) fetchPage() error {
	resp, err := it.conn.SendRequestWithWriter(it.ctx, protocol.MessageTypeLeaseList, leaseListPayloadWriter(it.pattern, it.cursor, it.limit))
	if err != nil {
		return fmt.Errorf("LIST request failed: %w", err)
	}
	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return fmt.Errorf("LIST failed: %w", mapLeaseError(err))
	}
	if !success {
		return errors.New("LIST failed: unexpected status")
	}
	items, next, err := parseLeaseListResponse(remaining)
	if err != nil {
		return fmt.Errorf("LIST failed: %w", err)
	}
	if len(items) == 0 && next != nil {
		return errors.New("LIST returned an empty page with more results pending")
	}
	it.items = items
	it.index = -1
	it.cursor = next
	it.started = true
	it.done = next == nil
	return nil
}

// Value returns the current item (valid only after a successful Next()).
func (it *leaseListIterator) Value() LeaseEntry {
	if it.index < 0 || it.index >= len(it.items) {
		return LeaseEntry{}
	}
	return it.items[it.index]
}

// Err returns the first non-EOF error encountered.
func (it *leaseListIterator) Err() error {
	return it.err
}

// Close releases any resources associated with the iterator.
// leaseListIterator holds no resources beyond the shared connection, so this is a no-op.
func (it *leaseListIterator) Close() error {
	return nil
}
