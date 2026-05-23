// Package queue implements the Fitz Queue domain client.
// Per CLIENT_SPEC.md: FIFO message queue with lease-based processing.
package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/core/reconnect"
	"github.com/cntryl/fitz-go/internal/core/subscriptions"
	"github.com/cntryl/fitz-go/internal/core/types"
	"github.com/cntryl/fitz-go/internal/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// QueueItem represents a received (reserved) queue message.
// Extend and Complete are called on the item; route and token are tracked internally.
type QueueItem struct {
	ID    uint64
	Token uint64
	Body  []byte

	route string
	conn  *connection.Connection
}

// AvailabilityNotification represents an availability notification from the queue.
type AvailabilityNotification struct {
	Route string
}

// AvailabilityHandler is called when availability notification arrives.
type AvailabilityHandler func(ctx context.Context, notif AvailabilityNotification) error

type reserveConfig struct {
	batchSize   uint32
	waitSeconds uint64
}

type ReserveOption func(*reserveConfig)

func WithBatchSize(batchSize uint32) ReserveOption {
	return func(cfg *reserveConfig) {
		cfg.batchSize = batchSize
	}
}

func WithWaitSeconds(waitSeconds uint64) ReserveOption {
	return func(cfg *reserveConfig) {
		cfg.waitSeconds = waitSeconds
	}
}

// Subscription represents an active queue availability subscription.
// Call Unsubscribe to stop receiving and release the subscription.
type Subscription struct {
	subID     uint64
	handlerID uint64
	pattern   string
	client    *client
}

// Unsubscribe removes this subscription.
func (s *Subscription) Unsubscribe() {
	if s.client != nil {
		s.client.unsubscribe(s)
	}
}

// Extend extends the lease on this queue item.
func (q *QueueItem) Extend(ctx context.Context, leaseSecs uint64) error {
	ctx, span := q.conn.Tracer().Start(ctx, "fitz.queue.Extend", trace.WithAttributes(
		attribute.String("fitz.route", q.route),
		attribute.Int64("fitz.message_id", int64(q.ID)),
	))
	defer span.End()
	resp, err := q.conn.SendRequestWithWriter(ctx, protocol.MessageTypeQueueExtend, extendPayloadWriter(q.route, q.ID, q.Token, leaseSecs))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("extend request failed: %w", err)
	}
	success, _, err := parseQueueResponse(resp)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("extend failed: %w", err)
	}
	if !success {
		recordErr := errors.New("extend failed: unexpected status")
		span.RecordError(recordErr)
		span.SetStatus(codes.Error, recordErr.Error())
		return recordErr
	}
	return nil
}

// Complete acknowledges processing of this queue item and removes it from the queue.
func (q *QueueItem) Complete(ctx context.Context) error {
	return q.CompleteWithToken(ctx, q.Token)
}

// CompleteWithToken acknowledges the item using an explicit token.
func (q *QueueItem) CompleteWithToken(ctx context.Context, token uint64) error {
	ctx, span := q.conn.Tracer().Start(ctx, "fitz.queue.Complete", trace.WithAttributes(
		attribute.String("fitz.route", q.route),
		attribute.Int64("fitz.message_id", int64(q.ID)),
	))
	defer span.End()
	resp, err := q.conn.SendRequestWithWriter(ctx, protocol.MessageTypeQueueComplete, completePayloadWriter(q.route, q.ID, token))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("complete request failed: %w", err)
	}
	success, _, err := parseQueueResponse(resp)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("complete failed: %w", err)
	}
	if !success {
		recordErr := errors.New("complete failed: unexpected status")
		span.RecordError(recordErr)
		span.SetStatus(codes.Error, recordErr.Error())
		return recordErr
	}
	return nil
}

// Client is the Queue domain client interface.
type Client interface {
	// Enqueue adds a message to the queue. Returns the server-assigned message ID.
	Enqueue(ctx context.Context, route string, body []byte) (msgID uint64, err error)

	// Reserve reserves up to batchSize messages with the given lease duration.
	// Each returned QueueItem has Extend and Complete methods.
	Reserve(ctx context.Context, route string, leaseSecs uint64, batchSize uint32) ([]*QueueItem, error)

	// ReserveWithOptions reserves queue items with explicit batch size and long-poll settings.
	ReserveWithOptions(ctx context.Context, route string, leaseSecs uint64, opts ...ReserveOption) ([]*QueueItem, error)

	// Subscribe registers a handler for availability notifications (empty -> non-empty transition).
	// Returns a Subscription that can be used to unsubscribe.
	// Per CLIENT_SPEC.md, the pattern parameter is optional; if provided, it will be used to filter notifications.
	Subscribe(ctx context.Context, pattern string, handler AvailabilityHandler) (*Subscription, error)
}

type client struct {
	conn *connection.Connection

	mu                      sync.Mutex
	subscriptions           *subscriptions.Registry[AvailabilityHandler]
	notifyHandlerInitOnce   sync.Once
	notifyHandlerRegistered atomic.Bool
}

// NewClient creates a new Queue domain client.
func NewClient(conn *connection.Connection) Client {
	return &client{
		conn:          conn,
		subscriptions: subscriptions.NewRegistry[AvailabilityHandler](),
	}
}

var _ reconnect.DomainRestorer = (*client)(nil)

// Enqueue per CLIENT_SPEC.md:
// Request: [route_len][route][body_len][body][has_delay(u8)][delay_secs?]
// Response: [status][message_id (u64 BE)]
func (c *client) Enqueue(ctx context.Context, route string, body []byte) (uint64, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.queue.Enqueue", trace.WithAttributes(attribute.String("fitz.route", route)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.DebugContext(ctx, "queue.Enqueue", "route", route)
	}

	// Validate route format
	if err := types.ValidateFixedRoute(route, "queue", 3); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, fmt.Errorf("invalid route: %w", err)
	}

	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeQueueEnqueue, enqueuePayloadWriter(route, body, 0))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, fmt.Errorf("enqueue request failed: %w", err)
	}

	success, remaining, err := parseQueueResponse(resp)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, fmt.Errorf("enqueue failed: %w", err)
	}
	if !success {
		recordErr := errors.New("enqueue failed: unexpected status")
		span.RecordError(recordErr)
		span.SetStatus(codes.Error, recordErr.Error())
		return 0, recordErr
	}

	if len(remaining) < 8 {
		recordErr := fmt.Errorf("enqueue response too short: got %d bytes", len(remaining))
		span.RecordError(recordErr)
		span.SetStatus(codes.Error, recordErr.Error())
		return 0, recordErr
	}

	msgID, _, err := connection.ReadU64BE(remaining, 0)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, fmt.Errorf("parse message_id: %w", err)
	}

	return msgID, nil
}

// Reserve per CLIENT_SPEC.md:
// Request: [route_len][route][lease_seconds][has_batch_size][batch_size?][has_wait_seconds][wait_seconds?]
// Response: [status][lease_count(u32)][{message_id, lease_token, body_len, body}...]
func (c *client) Reserve(ctx context.Context, route string, leaseSecs uint64, batchSize uint32) ([]*QueueItem, error) {
	return c.ReserveWithOptions(ctx, route, leaseSecs, WithBatchSize(batchSize))
}

// ReserveWithOptions per CLIENT_SPEC.md:
// Request: [route_len][route][lease_seconds][has_batch_size][batch_size?][has_wait_seconds][wait_seconds?]
// Response: [status][lease_count(u32)][{message_id, lease_token, body_len, body}...]
func (c *client) ReserveWithOptions(ctx context.Context, route string, leaseSecs uint64, opts ...ReserveOption) ([]*QueueItem, error) {
	cfg := reserveConfig{
		batchSize: 1,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	ctx, span := c.conn.Tracer().Start(ctx, "fitz.queue.Reserve", trace.WithAttributes(
		attribute.String("fitz.route", route),
		attribute.Int64("fitz.lease_secs", int64(leaseSecs)),
		attribute.Int("fitz.batch_size", int(cfg.batchSize)),
		attribute.Int64("fitz.wait_seconds", int64(cfg.waitSeconds)),
	))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.DebugContext(ctx, "queue.Reserve", "route", route, "lease_secs", leaseSecs, "batch_size", cfg.batchSize, "wait_seconds", cfg.waitSeconds)
	}

	// Validate route format
	if err := types.ValidateSelectorRoute(route, "queue", 3, false); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("invalid route: %w", err)
	}

	if cfg.waitSeconds == 0 {
		return c.reserveOnce(ctx, route, leaseSecs, cfg.batchSize)
	}

	items, err := c.reserveOnce(ctx, route, leaseSecs, cfg.batchSize)
	if err != nil || len(items) > 0 {
		return items, err
	}

	availability := make(chan struct{}, 1)
	sub, err := c.Subscribe(ctx, route, func(_ context.Context, _ AvailabilityNotification) error {
		select {
		case availability <- struct{}{}:
		default:
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()

	deadline := time.Now().Add(time.Duration(cfg.waitSeconds) * time.Second)
	for {
		items, err = c.reserveOnce(ctx, route, leaseSecs, cfg.batchSize)
		if err != nil || len(items) > 0 {
			return items, err
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return items, nil
		}

		timer := time.NewTimer(remaining)
		select {
		case <-availability:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			return c.reserveOnce(ctx, route, leaseSecs, cfg.batchSize)
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		}
	}
}

func (c *client) reserveOnce(
	ctx context.Context,
	route string,
	leaseSecs uint64,
	batchSize uint32,
) ([]*QueueItem, error) {
	resp, err := c.conn.SendRequestWithWriter(
		ctx,
		protocol.MessageTypeQueueReserve,
		reservePayloadWriter(route, leaseSecs, batchSize),
	)
	if err != nil {
		return nil, fmt.Errorf("reserve request failed: %w", err)
	}

	success, remaining, err := parseQueueResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("reserve failed: %w", err)
	}
	if !success {
		return nil, errors.New("reserve failed: unexpected status")
	}

	items, err := parseReserveItems(remaining, route, c.conn)
	if err != nil {
		return nil, err
	}

	return items, nil
}

func parseReserveItems(remaining []byte, route string, conn *connection.Connection) ([]*QueueItem, error) {
	count, offset, err := connection.ReadU32BE(remaining, 0)
	if err != nil {
		return nil, fmt.Errorf("parse lease_count: %w", err)
	}

	items := make([]*QueueItem, 0, count)
	for i := range count {
		item := &QueueItem{route: route, conn: conn}

		item.ID, offset, err = connection.ReadU64BE(remaining, offset)
		if err != nil {
			return nil, fmt.Errorf("parse message_id at item %d: %w", i, err)
		}

		item.Token, offset, err = connection.ReadU64BE(remaining, offset)
		if err != nil {
			return nil, fmt.Errorf("parse lease_token at item %d: %w", i, err)
		}

		var bodyData []byte
		bodyData, offset, err = connection.ReadBytes(remaining, offset)
		if err != nil {
			return nil, fmt.Errorf("parse body at item %d: %w", i, err)
		}
		item.Body = make([]byte, len(bodyData))
		copy(item.Body, bodyData)

		items = append(items, item)
	}

	if offset != len(remaining) {
		return nil, errors.New("reserve response has trailing bytes")
	}

	return items, nil
}

// initNotifyHandler registers the NOTIFY handler on first use.
func (c *client) initNotifyHandler() {
	c.notifyHandlerInitOnce.Do(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.conn.RegisterNotifyHandler(protocol.MessageTypeQueueNotify, c.handleNotify)
		c.notifyHandlerRegistered.Store(true)
	})
}

// handleNotify is called by the mux when a NOTIFY (209) frame arrives.
func (c *client) handleNotify(subID uint64, route string, payload []byte) {
	handlers := c.subscriptions.Handlers(subID)
	if len(handlers) == 0 {
		return
	}

	notif := AvailabilityNotification{
		Route: publicQueueRoute(route),
	}
	lifecycleCtx := c.conn.LifecycleContext()

	for _, handler := range handlers {
		if !c.conn.LaunchAsyncHandler(lifecycleCtx, "fitz.queue.handler", c.conn.AsyncHandlerTimeout(), func(handlerCtx context.Context, span trace.Span) {
			if err := handler(handlerCtx, notif); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				if log := c.conn.Logger(); log != nil {
					log.Warn("queue notify handler failed", "route", route, "error", err)
				}
			}
		}, trace.WithAttributes(
			attribute.Int64("fitz.subscription_id", int64(subID)),
			attribute.String("fitz.route", route),
		)) {
			if log := c.conn.Logger(); log != nil {
				log.Warn("queue notify handler dropped", "route", route, "sub_id", subID, "reason", "async handler queue full")
			}
		}
	}
}

// Subscribe registers a handler for availability notifications.
// Pattern should be a wildcard pattern (e.g., "queue://realm/area/resource/*" or "queue://realm/area/resource").
func (c *client) Subscribe(ctx context.Context, pattern string, handler AvailabilityHandler) (*Subscription, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.queue.Subscribe", trace.WithAttributes(attribute.String("fitz.pattern", pattern)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.DebugContext(ctx, "queue.Subscribe", "pattern", pattern)
	}
	if err := types.ValidateSelectorRoute(pattern, "queue", 3, true); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("invalid route: %w", err)
	}
	c.initNotifyHandler()

	subID, handlerID, err := c.subscriptions.Subscribe(pattern, handler, func(pattern string) (uint64, error) {
		return c.subscribeWire(ctx, pattern)
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return &Subscription{
		subID:     subID,
		handlerID: handlerID,
		pattern:   pattern,
		client:    c,
	}, nil
}

// unsubscribe removes a subscription.
func (c *client) unsubscribe(sub *Subscription) {
	if !c.subscriptions.Unsubscribe(sub.pattern, sub.handlerID) {
		return
	}
	c.conn.AddSubscriptions(-1)

	// Send UNSUBSCRIBE to server (best-effort, ignore errors).
	ctx := c.conn.LifecycleContext()
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeQueueUnsubscribe, unsubscribePayloadWriter(wireWatchPattern(sub.pattern)))
	if err != nil {
		return
	}
	if _, _, err := parseQueueResponse(resp); err != nil {
		return
	}
}

func (c *client) ReplaceConnection(conn *connection.Connection) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn = conn
	if c.notifyHandlerRegistered.Load() {
		c.conn.RegisterNotifyHandler(protocol.MessageTypeQueueNotify, c.handleNotify)
	}
}

func (c *client) RestoreSubscriptions(ctx context.Context) error {
	return c.subscriptions.Restore(
		func(pattern string) (uint64, error) {
			return c.subscribeWire(ctx, pattern)
		},
		func(pattern string, _ uint64) error {
			resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeQueueUnsubscribe, unsubscribePayloadWriter(wireWatchPattern(pattern)))
			if err != nil {
				return err
			}
			_, _, err = parseQueueResponse(resp)
			return err
		},
	)
}

func (c *client) subscribeWire(ctx context.Context, pattern string) (uint64, error) {
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeQueueSubscribe, subscribePayloadWriter(wireWatchPattern(pattern)))
	if err != nil {
		return 0, fmt.Errorf("subscribe request failed: %w", err)
	}

	success, remaining, err := parseQueueResponse(resp)
	if err != nil {
		return 0, fmt.Errorf("subscribe failed: %w", err)
	}
	if !success {
		return 0, errors.New("subscribe failed: unexpected status")
	}

	subID, err := parseSubscriptionID(remaining)
	if err != nil {
		return 0, err
	}
	c.conn.AddSubscriptions(1)
	return subID, nil
}

func wireWatchPattern(pattern string) string {
	if strings.HasSuffix(pattern, "/ready") {
		return pattern
	}
	return pattern + "/ready"
}

func publicQueueRoute(route string) string {
	return strings.TrimSuffix(route, "/ready")
}

func parseSubscriptionID(remaining []byte) (uint64, error) {
	switch {
	case len(remaining) == 8:
		subID, _, err := connection.ReadU64BE(remaining, 0)
		if err != nil {
			return 0, fmt.Errorf("parse subscription_id: %w", err)
		}
		return subID, nil
	case len(remaining) >= 9 && remaining[0] == 1:
		subID, _, err := connection.ReadU64BE(remaining, 1)
		if err != nil {
			return 0, fmt.Errorf("parse subscription_id: %w", err)
		}
		if len(remaining) != 9 {
			return 0, fmt.Errorf("subscribe response has trailing bytes: got %d bytes", len(remaining))
		}
		return subID, nil
	default:
		return 0, errors.New("subscribe response missing subscription_id")
	}
}
