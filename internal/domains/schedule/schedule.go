// Package schedule implements the Fitz Schedule domain client.
// Per CLIENT_SPEC.md: Cron-based task scheduling.
package schedule

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cntryl/fitz-go/v2/internal/core/connection"
	"github.com/cntryl/fitz-go/v2/internal/core/reconnect"
	"github.com/cntryl/fitz-go/v2/internal/core/subscriptions"
	"github.com/cntryl/fitz-go/v2/internal/core/types"
	"github.com/cntryl/fitz-go/v2/internal/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ScheduleEntry represents a schedule returned by a list page (per CLIENT_SPEC: route, cron, payload).
type ScheduleEntry struct {
	ID           string // Route (spec uses route as identity)
	Route        string
	Cron         string
	DeliveryMode ScheduleDeliveryMode
	Payload      []byte
}
type ScheduleListPage struct {
	Entries    []ScheduleEntry
	TotalCount uint64
}

type ScheduleDeliveryMode uint8

const (
	ScheduleDeliveryBroadcast ScheduleDeliveryMode = iota
	ScheduleDeliverySingle
)

// Notification is the payload delivered when a schedule fires (SCHEDULE_NOTIFY 705).
type Notification struct {
	Route   string
	Payload []byte
}

// ScheduleHandler is called when a schedule fires for a subscribed pattern.
// It is fire-and-forget and server does not ack handler result.
type ScheduleHandler func(ctx context.Context, n Notification) error

// Subscription represents an active subscription to schedule fire notifications.
// Call Unsubscribe to stop receiving notifications.
type Subscription struct {
	subID     uint64
	handlerID uint64
	pattern   string
	client    *client
}

// Unsubscribe stops receiving schedule fire notifications for this subscription.
func (s *Subscription) Unsubscribe() {
	if s.client != nil {
		s.client.unsubscribe(s)
	}
}

// Client is the Schedule domain client interface.
type Client interface {
	// Create creates a cron-based schedule at the given route (upsert per spec). Returns the schedule route (identity).
	Create(ctx context.Context, route string, cronExpr string, deliveryMode ScheduleDeliveryMode, payload []byte) (id string, err error)

	// Cancel cancels a schedule by route (route-based identity per CLIENT_SPEC).
	Cancel(ctx context.Context, route string) error

	List(ctx context.Context, offset *uint64, limit *uint64) (ScheduleListPage, error)

	// ListBySelector retrieves schedules matching a canonical schedule selector.
	ListBySelector(ctx context.Context, selector string) ([]ScheduleEntry, error)

	// Subscribe subscribes to schedule fire notifications for the given exact schedule route.
	// When a schedule fires, the handler is invoked with the schedule's payload.
	// Subscriptions are session-scoped and lost on disconnect.
	Subscribe(ctx context.Context, pattern string, handler ScheduleHandler) (*Subscription, error)
}

type client struct {
	conn   *connection.Connection
	connMu sync.RWMutex

	mu                      sync.Mutex
	subscriptions           *subscriptions.Registry[ScheduleHandler]
	notifyHandlerInitOnce   sync.Once
	notifyHandlerRegistered atomic.Bool
}

var _ reconnect.DomainRestorer = (*client)(nil)

func (c *client) initScheduleNotifyHandler() {
	c.notifyHandlerInitOnce.Do(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.currentConn().RegisterNotifyHandler(protocol.MessageTypeScheduleNotify, c.handleScheduleNotify)
		c.notifyHandlerRegistered.Store(true)
	})
}

func (c *client) handleScheduleNotify(subID uint64, route string, payload []byte) {
	conn := c.currentConn()
	handlers := c.subscriptions.Handlers(subID)
	if len(handlers) == 0 {
		return
	}

	lifecycleCtx := conn.LifecycleContext()
	for _, handler := range handlers {
		msg := Notification{
			Route:   route,
			Payload: append([]byte(nil), payload...),
		}
		if !conn.LaunchAsyncHandler(lifecycleCtx, "fitz.schedule.handler", conn.AsyncHandlerTimeout(), func(handlerCtx context.Context, span trace.Span) {
			if err := handler(handlerCtx, msg); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				if log := conn.Logger(); log != nil {
					log.Warn("schedule notify handler failed", "error", err)
				}
			}
		}, trace.WithAttributes(
			attribute.Int64("fitz.subscription_id", int64(subID)),
			attribute.String("fitz.route", route),
		)) {
			if log := conn.Logger(); log != nil {
				log.Warn("schedule notify handler dropped", "sub_id", subID, "reason", "async handler queue full")
			}
		}
	}
}

// NewClient creates a new Schedule domain client.
func NewClient(conn *connection.Connection) Client {
	return &client{conn: conn, subscriptions: subscriptions.NewRegistry[ScheduleHandler]()}
}

func (c *client) currentConn() *connection.Connection {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.conn
}

// Create per CLIENT_SPEC.md: Request [route_len][route][cron_len][cron][payload_len][payload].
// Response: status=0 (success); optional [u8 has_schedule_id][string schedule_id] when present.
// Returns route as identity (spec uses route-based identity).
func (c *client) Create(ctx context.Context, route string, cronExpr string, deliveryMode ScheduleDeliveryMode, payload []byte) (string, error) {
	conn := c.currentConn()
	ctx, span := conn.Tracer().Start(ctx, "fitz.schedule.Create", trace.WithAttributes(
		attribute.String("fitz.route", route),
		attribute.String("fitz.cron", cronExpr),
	))
	defer span.End()
	if log := conn.Logger(); log != nil {
		log.DebugContext(ctx, "schedule.Create", "route", route, "cron", cronExpr)
	}

	// Validate route format
	if err := types.ValidateScheduleRoute(route); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", fmt.Errorf("invalid schedule route: %w", err)
	}
	if err := validateCronExpression(cronExpr); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}
	if deliveryMode != ScheduleDeliveryBroadcast && deliveryMode != ScheduleDeliverySingle {
		return "", ErrScheduleInvalidDeliveryMode
	}

	resp, err := conn.SendRequestWithWriter(ctx, protocol.MessageTypeScheduleCreate, scheduleCreatePayloadWriter(route, cronExpr, deliveryMode, payload))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", fmt.Errorf("create request failed: %w", err)
	}

	success, remaining, err := connection.ParsePlainResponse(resp)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", fmt.Errorf("create failed: %w", mapScheduleError(err))
	}
	if !success {
		recordErr := errors.New("create failed: unexpected status")
		span.RecordError(recordErr)
		span.SetStatus(codes.Error, recordErr.Error())
		return "", recordErr
	}

	// Optional schedule_id when server sends it; otherwise route is identity.
	if len(remaining) == 0 {
		return route, nil
	}
	if remaining[0] != 1 {
		return "", errors.New("create response has invalid schedule_id flag")
	}
	id, offset, err := connection.ReadString(remaining, 1)
	if err != nil {
		return "", fmt.Errorf("parse schedule_id: %w", err)
	}
	if offset != len(remaining) {
		return "", errors.New("create response has trailing bytes")
	}
	return id, nil
}

// Cancel per CLIENT_SPEC.md: Request [route_len][route] (route-based identity).
func (c *client) Cancel(ctx context.Context, route string) error {
	conn := c.currentConn()
	ctx, span := conn.Tracer().Start(ctx, "fitz.schedule.Cancel", trace.WithAttributes(attribute.String("fitz.route", route)))
	defer span.End()
	if log := conn.Logger(); log != nil {
		log.DebugContext(ctx, "schedule.Cancel", "route", route)
	}

	// Validate route format
	if err := types.ValidateScheduleRoute(route); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("invalid schedule route: %w", err)
	}

	resp, err := conn.SendRequestWithWriter(ctx, protocol.MessageTypeScheduleCancel, scheduleCancelPayloadWriter(route))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("cancel request failed: %w", err)
	}

	success, remaining, err := connection.ParsePlainResponse(resp)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("cancel failed: %w", mapScheduleError(err))
	}
	if !success {
		recordErr := errors.New("cancel failed: unexpected status")
		span.RecordError(recordErr)
		span.SetStatus(codes.Error, recordErr.Error())
		return recordErr
	}
	if len(remaining) != 0 {
		return errors.New("cancel response has trailing bytes")
	}

	return nil
}

func (c *client) List(ctx context.Context, offset *uint64, limit *uint64) (ScheduleListPage, error) {
	resp, err := c.currentConn().SendRequestWithWriter(ctx, protocol.MessageTypeScheduleListPage, scheduleListPayloadWriter(offset, limit))
	if err != nil {
		return ScheduleListPage{}, err
	}
	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return ScheduleListPage{}, err
	}
	if !success {
		return ScheduleListPage{}, errors.New("schedule LIST failed")
	}
	if len(remaining) < 9 {
		return ScheduleListPage{}, errors.New("invalid schedule LIST response")
	}
	totalCount, _, err := connection.ReadU64BE(remaining, 0)
	if err != nil {
		return ScheduleListPage{}, err
	}
	entries, err := parseScheduleListEntries(remaining[8:])
	if err != nil {
		return ScheduleListPage{}, err
	}
	return ScheduleListPage{Entries: entries, TotalCount: totalCount}, nil
}

func parseScheduleListEntries(remaining []byte) ([]ScheduleEntry, error) {
	entries := make([]ScheduleEntry, 0)
	pos := 0
	for {
		if pos >= len(remaining) {
			return nil, errors.New("list response missing entry terminator")
		}
		hasEntry := remaining[pos]
		pos++

		switch hasEntry {
		case 0:
			if pos != len(remaining) {
				return nil, fmt.Errorf("list response has trailing bytes: %d", len(remaining)-pos)
			}
			return entries, nil
		case 1:
		default:
			return nil, fmt.Errorf("list response invalid has_entry flag: %d", hasEntry)
		}

		routeStr, newPos, err := connection.ReadString(remaining, pos)
		if err != nil {
			return nil, fmt.Errorf("list response invalid route: %w", err)
		}
		pos = newPos
		cronStr, newPos, err := connection.ReadString(remaining, pos)
		if err != nil {
			return nil, fmt.Errorf("list response invalid cron: %w", err)
		}
		pos = newPos
		if pos >= len(remaining) {
			return nil, errors.New("list response missing delivery mode")
		}
		deliveryMode := ScheduleDeliveryMode(remaining[pos])
		pos++
		if deliveryMode != ScheduleDeliveryBroadcast && deliveryMode != ScheduleDeliverySingle {
			return nil, fmt.Errorf("list response invalid delivery mode: %d", deliveryMode)
		}
		payloadBytes, newPos, err := connection.ReadBytes(remaining, pos)
		if err != nil {
			return nil, fmt.Errorf("list response invalid payload: %w", err)
		}
		pos = newPos
		payloadCopy := make([]byte, len(payloadBytes))
		copy(payloadCopy, payloadBytes)
		entries = append(entries, ScheduleEntry{
			ID:           routeStr,
			Route:        routeStr,
			Cron:         cronStr,
			DeliveryMode: deliveryMode,
			Payload:      payloadCopy,
		})
	}
}

// ListBySelector retrieves schedules matching a canonical schedule selector by
// consuming the offset-based LIST API.
func (c *client) ListBySelector(ctx context.Context, selector string) ([]ScheduleEntry, error) {
	if err := types.ValidateScheduleSelector(selector); err != nil {
		return nil, fmt.Errorf("invalid selector: %w", err)
	}
	var offset uint64
	limit := uint64(1000)
	matches := make([]ScheduleEntry, 0)
	for {
		page, err := c.List(ctx, &offset, &limit)
		if err != nil {
			return nil, err
		}
		for _, entry := range page.Entries {
			if !scheduleSelectorMatches(selector, entry.Route) {
				continue
			}
			matches = append(matches, entry)
		}
		offset += uint64(len(page.Entries))
		if offset >= page.TotalCount {
			return matches, nil
		}
		if len(page.Entries) == 0 {
			return nil, errors.New("schedule LIST returned an empty page before total_count")
		}
	}
}

// Subscribe per CLIENT_SPEC.md (703): Request [route]. Response: [status][optional u64 subscription_id].
func (c *client) Subscribe(ctx context.Context, pattern string, handler ScheduleHandler) (*Subscription, error) {
	conn := c.currentConn()
	ctx, span := conn.Tracer().Start(ctx, "fitz.schedule.Subscribe", trace.WithAttributes(attribute.String("fitz.pattern", pattern)))
	defer span.End()
	if log := conn.Logger(); log != nil {
		log.DebugContext(ctx, "schedule.Subscribe", "pattern", pattern)
	}
	c.initScheduleNotifyHandler()
	if err := types.ValidateRegistrationPattern(pattern, "schedule", 4); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("invalid schedule route: %w", err)
	}

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

// Unsubscribe per CLIENT_SPEC.md (704):
// Request: [string route]
func (c *client) unsubscribe(sub *Subscription) {
	if !c.subscriptions.Unsubscribe(sub.pattern, sub.handlerID) {
		return
	}
	conn := c.currentConn()
	conn.AddSubscriptions(-1)

	// Best-effort unsubscribe; ignore errors to match notice semantics.
	ctx := conn.LifecycleContext()
	resp, err := conn.SendRequestWithWriter(ctx, protocol.MessageTypeScheduleUnsubscribe, scheduleUnsubscribePayloadWriter(sub.pattern))
	if err != nil {
		return
	}
	if _, _, err := connection.ParsePlainResponse(resp); err != nil {
		return
	}
}

func (c *client) ReplaceConnection(conn *connection.Connection) {
	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()
	if c.notifyHandlerRegistered.Load() {
		conn.RegisterNotifyHandler(protocol.MessageTypeScheduleNotify, c.handleScheduleNotify)
	}
}

func (c *client) RestoreSubscriptions(ctx context.Context) error {
	return c.subscriptions.Restore(
		func(pattern string) (uint64, error) {
			return c.subscribeWire(ctx, pattern)
		},
		func(pattern string, _ uint64) error {
			conn := c.currentConn()
			resp, err := conn.SendRequestWithWriter(ctx, protocol.MessageTypeScheduleUnsubscribe, scheduleUnsubscribePayloadWriter(pattern))
			if err != nil {
				return err
			}
			if _, _, err = connection.ParsePlainResponse(resp); err != nil {
				return err
			}
			conn.AddSubscriptions(-1)
			return nil
		},
	)
}

func (c *client) subscribeWire(ctx context.Context, pattern string) (uint64, error) {
	conn := c.currentConn()
	resp, err := conn.SendRequestWithWriter(ctx, protocol.MessageTypeScheduleSubscribe, scheduleSubscribePayloadWriter(pattern))
	if err != nil {
		return 0, fmt.Errorf("subscribe request failed: %w", err)
	}

	success, remaining, err := connection.ParsePlainResponse(resp)
	if err != nil {
		return 0, fmt.Errorf("subscribe failed: %w", mapScheduleError(err))
	}
	if !success {
		return 0, errors.New("subscribe failed: unexpected status")
	}

	if len(remaining) < 1 {
		return 0, fmt.Errorf("subscribe response too short: got %d bytes", len(remaining))
	}
	if remaining[0] != 1 {
		return 0, errors.New("subscribe response missing subscription_id")
	}
	if len(remaining) != 9 {
		return 0, fmt.Errorf("subscribe response too short for subscription_id: got %d bytes", len(remaining))
	}

	subID, _, err := connection.ReadU64BE(remaining, 1)
	if err != nil {
		return 0, fmt.Errorf("parse subscription_id: %w", err)
	}
	conn.AddSubscriptions(1)
	return subID, nil
}

func filterScheduleEntries(entries []ScheduleEntry, selector string) []ScheduleEntry {
	filtered := make([]ScheduleEntry, 0, len(entries))
	for _, entry := range entries {
		if scheduleSelectorMatches(selector, entry.Route) {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

func scheduleSelectorMatches(selector string, route string) bool {
	selectorSegments, ok := scheduleSegments(selector)
	if !ok {
		return false
	}
	routeSegments, ok := scheduleSegments(route)
	if !ok || len(routeSegments) != 4 {
		return false
	}

	switch len(selectorSegments) {
	case 2:
		return selectorSegments[1] == "**" && selectorSegments[0] == routeSegments[0]
	case 3:
		return selectorSegments[2] == "*" &&
			selectorSegments[0] == routeSegments[0] &&
			selectorSegments[1] == routeSegments[1]
	case 4:
		if selectorSegments[3] == "*" {
			return selectorSegments[0] == routeSegments[0] &&
				selectorSegments[1] == routeSegments[1] &&
				selectorSegments[2] == routeSegments[2]
		}
		return selectorSegments[0] == routeSegments[0] &&
			selectorSegments[1] == routeSegments[1] &&
			selectorSegments[2] == routeSegments[2] &&
			selectorSegments[3] == routeSegments[3]
	default:
		return false
	}
}

func scheduleSegments(route string) ([]string, bool) {
	if !strings.HasPrefix(route, "schedule://") {
		return nil, false
	}

	path := strings.TrimPrefix(route, "schedule://")
	segments := strings.Split(path, "/")
	if len(segments) == 0 {
		return nil, false
	}
	if slices.Contains(segments, "") {
		return nil, false
	}
	return segments, true
}
