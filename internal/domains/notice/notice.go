// Package notice implements the Fitz Notice domain client.
// Per CLIENT_SPEC.md: Pub/sub with wildcard pattern matching.
package notice

import (
	"context"
	"fmt"
	"sync"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/core/reconnect"
	"github.com/cntryl/fitz-go/internal/core/types"
	"github.com/cntryl/fitz-go/internal/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// NoticeMsg represents a received notification.
type NoticeMsg struct {
	Route string
	Body  []byte
}

// NoticeHandler is called when a notification arrives.
type NoticeHandler func(ctx context.Context, msg NoticeMsg) error

// Subscription represents an active notice subscription.
// Call Unsubscribe to stop receiving and release the subscription.
type Subscription struct {
	subID   uint64
	route   string
	client  *client
	handler NoticeHandler
}

// Unsubscribe removes this subscription.
func (s *Subscription) Unsubscribe() {
	if s.client != nil {
		s.client.unsubscribe(s)
	}
}

// Client is the Notice domain client interface.
type Client interface {
	// Publish sends a notification to a route (fire-and-forget).
	Publish(ctx context.Context, route string, body []byte) error

	// Subscribe registers a handler for notifications matching the pattern.
	// Returns a Subscription that can be used to unsubscribe.
	Subscribe(ctx context.Context, pattern string, handler NoticeHandler) (*Subscription, error)
}

type client struct {
	conn *connection.Connection

	mu            sync.RWMutex
	subscriptions map[uint64]*Subscription // subID -> subscription
	initialized   bool
}

// NewClient creates a new Notice domain client.
func NewClient(conn *connection.Connection) Client {
	c := &client{
		conn:          conn,
		subscriptions: make(map[uint64]*Subscription),
	}
	return c
}

var _ reconnect.DomainRestorer = (*client)(nil)

// initNotifyHandler registers the NOTIFY handler on first use.
func (c *client) initNotifyHandler() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialized {
		return
	}
	c.initialized = true
	c.conn.RegisterNotifyHandler(protocol.MessageTypeNoticeNotify, c.handleNotify)
}

// handleNotify is called by the mux when a NOTIFY (504) frame arrives.
func (c *client) handleNotify(subID uint64, route string, payload []byte) {
	c.mu.RLock()
	sub, ok := c.subscriptions[subID]
	c.mu.RUnlock()

	if !ok {
		return // Unknown subscription
	}

	msg := NoticeMsg{
		Route: route,
		Body:  make([]byte, len(payload)),
	}
	copy(msg.Body, payload)

	// Call handler asynchronously to avoid blocking the dispatch loop
	go func() {
		_ = sub.handler(context.Background(), msg)
	}()
}

// Publish per CLIENT_SPEC.md:
// Request: [route_len][route][payload_len][payload]
// Notice PUBLISH is fire-and-forget — the server does not send a response frame.
func (c *client) Publish(ctx context.Context, route string, body []byte) error {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.notice.Publish", trace.WithAttributes(attribute.String("fitz.route", route)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("notice.Publish", "route", route)
	}

	// Validate route format (exact route for publish, not pattern)
	if err := types.ValidateRoute(route, "notice"); err != nil {
		return fmt.Errorf("invalid route: %w", err)
	}

	if err := c.conn.SendFireAndForgetWithWriter(ctx, protocol.MessageTypeNoticePublish, publishPayloadWriter(route, body)); err != nil {
		return fmt.Errorf("PUBLISH failed: %w", err)
	}

	return nil
}

// Subscribe per CLIENT_SPEC.md:
// Request: [pattern_len][pattern]
// Response: [status][subscription_id(u64)]
func (c *client) Subscribe(ctx context.Context, pattern string, handler NoticeHandler) (*Subscription, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.notice.Subscribe", trace.WithAttributes(attribute.String("fitz.pattern", pattern)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("notice.Subscribe", "pattern", pattern)
	}
	c.initNotifyHandler()

	sub, err := c.subscribe(ctx, pattern, handler)
	if err != nil {
		return nil, err
	}
	return sub, nil
}

// unsubscribe removes a subscription.
func (c *client) unsubscribe(sub *Subscription) {
	c.mu.Lock()
	delete(c.subscriptions, sub.subID)
	c.mu.Unlock()

	// Send UNSUBSCRIBE to server (best-effort, ignore errors).
	// Server expects [string pattern] (the original subscription pattern).
	ctx := context.Background()
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeNoticeUnsubscribe, unsubscribePayloadWriter(sub.route))
	if err != nil {
		return
	}
	connection.ParseStandardResponse(resp)
}

func (c *client) ReplaceConnection(conn *connection.Connection) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn = conn
	if c.initialized {
		c.conn.RegisterNotifyHandler(protocol.MessageTypeNoticeNotify, c.handleNotify)
	}
}

func (c *client) RestoreSubscriptions(ctx context.Context) error {
	c.mu.RLock()
	snapshot := make([]*Subscription, 0, len(c.subscriptions))
	for _, sub := range c.subscriptions {
		snapshot = append(snapshot, &Subscription{route: sub.route, handler: sub.handler, client: c})
	}
	c.mu.RUnlock()

	restored := make(map[uint64]*Subscription, len(snapshot))
	for _, sub := range snapshot {
		restoredSub, err := c.subscribe(ctx, sub.route, sub.handler)
		if err != nil {
			return err
		}
		restored[restoredSub.subID] = restoredSub
	}

	c.mu.Lock()
	c.subscriptions = restored
	c.mu.Unlock()
	return nil
}

func (c *client) subscribe(ctx context.Context, pattern string, handler NoticeHandler) (*Subscription, error) {
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeNoticeSubscribe, subscribePayloadWriter(pattern))
	if err != nil {
		return nil, fmt.Errorf("SUBSCRIBE request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("SUBSCRIBE failed: %w", mapNoticeError(err.Error()))
	}
	if !success {
		return nil, fmt.Errorf("SUBSCRIBE failed: unexpected status")
	}

	if len(remaining) < 1 {
		return nil, fmt.Errorf("SUBSCRIBE response too short: got %d bytes", len(remaining))
	}
	if remaining[0] != 1 {
		return nil, fmt.Errorf("SUBSCRIBE response missing subscription_id")
	}
	if len(remaining) < 9 {
		return nil, fmt.Errorf("SUBSCRIBE response too short for subscription_id: got %d bytes", len(remaining))
	}

	subID, _, err := connection.ReadU64BE(remaining, 1)
	if err != nil {
		return nil, fmt.Errorf("parse subscription_id: %w", err)
	}

	sub := &Subscription{
		subID:   subID,
		route:   pattern,
		client:  c,
		handler: handler,
	}
	c.mu.Lock()
	c.subscriptions[subID] = sub
	c.mu.Unlock()
	return sub, nil
}
