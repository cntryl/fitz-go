// Package notice implements the Fitz Notice domain client.
// Per CLIENT_SPEC.md: Pub/sub with wildcard pattern matching.
package notice

import (
	"context"
	"errors"
	"fmt"
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
	subID     uint64
	handlerID uint64
	route     string
	client    *client
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
	conn   *connection.Connection
	connMu sync.RWMutex

	mu                      sync.Mutex
	subscriptions           *subscriptions.Registry[NoticeHandler]
	notifyHandlerInitOnce   sync.Once
	notifyHandlerRegistered atomic.Bool
}

func (c *client) currentConn() *connection.Connection {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.conn
}

// NewClient creates a new Notice domain client.
func NewClient(conn *connection.Connection) Client {
	c := &client{
		conn:          conn,
		subscriptions: subscriptions.NewRegistry[NoticeHandler](),
	}
	return c
}

var _ reconnect.DomainRestorer = (*client)(nil)

// initNotifyHandler registers the NOTIFY handler on first use.
func (c *client) initNotifyHandler() {
	c.notifyHandlerInitOnce.Do(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.currentConn().RegisterNotifyHandler(protocol.MessageTypeNoticeNotify, c.handleNotify)
		c.notifyHandlerRegistered.Store(true)
	})
}

// handleNotify is called by the mux when a NOTIFY (504) frame arrives.
func (c *client) handleNotify(subID uint64, route string, payload []byte) {
	handlers := c.subscriptions.Handlers(subID)
	if len(handlers) == 0 {
		return
	}

	lifecycleCtx := c.currentConn().LifecycleContext()
	for _, handler := range handlers {
		msg := NoticeMsg{
			Route: route,
			Body:  append([]byte(nil), payload...),
		}
		if !c.currentConn().LaunchAsyncHandler(lifecycleCtx, "fitz.notice.handler", c.currentConn().AsyncHandlerTimeout(), func(handlerCtx context.Context, span trace.Span) {
			if err := handler(handlerCtx, msg); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				if log := c.currentConn().Logger(); log != nil {
					log.Warn("notice handler failed", "route", route, "error", err)
				}
			}
		}, trace.WithAttributes(
			attribute.Int64("fitz.subscription_id", int64(subID)),
			attribute.String("fitz.route", route),
		)) {
			if log := c.currentConn().Logger(); log != nil {
				log.Warn("notice handler dropped", "route", route, "sub_id", subID, "reason", "async handler queue full")
			}
		}
	}
}

// Publish per CLIENT_SPEC.md:
// Request: [route_len][route][payload_len][payload]
// Notice PUBLISH is fire-and-forget - the server does not send a response frame.
func (c *client) Publish(ctx context.Context, route string, body []byte) error {
	ctx, span := c.currentConn().Tracer().Start(ctx, "fitz.notice.Publish", trace.WithAttributes(attribute.String("fitz.route", route)))
	defer span.End()
	if log := c.currentConn().Logger(); log != nil {
		log.DebugContext(ctx, "notice.Publish", "route", route)
	}

	// Validate route format (exact route for publish, not pattern)
	if err := types.ValidateFixedRoute(route, "notice", 3); err != nil {
		return fmt.Errorf("invalid route: %w", err)
	}

	if err := c.currentConn().SendFireAndForgetWithWriter(ctx, protocol.MessageTypeNoticePublish, publishPayloadWriter(route, body)); err != nil {
		return fmt.Errorf("publish failed: %w", err)
	}

	return nil
}

// Subscribe per CLIENT_SPEC.md:
// Request: [pattern_len][pattern]
// Response: [status][subscription_id(u64)]
func (c *client) Subscribe(ctx context.Context, pattern string, handler NoticeHandler) (*Subscription, error) {
	ctx, span := c.currentConn().Tracer().Start(ctx, "fitz.notice.Subscribe", trace.WithAttributes(attribute.String("fitz.pattern", pattern)))
	defer span.End()
	if log := c.currentConn().Logger(); log != nil {
		log.DebugContext(ctx, "notice.Subscribe", "pattern", pattern)
	}
	if err := types.ValidateRegistrationPattern(pattern, "notice", 0); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("invalid route: %w", err)
	}
	c.initNotifyHandler()

	subID, handlerID, err := c.subscriptions.Subscribe(pattern, handler, func(pattern string) (uint64, error) {
		return c.subscribeWire(ctx, pattern)
	})
	if err != nil {
		return nil, err
	}
	return &Subscription{
		subID:     subID,
		handlerID: handlerID,
		route:     pattern,
		client:    c,
	}, nil
}

// unsubscribe removes a subscription.
func (c *client) unsubscribe(sub *Subscription) {
	if !c.subscriptions.Unsubscribe(sub.route, sub.handlerID) {
		return
	}
	c.currentConn().AddSubscriptions(-1)

	// Send UNSUBSCRIBE to server (best-effort, ignore errors).
	// Server expects [u64 subscription_id].
	ctx := c.currentConn().LifecycleContext()
	resp, err := c.currentConn().SendRequestWithWriter(ctx, protocol.MessageTypeNoticeUnsubscribe, unsubscribePayloadWriter(sub.subID))
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
	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()
	if c.notifyHandlerRegistered.Load() {
		c.currentConn().RegisterNotifyHandler(protocol.MessageTypeNoticeNotify, c.handleNotify)
	}
}

func (c *client) RestoreSubscriptions(ctx context.Context) error {
	return c.subscriptions.Restore(
		func(pattern string) (uint64, error) {
			return c.subscribeWire(ctx, pattern)
		},
		func(_ string, subID uint64) error {
			resp, err := c.currentConn().SendRequestWithWriter(ctx, protocol.MessageTypeNoticeUnsubscribe, unsubscribePayloadWriter(subID))
			if err != nil {
				return err
			}
			if _, _, err = connection.ParseStandardResponse(resp); err != nil {
				return err
			}
			c.currentConn().AddSubscriptions(-1)
			return nil
		},
	)
}

func (c *client) subscribeWire(ctx context.Context, pattern string) (uint64, error) {
	resp, err := c.currentConn().SendRequestWithWriter(ctx, protocol.MessageTypeNoticeSubscribe, subscribePayloadWriter(pattern))
	if err != nil {
		return 0, fmt.Errorf("subscribe request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return 0, fmt.Errorf("subscribe failed: %w", mapNoticeError(err))
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
	if len(remaining) < 9 {
		return 0, fmt.Errorf("subscribe response too short for subscription_id: got %d bytes", len(remaining))
	}

	subID, _, err := connection.ReadU64BE(remaining, 1)
	if err != nil {
		return 0, fmt.Errorf("parse subscription_id: %w", err)
	}
	c.currentConn().AddSubscriptions(1)
	return subID, nil
}
