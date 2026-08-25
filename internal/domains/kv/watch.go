package kv

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cntryl/fitz-go/v2/internal/core/connection"
	"github.com/cntryl/fitz-go/v2/internal/core/encoding"
	coreerrors "github.com/cntryl/fitz-go/v2/internal/core/errors"
	"github.com/cntryl/fitz-go/v2/internal/core/subscriptions"
	"github.com/cntryl/fitz-go/v2/internal/core/types"
	"github.com/cntryl/fitz-go/v2/internal/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type ChangeNotification struct {
	Route         string
	MutationCount uint64
}

type ChangeHandler func(context.Context, ChangeNotification) error

type Subscription struct {
	handlerID  uint64
	pattern    string
	client     *client
	completion *subscriptions.Completion
}

func (s *Subscription) Unsubscribe() {
	if s != nil && s.client != nil {
		s.client.unsubscribe(s)
	}
}

func (s *Subscription) Completion() <-chan error {
	if s == nil || s.completion == nil {
		return nil
	}
	return s.completion.Done()
}

// Subscribe registers a handler for exact KV routes matched by pattern.
// Patterns use whole-segment * and ** wildcards, and notifications carry the concrete route.
func (c *client) Subscribe(ctx context.Context, pattern string, handler ChangeHandler) (*Subscription, error) {
	conn := c.currentConnection()
	ctx, span := conn.Tracer().Start(ctx, "fitz.kv.Subscribe", trace.WithAttributes(attribute.String("fitz.pattern", pattern)))
	defer span.End()
	if log := conn.Logger(); log != nil {
		log.DebugContext(ctx, "kv.Subscribe", "pattern", pattern)
	}
	if err := types.ValidateRegistrationPattern(pattern, "kv", 3); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("invalid KV subscription pattern: %w", err)
	}
	c.initNotifyHandler()
	_, handlerID, err := c.subscriptions.Subscribe(pattern, handler, func(pattern string) (uint64, error) {
		return c.subscribeWire(ctx, pattern)
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return &Subscription{handlerID: handlerID, pattern: pattern, client: c, completion: c.subscriptions.Completion(pattern, handlerID)}, nil
}

func (c *client) initNotifyHandler() {
	c.notifyHandlerInitOnce.Do(func() {
		conn := c.currentConnection()
		conn.RegisterNotifyHandler(protocol.MessageTypeKvNotify, c.handleNotify)
		c.notifyHandlerRegistered.Store(true)
	})
}

func (c *client) handleNotify(subID uint64, route string, payload []byte) {
	if len(payload) != 8 {
		return
	}
	notification := ChangeNotification{Route: route, MutationCount: binary.BigEndian.Uint64(payload)}
	conn := c.currentConnection()
	for _, registration := range c.subscriptions.Registrations(subID) {
		handler := registration.Handler
		if !conn.LaunchAsyncHandler(conn.LifecycleContext(), "fitz.kv.handler", conn.AsyncHandlerTimeout(), func(handlerCtx context.Context, span trace.Span) {
			if err := handler(handlerCtx, notification); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
		}, trace.WithAttributes(
			attribute.Int64("fitz.subscription_id", int64(subID)),
			attribute.String("fitz.route", route),
		)) {
			registration.Completion.Complete(&coreerrors.AsyncHandlerOverflowError{Domain: "kv", SubscriptionID: subID})
			go c.unsubscribe(&Subscription{handlerID: registration.HandlerID, pattern: registration.Pattern, client: c, completion: registration.Completion})
		}
	}
}

func (c *client) unsubscribe(subscription *Subscription) {
	if !c.subscriptions.Unsubscribe(subscription.pattern, subscription.handlerID) {
		return
	}
	conn := c.currentConnection()
	conn.AddSubscriptions(-1)
	response, err := conn.SendRequestWithWriter(
		conn.LifecycleContext(),
		protocol.MessageTypeKvUnsubscribe,
		kvPatternPayloadWriter(subscription.pattern),
	)
	if err == nil {
		_, _, _ = connection.ParseStandardResponse(response)
	}
}

func (c *client) subscribeWire(ctx context.Context, pattern string) (uint64, error) {
	conn := c.currentConnection()
	response, err := conn.SendRequestWithWriter(ctx, protocol.MessageTypeKvSubscribe, kvPatternPayloadWriter(pattern))
	if err != nil {
		return 0, fmt.Errorf("KV SUBSCRIBE request failed: %w", err)
	}
	success, remaining, err := connection.ParseStandardResponse(response)
	if err != nil {
		return 0, fmt.Errorf("KV SUBSCRIBE failed: %w", err)
	}
	if !success {
		return 0, errors.New("KV SUBSCRIBE returned unexpected status")
	}
	if len(remaining) != 8 {
		return 0, fmt.Errorf("KV SUBSCRIBE response must contain one subscription id, got %d bytes", len(remaining))
	}
	conn.AddSubscriptions(1)
	return binary.BigEndian.Uint64(remaining), nil
}

func (c *client) RestoreSubscriptions(ctx context.Context) error {
	return c.subscriptions.Restore(
		func(pattern string) (uint64, error) { return c.subscribeWire(ctx, pattern) },
		func(pattern string, _ uint64) error {
			conn := c.currentConnection()
			response, err := conn.SendRequestWithWriter(ctx, protocol.MessageTypeKvUnsubscribe, kvPatternPayloadWriter(pattern))
			if err != nil {
				return err
			}
			if _, _, err = connection.ParseStandardResponse(response); err != nil {
				return err
			}
			conn.AddSubscriptions(-1)
			return nil
		},
	)
}

func kvPatternPayloadWriter(pattern string) func(*bytes.Buffer) {
	return func(buffer *bytes.Buffer) { encoding.WriteRoute(buffer, pattern) }
}
