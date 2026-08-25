package fitz

import (
	"context"

	internalnotice "github.com/cntryl/fitz-go/v2/internal/domains/notice"
)

var (
	ErrNoticeRouteInvalid = internalnotice.ErrNoticeRouteInvalid
	ErrNoticeTimeout      = internalnotice.ErrNoticeTimeout
	ErrNoticeSendFailed   = internalnotice.ErrNoticeSendFailed
)

type NoticeMsg struct {
	Route string
	Body  []byte
}

type NoticeHandler func(context.Context, NoticeMsg) error

type NoticeSubscription struct {
	inner *internalnotice.Subscription
}

// Unsubscribe stops receiving notice messages for this subscription.
func (s *NoticeSubscription) Unsubscribe() {
	if s != nil && s.inner != nil {
		s.inner.Unsubscribe()
	}
}

// Completion yields nil after unsubscribe or a typed terminal error when local
// callback delivery can no longer continue.
func (s *NoticeSubscription) Completion() <-chan error {
	if s == nil || s.inner == nil {
		return nil
	}
	return s.inner.Completion()
}

type NoticeClient interface {
	Publish(ctx context.Context, route string, body []byte) error
	Subscribe(ctx context.Context, pattern string, handler NoticeHandler) (*NoticeSubscription, error)
}

type noticeClient struct {
	inner internalnotice.Client
}

// Publish sends a notice payload to a fixed route.
func (c *noticeClient) Publish(ctx context.Context, route string, body []byte) error {
	return c.inner.Publish(ctx, route, body)
}

// Subscribe registers a notice handler for the route pattern.
func (c *noticeClient) Subscribe(ctx context.Context, pattern string, handler NoticeHandler) (*NoticeSubscription, error) {
	sub, err := c.inner.Subscribe(ctx, pattern, func(ctx context.Context, msg internalnotice.NoticeMsg) error {
		return handler(ctx, NoticeMsg{
			Route: msg.Route,
			Body:  msg.Body,
		})
	})
	if err != nil {
		return nil, err
	}
	return &NoticeSubscription{inner: sub}, nil
}
