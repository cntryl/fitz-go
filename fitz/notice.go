package fitz

import (
	"context"

	internalnotice "github.com/cntryl/fitz-go/internal/domains/notice"
)

type NoticeMsg struct {
	Route string
	Body  []byte
}

type NoticeHandler func(context.Context, NoticeMsg) error

type NoticeSubscription struct {
	inner *internalnotice.Subscription
}

func (s *NoticeSubscription) Unsubscribe() {
	if s != nil && s.inner != nil {
		s.inner.Unsubscribe()
	}
}

type NoticeClient interface {
	Publish(ctx context.Context, route string, body []byte) error
	Subscribe(ctx context.Context, pattern string, handler NoticeHandler) (*NoticeSubscription, error)
}

type noticeClient struct {
	inner internalnotice.Client
}

func (c *noticeClient) Publish(ctx context.Context, route string, body []byte) error {
	return c.inner.Publish(ctx, route, body)
}

func (c *noticeClient) Subscribe(ctx context.Context, pattern string, handler NoticeHandler) (*NoticeSubscription, error) {
	sub, err := c.inner.Subscribe(ctx, pattern, func(ctx context.Context, msg internalnotice.NoticeMsg) error {
		return handler(ctx, NoticeMsg{
			Route: msg.Route,
			Body:  append([]byte(nil), msg.Body...),
		})
	})
	if err != nil {
		return nil, err
	}
	return &NoticeSubscription{inner: sub}, nil
}
