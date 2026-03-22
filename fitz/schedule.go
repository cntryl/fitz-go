package fitz

import (
	"context"

	internalschedule "github.com/cntryl/fitz-go/internal/domains/schedule"
)

type ScheduleEntry struct {
	ID      string
	Route   string
	Cron    string
	Payload []byte
}

type ScheduleNotification struct {
	Payload []byte
}

type ScheduleHandler func(context.Context, ScheduleNotification)

type ScheduleSubscription struct {
	inner *internalschedule.Subscription
}

func (s *ScheduleSubscription) Unsubscribe() {
	if s != nil && s.inner != nil {
		s.inner.Unsubscribe()
	}
}

type ScheduleClient interface {
	Create(ctx context.Context, route string, cronExpr string, payload []byte) (id string, err error)
	Cancel(ctx context.Context, route string) error
	List(ctx context.Context, offset, limit uint64) ([]ScheduleEntry, uint64, error)
	ListBySelector(ctx context.Context, selector string, offset, limit uint64) ([]ScheduleEntry, uint64, error)
	Subscribe(ctx context.Context, pattern string, handler ScheduleHandler) (*ScheduleSubscription, error)
}

type scheduleClient struct {
	inner internalschedule.Client
}

func (c *scheduleClient) Create(ctx context.Context, route string, cronExpr string, payload []byte) (string, error) {
	return c.inner.Create(ctx, route, cronExpr, payload)
}

func (c *scheduleClient) Cancel(ctx context.Context, route string) error {
	return c.inner.Cancel(ctx, route)
}

func (c *scheduleClient) List(ctx context.Context, offset, limit uint64) ([]ScheduleEntry, uint64, error) {
	entries, totalCount, err := c.inner.List(ctx, offset, limit)
	if err != nil {
		return nil, 0, err
	}
	return copyScheduleEntries(entries), totalCount, nil
}

func (c *scheduleClient) ListBySelector(ctx context.Context, selector string, offset, limit uint64) ([]ScheduleEntry, uint64, error) {
	entries, totalCount, err := c.inner.ListBySelector(ctx, selector, offset, limit)
	if err != nil {
		return nil, 0, err
	}
	return copyScheduleEntries(entries), totalCount, nil
}

func copyScheduleEntries(entries []internalschedule.ScheduleEntry) []ScheduleEntry {
	publicEntries := make([]ScheduleEntry, 0, len(entries))
	for _, entry := range entries {
		publicEntries = append(publicEntries, ScheduleEntry{
			ID:      entry.ID,
			Route:   entry.Route,
			Cron:    entry.Cron,
			Payload: append([]byte(nil), entry.Payload...),
		})
	}
	return publicEntries
}

func (c *scheduleClient) Subscribe(ctx context.Context, pattern string, handler ScheduleHandler) (*ScheduleSubscription, error) {
	sub, err := c.inner.Subscribe(ctx, pattern, func(ctx context.Context, notification internalschedule.Notification) {
		handler(ctx, ScheduleNotification{Payload: append([]byte(nil), notification.Payload...)})
	})
	if err != nil {
		return nil, err
	}
	return &ScheduleSubscription{inner: sub}, nil
}

var (
	ErrScheduleNotFound = internalschedule.ErrScheduleNotFound
)
