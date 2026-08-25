package fitz

import (
	"context"

	internalschedule "github.com/cntryl/fitz-go/v2/internal/domains/schedule"
)

type ScheduleEntry struct {
	ID           string
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

type ScheduleNotification struct {
	Route   string
	Payload []byte
}

type ScheduleHandler func(context.Context, ScheduleNotification) error

type ScheduleSubscription struct {
	inner *internalschedule.Subscription
}

// Unsubscribe stops receiving schedule fire notifications.
func (s *ScheduleSubscription) Unsubscribe() {
	if s != nil && s.inner != nil {
		s.inner.Unsubscribe()
	}
}

func (s *ScheduleSubscription) Completion() <-chan error {
	if s == nil || s.inner == nil {
		return nil
	}
	return s.inner.Completion()
}

type ScheduleClient interface {
	Create(ctx context.Context, route string, cronExpr string, deliveryMode ScheduleDeliveryMode, payload []byte) (id string, err error)
	Cancel(ctx context.Context, route string) error
	List(ctx context.Context, offset *uint64, limit *uint64) (ScheduleListPage, error)
	ListBySelector(ctx context.Context, selector string) ([]ScheduleEntry, error)
	WaitForNotifications(ctx context.Context, route string) (Iterator[ScheduleNotification], error)
	Subscribe(ctx context.Context, pattern string, handler ScheduleHandler) (*ScheduleSubscription, error)
}

type scheduleClient struct {
	inner internalschedule.Client
}

// Create creates or updates a schedule for the route.
func (c *scheduleClient) Create(ctx context.Context, route string, cronExpr string, deliveryMode ScheduleDeliveryMode, payload []byte) (string, error) {
	return c.inner.Create(ctx, route, cronExpr, internalschedule.ScheduleDeliveryMode(deliveryMode), payload)
}

// Cancel removes a schedule by route.
func (c *scheduleClient) Cancel(ctx context.Context, route string) error {
	return c.inner.Cancel(ctx, route)
}

func (c *scheduleClient) List(ctx context.Context, offset *uint64, limit *uint64) (ScheduleListPage, error) {
	page, err := c.inner.List(ctx, offset, limit)
	if err != nil {
		return ScheduleListPage{}, err
	}
	return ScheduleListPage{Entries: copyScheduleEntries(page.Entries), TotalCount: page.TotalCount}, nil
}

// ListBySelector returns schedules matching a canonical selector.
func (c *scheduleClient) ListBySelector(ctx context.Context, selector string) ([]ScheduleEntry, error) {
	entries, err := c.inner.ListBySelector(ctx, selector)
	if err != nil {
		return nil, err
	}
	return copyScheduleEntries(entries), nil
}

func copyScheduleEntries(entries []internalschedule.ScheduleEntry) []ScheduleEntry {
	publicEntries := make([]ScheduleEntry, 0, len(entries))
	for _, entry := range entries {
		publicEntries = append(publicEntries, ScheduleEntry{
			ID:           entry.ID,
			Route:        entry.Route,
			Cron:         entry.Cron,
			DeliveryMode: ScheduleDeliveryMode(entry.DeliveryMode),
			Payload:      entry.Payload,
		})
	}
	return publicEntries
}

// Subscribe registers a handler for schedule fire notifications.
func (c *scheduleClient) Subscribe(ctx context.Context, pattern string, handler ScheduleHandler) (*ScheduleSubscription, error) {
	sub, err := c.inner.Subscribe(ctx, pattern, func(ctx context.Context, notification internalschedule.Notification) error {
		return handler(ctx, ScheduleNotification{Route: notification.Route, Payload: notification.Payload})
	})
	if err != nil {
		return nil, err
	}
	return &ScheduleSubscription{inner: sub}, nil
}

// WaitForNotifications returns an iterator of schedule fire notifications.
func (c *scheduleClient) WaitForNotifications(ctx context.Context, route string) (Iterator[ScheduleNotification], error) {
	return startManagedPushIterator(ctx, 16,
		func(helperCtx context.Context, emit func(ScheduleNotification) error) (func(), <-chan error, error) {
			subscription, err := c.Subscribe(helperCtx, route, func(_ context.Context, notification ScheduleNotification) error {
				return emit(notification)
			})
			if err != nil {
				return nil, nil, err
			}
			return subscription.Unsubscribe, subscription.Completion(), nil
		})
}

var (
	ErrScheduleNotFound            = internalschedule.ErrScheduleNotFound
	ErrScheduleInvalidDeliveryMode = internalschedule.ErrScheduleInvalidDeliveryMode
)
