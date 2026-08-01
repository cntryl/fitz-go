package fitz

import (
	"context"

	internalqueue "github.com/cntryl/fitz-go/internal/domains/queue"
)

type QueueAvailabilityNotification struct {
	Route            string
	ReadyMessages    uint64
	DelayedMessages  uint64
	InflightMessages uint64
}

type QueueAvailabilityHandler func(context.Context, QueueAvailabilityNotification) error

type queueReserveConfig struct {
	batchSize   uint32
	waitSeconds uint64
}

type QueueReserveOption func(*queueReserveConfig)

func WithQueueReserveBatchSize(batchSize uint32) QueueReserveOption {
	return func(cfg *queueReserveConfig) {
		cfg.batchSize = batchSize
	}
}

func WithQueueReserveWaitSeconds(waitSeconds uint64) QueueReserveOption {
	return func(cfg *queueReserveConfig) {
		cfg.waitSeconds = waitSeconds
	}
}

type QueueSubscription struct {
	inner *internalqueue.Subscription
}

// Unsubscribe stops receiving queue availability notifications.
func (s *QueueSubscription) Unsubscribe() {
	if s != nil && s.inner != nil {
		s.inner.Unsubscribe()
	}
}

type QueueClient interface {
	Enqueue(ctx context.Context, route string, body []byte) (uint64, error)
	Reserve(ctx context.Context, route string, leaseSecs uint64, batchSize uint32) ([]*QueueItem, error)
	ReserveWithOptions(ctx context.Context, route string, leaseSecs uint64, opts ...QueueReserveOption) ([]*QueueItem, error)
	ReserveWhenAvailable(ctx context.Context, route string, leaseSecs uint64, batchSize uint32) (Iterator[[]*QueueItem], error)
	Subscribe(ctx context.Context, pattern string, handler QueueAvailabilityHandler) (*QueueSubscription, error)
}

type QueueItem struct {
	id    uint64
	token uint64
	Body  []byte

	inner *internalqueue.QueueItem
}

type queueClient struct {
	inner internalqueue.Client
}

func wrapQueueItem(item *internalqueue.QueueItem) *QueueItem {
	if item == nil {
		return nil
	}
	return &QueueItem{
		id:    item.ID,
		token: item.Token,
		Body:  item.Body,
		inner: item,
	}
}

// Extend renews this queue item's lease duration.
func (q *QueueItem) Extend(ctx context.Context, leaseSecs uint64) error {
	return q.inner.Extend(ctx, leaseSecs)
}

// Complete acknowledges this queue item using its current token.
func (q *QueueItem) Complete(ctx context.Context) error {
	return q.inner.Complete(ctx)
}

// CompleteWithToken acknowledges this queue item with an explicit token.
func (q *QueueItem) CompleteWithToken(ctx context.Context, token uint64) error {
	return q.inner.CompleteWithToken(ctx, token)
}

func (c *queueClient) Enqueue(ctx context.Context, route string, body []byte) (uint64, error) {
	return c.inner.Enqueue(ctx, route, body)
}

func (c *queueClient) Reserve(ctx context.Context, route string, leaseSecs uint64, batchSize uint32) ([]*QueueItem, error) {
	items, err := c.inner.Reserve(ctx, route, leaseSecs, batchSize)
	if err != nil {
		return nil, err
	}
	wrapped := make([]*QueueItem, 0, len(items))
	for _, item := range items {
		wrapped = append(wrapped, wrapQueueItem(item))
	}
	return wrapped, nil
}

func (c *queueClient) ReserveWithOptions(ctx context.Context, route string, leaseSecs uint64, opts ...QueueReserveOption) ([]*QueueItem, error) {
	cfg := queueReserveConfig{
		batchSize: 1,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	internalOpts := []internalqueue.ReserveOption{
		internalqueue.WithBatchSize(cfg.batchSize),
	}
	if cfg.waitSeconds > 0 {
		internalOpts = append(internalOpts, internalqueue.WithWaitSeconds(cfg.waitSeconds))
	}

	items, err := c.inner.ReserveWithOptions(ctx, route, leaseSecs, internalOpts...)
	if err != nil {
		return nil, err
	}
	wrapped := make([]*QueueItem, 0, len(items))
	for _, item := range items {
		wrapped = append(wrapped, wrapQueueItem(item))
	}
	return wrapped, nil
}

// ReserveWhenAvailable yields non-empty reserve batches. Availability
// notifications only wake the loop; Reserve remains authoritative.
func (c *queueClient) ReserveWhenAvailable(ctx context.Context, route string, leaseSecs uint64, batchSize uint32) (Iterator[[]*QueueItem], error) {
	if batchSize == 0 {
		batchSize = 1
	}
	return startManagedPollingIterator(ctx,
		func(helperCtx context.Context, wake func()) (func(), error) {
			subscription, err := c.Subscribe(helperCtx, route, func(context.Context, QueueAvailabilityNotification) error { wake(); return nil })
			if err != nil {
				return nil, err
			}
			return subscription.Unsubscribe, nil
		},
		func(helperCtx context.Context) (managedPollResult[[]*QueueItem], error) {
			reserved, err := c.Reserve(helperCtx, route, leaseSecs, batchSize)
			return managedPollResult[[]*QueueItem]{value: reserved, emit: len(reserved) > 0, pollAgain: len(reserved) > 0}, err
		})
}

func (c *queueClient) Subscribe(ctx context.Context, pattern string, handler QueueAvailabilityHandler) (*QueueSubscription, error) {
	subscription, err := c.inner.Subscribe(ctx, pattern, func(ctx context.Context, notification internalqueue.AvailabilityNotification) error {
		return handler(ctx, QueueAvailabilityNotification{
			Route:            notification.Route,
			ReadyMessages:    notification.ReadyMessages,
			DelayedMessages:  notification.DelayedMessages,
			InflightMessages: notification.InflightMessages,
		})
	})
	if err != nil {
		return nil, err
	}
	return &QueueSubscription{inner: subscription}, nil
}

var (
	ErrQueueInvalidToken    = internalqueue.ErrInvalidToken
	ErrQueueLeaseExpired    = internalqueue.ErrLeaseExpiredQ
	ErrQueueMessageNotFound = internalqueue.ErrMessageNotFound
	ErrQueueNotFound        = internalqueue.ErrQueueNotFound
	ErrQueueFull            = internalqueue.ErrQueueFull
)
