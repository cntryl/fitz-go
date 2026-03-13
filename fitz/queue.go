package fitz

import (
	"context"

	internalqueue "github.com/cntryl/fitz-go/internal/domains/queue"
)

type QueueAvailabilityNotification = internalqueue.AvailabilityNotification
type QueueAvailabilityHandler = internalqueue.AvailabilityHandler
type QueueSubscription = internalqueue.Subscription

type QueueClient interface {
	Enqueue(ctx context.Context, route string, body []byte) (uint64, error)
	Reserve(ctx context.Context, route string, leaseSecs uint64, batchSize uint32) ([]*QueueItem, error)
	Subscribe(ctx context.Context, pattern string, handler QueueAvailabilityHandler) (*QueueSubscription, error)
}

type QueueItem struct {
	ID    uint64
	Token uint64
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
		ID:    item.ID,
		Token: item.Token,
		Body:  append([]byte(nil), item.Body...),
		inner: item,
	}
}

func (q *QueueItem) Extend(ctx context.Context, leaseSecs uint64) error {
	return q.inner.Extend(ctx, leaseSecs)
}

func (q *QueueItem) Complete(ctx context.Context) error {
	return q.inner.Ack(ctx)
}

func (q *QueueItem) CompleteWithToken(ctx context.Context, token uint64) error {
	return q.inner.AckWithToken(ctx, token)
}

func (c *queueClient) Enqueue(ctx context.Context, route string, body []byte) (uint64, error) {
	return c.inner.Send(ctx, route, body)
}

func (c *queueClient) Reserve(ctx context.Context, route string, leaseSecs uint64, batchSize uint32) ([]*QueueItem, error) {
	items, err := c.inner.Receive(ctx, route, leaseSecs, batchSize)
	if err != nil {
		return nil, err
	}
	wrapped := make([]*QueueItem, 0, len(items))
	for _, item := range items {
		wrapped = append(wrapped, wrapQueueItem(item))
	}
	return wrapped, nil
}

func (c *queueClient) Subscribe(ctx context.Context, pattern string, handler QueueAvailabilityHandler) (*QueueSubscription, error) {
	return c.inner.Subscribe(ctx, pattern, handler)
}

var (
	ErrQueueInvalidToken    = internalqueue.ErrInvalidToken
	ErrQueueLeaseExpired    = internalqueue.ErrLeaseExpiredQ
	ErrQueueMessageNotFound = internalqueue.ErrMessageNotFound
	ErrQueueNotFound        = internalqueue.ErrQueueNotFound
	ErrQueueFull            = internalqueue.ErrQueueFull
)
