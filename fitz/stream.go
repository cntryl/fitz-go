package fitz

import (
	"context"

	internalstream "github.com/cntryl/fitz-go/internal/domains/stream"
)

type StreamRecord = internalstream.Record
type StreamMetadata = internalstream.Metadata
type StreamCommitNotification = internalstream.CommitNotification
type StreamCommitHandler = internalstream.CommitHandler
type StreamSubscription = internalstream.Subscription
type StreamSession = internalstream.StreamSession

type StreamClient interface {
	Begin(ctx context.Context, route string, expectedOffset uint64) (StreamSession, error)
	Read(ctx context.Context, route string, fromOffset uint64, limit uint64) (Iterator[StreamRecord], error)
	Peek(ctx context.Context, route string) (*StreamRecord, error)
	Metadata(ctx context.Context, route string) (*StreamMetadata, error)
	Subscribe(ctx context.Context, pattern string, handler StreamCommitHandler) (*StreamSubscription, error)
}

type streamClient struct {
	inner internalstream.Client
}

func (c *streamClient) Begin(ctx context.Context, route string, expectedOffset uint64) (StreamSession, error) {
	return c.inner.Begin(ctx, route, expectedOffset)
}

func (c *streamClient) Read(ctx context.Context, route string, fromOffset uint64, limit uint64) (Iterator[StreamRecord], error) {
	return c.inner.Read(ctx, route, fromOffset, limit)
}

func (c *streamClient) Peek(ctx context.Context, route string) (*StreamRecord, error) {
	return c.inner.Peek(ctx, route)
}

func (c *streamClient) Metadata(ctx context.Context, route string) (*StreamMetadata, error) {
	return c.inner.GetMetadata(ctx, route)
}

func (c *streamClient) Subscribe(ctx context.Context, pattern string, handler StreamCommitHandler) (*StreamSubscription, error) {
	return c.inner.Subscribe(ctx, pattern, handler)
}
