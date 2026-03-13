package fitz

import (
	"context"

	internalstream "github.com/cntryl/fitz-go/internal/domains/stream"
)

type StreamRecord struct {
	Offset uint64
	Body   []byte
}

type StreamMetadata struct {
	FirstOffset uint64
	LastOffset  uint64
	RecordCount uint64
}

type StreamCommitNotification struct {
	Route string
}

type StreamCommitHandler func(context.Context, StreamCommitNotification) error

type StreamSubscription struct {
	inner *internalstream.Subscription
}

func (s *StreamSubscription) Unsubscribe() {
	if s != nil && s.inner != nil {
		s.inner.Unsubscribe()
	}
}

type StreamSession interface {
	Append(ctx context.Context, body []byte) (offset uint64, err error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

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

type streamSession struct {
	inner internalstream.StreamSession
}

type streamRecordIterator struct {
	inner   Iterator[internalstream.Record]
	current StreamRecord
}

func (c *streamClient) Begin(ctx context.Context, route string, expectedOffset uint64) (StreamSession, error) {
	session, err := c.inner.Begin(ctx, route, expectedOffset)
	if err != nil {
		return nil, err
	}
	return &streamSession{inner: session}, nil
}

func (c *streamClient) Read(ctx context.Context, route string, fromOffset uint64, limit uint64) (Iterator[StreamRecord], error) {
	iter, err := c.inner.Read(ctx, route, fromOffset, limit)
	if err != nil {
		return nil, err
	}
	return &streamRecordIterator{inner: iter}, nil
}

func (c *streamClient) Peek(ctx context.Context, route string) (*StreamRecord, error) {
	record, err := c.inner.Peek(ctx, route)
	if err != nil || record == nil {
		return nil, err
	}
	return &StreamRecord{
		Offset: record.Offset,
		Body:   append([]byte(nil), record.Body...),
	}, nil
}

func (c *streamClient) Metadata(ctx context.Context, route string) (*StreamMetadata, error) {
	meta, err := c.inner.Metadata(ctx, route)
	if err != nil || meta == nil {
		return nil, err
	}
	return &StreamMetadata{
		FirstOffset: meta.FirstOffset,
		LastOffset:  meta.LastOffset,
		RecordCount: meta.RecordCount,
	}, nil
}

func (c *streamClient) Subscribe(ctx context.Context, pattern string, handler StreamCommitHandler) (*StreamSubscription, error) {
	subscription, err := c.inner.Subscribe(ctx, pattern, func(ctx context.Context, notif internalstream.CommitNotification) error {
		return handler(ctx, StreamCommitNotification{Route: notif.Route})
	})
	if err != nil {
		return nil, err
	}
	return &StreamSubscription{inner: subscription}, nil
}

func (s *streamSession) Append(ctx context.Context, body []byte) (offset uint64, err error) {
	return s.inner.Append(ctx, body)
}

func (s *streamSession) Commit(ctx context.Context) error {
	return s.inner.Commit(ctx)
}

func (s *streamSession) Rollback(ctx context.Context) error {
	return s.inner.Rollback(ctx)
}

func (it *streamRecordIterator) Next() bool {
	if !it.inner.Next() {
		return false
	}
	record := it.inner.Value()
	it.current = StreamRecord{
		Offset: record.Offset,
		Body:   append([]byte(nil), record.Body...),
	}
	return true
}

func (it *streamRecordIterator) Value() StreamRecord {
	return it.current
}

func (it *streamRecordIterator) Err() error {
	return it.inner.Err()
}

func (it *streamRecordIterator) Close() error {
	return it.inner.Close()
}
