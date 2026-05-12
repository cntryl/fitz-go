package fitz

import (
	"context"

	internalstream "github.com/cntryl/fitz-go/internal/domains/stream"
)

type StreamFilterClauseKind = internalstream.StreamFilterClauseKind

const (
	StreamFilterEquals     = internalstream.StreamFilterEquals
	StreamFilterNotEquals  = internalstream.StreamFilterNotEquals
	StreamFilterStartsWith = internalstream.StreamFilterStartsWith
	StreamFilterAnyOf      = internalstream.StreamFilterAnyOf
)

type StreamFilterClause = internalstream.StreamFilterClause

type StreamFilterSet = internalstream.StreamFilterSet

type StreamAppendOptions = internalstream.StreamAppendOptions

type StreamReadOptions = internalstream.StreamReadOptions

type StreamFilteredReason = internalstream.FilteredReason

const (
	StreamFilteredReasonServerFilter = internalstream.FilteredReasonServerFilter
	StreamFilteredReasonPermission   = internalstream.FilteredReasonPermission
	StreamFilteredReasonProjection   = internalstream.FilteredReasonProjection
)

type StreamReadItemKind = internalstream.ReadItemKind

const (
	StreamReadItemEvent         = internalstream.ReadItemEvent
	StreamReadItemFiltered      = internalstream.ReadItemFiltered
	StreamReadItemFilteredRange = internalstream.ReadItemFilteredRange
)

type StreamRecord struct {
	Offset      uint64
	AreaOffset  *uint64
	RealmOffset *uint64
	Body        []byte
	Metadata    []byte
	Timestamp   uint64
}

type StreamReadItem struct {
	Kind       StreamReadItemKind
	Record     *StreamRecord
	Offset     uint64
	FromOffset uint64
	ToOffset   uint64
	Reason     *StreamFilteredReason
}

type StreamReadCursor struct {
	LastResourceOffset uint64
	LastAreaOffset     *uint64
	LastRealmOffset    *uint64
	HasMore            bool
}

type StreamReadPage struct {
	Items  []StreamReadItem
	Cursor StreamReadCursor
}

type StreamMetadata struct {
	FirstOffset    uint64
	LastOffset     uint64
	RecordCount    uint64
	MaxBatchEvents uint64
	MaxBatchBytes  uint64
	TTLSeconds     *uint64
	AreaWatermark  uint64
	RealmWatermark uint64
}

type StreamCommitMode uint8

const (
	StreamCommitBuffered StreamCommitMode = StreamCommitMode(internalstream.CommitModeBuffered)
	StreamCommitSync     StreamCommitMode = StreamCommitMode(internalstream.CommitModeSync)
)

type StreamCommitNotification struct {
	Route               string
	Event               string
	FirstResourceOffset uint64
	LastResourceOffset  uint64
	FirstAreaOffset     uint64
	LastAreaOffset      uint64
	FirstRealmOffset    uint64
	LastRealmOffset     uint64
	BatchSize           uint64
}

type StreamCommitHandler func(context.Context, StreamCommitNotification) error

type StreamSubscription struct {
	inner *internalstream.Subscription
}

// Unsubscribe stops receiving stream commit notifications.
func (s *StreamSubscription) Unsubscribe() {
	if s != nil && s.inner != nil {
		s.inner.Unsubscribe()
	}
}

type StreamSession interface {
	Append(ctx context.Context, expectedOffset uint64, body []byte, opts ...*StreamAppendOptions) (offset uint64, err error)
	Commit(ctx context.Context, mode StreamCommitMode) error
	Rollback(ctx context.Context) error
}

type StreamClient interface {
	Begin(ctx context.Context, route string) (StreamSession, error)
	Read(ctx context.Context, route string, fromOffset uint64, limit uint64, opts ...*StreamReadOptions) (Iterator[StreamRecord], error)
	ReadPage(ctx context.Context, route string, fromOffset uint64, limit uint64, opts ...*StreamReadOptions) (*StreamReadPage, error)
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

// Begin starts a stream write session for the provided route.
func (c *streamClient) Begin(ctx context.Context, route string) (StreamSession, error) {
	session, err := c.inner.Begin(ctx, route)
	if err != nil {
		return nil, err
	}
	return &streamSession{inner: session}, nil
}

// Read returns an iterator of records starting at fromOffset.
func (c *streamClient) Read(ctx context.Context, route string, fromOffset uint64, limit uint64, opts ...*StreamReadOptions) (Iterator[StreamRecord], error) {
	iter, err := c.inner.Read(ctx, route, fromOffset, limit, opts...)
	if err != nil {
		return nil, err
	}
	return &streamRecordIterator{inner: iter}, nil
}

// ReadPage returns a page containing events and filtered markers.
func (c *streamClient) ReadPage(ctx context.Context, route string, fromOffset uint64, limit uint64, opts ...*StreamReadOptions) (*StreamReadPage, error) {
	page, err := c.inner.ReadPage(ctx, route, fromOffset, limit, opts...)
	if err != nil || page == nil {
		return nil, err
	}

	items := make([]StreamReadItem, 0, len(page.Items))
	for _, item := range page.Items {
		converted := StreamReadItem{
			Kind:       item.Kind,
			Offset:     item.Offset,
			FromOffset: item.FromOffset,
			ToOffset:   item.ToOffset,
			Reason:     cloneFilteredReasonPtr(item.Reason),
		}
		if item.Record != nil {
			converted.Record = &StreamRecord{
				Offset:      item.Record.Offset,
				AreaOffset:  cloneUint64Ptr(item.Record.AreaOffset),
				RealmOffset: cloneUint64Ptr(item.Record.RealmOffset),
				Body:        append([]byte(nil), item.Record.Body...),
				Metadata:    append([]byte(nil), item.Record.Metadata...),
				Timestamp:   item.Record.Timestamp,
			}
		}
		items = append(items, converted)
	}

	return &StreamReadPage{
		Items: items,
		Cursor: StreamReadCursor{
			LastResourceOffset: page.Cursor.LastResourceOffset,
			LastAreaOffset:     cloneUint64Ptr(page.Cursor.LastAreaOffset),
			LastRealmOffset:    cloneUint64Ptr(page.Cursor.LastRealmOffset),
			HasMore:            page.Cursor.HasMore,
		},
	}, nil
}

// Peek returns the latest record for the route, if present.
func (c *streamClient) Peek(ctx context.Context, route string) (*StreamRecord, error) {
	record, err := c.inner.Peek(ctx, route)
	if err != nil || record == nil {
		return nil, err
	}
	return &StreamRecord{
		Offset:      record.Offset,
		AreaOffset:  cloneUint64Ptr(record.AreaOffset),
		RealmOffset: cloneUint64Ptr(record.RealmOffset),
		Body:        append([]byte(nil), record.Body...),
		Metadata:    append([]byte(nil), record.Metadata...),
		Timestamp:   record.Timestamp,
	}, nil
}

// Metadata returns stream metadata for the route.
func (c *streamClient) Metadata(ctx context.Context, route string) (*StreamMetadata, error) {
	meta, err := c.inner.Metadata(ctx, route)
	if err != nil || meta == nil {
		return nil, err
	}
	return &StreamMetadata{
		FirstOffset:    meta.FirstOffset,
		LastOffset:     meta.LastOffset,
		RecordCount:    meta.RecordCount,
		MaxBatchEvents: meta.MaxBatchEvents,
		MaxBatchBytes:  meta.MaxBatchBytes,
		TTLSeconds:     cloneUint64Ptr(meta.TTLSeconds),
		AreaWatermark:  meta.AreaWatermark,
		RealmWatermark: meta.RealmWatermark,
	}, nil
}

// Subscribe registers a commit notification handler for the route pattern.
func (c *streamClient) Subscribe(ctx context.Context, pattern string, handler StreamCommitHandler) (*StreamSubscription, error) {
	subscription, err := c.inner.Subscribe(ctx, pattern, func(ctx context.Context, notif internalstream.CommitNotification) error {
		return handler(ctx, StreamCommitNotification{
			Route:               notif.Route,
			Event:               notif.Event,
			FirstResourceOffset: notif.FirstResourceOffset,
			LastResourceOffset:  notif.LastResourceOffset,
			FirstAreaOffset:     notif.FirstAreaOffset,
			LastAreaOffset:      notif.LastAreaOffset,
			FirstRealmOffset:    notif.FirstRealmOffset,
			LastRealmOffset:     notif.LastRealmOffset,
			BatchSize:           notif.BatchSize,
		})
	})
	if err != nil {
		return nil, err
	}
	return &StreamSubscription{inner: subscription}, nil
}

// Append writes a record in the active stream session.
func (s *streamSession) Append(ctx context.Context, expectedOffset uint64, body []byte, opts ...*StreamAppendOptions) (offset uint64, err error) {
	return s.inner.Append(ctx, expectedOffset, body, opts...)
}

// Commit finalizes the active stream session.
func (s *streamSession) Commit(ctx context.Context, mode StreamCommitMode) error {
	return s.inner.Commit(ctx, internalstream.CommitMode(mode))
}

// Rollback aborts the active stream session.
func (s *streamSession) Rollback(ctx context.Context) error {
	return s.inner.Rollback(ctx)
}

// Next advances to the next record.
func (it *streamRecordIterator) Next() bool {
	if !it.inner.Next() {
		return false
	}
	record := it.inner.Value()
	it.current = StreamRecord{
		Offset:      record.Offset,
		AreaOffset:  cloneUint64Ptr(record.AreaOffset),
		RealmOffset: cloneUint64Ptr(record.RealmOffset),
		Body:        append([]byte(nil), record.Body...),
		Metadata:    append([]byte(nil), record.Metadata...),
		Timestamp:   record.Timestamp,
	}
	return true
}

// Value returns the current record.
func (it *streamRecordIterator) Value() StreamRecord {
	return it.current
}

// Err returns the terminal iterator error, if any.
func (it *streamRecordIterator) Err() error {
	return it.inner.Err()
}

// Close releases iterator resources.
func (it *streamRecordIterator) Close() error {
	return it.inner.Close()
}

func cloneUint64Ptr(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneFilteredReasonPtr(value *internalstream.FilteredReason) *StreamFilteredReason {
	if value == nil {
		return nil
	}
	clone := StreamFilteredReason(*value)
	return &clone
}
