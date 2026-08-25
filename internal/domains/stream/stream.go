// Package stream implements the Fitz Stream domain client.
// Per CLIENT_SPEC.md: Append-only log with transactional semantics.
package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/cntryl/fitz-go/v2/internal/core/connection"
	coreerrors "github.com/cntryl/fitz-go/v2/internal/core/errors"
	"github.com/cntryl/fitz-go/v2/internal/core/iter"
	"github.com/cntryl/fitz-go/v2/internal/core/reconnect"
	"github.com/cntryl/fitz-go/v2/internal/core/subscriptions"
	"github.com/cntryl/fitz-go/v2/internal/core/types"
	"github.com/cntryl/fitz-go/v2/internal/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func parsePlainStreamResponse(payload []byte) (bool, []byte, error) {
	if len(payload) == 0 {
		return false, nil, errors.New("response too short")
	}
	if payload[0] == 0 {
		return true, payload[1:], nil
	}
	if payload[0] != 1 {
		return false, nil, fmt.Errorf("unknown response status: %d", payload[0])
	}
	message, end, err := connection.ReadString(payload, 1)
	if err != nil {
		return false, nil, fmt.Errorf("decode stream error: %w", err)
	}
	if end != len(payload) {
		return false, nil, errors.New("trailing bytes after stream error")
	}
	return false, nil, errors.New(message)
}

func parseStreamReadResponse(payload []byte) (bool, []byte, error) {
	return connection.ParseStandardResponse(payload)
}

// Record represents a single stream record.
type Record struct {
	Route        string
	Offset       uint64
	AreaOffset   *uint64
	RealmOffset  *uint64
	GlobalOffset *uint64
	Body         []byte
	Metadata     []byte
	Timestamp    uint64
}

type FilteredReason string

const (
	FilteredReasonServerFilter FilteredReason = "server_filter"
	FilteredReasonPermission   FilteredReason = "permission"
	FilteredReasonProjection   FilteredReason = "projection"
)

type ReadItemKind uint8

const (
	ReadItemEvent ReadItemKind = iota
	ReadItemFiltered
	ReadItemFilteredRange
)

type ReadItem struct {
	Route      string
	Kind       ReadItemKind
	Record     *Record
	Offset     uint64
	FromOffset uint64
	ToOffset   uint64
	Reason     *FilteredReason
}

type ReadCursor struct {
	LastResourceOffset uint64
	LastAreaOffset     *uint64
	LastRealmOffset    *uint64
	LastGlobalOffset   *uint64
	CursorFingerprint  *uint64
	CapturedWatermark  *uint64
	HasMore            bool
}

type ReadPage struct {
	Items  []ReadItem
	Cursor ReadCursor
}

// Metadata holds stream metadata.
type Metadata struct {
	FirstOffset    uint64
	LastOffset     uint64
	RecordCount    uint64
	MaxBatchEvents uint64
	MaxBatchBytes  uint64
	TTLSeconds     *uint64
	AreaWatermark  uint64
	RealmWatermark uint64
}

// CommitNotification is a notification of stream data availability.
type CommitNotification struct {
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

// CommitHandler handles stream commit notifications.
type CommitHandler func(context.Context, CommitNotification) error

// Subscription represents a stream subscription.
type Subscription struct {
	subID      uint64
	handlerID  uint64
	pattern    string
	client     *client
	completion *subscriptions.Completion
}

// Unsubscribe removes the subscription.
func (sub *Subscription) Unsubscribe() {
	if sub.client != nil {
		sub.client.unsubscribe(sub)
	}
}

func (sub *Subscription) Completion() <-chan error {
	if sub == nil || sub.completion == nil {
		return nil
	}
	return sub.completion.Done()
}

// StreamSession is a write session for appending to a stream.
// Obtained from Begin; use Append, then Commit or Rollback.
// Expected offset (OCC) is provided on each Append call and tracked by the session/server.
// Per CLIENT_SPEC.md, operations on a session MUST be sequential.
type StreamSession interface {
	// Append adds a record to the stream. Returns the assigned offset when available.
	Append(ctx context.Context, expectedOffset uint64, body []byte, opts ...*StreamAppendOptions) (offset uint64, err error)
	// Commit finalizes the write session and makes appends durable.
	Commit(ctx context.Context, mode CommitMode) error
	// Rollback discards uncommitted appends.
	Rollback(ctx context.Context) error
}

// Client is the Stream domain client interface.
type Client interface {
	// Begin starts a write session on the given route.
	// Returns a session on which to call Append, then Commit or Rollback.
	Begin(ctx context.Context, route string) (StreamSession, error)

	// Read reads records from the given route starting at fromOffset.
	Read(ctx context.Context, route string, fromOffset uint64, limit uint64, opts ...*StreamReadOptions) (iter.Iterator[Record], error)

	// ReadPage reads a raw replay page, including filtered markers and cursor metadata.
	ReadPage(ctx context.Context, route string, fromOffset uint64, limit uint64, opts ...*StreamReadOptions) (*ReadPage, error)

	// Peek returns the most recent record in the stream.
	Peek(ctx context.Context, route string) (*Record, error)

	// Metadata returns stream metadata.
	Metadata(ctx context.Context, route string) (*Metadata, error)

	// Subscribe registers a handler for stream commit notifications.
	// Pattern should be a wildcard pattern (e.g., "stream://realm/area/resource/available").
	Subscribe(ctx context.Context, pattern string, handler CommitHandler) (*Subscription, error)
}

type client struct {
	conn                    *connection.Connection
	connMu                  sync.RWMutex
	mu                      sync.Mutex
	subscriptions           *subscriptions.Registry[CommitHandler]
	notifyHandlerInitOnce   sync.Once
	notifyHandlerRegistered atomic.Bool
}

// session is the concrete implementation of StreamSession.
type session struct {
	route     string
	sessionID uint64
	conn      *connection.Connection
}

// NewClient creates a new Stream domain client.
func NewClient(conn *connection.Connection) Client {
	return &client{
		conn:          conn,
		subscriptions: subscriptions.NewRegistry[CommitHandler](),
	}
}

var _ reconnect.DomainRestorer = (*client)(nil)

// Begin per server stream_codec.rs:
// Request: [string route][optional bytes ingest_metadata]
// Response: [status][u64 session_id][bytes data]
// Expected offset (OCC) is sent on Append; Begin creates session state only.
func (c *client) Begin(ctx context.Context, route string) (StreamSession, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.stream.Begin", trace.WithAttributes(
		attribute.String("fitz.route", route),
	))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.DebugContext(ctx, "stream.Begin", "route", route)
	}

	// Validate route format
	if err := types.ValidateFixedRoute(route, "stream", 3); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("invalid route: %w", err)
	}

	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeStreamBegin, streamBeginPayloadWriter(route, nil))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("begin request failed: %w", err)
	}

	success, remaining, err := parsePlainStreamResponse(resp)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("begin failed: %w", mapStreamError(err))
	}
	if !success {
		recordErr := errors.New("begin failed: unexpected status")
		span.RecordError(recordErr)
		span.SetStatus(codes.Error, recordErr.Error())
		return nil, recordErr
	}

	if len(remaining) < 12 {
		recordErr := errors.New("begin response too short")
		span.RecordError(recordErr)
		span.SetStatus(codes.Error, recordErr.Error())
		return nil, recordErr
	}
	sessionID, dataOffset, err := connection.ReadU64BE(remaining, 0)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("parse session_id: %w", err)
	}
	if _, end, err := connection.ReadBytes(remaining, dataOffset); err != nil || end != len(remaining) {
		return nil, errors.New("begin response contains invalid data payload")
	}

	return &session{route: route, sessionID: sessionID, conn: c.conn}, nil
}

// Append per server stream_codec.rs. Expected offset is supplied on every call.
// Request: [u64 session_id][u64 expected_offset][bytes body][optional bytes metadata][optional string discriminator]
func (s *session) Append(ctx context.Context, expectedOffset uint64, body []byte, opts ...*StreamAppendOptions) (uint64, error) {
	ctx, span := s.conn.Tracer().Start(ctx, "fitz.stream.Append", trace.WithAttributes(
		attribute.String("fitz.route", s.route),
		attribute.Int64("fitz.session_id", int64(s.sessionID)),
		attribute.Int64("fitz.expected_offset", int64(expectedOffset)),
	))
	defer span.End()
	if err := s.conn.CheckLiveHandle(); err != nil {
		return 0, err
	}
	resp, err := s.conn.SendRequestWithWriter(ctx, protocol.MessageTypeStreamAppend, streamAppendPayloadWriter(s.sessionID, expectedOffset, body, nil, opts...))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, fmt.Errorf("send request failed: %w", err)
	}

	success, remaining, err := parsePlainStreamResponse(resp)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, fmt.Errorf("send failed: %w", mapStreamError(err))
	}
	if !success {
		recordErr := errors.New("send failed: unexpected status")
		span.RecordError(recordErr)
		span.SetStatus(codes.Error, recordErr.Error())
		return 0, recordErr
	}

	if data, end, err := connection.ReadBytes(remaining, 0); err == nil && end == len(remaining) {
		if len(data) >= 8 {
			assignedOffset, _, _ := connection.ReadU64BE(data, 0)
			return assignedOffset, nil
		}
	}
	return 0, nil
}

// Commit per server stream_codec.rs:
// Request: [u64 session_id][u8 mode] where mode: 0=Buffered, 1=Sync
func (s *session) Commit(ctx context.Context, mode CommitMode) error {
	ctx, span := s.conn.Tracer().Start(ctx, "fitz.stream.Commit", trace.WithAttributes(
		attribute.String("fitz.route", s.route),
		attribute.Int64("fitz.session_id", int64(s.sessionID)),
	))
	defer span.End()
	if err := s.conn.CheckLiveHandle(); err != nil {
		return err
	}
	resp, err := s.conn.SendRequestWithWriter(ctx, protocol.MessageTypeStreamCommit, streamCommitPayloadWriter(s.sessionID, mode))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("commit request failed: %w", err)
	}

	success, _, err := parsePlainStreamResponse(resp)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("commit failed: %w", mapStreamError(err))
	}
	if !success {
		recordErr := errors.New("commit failed: unexpected status")
		span.RecordError(recordErr)
		span.SetStatus(codes.Error, recordErr.Error())
		return recordErr
	}
	return nil
}

// Rollback per server stream_codec.rs:
// Request: [u64 session_id]
func (s *session) Rollback(ctx context.Context) error {
	ctx, span := s.conn.Tracer().Start(ctx, "fitz.stream.Rollback", trace.WithAttributes(
		attribute.String("fitz.route", s.route),
		attribute.Int64("fitz.session_id", int64(s.sessionID)),
	))
	defer span.End()
	if err := s.conn.CheckLiveHandle(); err != nil {
		return err
	}
	resp, err := s.conn.SendRequestWithWriter(ctx, protocol.MessageTypeStreamRollback, streamRollbackPayloadWriter(s.sessionID))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("rollback request failed: %w", err)
	}

	success, _, err := parsePlainStreamResponse(resp)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("rollback failed: %w", mapStreamError(err))
	}
	if !success {
		recordErr := errors.New("rollback failed: unexpected status")
		span.RecordError(recordErr)
		span.SetStatus(codes.Error, recordErr.Error())
		return recordErr
	}
	return nil
}

// Read per server stream_codec.rs:
// Request: [string route][u64 from_offset][u64 limit][optional bincode filter]
// Response: [status][record data]
func (c *client) Read(ctx context.Context, route string, fromOffset uint64, limit uint64, opts ...*StreamReadOptions) (iter.Iterator[Record], error) {
	page, err := c.ReadPage(ctx, route, fromOffset, limit, opts...)
	if err != nil {
		return nil, err
	}
	return iter.NewSliceIterator(flattenReadItems(page.Items)), nil
}

// ReadPage per server stream_codec.rs:
// Request: [string route][u64 from_offset][u64 limit][optional bincode filter]
// Response: [status][u8 has_session_id][u64?][bytes data]
func (c *client) ReadPage(ctx context.Context, route string, fromOffset uint64, limit uint64, opts ...*StreamReadOptions) (*ReadPage, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.stream.Read", trace.WithAttributes(
		attribute.String("fitz.route", route),
		attribute.Int64("fitz.from_offset", int64(fromOffset)),
		attribute.Int64("fitz.limit", int64(limit)),
	))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.DebugContext(ctx, "stream.Read", "route", route, "from_offset", fromOffset, "limit", limit)
	}
	if err := types.ValidateStreamSelector(route); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("invalid route: %w", err)
	}
	var readOptions *StreamReadOptions
	if len(opts) > 0 && opts[0] != nil {
		readOptions = opts[0]
	}
	var page *ReadPage
	err := c.conn.RunWithRetry(ctx, func() error {
		resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeStreamRead, streamReadPayloadWriter(route, fromOffset, limit, readOptions))
		if err != nil {
			return fmt.Errorf("read request failed: %w", err)
		}

		success, remaining, err := parseStreamReadResponse(resp)
		if err != nil {
			return fmt.Errorf("read failed: %w", mapStreamError(err))
		}
		if !success {
			return errors.New("read failed: unexpected status")
		}

		data, err := skipOptionalSessionIDAndGetData(remaining)
		if err != nil {
			return fmt.Errorf("parse read response envelope: %w", err)
		}

		page, err = parseReadPageResponse(data, route)
		if err != nil {
			return fmt.Errorf("parse read response: %w", err)
		}
		return nil
	}, isStreamReadRetryable)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	return page, nil
}

// Peek per server stream_codec.rs:
// Request: [string route]
// Response: [status][u8 has_session_id][u64?][bytes data]
func (c *client) Peek(ctx context.Context, route string) (*Record, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.stream.Peek", trace.WithAttributes(attribute.String("fitz.route", route)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.DebugContext(ctx, "stream.Peek", "route", route)
	}
	if err := types.ValidateFixedRoute(route, "stream", 3); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("invalid route: %w", err)
	}
	var record *Record
	err := c.conn.RunWithRetry(ctx, func() error {
		resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeStreamLast, streamLastPayloadWriter(route))
		if err != nil {
			return fmt.Errorf("peek request failed: %w", err)
		}

		success, remaining, err := parsePlainStreamResponse(resp)
		if err != nil {
			return fmt.Errorf("peek failed: %w", mapStreamError(err))
		}
		if !success {
			return errors.New("peek failed: unexpected status")
		}

		if len(remaining) == 0 {
			record = nil
			return nil
		}

		record, err = parseLastResponse(remaining, route)
		if err != nil {
			return fmt.Errorf("parse peek response: %w", err)
		}
		return nil
	}, isStreamReadRetryable)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	return record, nil
}

// Metadata per server stream_codec.rs:
// Request: [string route]
// Response: [status][metadata data]
func (c *client) Metadata(ctx context.Context, route string) (*Metadata, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.stream.Metadata", trace.WithAttributes(attribute.String("fitz.route", route)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.DebugContext(ctx, "stream.Metadata", "route", route)
	}
	if err := types.ValidateFixedRoute(route, "stream", 3); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("invalid route: %w", err)
	}
	var meta *Metadata
	err := c.conn.RunWithRetry(ctx, func() error {
		resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeStreamGetMetadata, streamGetMetadataPayloadWriter(route))
		if err != nil {
			return fmt.Errorf("get_metadata request failed: %w", err)
		}

		success, remaining, err := parsePlainStreamResponse(resp)
		if err != nil {
			return fmt.Errorf("get_metadata failed: %w", mapStreamError(err))
		}
		if !success {
			return errors.New("get_metadata failed: unexpected status")
		}

		meta, err = parseMetadataPayload(remaining)
		if err != nil {
			return fmt.Errorf("parse GET_METADATA response: %w", err)
		}
		return nil
	}, isStreamReadRetryable)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	return meta, nil
}

// skipOptionalSessionIDAndGetData parses the common stream response format:
// [u8 has_session_id][u64 session_id if has=1][bytes data]
// Returns the decoded data payload.
func skipOptionalSessionIDAndGetData(remaining []byte) ([]byte, error) {
	offset := 0
	if len(remaining) == 0 {
		return nil, errors.New("stream response missing payload")
	}

	if _, newOffset, err := readOptionalU64(remaining, offset); err != nil {
		return nil, fmt.Errorf("parse optional session_id: %w", err)
	} else {
		offset = newOffset
	}

	data, newOffset, err := connection.ReadBytes(remaining, offset)
	if err != nil {
		return nil, fmt.Errorf("parse response data: %w", err)
	}
	if newOffset != len(remaining) {
		return nil, errors.New("stream response has trailing bytes")
	}

	return data, nil
}

// parseReadPageResponse parses a raw replay page from a READ response data blob.
func parseReadPageResponse(data []byte, selector string) (*ReadPage, error) {
	scope, err := types.ClassifyStreamSelector(selector)
	if err != nil {
		return nil, fmt.Errorf("invalid stream selector for READ response: %w", err)
	}
	if len(data) == 0 {
		return &ReadPage{Items: nil, Cursor: ReadCursor{}}, nil
	}

	count, offset, err := connection.ReadU32BE(data, 0)
	if err != nil {
		return nil, fmt.Errorf("parse record count: %w", err)
	}

	items := make([]ReadItem, 0, count)
	global := scope == types.StreamScopeGlobal
	for i := range count {
		concreteRoute, newOffset, err := connection.ReadString(data, offset)
		if err != nil {
			return nil, fmt.Errorf("parse concrete route at item %d: %w", i, err)
		}
		if err = types.ValidateFixedRoute(concreteRoute, "stream", 3); err != nil {
			return nil, fmt.Errorf("parse concrete route at item %d: %w", i, err)
		}
		offset = newOffset
		item, newOffset, err := decodeStreamReadItemAt(data, offset, global)
		if err != nil {
			return nil, fmt.Errorf("parse read item %d: %w", i, err)
		}
		item.Route = concreteRoute
		if item.Record != nil {
			item.Record.Route = concreteRoute
		}
		items = append(items, *item)
		offset = newOffset
	}

	cursor := ReadCursor{}
	if cursor.LastResourceOffset, offset, err = connection.ReadU64BE(data, offset); err != nil {
		return nil, fmt.Errorf("parse last_resource_offset: %w", err)
	}
	if cursor.LastAreaOffset, offset, err = readOptionalU64(data, offset); err != nil {
		return nil, fmt.Errorf("parse last_area_offset: %w", err)
	}
	if cursor.LastRealmOffset, offset, err = readOptionalU64(data, offset); err != nil {
		return nil, fmt.Errorf("parse last_realm_offset: %w", err)
	}
	if global {
		if cursor.LastGlobalOffset, offset, err = readOptionalU64(data, offset); err != nil {
			return nil, fmt.Errorf("parse last_global_offset: %w", err)
		}
	}
	if offset >= len(data) || (data[offset] != 0 && data[offset] != 1) {
		return nil, errors.New("invalid has_more flag")
	}
	cursor.HasMore = data[offset] == 1
	offset++
	if global {
		if cursor.CursorFingerprint, offset, err = readOptionalU64(data, offset); err != nil {
			return nil, fmt.Errorf("parse cursor_fingerprint: %w", err)
		}
		if cursor.CapturedWatermark, offset, err = readOptionalU64(data, offset); err != nil {
			return nil, fmt.Errorf("parse captured_watermark: %w", err)
		}
	}
	if err != nil {
		return nil, err
	}
	if offset != len(data) {
		return nil, errors.New("read response has trailing bytes")
	}

	return &ReadPage{Items: items, Cursor: cursor}, nil
}

// parseRecord parses a single record from the payload at the given offset.
func parseRecord(data []byte, offset int) (*Record, error) {
	rec, newOffset, err := decodeStreamRecordAt(data, offset, false)
	if err != nil {
		return nil, err
	}
	if newOffset != len(data) {
		return nil, errors.New("parse record has trailing bytes")
	}
	return rec, nil
}

func parseLastResponse(data []byte, route string) (*Record, error) {
	if err := types.ValidateFixedRoute(route, "stream", 3); err != nil {
		return nil, fmt.Errorf("invalid concrete route in response: %w", err)
	}
	record, newOffset, err := decodeStreamRecordAt(data, 0, false)
	if err != nil {
		return nil, err
	}
	if newOffset != len(data) {
		return nil, errors.New("parse last response has trailing bytes")
	}
	record.Route = route
	return record, nil
}

func decodeStreamRecordAt(data []byte, offset int, global bool) (*Record, int, error) {
	rec := &Record{}

	var err error
	rec.Offset, offset, err = connection.ReadU64BE(data, offset)
	if err != nil {
		return nil, offset, fmt.Errorf("parse record offset: %w", err)
	}

	rec.AreaOffset, offset, err = readOptionalU64(data, offset)
	if err != nil {
		return nil, offset, fmt.Errorf("parse record area_offset: %w", err)
	}

	rec.RealmOffset, offset, err = readOptionalU64(data, offset)
	if err != nil {
		return nil, offset, fmt.Errorf("parse record realm_offset: %w", err)
	}
	if global {
		rec.GlobalOffset, offset, err = readOptionalU64(data, offset)
		if err != nil {
			return nil, offset, fmt.Errorf("parse record global_offset: %w", err)
		}
	}

	bodyData, offset, err := connection.ReadBytes(data, offset)
	if err != nil {
		return nil, offset, fmt.Errorf("parse record body: %w", err)
	}
	rec.Body = append([]byte(nil), bodyData...)

	rec.Metadata, offset, err = readOptionalBytes(data, offset)
	if err != nil {
		return nil, offset, fmt.Errorf("parse record metadata: %w", err)
	}

	rec.Timestamp, offset, err = connection.ReadU64BE(data, offset)
	if err != nil {
		return nil, offset, fmt.Errorf("parse record timestamp: %w", err)
	}

	return rec, offset, nil
}

func decodeStreamReadItemAt(data []byte, offset int, global bool) (*ReadItem, int, error) {
	if offset >= len(data) {
		return nil, offset, fmt.Errorf("parse read item tag: %w", io.ErrUnexpectedEOF)
	}

	tag := data[offset]
	offset++
	item := &ReadItem{Kind: ReadItemKind(tag)}

	var err error
	switch tag {
	case 0:
		item.Record, offset, err = decodeStreamRecordAt(data, offset, global)
		if err != nil {
			return nil, offset, err
		}
	case 1:
		item.Offset, offset, err = connection.ReadU64BE(data, offset)
		if err != nil {
			return nil, offset, fmt.Errorf("parse filtered offset: %w", err)
		}
		item.Reason, offset, err = decodeFilteredReasonAt(data, offset)
		if err != nil {
			return nil, offset, err
		}
	case 2:
		item.FromOffset, offset, err = connection.ReadU64BE(data, offset)
		if err != nil {
			return nil, offset, fmt.Errorf("parse filtered range from_offset: %w", err)
		}
		item.ToOffset, offset, err = connection.ReadU64BE(data, offset)
		if err != nil {
			return nil, offset, fmt.Errorf("parse filtered range to_offset: %w", err)
		}
		item.Reason, offset, err = decodeFilteredReasonAt(data, offset)
		if err != nil {
			return nil, offset, err
		}
	default:
		return nil, offset, fmt.Errorf("unknown stream read item tag: %d", tag)
	}

	return item, offset, nil
}

func decodeFilteredReasonAt(data []byte, offset int) (*FilteredReason, int, error) {
	if offset >= len(data) {
		return nil, offset, fmt.Errorf("parse filtered reason: %w", io.ErrUnexpectedEOF)
	}

	var reason FilteredReason
	switch data[offset] {
	case 0:
		return nil, offset + 1, nil
	case 1:
		reason = FilteredReasonServerFilter
	case 2:
		reason = FilteredReasonPermission
	case 3:
		reason = FilteredReasonProjection
	default:
		return nil, offset, fmt.Errorf("parse filtered reason: invalid tag %d", data[offset])
	}

	offset++
	return &reason, offset, nil
}

func flattenReadItems(items []ReadItem) []Record {
	records := make([]Record, 0, len(items))
	for _, item := range items {
		if item.Kind == ReadItemEvent && item.Record != nil {
			records = append(records, *item.Record)
		}
	}
	return records
}

func isStreamReadRetryable(err error) bool {
	return connection.IsTransientRetryable(err)
}

func parseMetadataPayload(data []byte) (*Metadata, error) {
	meta := &Metadata{}
	offset := 0
	var err error

	if firstOffset, newOffset, err := readOptionalU64(data, offset); err != nil {
		return nil, fmt.Errorf("parse first_offset: %w", err)
	} else {
		if firstOffset != nil {
			meta.FirstOffset = *firstOffset
		}
		offset = newOffset
	}

	if lastOffset, newOffset, err := readOptionalU64(data, offset); err != nil {
		return nil, fmt.Errorf("parse last_offset: %w", err)
	} else {
		if lastOffset != nil {
			meta.LastOffset = *lastOffset
		}
		offset = newOffset
	}

	if meta.RecordCount, offset, err = connection.ReadU64BE(data, offset); err != nil {
		return nil, fmt.Errorf("parse record_count: %w", err)
	}
	if meta.MaxBatchEvents, offset, err = connection.ReadU64BE(data, offset); err != nil {
		return nil, fmt.Errorf("parse max_batch_events: %w", err)
	}
	if meta.MaxBatchBytes, offset, err = connection.ReadU64BE(data, offset); err != nil {
		return nil, fmt.Errorf("parse max_batch_bytes: %w", err)
	}
	if meta.TTLSeconds, offset, err = readOptionalU64(data, offset); err != nil {
		return nil, fmt.Errorf("parse ttl_seconds: %w", err)
	}
	if meta.AreaWatermark, offset, err = connection.ReadU64BE(data, offset); err != nil {
		return nil, fmt.Errorf("parse area_watermark: %w", err)
	}
	if meta.RealmWatermark, offset, err = connection.ReadU64BE(data, offset); err != nil {
		return nil, fmt.Errorf("parse realm_watermark: %w", err)
	}

	if offset != len(data) {
		return nil, errors.New("get_metadata response has trailing bytes")
	}

	return meta, nil
}

func readOptionalU64(data []byte, offset int) (*uint64, int, error) {
	if offset >= len(data) {
		return nil, offset, errors.New("optional u64 flag missing")
	}

	flag := data[offset]
	offset++
	switch flag {
	case 0:
		return nil, offset, nil
	case 1:
		value, newOffset, err := connection.ReadU64BE(data, offset)
		if err != nil {
			return nil, newOffset, err
		}
		return &value, newOffset, nil
	default:
		return nil, offset, fmt.Errorf("invalid optional u64 flag: %d", flag)
	}
}

func readOptionalBytes(data []byte, offset int) ([]byte, int, error) {
	if offset >= len(data) {
		return nil, offset, errors.New("optional bytes flag missing")
	}

	flag := data[offset]
	offset++
	switch flag {
	case 0:
		return nil, offset, nil
	case 1:
		value, newOffset, err := connection.ReadBytes(data, offset)
		if err != nil {
			return nil, newOffset, err
		}
		return append([]byte(nil), value...), newOffset, nil
	default:
		return nil, offset, fmt.Errorf("invalid optional bytes flag: %d", flag)
	}
}

// initNotifyHandler registers the NOTIFY handler on first use.
func (c *client) initNotifyHandler() {
	c.notifyHandlerInitOnce.Do(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.conn.RegisterNotifyHandler(protocol.MessageTypeStreamNotify, c.handleNotify)
		c.notifyHandlerRegistered.Store(true)
	})
}

// handleNotify is called by the mux when a NOTIFY (609) frame arrives.
func (c *client) handleNotify(subID uint64, route string, payload []byte) {
	registrations := c.subscriptions.Registrations(subID)
	if len(registrations) == 0 {
		return
	}

	notif := CommitNotification{
		Route: route,
	}
	if len(payload) > 0 {
		var decoded struct {
			Event               string `json:"event"`
			FirstResourceOffset uint64 `json:"first_resource_offset"`
			LastResourceOffset  uint64 `json:"last_resource_offset"`
			FirstAreaOffset     uint64 `json:"first_area_offset"`
			LastAreaOffset      uint64 `json:"last_area_offset"`
			FirstRealmOffset    uint64 `json:"first_realm_offset"`
			LastRealmOffset     uint64 `json:"last_realm_offset"`
			BatchSize           uint64 `json:"batch_size"`
		}
		if err := json.Unmarshal(payload, &decoded); err == nil {
			notif.Event = decoded.Event
			notif.FirstResourceOffset = decoded.FirstResourceOffset
			notif.LastResourceOffset = decoded.LastResourceOffset
			notif.FirstAreaOffset = decoded.FirstAreaOffset
			notif.LastAreaOffset = decoded.LastAreaOffset
			notif.FirstRealmOffset = decoded.FirstRealmOffset
			notif.LastRealmOffset = decoded.LastRealmOffset
			notif.BatchSize = decoded.BatchSize
		}
	}
	lifecycleCtx := c.conn.LifecycleContext()

	for _, registration := range registrations {
		handler := registration.Handler
		if !c.conn.LaunchAsyncHandler(lifecycleCtx, "fitz.stream.handler", c.conn.AsyncHandlerTimeout(), func(handlerCtx context.Context, span trace.Span) {
			if err := handler(handlerCtx, notif); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				if log := c.conn.Logger(); log != nil {
					log.Warn("stream notify handler failed", "route", route, "error", err)
				}
			}
		}, trace.WithAttributes(
			attribute.Int64("fitz.subscription_id", int64(subID)),
			attribute.String("fitz.route", route),
		)) {
			registration.Completion.Complete(&coreerrors.AsyncHandlerOverflowError{Domain: "stream", SubscriptionID: subID})
			go c.unsubscribe(&Subscription{subID: subID, handlerID: registration.HandlerID, pattern: registration.Pattern, client: c, completion: registration.Completion})
			if log := c.conn.Logger(); log != nil {
				log.Warn("stream notify handler dropped", "route", route, "sub_id", subID, "reason", "async handler queue full")
			}
		}
	}
}

// Subscribe registers a handler for stream commit notifications.
// Pattern may be exact or use whole-segment wildcards (e.g., "stream://realm/area/*" or "stream://realm/**").
func (c *client) Subscribe(ctx context.Context, pattern string, handler CommitHandler) (*Subscription, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.stream.Subscribe", trace.WithAttributes(attribute.String("fitz.pattern", pattern)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.DebugContext(ctx, "stream.Subscribe", "pattern", pattern)
	}
	if err := types.ValidateStreamSelector(pattern); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("invalid route: %w", err)
	}
	c.initNotifyHandler()

	subID, handlerID, err := c.subscriptions.Subscribe(pattern, handler, func(pattern string) (uint64, error) {
		return c.subscribeWire(ctx, pattern)
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return &Subscription{
		subID:      subID,
		handlerID:  handlerID,
		pattern:    pattern,
		client:     c,
		completion: c.subscriptions.Completion(pattern, handlerID),
	}, nil
}

// unsubscribe removes a subscription.
func (c *client) unsubscribe(sub *Subscription) {
	if !c.subscriptions.Unsubscribe(sub.pattern, sub.handlerID) {
		return
	}
	c.conn.AddSubscriptions(-1)

	// Send UNSUBSCRIBE to server (best-effort, ignore errors).
	ctx := c.conn.LifecycleContext()
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeStreamUnsubscribe, unsubscribePayloadWriter(sub.pattern))
	if err != nil {
		return
	}
	if _, _, err := parsePlainStreamResponse(resp); err != nil {
		return
	}
}

func (c *client) ReplaceConnection(conn *connection.Connection) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()
	if c.notifyHandlerRegistered.Load() {
		c.conn.RegisterNotifyHandler(protocol.MessageTypeStreamNotify, c.handleNotify)
	}
}

func (c *client) RestoreSubscriptions(ctx context.Context) error {
	return c.subscriptions.Restore(
		func(pattern string) (uint64, error) {
			return c.subscribeWire(ctx, pattern)
		},
		func(pattern string, _ uint64) error {
			resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeStreamUnsubscribe, unsubscribePayloadWriter(pattern))
			if err != nil {
				return err
			}
			if _, _, err = parsePlainStreamResponse(resp); err != nil {
				return err
			}
			c.conn.AddSubscriptions(-1)
			return nil
		},
	)
}

func (c *client) subscribeWire(ctx context.Context, pattern string) (uint64, error) {
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeStreamSubscribe, subscribePayloadWriter(pattern))
	if err != nil {
		return 0, fmt.Errorf("subscribe request failed: %w", err)
	}

	success, remaining, err := parsePlainStreamResponse(resp)
	if err != nil {
		return 0, fmt.Errorf("subscribe failed: %w", mapStreamError(err))
	}
	if !success {
		return 0, errors.New("subscribe failed: unexpected status")
	}

	if len(remaining) < 1 {
		return 0, fmt.Errorf("subscribe response too short: got %d bytes", len(remaining))
	}
	if remaining[0] != 1 {
		return 0, errors.New("subscribe response missing subscription_id")
	}
	if len(remaining) < 9 {
		return 0, fmt.Errorf("subscribe response too short for subscription_id: got %d bytes", len(remaining))
	}

	subID, _, err := connection.ReadU64BE(remaining, 1)
	if err != nil {
		return 0, fmt.Errorf("parse subscription_id: %w", err)
	}
	c.conn.AddSubscriptions(1)
	return subID, nil
}
