package stream

import (
	"bytes"
	"errors"

	"github.com/cntryl/fitz-go/v2/internal/core/encoding"
	coreerrors "github.com/cntryl/fitz-go/v2/internal/core/errors"
)

// Wire opcodes for Stream domain (per CLIENT_SPEC.md). Values are message type identifiers.
const (
	StreamBegin       uint16 = 600
	StreamAppend      uint16 = 601
	StreamCommit      uint16 = 602
	StreamRollback    uint16 = 603
	StreamRead        uint16 = 604
	StreamLast        uint16 = 605
	StreamGetMetadata uint16 = 606
	StreamSubscribe   uint16 = 607
	StreamUnsubscribe uint16 = 608
	StreamNotify      uint16 = 609 // Server -> Client only
)

type CommitMode uint8

const (
	CommitModeBuffered CommitMode = 0
	CommitModeSync     CommitMode = 1
)

type StreamFilterClauseKind string

const (
	StreamFilterEquals     StreamFilterClauseKind = "Equals"
	StreamFilterNotEquals  StreamFilterClauseKind = "NotEquals"
	StreamFilterStartsWith StreamFilterClauseKind = "StartsWith"
	StreamFilterAnyOf      StreamFilterClauseKind = "AnyOf"
)

type StreamFilterClause struct {
	Kind   StreamFilterClauseKind
	Value  string
	Values []string
}

type StreamFilterSet struct {
	Clauses []StreamFilterClause
}

type StreamAppendOptions struct {
	Discriminator *string
}

type StreamReadOptions struct {
	MaxBytes          *uint64
	Filter            *StreamFilterSet
	CursorFingerprint *uint64
	CapturedWatermark *uint64
}

// Domain-specific errors. Returned when the server rejects a stream operation.
//   - ErrStreamNotFound: the stream route does not exist.
//   - ErrStreamConflict: Begin failed due to expected-offset mismatch (OCC).
//   - ErrStreamReadError: read failed (e.g. offset beyond watermark).
var (
	ErrStreamNotFound  = errors.New("stream not found")
	ErrStreamConflict  = errors.New("stream conflict")
	ErrStreamReadError = errors.New("stream read error")
)

// mapStreamError maps a broker error to a domain-specific Go error.
func mapStreamError(err error) error {
	if err == nil {
		return nil
	}

	var domainErr *coreerrors.DomainError
	if errors.As(err, &domainErr) {
		switch uint32(domainErr.Code) {
		case coreerrors.StreamConcurrencyConflict:
			return errors.Join(ErrStreamConflict, err)
		case coreerrors.StreamResourceNotFound:
			return errors.Join(ErrStreamNotFound, err)
		case coreerrors.StreamOffsetTooFarAhead, coreerrors.StreamInvalidReadBound, coreerrors.StreamReadBeyondWatermark:
			return errors.Join(ErrStreamReadError, err)
		default:
			return err
		}
	}

	// Legacy errors have no structured classification; preserve the original cause.
	return err
}

// encodeStreamBegin encodes a STREAM BEGIN request per CLIENT_SPEC.md.
// Wire format: [string route][u8 has_ingest_metadata][bytes? ingest_metadata]
// ingestMetadata is optional; pass nil to omit.
func encodeStreamBegin(route string, ingestMetadata []byte) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		if ingestMetadata != nil {
			buf.WriteByte(1)
			encoding.WriteBytes(buf, ingestMetadata)
		} else {
			buf.WriteByte(0)
		}
	}), nil
}

// encodeStreamAppend encodes a STREAM APPEND request per CLIENT_SPEC.md.
// Wire format: [u64 session_id][u64 expected_offset][bytes body][u8 has_metadata][bytes? metadata][u8 has_discriminator][string? discriminator]
// metadata and discriminator are optional; pass nil to omit.
func encodeStreamAppend(sessionID uint64, expectedOffset uint64, body []byte, metadata []byte, opts ...*StreamAppendOptions) ([]byte, error) {
	var discriminator *string
	if len(opts) > 0 && opts[0] != nil {
		discriminator = opts[0].Discriminator
	}

	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, sessionID)
		encoding.WriteU64(buf, expectedOffset)
		encoding.WriteBytes(buf, body)
		if metadata != nil {
			buf.WriteByte(1)
			encoding.WriteBytes(buf, metadata)
		} else {
			buf.WriteByte(0)
		}
		if discriminator != nil && *discriminator != "" {
			buf.WriteByte(1)
			encoding.WriteString(buf, *discriminator)
		} else {
			buf.WriteByte(0)
		}
	}), nil
}

// encodeStreamCommit encodes a STREAM COMMIT request per CLIENT_SPEC.md.
// Wire format: [u64 session_id][u8 mode]
// mode: 0=Buffered, 1=Sync
func encodeStreamCommit(sessionID uint64, mode CommitMode) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, sessionID)
		buf.WriteByte(byte(mode))
	}), nil
}

// encodeStreamRollback encodes a STREAM ROLLBACK request per CLIENT_SPEC.md.
// Wire format: [u64 session_id]
func encodeStreamRollback(sessionID uint64) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, sessionID)
	}), nil
}

// encodeStreamRead encodes a STREAM READ request per CLIENT_SPEC.md.
// Wire format: [string route][u64 from_offset][u64 limit][u8 has_max_bytes][u64? max_bytes][u8 has_filter][u32 filter_len?][bincode? filter]
// filter is optional; pass nil to omit.
func encodeStreamRead(route string, fromOffset uint64, limit uint64, opts *StreamReadOptions) ([]byte, error) {
	var filterBytes []byte
	if opts != nil && opts.Filter != nil && len(opts.Filter.Clauses) > 0 {
		filterBytes = encodeStreamFilterSet(opts.Filter)
	}

	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteU64(buf, fromOffset)
		encoding.WriteU64(buf, limit)
		if opts != nil && opts.MaxBytes != nil {
			buf.WriteByte(1)
			encoding.WriteU64(buf, *opts.MaxBytes)
		} else {
			buf.WriteByte(0)
		}
		if filterBytes != nil {
			buf.WriteByte(1)
			encoding.WriteU32(buf, uint32(len(filterBytes)))
			encoding.WriteBytesRaw(buf, filterBytes)
		} else {
			buf.WriteByte(0)
		}
		if opts != nil && opts.CursorFingerprint != nil {
			encoding.WriteOptionalU64(buf, opts.CursorFingerprint)
		} else {
			encoding.WriteOptionalU64(buf, nil)
		}
		if opts != nil && opts.CapturedWatermark != nil {
			encoding.WriteOptionalU64(buf, opts.CapturedWatermark)
		} else {
			encoding.WriteOptionalU64(buf, nil)
		}
	}), nil
}

// encodeStreamLast encodes a STREAM LAST request per CLIENT_SPEC.md.
// Wire format: [string route]
func encodeStreamLast(route string) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
	}), nil
}

// encodeStreamGetMetadata encodes a STREAM GET_METADATA request per CLIENT_SPEC.md.
// Wire format: [string route]
func encodeStreamGetMetadata(route string) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
	}), nil
}

// encodeStreamSubscribe encodes a STREAM SUBSCRIBE request per CLIENT_SPEC.md.
// Wire format: [string route][u64 from_offset]
func encodeStreamSubscribe(route string, fromOffset uint64) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteU64(buf, fromOffset)
	}), nil
}

// encodeStreamUnsubscribe encodes a STREAM UNSUBSCRIBE request per CLIENT_SPEC.md.
// Wire format: [string route]
func encodeStreamUnsubscribe(route string) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
	}), nil
}

// Payload writer helpers for zero-copy frame encoding

func streamBeginPayloadWriter(route string, ingestMetadata []byte) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		if ingestMetadata != nil {
			buf.WriteByte(1)
			encoding.WriteBytes(buf, ingestMetadata)
		} else {
			buf.WriteByte(0)
		}
	}
}

func streamAppendPayloadWriter(sessionID uint64, expectedOffset uint64, body []byte, metadata []byte, opts ...*StreamAppendOptions) func(*bytes.Buffer) {
	var discriminator *string
	if len(opts) > 0 && opts[0] != nil {
		discriminator = opts[0].Discriminator
	}

	return func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, sessionID)
		encoding.WriteU64(buf, expectedOffset)
		encoding.WriteBytes(buf, body)
		if metadata != nil {
			buf.WriteByte(1)
			encoding.WriteBytes(buf, metadata)
		} else {
			buf.WriteByte(0)
		}
		if discriminator != nil && *discriminator != "" {
			buf.WriteByte(1)
			encoding.WriteString(buf, *discriminator)
		} else {
			buf.WriteByte(0)
		}
	}
}

func streamCommitPayloadWriter(sessionID uint64, mode CommitMode) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, sessionID)
		buf.WriteByte(byte(mode))
	}
}

func streamRollbackPayloadWriter(sessionID uint64) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, sessionID)
	}
}

func streamReadPayloadWriter(route string, fromOffset uint64, limit uint64, opts *StreamReadOptions) func(*bytes.Buffer) {
	var filterBytes []byte
	if opts != nil && opts.Filter != nil && len(opts.Filter.Clauses) > 0 {
		filterBytes = encodeStreamFilterSet(opts.Filter)
	}

	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteU64(buf, fromOffset)
		encoding.WriteU64(buf, limit)
		if opts != nil && opts.MaxBytes != nil {
			buf.WriteByte(1)
			encoding.WriteU64(buf, *opts.MaxBytes)
		} else {
			buf.WriteByte(0)
		}
		if filterBytes != nil {
			buf.WriteByte(1)
			encoding.WriteU32(buf, uint32(len(filterBytes)))
			encoding.WriteBytesRaw(buf, filterBytes)
		} else {
			buf.WriteByte(0)
		}
		if opts != nil && opts.CursorFingerprint != nil {
			encoding.WriteOptionalU64(buf, opts.CursorFingerprint)
		} else {
			encoding.WriteOptionalU64(buf, nil)
		}
		if opts != nil && opts.CapturedWatermark != nil {
			encoding.WriteOptionalU64(buf, opts.CapturedWatermark)
		} else {
			encoding.WriteOptionalU64(buf, nil)
		}
	}
}

func streamLastPayloadWriter(route string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
	}
}

func streamGetMetadataPayloadWriter(route string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
	}
}

func encodeStreamFilterSet(filter *StreamFilterSet) []byte {
	if filter == nil || len(filter.Clauses) == 0 {
		return nil
	}

	buf := &bytes.Buffer{}
	buf.WriteByte(0)
	buf.WriteByte(0xf1)
	encoding.WriteU32(buf, uint32(len(filter.Clauses)))
	for _, clause := range filter.Clauses {
		encodeStreamFilterClause(buf, clause)
	}

	return append([]byte(nil), buf.Bytes()...)
}

func encodeStreamFilterClause(buf *bytes.Buffer, clause StreamFilterClause) {
	switch clause.Kind {
	case StreamFilterEquals:
		buf.WriteByte(0)
		encoding.WriteString(buf, clause.Value)
	case StreamFilterNotEquals:
		buf.WriteByte(1)
		encoding.WriteString(buf, clause.Value)
	case StreamFilterStartsWith:
		buf.WriteByte(2)
		encoding.WriteString(buf, clause.Value)
	case StreamFilterAnyOf:
		buf.WriteByte(3)
		encoding.WriteU32(buf, uint32(len(clause.Values)))
		for _, value := range clause.Values {
			encoding.WriteString(buf, value)
		}
	}
}

// Subscription payload writers for SUBSCRIBE/UNSUBSCRIBE

func subscribePayloadWriter(pattern string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, pattern)
	}
}

func unsubscribePayloadWriter(pattern string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, pattern)
	}
}
