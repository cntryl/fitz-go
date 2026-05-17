//nolint:gosec
package stream

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"

	"github.com/cntryl/fitz-go/internal/core/encoding"
	coreerrors "github.com/cntryl/fitz-go/internal/core/errors"
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
	MaxBytes *uint64
	Filter   *StreamFilterSet
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
			return ErrStreamConflict
		case coreerrors.StreamResourceNotFound:
			return ErrStreamNotFound
		case coreerrors.StreamOffsetTooFarAhead, coreerrors.StreamInvalidReadBound, coreerrors.StreamReadBeyondWatermark:
			return ErrStreamReadError
		default:
			return err
		}
	}

	l := strings.ToLower(err.Error())
	switch {
	case strings.Contains(l, "not found"):
		return ErrStreamNotFound
	case strings.Contains(l, "conflict"):
		return ErrStreamConflict
	default:
		return err
	}
}

// EncodeStreamBegin encodes a STREAM BEGIN request per CLIENT_SPEC.md.
// Wire format: [string route][u8 has_ingest_metadata][bytes? ingest_metadata]
// ingestMetadata is optional; pass nil to omit.
func EncodeStreamBegin(route string, ingestMetadata []byte) ([]byte, error) {
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

// EncodeStreamAppend encodes a STREAM APPEND request per CLIENT_SPEC.md.
// Wire format: [u64 session_id][u64 expected_offset][bytes body][u8 has_metadata][bytes? metadata][u8 has_discriminator][string? discriminator]
// metadata and discriminator are optional; pass nil to omit.
func EncodeStreamAppend(sessionID uint64, expectedOffset uint64, body []byte, metadata []byte, opts ...*StreamAppendOptions) ([]byte, error) {
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

// EncodeStreamCommit encodes a STREAM COMMIT request per CLIENT_SPEC.md.
// Wire format: [u64 session_id][u8 mode]
// mode: 0=Buffered, 1=Sync
func EncodeStreamCommit(sessionID uint64, mode CommitMode) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, sessionID)
		buf.WriteByte(byte(mode))
	}), nil
}

// EncodeStreamRollback encodes a STREAM ROLLBACK request per CLIENT_SPEC.md.
// Wire format: [u64 session_id]
func EncodeStreamRollback(sessionID uint64) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, sessionID)
	}), nil
}

// EncodeStreamRead encodes a STREAM READ request per CLIENT_SPEC.md.
// Wire format: [string route][u64 from_offset][u64 limit][u8 has_max_bytes][u64? max_bytes][u8 has_filter][u32 filter_len?][bincode? filter]
// filter is optional; pass nil to omit.
func EncodeStreamRead(route string, fromOffset uint64, limit uint64, opts *StreamReadOptions) ([]byte, error) {
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
	}), nil
}

// EncodeStreamLast encodes a STREAM LAST request per CLIENT_SPEC.md.
// Wire format: [string route]
func EncodeStreamLast(route string) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
	}), nil
}

// EncodeStreamGetMetadata encodes a STREAM GET_METADATA request per CLIENT_SPEC.md.
// Wire format: [string route]
func EncodeStreamGetMetadata(route string) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
	}), nil
}

// EncodeStreamSubscribe encodes a STREAM SUBSCRIBE request per CLIENT_SPEC.md.
// Wire format: [string route][u64 from_offset]
func EncodeStreamSubscribe(route string, fromOffset uint64) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteU64(buf, fromOffset)
	}), nil
}

// EncodeStreamUnsubscribe encodes a STREAM UNSUBSCRIBE request per CLIENT_SPEC.md.
// Wire format: [string route]
func EncodeStreamUnsubscribe(route string) ([]byte, error) {
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
	writeU64LE(buf, uint64(len(filter.Clauses)))
	for _, clause := range filter.Clauses {
		encodeStreamFilterClause(buf, clause)
	}

	return append([]byte(nil), buf.Bytes()...)
}

func encodeStreamFilterClause(buf *bytes.Buffer, clause StreamFilterClause) {
	switch clause.Kind {
	case StreamFilterEquals:
		writeU32LE(buf, 0)
		writeLEString(buf, clause.Value)
	case StreamFilterNotEquals:
		writeU32LE(buf, 1)
		writeLEString(buf, clause.Value)
	case StreamFilterStartsWith:
		writeU32LE(buf, 2)
		writeLEString(buf, clause.Value)
	case StreamFilterAnyOf:
		writeU32LE(buf, 3)
		writeU64LE(buf, uint64(len(clause.Values)))
		for _, value := range clause.Values {
			writeLEString(buf, value)
		}
	}
}

func writeU32LE(buf *bytes.Buffer, value uint32) {
	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[:], value)
	buf.Write(scratch[:])
}

func writeU64LE(buf *bytes.Buffer, value uint64) {
	var scratch [8]byte
	binary.LittleEndian.PutUint64(scratch[:], value)
	buf.Write(scratch[:])
}

func writeLEString(buf *bytes.Buffer, value string) {
	bytesValue := []byte(value)
	writeU64LE(buf, uint64(len(bytesValue)))
	buf.Write(bytesValue)
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
