//nolint:gosec
package stream

import (
	"errors"
	"io"
	"testing"

	"github.com/cntryl/fitz-go/internal/core/connection"
	coreerrors "github.com/cntryl/fitz-go/internal/core/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShouldMapStreamError tests error message mapping.
func TestShouldMapStreamErrorGivenBrokerMessageWhenMapStreamErrorCalled(t *testing.T) {
	t.Run("map stream not found error", func(t *testing.T) {
		// Arrange
		errMsg := coreerrors.NewDomainError(coreerrors.StreamResourceNotFound, "stream not found")

		// Act
		mapped := mapStreamError(errMsg)

		// Assert
		assert.Equal(t, ErrStreamNotFound, mapped)
	})

	t.Run("map stream not found case insensitive", func(t *testing.T) {
		// Arrange
		errMsg := coreerrors.NewDomainError(coreerrors.StreamResourceNotFound, "Stream NOT FOUND in realm")

		// Act
		mapped := mapStreamError(errMsg)

		// Assert
		assert.Equal(t, ErrStreamNotFound, mapped)
	})

	t.Run("map stream conflict error", func(t *testing.T) {
		// Arrange
		errMsg := coreerrors.NewDomainError(coreerrors.StreamConcurrencyConflict, "stream conflict detected")

		// Act
		mapped := mapStreamError(errMsg)

		// Assert
		assert.Equal(t, ErrStreamConflict, mapped)
	})

	t.Run("map stream conflict case insensitive", func(t *testing.T) {
		// Arrange
		errMsg := coreerrors.NewDomainError(coreerrors.StreamConcurrencyConflict, "CONFLICT writing to stream")

		// Act
		mapped := mapStreamError(errMsg)

		// Assert
		assert.Equal(t, ErrStreamConflict, mapped)
	})

	t.Run("unknown error returns wrapped message", func(t *testing.T) {
		// Arrange
		errMsg := errors.New("unexpected stream condition")

		// Act
		mapped := mapStreamError(errMsg)

		// Assert
		assert.Error(t, mapped)
		assert.Equal(t, errMsg, mapped)
	})

	t.Run("empty error message", func(t *testing.T) {
		// Arrange
		errMsg := errors.New("")

		// Act
		mapped := mapStreamError(errMsg)

		// Assert
		assert.Error(t, mapped)
	})

	t.Run("preserve typed stream limit error", func(t *testing.T) {
		// Arrange
		errMsg := coreerrors.NewDomainError(coreerrors.StreamSubscriptionLimit, "subscription limit reached")

		// Act
		mapped := mapStreamError(errMsg)

		// Assert
		var domainErr *coreerrors.DomainError
		assert.ErrorAs(t, mapped, &domainErr)
		assert.Equal(t, uint32(coreerrors.StreamSubscriptionLimit), uint32(domainErr.Code))
	})
}

// TestShouldDefineStreamOpcodes tests that Stream opcodes are properly defined.
func TestShouldDefineStreamOpcodesGivenConstantsWhenRead(t *testing.T) {
	t.Run("begin opcode", func(t *testing.T) {
		assert.Equal(t, uint16(600), StreamBegin)
	})

	t.Run("append opcode", func(t *testing.T) {
		assert.Equal(t, uint16(601), StreamAppend)
	})

	t.Run("commit opcode", func(t *testing.T) {
		assert.Equal(t, uint16(602), StreamCommit)
	})

	t.Run("rollback opcode", func(t *testing.T) {
		assert.Equal(t, uint16(603), StreamRollback)
	})

	t.Run("read opcode", func(t *testing.T) {
		assert.Equal(t, uint16(604), StreamRead)
	})

	t.Run("last opcode", func(t *testing.T) {
		assert.Equal(t, uint16(605), StreamLast)
	})

	t.Run("get metadata opcode", func(t *testing.T) {
		assert.Equal(t, uint16(606), StreamGetMetadata)
	})

	t.Run("subscribe opcode", func(t *testing.T) {
		assert.Equal(t, uint16(607), StreamSubscribe)
	})

	t.Run("unsubscribe opcode", func(t *testing.T) {
		assert.Equal(t, uint16(608), StreamUnsubscribe)
	})

	t.Run("notify opcode server only", func(t *testing.T) {
		assert.Equal(t, uint16(609), StreamNotify)
	})

	t.Run("opcodes are sequential", func(t *testing.T) {
		assert.Equal(t, StreamBegin+1, StreamAppend)
		assert.Equal(t, StreamAppend+1, StreamCommit)
		assert.Equal(t, StreamCommit+1, StreamRollback)
		assert.Equal(t, StreamRollback+1, StreamRead)
		assert.Equal(t, StreamRead+1, StreamLast)
		assert.Equal(t, StreamLast+1, StreamGetMetadata)
		assert.Equal(t, StreamGetMetadata+1, StreamSubscribe)
		assert.Equal(t, StreamSubscribe+1, StreamUnsubscribe)
		assert.Equal(t, StreamUnsubscribe+1, StreamNotify)
	})

	t.Run("all opcodes in 600 range", func(t *testing.T) {
		assert.GreaterOrEqual(t, StreamBegin, uint16(600))
		assert.LessOrEqual(t, StreamNotify, uint16(609))
	})
}

// TestShouldDefineStreamErrors tests that Stream error variables are defined.
func TestShouldDefineStreamErrorsGivenSentinelValuesWhenRead(t *testing.T) {
	t.Run("stream not found error", func(t *testing.T) {
		assert.Error(t, ErrStreamNotFound)
		assert.Equal(t, "stream not found", ErrStreamNotFound.Error())
	})

	t.Run("stream conflict error", func(t *testing.T) {
		assert.Error(t, ErrStreamConflict)
		assert.Equal(t, "stream conflict", ErrStreamConflict.Error())
	})

	t.Run("stream read error", func(t *testing.T) {
		assert.Error(t, ErrStreamReadError)
		assert.Equal(t, "stream read error", ErrStreamReadError.Error())
	})
}

// TestShouldDefineStreamTransactionModes tests transaction mode constants.
func TestShouldDefineStreamTransactionModesGivenConstantsWhenRead(t *testing.T) {
	t.Run("stream has begin operation", func(t *testing.T) {
		// Stream should support transaction-like operations
		assert.Positive(t, StreamBegin)
	})

	t.Run("stream has append operation", func(t *testing.T) {
		assert.Positive(t, StreamAppend)
	})

	t.Run("stream has commit operation", func(t *testing.T) {
		assert.Positive(t, StreamCommit)
	})

	t.Run("stream has rollback operation", func(t *testing.T) {
		assert.Positive(t, StreamRollback)
	})
}

// TestShouldEncodeStreamBegin tests STREAM BEGIN encoding.
func TestShouldEncodeStreamBeginGivenRouteAndOffsetWhenPayloadWritten(t *testing.T) {
	t.Run("without ingest metadata", func(t *testing.T) {
		// Arrange
		route := "stream://acme/logs/app"

		// Act
		payload, err := EncodeStreamBegin(route, nil)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		offset := 0
		routeLen, newOffset, err := connection.ReadU32BE(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, uint32(len(route)), routeLen)
		offset = newOffset
		assert.Equal(t, route, string(payload[offset:offset+int(routeLen)]))
		offset += int(routeLen)
		require.Less(t, offset, len(payload))
		assert.Equal(t, byte(0), payload[offset])
	})

	t.Run("with ingest metadata", func(t *testing.T) {
		// Arrange
		route := "stream://acme/logs/app"
		metadata := []byte("meta")

		// Act
		payload, err := EncodeStreamBegin(route, metadata)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		offset := 0
		routeLen, newOffset, err := connection.ReadU32BE(payload, offset)
		require.NoError(t, err)
		offset = newOffset + int(routeLen)
		require.Less(t, offset, len(payload))
		assert.Equal(t, byte(1), payload[offset])
		offset++
		meta, _, err := connection.ReadBytes(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, metadata, meta)
	})
}

// TestShouldEncodeStreamAppend tests STREAM APPEND encoding.
func TestShouldEncodeStreamAppendGivenSessionAndBodyWhenPayloadWritten(t *testing.T) {
	t.Run("without metadata", func(t *testing.T) {
		// Arrange
		sessionID := uint64(123)
		expectedOffset := uint64(456)
		body := []byte("payload")

		// Act
		payload, err := EncodeStreamAppend(sessionID, expectedOffset, body, nil)

		// Assert
		require.NoError(t, err)
		offset := 0
		actualSessionID, newOffset, err := connection.ReadU64BE(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, sessionID, actualSessionID)
		offset = newOffset
		actualExpectedOffset, newOffset, err := connection.ReadU64BE(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, expectedOffset, actualExpectedOffset)
		offset = newOffset
		actualBody, newOffset, err := connection.ReadBytes(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, body, actualBody)
		offset = newOffset
		require.Less(t, offset, len(payload))
		assert.Equal(t, byte(0), payload[offset])
	})

	t.Run("with metadata", func(t *testing.T) {
		// Arrange
		sessionID := uint64(456)
		expectedOffset := uint64(789)
		body := []byte("payload")
		metadata := []byte("meta")

		// Act
		payload, err := EncodeStreamAppend(sessionID, expectedOffset, body, metadata)

		// Assert
		require.NoError(t, err)
		offset := 0
		actualSessionID, newOffset, err := connection.ReadU64BE(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, sessionID, actualSessionID)
		offset = newOffset
		actualExpectedOffset, newOffset, err := connection.ReadU64BE(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, expectedOffset, actualExpectedOffset)
		offset = newOffset
		_, newOffset, err = connection.ReadBytes(payload, offset)
		require.NoError(t, err)
		offset = newOffset
		assert.Equal(t, byte(1), payload[offset])
		offset++
		actualMetadata, _, err := connection.ReadBytes(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, metadata, actualMetadata)
	})

	t.Run("with discriminator", func(t *testing.T) {
		// Arrange
		sessionID := uint64(789)
		expectedOffset := uint64(321)
		body := []byte("payload")
		discriminator := "proj.alpha"

		// Act
		payload, err := EncodeStreamAppend(sessionID, expectedOffset, body, nil, &StreamAppendOptions{Discriminator: &discriminator})

		// Assert
		require.NoError(t, err)
		offset := 0
		actualSessionID, newOffset, err := connection.ReadU64BE(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, sessionID, actualSessionID)
		offset = newOffset
		actualExpectedOffset, newOffset, err := connection.ReadU64BE(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, expectedOffset, actualExpectedOffset)
		offset = newOffset
		actualBody, newOffset, err := connection.ReadBytes(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, body, actualBody)
		offset = newOffset
		assert.Equal(t, byte(0), payload[offset])
		offset++
		assert.Equal(t, byte(1), payload[offset])
		offset++
		discriminatorLen, newOffset, err := connection.ReadU32BE(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, uint32(len(discriminator)), discriminatorLen)
		offset = newOffset
		assert.Equal(t, discriminator, string(payload[offset:offset+int(discriminatorLen)]))
	})
}

// TestShouldEncodeStreamCommit tests STREAM COMMIT encoding.
func TestShouldEncodeStreamCommitGivenSessionAndModeWhenPayloadWritten(t *testing.T) {
	t.Run("buffered mode", func(t *testing.T) {
		// Arrange
		sessionID := uint64(999)
		mode := CommitModeBuffered

		// Act
		payload, err := EncodeStreamCommit(sessionID, mode)

		// Assert
		require.NoError(t, err)
		actualSessionID, offset, err := connection.ReadU64BE(payload, 0)
		require.NoError(t, err)
		assert.Equal(t, sessionID, actualSessionID)
		require.Less(t, offset, len(payload))
		assert.Equal(t, byte(mode), payload[offset])
	})
}

// TestShouldEncodeStreamRollback tests STREAM ROLLBACK encoding.
func TestShouldEncodeStreamRollbackGivenSessionWhenPayloadWritten(t *testing.T) {
	t.Run("valid rollback", func(t *testing.T) {
		// Arrange
		sessionID := uint64(456)

		// Act
		payload, err := EncodeStreamRollback(sessionID)

		// Assert
		require.NoError(t, err)
		actualSessionID, _, err := connection.ReadU64BE(payload, 0)
		require.NoError(t, err)
		assert.Equal(t, sessionID, actualSessionID)
	})
}

// TestShouldEncodeStreamRead tests STREAM READ encoding.
func TestShouldEncodeStreamReadGivenBoundsWhenPayloadWritten(t *testing.T) {
	t.Run("without filter", func(t *testing.T) {
		// Arrange
		route := "stream://acme/logs/app"
		fromOffset := uint64(10)
		limit := uint64(100)

		// Act
		payload, err := EncodeStreamRead(route, fromOffset, limit, nil)

		// Assert
		require.NoError(t, err)
		offset := 0
		routeLen, newOffset, err := connection.ReadU32BE(payload, offset)
		require.NoError(t, err)
		offset = newOffset + int(routeLen)
		actualFrom, newOffset, err := connection.ReadU64BE(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, fromOffset, actualFrom)
		offset = newOffset
		actualLimit, newOffset, err := connection.ReadU64BE(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, limit, actualLimit)
		offset = newOffset
		assert.Equal(t, byte(0), payload[offset])
		offset++
		assert.Equal(t, byte(0), payload[offset])
	})

	t.Run("with filter", func(t *testing.T) {
		// Arrange
		route := "stream://acme/logs/app"
		fromOffset := uint64(1)
		limit := uint64(10)
		filter := &StreamFilterSet{Clauses: []StreamFilterClause{{Kind: StreamFilterEquals, Value: "proj.alpha"}}}

		// Act
		payload, err := EncodeStreamRead(route, fromOffset, limit, &StreamReadOptions{Filter: filter})

		// Assert
		require.NoError(t, err)
		offset := 0
		routeLen, newOffset, err := connection.ReadU32BE(payload, offset)
		require.NoError(t, err)
		offset = newOffset + int(routeLen)
		_, newOffset, err = connection.ReadU64BE(payload, offset)
		require.NoError(t, err)
		offset = newOffset
		_, newOffset, err = connection.ReadU64BE(payload, offset)
		require.NoError(t, err)
		offset = newOffset
		assert.Equal(t, byte(0), payload[offset])
		offset++
		assert.Equal(t, byte(1), payload[offset])
		offset++
		filterLength, newOffset, err := connection.ReadU32BE(payload, offset)
		require.NoError(t, err)
		assert.Positive(t, filterLength)
		offset = newOffset
		expectedFilter := []byte{
			0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			'p', 'r', 'o', 'j', '.', 'a', 'l', 'p', 'h', 'a',
		}
		assert.Equal(t, expectedFilter, payload[offset:offset+int(filterLength)])
	})
}

// TestShouldEncodeStreamLast tests STREAM LAST encoding.
func TestShouldEncodeStreamLastGivenRouteWhenPayloadWritten(t *testing.T) {
	t.Run("valid route", func(t *testing.T) {
		// Arrange
		route := "stream://acme/logs/app"

		// Act
		payload, err := EncodeStreamLast(route)

		// Assert
		require.NoError(t, err)
		routeLen, _, err := connection.ReadU32BE(payload, 0)
		require.NoError(t, err)
		assert.Equal(t, uint32(len(route)), routeLen)
	})
}

// TestShouldEncodeStreamGetMetadata tests STREAM GET_METADATA encoding.
func TestShouldEncodeStreamGetMetadataGivenRouteWhenPayloadWritten(t *testing.T) {
	t.Run("valid route", func(t *testing.T) {
		// Arrange
		route := "stream://acme/metadata"

		// Act
		payload, err := EncodeStreamGetMetadata(route)

		// Assert
		require.NoError(t, err)
		routeLen, _, err := connection.ReadU32BE(payload, 0)
		require.NoError(t, err)
		assert.Equal(t, uint32(len(route)), routeLen)
	})
}

// TestShouldEncodeStreamSubscribe tests STREAM SUBSCRIBE encoding.
func TestShouldEncodeStreamSubscribeGivenPatternWhenPayloadWritten(t *testing.T) {
	t.Run("valid route and offset", func(t *testing.T) {
		// Arrange
		route := "stream://acme/sub"
		fromOffset := uint64(99)

		// Act
		payload, err := EncodeStreamSubscribe(route, fromOffset)

		// Assert
		require.NoError(t, err)
		offset := 0
		routeLen, newOffset, err := connection.ReadU32BE(payload, offset)
		require.NoError(t, err)
		offset = newOffset + int(routeLen)
		actualOffset, _, err := connection.ReadU64BE(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, fromOffset, actualOffset)
	})
}

// TestShouldEncodeStreamUnsubscribe tests STREAM UNSUBSCRIBE encoding.
func TestShouldEncodeStreamUnsubscribeGivenPatternWhenPayloadWritten(t *testing.T) {
	t.Run("valid route", func(t *testing.T) {
		// Arrange
		route := "stream://acme/unsub"

		// Act
		payload, err := EncodeStreamUnsubscribe(route)

		// Assert
		require.NoError(t, err)
		routeLen, _, err := connection.ReadU32BE(payload, 0)
		require.NoError(t, err)
		assert.Equal(t, uint32(len(route)), routeLen)
	})
}

// TestShouldParseStreamRecord tests the stream record wire format parser.
func TestShouldParseStreamRecordGivenFullPayloadWhenParseRecordCalled(t *testing.T) {
	body := []byte("record-body")
	metadata := []byte("meta")
	areaOffset := uint64(11)
	realmOffset := uint64(17)
	timestamp := uint64(23)

	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	connection.WriteU64BE(buf, 7)
	buf.WriteByte(1)
	connection.WriteU64BE(buf, areaOffset)
	buf.WriteByte(1)
	connection.WriteU64BE(buf, realmOffset)
	connection.WriteBytes(buf, body)
	buf.WriteByte(1)
	connection.WriteBytes(buf, metadata)
	connection.WriteU64BE(buf, timestamp)

	payload := make([]byte, buf.Len())
	copy(payload, buf.Bytes())

	record, err := parseRecord(payload, 0)
	require.NoError(t, err)
	require.NotNil(t, record)
	assert.Equal(t, uint64(7), record.Offset)
	require.NotNil(t, record.AreaOffset)
	assert.Equal(t, areaOffset, *record.AreaOffset)
	require.NotNil(t, record.RealmOffset)
	assert.Equal(t, realmOffset, *record.RealmOffset)
	assert.Equal(t, body, record.Body)
	assert.Equal(t, metadata, record.Metadata)
	assert.Equal(t, timestamp, record.Timestamp)
}

// TestShouldParseStreamReadResponse tests the count-prefixed stream read envelope.
func TestShouldParseStreamReadResponseGivenTrailerWhenParseReadResponseCalled(t *testing.T) {
	body := []byte("record-body")
	metadata := []byte("meta")
	areaOffset := uint64(11)
	realmOffset := uint64(17)
	timestamp := uint64(23)

	recordBuf := connection.GetBuffer()
	defer connection.PutBuffer(recordBuf)
	connection.WriteU64BE(recordBuf, 7)
	recordBuf.WriteByte(1)
	connection.WriteU64BE(recordBuf, areaOffset)
	recordBuf.WriteByte(1)
	connection.WriteU64BE(recordBuf, realmOffset)
	connection.WriteBytes(recordBuf, body)
	recordBuf.WriteByte(1)
	connection.WriteBytes(recordBuf, metadata)
	connection.WriteU64BE(recordBuf, timestamp)

	dataBuf := connection.GetBuffer()
	defer connection.PutBuffer(dataBuf)
	connection.WriteU32BE(dataBuf, 1)
	dataBuf.WriteByte(0)
	dataBuf.Write(recordBuf.Bytes())
	connection.WriteU64BE(dataBuf, 99)
	dataBuf.WriteByte(1)
	connection.WriteU64BE(dataBuf, 101)
	dataBuf.WriteByte(0)
	dataBuf.WriteByte(1)

	payload := make([]byte, dataBuf.Len())
	copy(payload, dataBuf.Bytes())

	records, err := parseReadResponse(payload)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, uint64(7), records[0].Offset)
	assert.Equal(t, body, records[0].Body)
	require.NotNil(t, records[0].AreaOffset)
	assert.Equal(t, areaOffset, *records[0].AreaOffset)
	require.NotNil(t, records[0].RealmOffset)
	assert.Equal(t, realmOffset, *records[0].RealmOffset)
	assert.Equal(t, metadata, records[0].Metadata)
	assert.Equal(t, timestamp, records[0].Timestamp)
}

func TestShouldParseStreamReadPageGivenFilteredItemsWhenParseReadPageResponseCalled(t *testing.T) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteU32BE(buf, 3)
	buf.WriteByte(0)
	connection.WriteU64BE(buf, 41)
	buf.WriteByte(1)
	connection.WriteU64BE(buf, 51)
	buf.WriteByte(0)
	connection.WriteBytes(buf, []byte("alpha"))
	buf.WriteByte(0)
	connection.WriteU64BE(buf, 111)
	buf.WriteByte(1)
	connection.WriteU64BE(buf, 42)
	buf.WriteByte(1)
	buf.WriteByte(2)
	connection.WriteU64BE(buf, 43)
	connection.WriteU64BE(buf, 45)
	buf.WriteByte(2)
	connection.WriteU64BE(buf, 45)
	buf.WriteByte(1)
	connection.WriteU64BE(buf, 52)
	buf.WriteByte(0)
	buf.WriteByte(1)

	payload := make([]byte, buf.Len())
	copy(payload, buf.Bytes())

	page, err := parseReadPageResponse(payload)
	require.NoError(t, err)
	require.NotNil(t, page)
	assert.Equal(t, uint64(45), page.Cursor.LastResourceOffset)
	require.NotNil(t, page.Cursor.LastAreaOffset)
	assert.Equal(t, uint64(52), *page.Cursor.LastAreaOffset)
	assert.Nil(t, page.Cursor.LastRealmOffset)
	assert.True(t, page.Cursor.HasMore)
	require.Len(t, page.Items, 3)
	assert.Equal(t, ReadItemEvent, page.Items[0].Kind)
	require.NotNil(t, page.Items[0].Record)
	assert.Equal(t, uint64(41), page.Items[0].Record.Offset)
	assert.Equal(t, ReadItemFiltered, page.Items[1].Kind)
	assert.Equal(t, uint64(42), page.Items[1].Offset)
	require.NotNil(t, page.Items[1].Reason)
	assert.Equal(t, FilteredReasonServerFilter, *page.Items[1].Reason)
	assert.Equal(t, ReadItemFilteredRange, page.Items[2].Kind)
	assert.Equal(t, uint64(43), page.Items[2].FromOffset)
	assert.Equal(t, uint64(45), page.Items[2].ToOffset)
	require.NotNil(t, page.Items[2].Reason)
	assert.Equal(t, FilteredReasonPermission, *page.Items[2].Reason)
}

// TestShouldParseStreamMetadata tests the metadata payload parser.
func TestShouldParseStreamMetadataGivenFullPayloadWhenParseMetadataPayloadCalled(t *testing.T) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteU8(buf, 1)
	connection.WriteU64BE(buf, 3)
	connection.WriteU8(buf, 0)
	connection.WriteU64BE(buf, 5)
	connection.WriteU64BE(buf, 7)
	connection.WriteU64BE(buf, 9)
	connection.WriteU8(buf, 1)
	connection.WriteU64BE(buf, 11)
	connection.WriteU64BE(buf, 13)
	connection.WriteU64BE(buf, 17)

	payload := make([]byte, buf.Len())
	copy(payload, buf.Bytes())

	meta, err := parseMetadataPayload(payload)
	require.NoError(t, err)
	require.NotNil(t, meta)
	assert.Equal(t, uint64(3), meta.FirstOffset)
	assert.Equal(t, uint64(0), meta.LastOffset)
	assert.Equal(t, uint64(5), meta.RecordCount)
	assert.Equal(t, uint64(7), meta.MaxBatchEvents)
	assert.Equal(t, uint64(9), meta.MaxBatchBytes)
	require.NotNil(t, meta.TTLSeconds)
	assert.Equal(t, uint64(11), *meta.TTLSeconds)
	assert.Equal(t, uint64(13), meta.AreaWatermark)
	assert.Equal(t, uint64(17), meta.RealmWatermark)
}

func TestShouldRejectMalformedStreamEnvelopeWhenSkipOptionalSessionIDAndGetDataCalled(t *testing.T) {
	t.Run("incomplete optional session id", func(t *testing.T) {
		payload := []byte{1, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}

		data, err := skipOptionalSessionIDAndGetData(payload)

		require.Error(t, err)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		assert.Nil(t, data)
	})

	t.Run("truncated data blob", func(t *testing.T) {
		payload := []byte{0}
		payload = append(payload, []byte{0, 0, 0, 5, 0x01, 0x02}...)

		data, err := skipOptionalSessionIDAndGetData(payload)

		require.Error(t, err)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		assert.Nil(t, data)
	})
}

// Benchmarks

func BenchmarkEncodeStreamBegin(b *testing.B) {
	b.Run("without metadata", func(b *testing.B) {
		route := "stream://acme/logs/app"

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = EncodeStreamBegin(route, nil)
		}
	})

	b.Run("with metadata", func(b *testing.B) {
		route := "stream://acme/logs/app"
		metadata := []byte("meta")

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = EncodeStreamBegin(route, metadata)
		}
	})
}

func BenchmarkEncodeStreamAppend(b *testing.B) {
	b.Run("without metadata", func(b *testing.B) {
		sessionID := uint64(10)
		expectedOffset := uint64(11)
		body := []byte("payload")

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = EncodeStreamAppend(sessionID, expectedOffset, body, nil)
		}
	})

	b.Run("with metadata", func(b *testing.B) {
		sessionID := uint64(10)
		expectedOffset := uint64(11)
		body := []byte("payload")
		metadata := []byte("meta")

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = EncodeStreamAppend(sessionID, expectedOffset, body, metadata)
		}
	})
}

func BenchmarkEncodeStreamCommit(b *testing.B) {
	sessionID := uint64(10)
	mode := CommitModeBuffered

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = EncodeStreamCommit(sessionID, mode)
	}
}

func BenchmarkEncodeStreamRollback(b *testing.B) {
	sessionID := uint64(10)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = EncodeStreamRollback(sessionID)
	}
}

func BenchmarkEncodeStreamRead(b *testing.B) {
	b.Run("without filter", func(b *testing.B) {
		route := "stream://acme/logs/app"
		fromOffset := uint64(1)
		limit := uint64(10)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = EncodeStreamRead(route, fromOffset, limit, nil)
		}
	})

	b.Run("with filter", func(b *testing.B) {
		route := "stream://acme/logs/app"
		fromOffset := uint64(1)
		limit := uint64(10)
		filter := &StreamFilterSet{Clauses: []StreamFilterClause{{Kind: StreamFilterEquals, Value: "proj.alpha"}}}

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = EncodeStreamRead(route, fromOffset, limit, &StreamReadOptions{Filter: filter})
		}
	})
}

func BenchmarkEncodeStreamLast(b *testing.B) {
	route := "stream://acme/logs/app"

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = EncodeStreamLast(route)
	}
}

func BenchmarkEncodeStreamGetMetadata(b *testing.B) {
	route := "stream://acme/logs/app"

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = EncodeStreamGetMetadata(route)
	}
}

func BenchmarkEncodeStreamSubscribe(b *testing.B) {
	route := "stream://acme/logs/app"
	fromOffset := uint64(1)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = EncodeStreamSubscribe(route, fromOffset)
	}
}

func BenchmarkEncodeStreamUnsubscribe(b *testing.B) {
	route := "stream://acme/logs/app"

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = EncodeStreamUnsubscribe(route)
	}
}

func BenchmarkParseStreamReadResponse(b *testing.B) {
	// data blob: [u32 count=1][tag=event][record][u64 last_resource_offset][opt area][opt realm][u8 has_more]
	recordBuf := connection.GetBuffer()
	defer connection.PutBuffer(recordBuf)
	connection.WriteU64BE(recordBuf, 1)
	recordBuf.WriteByte(1)
	connection.WriteU64BE(recordBuf, 2)
	recordBuf.WriteByte(1)
	connection.WriteU64BE(recordBuf, 3)
	connection.WriteBytes(recordBuf, []byte("record1"))
	recordBuf.WriteByte(0)
	connection.WriteU64BE(recordBuf, 4)

	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	connection.WriteU32BE(buf, 1)
	buf.WriteByte(0)
	buf.Write(recordBuf.Bytes())
	connection.WriteU64BE(buf, 9)
	buf.WriteByte(0)
	buf.WriteByte(0)
	buf.WriteByte(1)
	payload := make([]byte, buf.Len())
	copy(payload, buf.Bytes())
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = parseReadResponse(payload)
	}
}

func BenchmarkParseStreamLastResponse(b *testing.B) {
	// single record: [u64 offset][opt area][opt realm][bytes body][opt metadata][u64 timestamp]
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	connection.WriteU64BE(buf, 1)
	buf.WriteByte(0)
	buf.WriteByte(0)
	connection.WriteBytes(buf, []byte("last"))
	buf.WriteByte(0)
	connection.WriteU64BE(buf, 5)
	payload := make([]byte, buf.Len())
	copy(payload, buf.Bytes())
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = parseRecord(payload, 0)
	}
}
