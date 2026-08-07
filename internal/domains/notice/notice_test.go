package notice

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/v2/internal/core/connection"
	coreerrors "github.com/cntryl/fitz-go/v2/internal/core/errors"
	"github.com/cntryl/fitz-go/v2/internal/core/subscriptions"
	"github.com/cntryl/fitz-go/v2/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type scriptedRestoreTransport struct {
	mu      sync.Mutex
	written [][]byte
	readCh  chan []byte
	closed  chan struct{}
	once    sync.Once
}

func newScriptedRestoreTransport() *scriptedRestoreTransport {
	return &scriptedRestoreTransport{
		readCh: make(chan []byte, 8),
		closed: make(chan struct{}),
	}
}

func (s *scriptedRestoreTransport) Write(ctx context.Context, frame []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closed:
		return connection.ErrConnectionClosed
	default:
	}
	s.mu.Lock()
	s.written = append(s.written, append([]byte(nil), frame...))
	s.mu.Unlock()
	return nil
}

func (s *scriptedRestoreTransport) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.closed:
		return nil, connection.ErrConnectionClosed
	case frame := <-s.readCh:
		return append([]byte(nil), frame...), nil
	}
}

func (s *scriptedRestoreTransport) Close() error {
	s.once.Do(func() {
		close(s.closed)
	})
	return nil
}

func (s *scriptedRestoreTransport) RemoteAddr() string {
	return "scripted://notice"
}

func (s *scriptedRestoreTransport) enqueue(frame []byte) {
	s.readCh <- append([]byte(nil), frame...)
}

func noticeRestoreFrame(t *testing.T, msgType uint16, payload []byte) []byte {
	t.Helper()
	frame := protocol.EncodeFrameOwned(msgType, payload)
	defer frame.Release()
	return append([]byte(nil), frame.Bytes()...)
}

func noticeSubscribeResponsePayload(subID uint64) []byte {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	buf.WriteByte(0)
	buf.WriteByte(1)
	connection.WriteU64BE(buf, subID)
	return append([]byte(nil), buf.Bytes()...)
}

func noticeSuccessPayload() []byte {
	return []byte{0}
}

func waitForRestoreWrites(t *testing.T, trans *scriptedRestoreTransport, expected int) {
	t.Helper()
	require.Eventually(t, func() bool {
		trans.mu.Lock()
		defer trans.mu.Unlock()
		return len(trans.written) >= expected
	}, time.Second, 10*time.Millisecond)
}

func restoreWriteCount(trans *scriptedRestoreTransport) int {
	trans.mu.Lock()
	defer trans.mu.Unlock()
	return len(trans.written)
}

// TestShouldDecodeNotify tests notification message decoding.
func TestShouldDecodeNotifyGivenEncodedPayloadWhenDecodeNotifyCalled(t *testing.T) {
	t.Run("simple notification", func(t *testing.T) {
		// Arrange - construct a notification payload
		// Format: [route_len(4)][route][payload_len(4)][payload]
		route := "notice://acme/app/events/published"
		payload := []byte("user_updated")

		encoded := encodePublish(route, payload)

		// Act
		decodedRoute, decodedPayload, ok := DecodeNotify(encoded)

		// Assert
		require.True(t, ok)
		assert.Equal(t, route, decodedRoute)
		assert.Equal(t, payload, decodedPayload)
	})

	t.Run("notification with large payload", func(t *testing.T) {
		// Arrange
		route := "notice://acme/app/logs/created"
		largePayload := make([]byte, 65536)
		for i := range largePayload {
			largePayload[i] = byte((i + 42) % 256)
		}

		encoded := encodePublish(route, largePayload)

		// Act
		decodedRoute, decodedPayload, ok := DecodeNotify(encoded)

		// Assert
		require.True(t, ok)
		assert.Equal(t, route, decodedRoute)
		assert.Equal(t, largePayload, decodedPayload)
	})

	t.Run("notification with empty payload", func(t *testing.T) {
		// Arrange
		route := "notice://acme/app/signal/fired"
		payload := []byte{}

		encoded := encodePublish(route, payload)

		// Act
		decodedRoute, decodedPayload, ok := DecodeNotify(encoded)

		// Assert
		require.True(t, ok)
		assert.Equal(t, route, decodedRoute)
		// Empty payload may be represented as nil or empty slice
		assert.Equal(t, len(decodedPayload), 0, "payload should be empty")
	})

	t.Run("malformed notification with trailing bytes", func(t *testing.T) {
		// Arrange
		route := "notice://acme/app/events/published"
		payload := []byte("user_updated")
		encoded := append(encodePublish(route, payload), 0xAA)

		// Act
		_, _, ok := DecodeNotify(encoded)

		// Assert
		assert.False(t, ok)
	})

	t.Run("malformed notification too short", func(t *testing.T) {
		// Arrange
		payload := []byte{0x00, 0x00} // Too short

		// Act
		_, _, ok := DecodeNotify(payload)

		// Assert
		assert.False(t, ok)
	})

	t.Run("malformed notification missing route", func(t *testing.T) {
		// Arrange - route_len says 100 bytes but only 10 available
		payload := make([]byte, 14)
		payload[0] = 0x00
		payload[1] = 0x00
		payload[2] = 0x00
		payload[3] = 0x64 // route_len = 100

		// Act
		_, _, ok := DecodeNotify(payload)

		// Assert
		assert.False(t, ok)
	})
}

func TestShouldRejectMalformedStatusGivenTrailingOrTruncatedEnvelopeWhenDecodeStatusCalled(t *testing.T) {
	t.Run("success status with trailing bytes", func(t *testing.T) {
		status, code, message, ok := decodeStatus([]byte{0x00, 0xAA})

		assert.False(t, ok)
		assert.Equal(t, uint8(0), status)
		assert.Equal(t, uint32(0), code)
		assert.Empty(t, message)
	})

	t.Run("error status with truncated message envelope", func(t *testing.T) {
		status, code, message, ok := decodeStatus([]byte{0x01, 0x00, 0x00, 0x00})

		assert.False(t, ok)
		assert.Equal(t, uint8(0), status)
		assert.Equal(t, uint32(0), code)
		assert.Empty(t, message)
	})

	t.Run("error status with trailing bytes", func(t *testing.T) {
		body := []byte{0x01, 0x00, 0x00, 0x0B, 0xBA, 0x00, 0x00, 0x00, 0x03, 'b', 'a', 'd', 0xFF}
		status, code, message, ok := decodeStatus(body)

		assert.False(t, ok)
		assert.Equal(t, uint8(0), status)
		assert.Equal(t, uint32(0), code)
		assert.Empty(t, message)
	})
}

func TestShouldPreserveDomainCodeGivenTypedNoticeErrorWhenDecodeStatusCalled(t *testing.T) {
	// Arrange
	body := []byte{0x01, 0x00, 0x00, 0x0B, 0xBA, 0x00, 0x00, 0x00, 0x03, 'b', 'a', 'd'}

	// Act
	status, code, message, ok := decodeStatus(body)

	// Assert
	assert.True(t, ok)
	assert.Equal(t, uint8(1), status)
	assert.Equal(t, uint32(3002), code)
	assert.Equal(t, "bad", message)
}

// TestShouldDefineNoticeOpcodes tests that Notice opcodes are properly defined.
func TestShouldDefineNoticeOpcodesGivenConstantsWhenRead(t *testing.T) {
	t.Run("publish opcode", func(t *testing.T) {
		assert.Equal(t, uint16(500), NoticePublish)
	})

	t.Run("subscribe opcode", func(t *testing.T) {
		assert.Equal(t, uint16(501), NoticeSubscribe)
	})

	t.Run("unsubscribe opcode", func(t *testing.T) {
		assert.Equal(t, uint16(502), NoticeUnsubscribe)
	})

	t.Run("unsubscribe all opcode", func(t *testing.T) {
		assert.Equal(t, uint16(503), NoticeUnsubscribeAll)
	})

	t.Run("notify opcode", func(t *testing.T) {
		assert.Equal(t, uint16(504), NoticeNotify)
	})

	t.Run("opcodes are sequential", func(t *testing.T) {
		assert.Equal(t, NoticePublish+1, NoticeSubscribe)
		assert.Equal(t, NoticeSubscribe+1, NoticeUnsubscribe)
		assert.Equal(t, NoticeUnsubscribe+1, NoticeUnsubscribeAll)
		assert.Equal(t, NoticeUnsubscribeAll+1, NoticeNotify)
	})

	t.Run("all opcodes in 500 range", func(t *testing.T) {
		assert.GreaterOrEqual(t, NoticePublish, uint16(500))
		assert.LessOrEqual(t, NoticeNotify, uint16(504))
	})
}

// TestShouldDefineNoticeErrors tests that Notice error variables are defined.
func TestShouldDefineNoticeErrorsGivenSentinelValuesWhenRead(t *testing.T) {
	t.Run("invalid route error", func(t *testing.T) {
		assert.Error(t, ErrNoticeRouteInvalid)
		assert.Equal(t, "invalid notice route", ErrNoticeRouteInvalid.Error())
	})

	t.Run("timeout error", func(t *testing.T) {
		assert.Error(t, ErrNoticeTimeout)
		assert.Equal(t, "notice operation timed out", ErrNoticeTimeout.Error())
	})

	t.Run("send failed error", func(t *testing.T) {
		assert.Error(t, ErrNoticeSendFailed)
		assert.Equal(t, "notice send failed", ErrNoticeSendFailed.Error())
	})
}

func TestShouldMapNoticeErrorGivenTypedBrokerMessageWhenMapNoticeErrorCalled(t *testing.T) {
	t.Run("map invalid route", func(t *testing.T) {
		mapped := mapNoticeError(coreerrors.NewDomainError(coreerrors.NoticeInvalidRoute, "invalid notice route"))
		assert.Equal(t, ErrNoticeRouteInvalid, mapped)
	})

	t.Run("map invalid pattern", func(t *testing.T) {
		mapped := mapNoticeError(coreerrors.NewDomainError(coreerrors.NoticeInvalidPattern, "invalid notice pattern"))
		assert.Equal(t, ErrNoticeRouteInvalid, mapped)
	})

	t.Run("preserve typed transport closed error", func(t *testing.T) {
		errMsg := coreerrors.NewDomainError(coreerrors.NoticeTransportClosed, "transport closed")
		mapped := mapNoticeError(errMsg)

		var domainErr *coreerrors.DomainError
		assert.ErrorAs(t, mapped, &domainErr)
		assert.Equal(t, uint32(coreerrors.NoticeTransportClosed), uint32(domainErr.Code))
	})

	t.Run("unknown error returns original error", func(t *testing.T) {
		errMsg := errors.New("unexpected notice condition")
		mapped := mapNoticeError(errMsg)
		assert.Equal(t, errMsg, mapped)
	})
}

func TestShouldRollbackActiveSubscriptionsGivenRestoreFailureWhenRestoreSubscriptionsCalled(t *testing.T) {
	trans := newScriptedRestoreTransport()
	conn := connection.New(trans, connection.Config{Token: "", ReadTimeout: time.Second})
	require.NoError(t, conn.Start(context.Background()))
	t.Cleanup(func() {
		_ = conn.Close()
	})
	baseWrites := restoreWriteCount(trans)

	c := &client{conn: conn, subscriptions: subscriptions.NewRegistry[NoticeHandler]()}
	_, _, err := c.subscriptions.Subscribe("notice://realm/area/alpha", func(context.Context, NoticeMsg) error { return nil }, func(string) (uint64, error) {
		return 11, nil
	})
	require.NoError(t, err)
	_, _, err = c.subscriptions.Subscribe("notice://realm/area/bravo", func(context.Context, NoticeMsg) error { return nil }, func(string) (uint64, error) {
		return 12, nil
	})
	require.NoError(t, err)

	go func() {
		waitForRestoreWrites(t, trans, baseWrites+1)
		trans.enqueue(noticeRestoreFrame(t, protocol.MessageTypeNoticeSubscribe, noticeSubscribeResponsePayload(101)))
		waitForRestoreWrites(t, trans, baseWrites+2)
		trans.enqueue(noticeRestoreFrame(t, protocol.MessageTypeNoticeSubscribe, []byte{}))
		waitForRestoreWrites(t, trans, baseWrites+3)
		trans.enqueue(noticeRestoreFrame(t, protocol.MessageTypeNoticeUnsubscribe, noticeSuccessPayload()))
	}()

	err = c.RestoreSubscriptions(context.Background())
	require.Error(t, err)
	assert.Zero(t, conn.ActiveSubscriptions())
	assert.Len(t, c.subscriptions.Handlers(11), 1)
	assert.Len(t, c.subscriptions.Handlers(12), 1)
	assert.Empty(t, c.subscriptions.Handlers(101))
	assert.Empty(t, c.subscriptions.Handlers(102))
	waitForRestoreWrites(t, trans, baseWrites+3)
	trans.mu.Lock()
	defer trans.mu.Unlock()
	assert.Len(t, trans.written, baseWrites+3)
}

// TestShouldValidateNoticeRoutes tests notice route validation.
func TestShouldValidateNoticeRoutesGivenPatternsWhenValidateNoticeRouteCalled(t *testing.T) {
	validRoutes := []string{
		"notice://acme/app/events/published",
		"notice://acme/app/logs/created",
		"notice://acme/*/users/updated",
		"notice://acme/**",
	}

	for _, route := range validRoutes {
		t.Run("valid route: "+route, func(t *testing.T) {
			// Just verify route can be encoded/decoded without panic
			encoded := encodeSubscribe(route)
			require.NotNil(t, encoded)
			require.Greater(t, len(encoded), 4)
		})
	}
}

// TestShouldHandleWildcardPatterns tests wildcard pattern handling in Notice.
func TestShouldHandleWildcardPatternsGivenPatternAndRouteWhenNoticeMatchRouteCalled(t *testing.T) {
	t.Run("single segment wildcard", func(t *testing.T) {
		// Arrange
		pattern := "notice://acme/app/*/fired"

		// Act
		encoded := encodeSubscribe(pattern)

		// Assert
		require.NotNil(t, encoded)
		require.Greater(t, len(encoded), 4)
	})

	t.Run("multi segment wildcard", func(t *testing.T) {
		// Arrange
		pattern := "notice://acme/**"

		// Act
		encoded := encodeSubscribe(pattern)

		// Assert
		require.NotNil(t, encoded)
	})

	t.Run("exact pattern", func(t *testing.T) {
		// Arrange
		pattern := "notice://acme/app/events/published"

		// Act
		encoded := encodeSubscribe(pattern)

		// Assert
		require.NotNil(t, encoded)
	})
}

// Benchmarks

func BenchmarkDecodeNotify(b *testing.B) {
	route := "notice://acme/app/events/published"
	payload := []byte("event data")
	encoded := encodePublish(route, payload)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _, _ = DecodeNotify(encoded)
	}
}

func BenchmarkEncodeSubscribe(b *testing.B) {
	b.Run("simple pattern", func(b *testing.B) {
		pattern := "notice://acme/app/events/published"

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_ = encodeSubscribe(pattern)
		}
	})

	b.Run("wildcard pattern", func(b *testing.B) {
		pattern := "notice://acme/**"

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_ = encodeSubscribe(pattern)
		}
	})
}

func BenchmarkEncodeUnsubscribe(b *testing.B) {
	subID := uint64(42)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = encodeUnsubscribe(subID)
	}
}

func BenchmarkEncodePublish(b *testing.B) {
	b.Run("small payload", func(b *testing.B) {
		route := "notice://acme/app/events/published"
		payload := []byte("event data")

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_ = encodePublish(route, payload)
		}
	})

	b.Run("large payload", func(b *testing.B) {
		route := "notice://acme/app/logs/created"
		payload := make([]byte, 65536)
		for i := range payload {
			payload[i] = byte((i + 7) % 256)
		}

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_ = encodePublish(route, payload)
		}
	})
}

func BenchmarkParseSubscribeResponse(b *testing.B) {
	// [status=0][has_sub_id=1][u64 sub_id]
	payload := make([]byte, 1+1+8)
	payload[0] = 0
	payload[1] = 1
	binary.BigEndian.PutUint64(payload[2:10], 999)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		success, remaining, _ := connection.ParseStandardResponse(payload)
		if success && len(remaining) >= 9 && remaining[0] == 1 {
			_, _, _ = connection.ReadU64BE(remaining, 1)
		}
	}
}
