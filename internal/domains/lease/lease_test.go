package lease

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/v2/internal/core/connection"
	coreerrors "github.com/cntryl/fitz-go/v2/internal/core/errors"
	"github.com/cntryl/fitz-go/v2/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type scriptedLeaseRestoreTransport struct {
	mu      sync.Mutex
	written [][]byte
	readCh  chan []byte
	closed  chan struct{}
	once    sync.Once
}

func newScriptedLeaseRestoreTransport() *scriptedLeaseRestoreTransport {
	return &scriptedLeaseRestoreTransport{
		readCh: make(chan []byte, 8),
		closed: make(chan struct{}),
	}
}

func (s *scriptedLeaseRestoreTransport) Write(ctx context.Context, frame []byte) error {
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

func (s *scriptedLeaseRestoreTransport) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.closed:
		return nil, connection.ErrConnectionClosed
	case frame := <-s.readCh:
		return append([]byte(nil), frame...), nil
	}
}

func (s *scriptedLeaseRestoreTransport) Close() error {
	s.once.Do(func() {
		close(s.closed)
	})
	return nil
}

func (s *scriptedLeaseRestoreTransport) RemoteAddr() string {
	return "scripted://lease"
}

func (s *scriptedLeaseRestoreTransport) enqueue(frame []byte) {
	s.readCh <- append([]byte(nil), frame...)
}

func scriptedLeaseFrame(t *testing.T, msgType uint16, payload []byte) []byte {
	t.Helper()
	frame := protocol.EncodeFrameOwned(msgType, payload)
	defer frame.Release()
	return append([]byte(nil), frame.Bytes()...)
}

func leaseSubscribeResponsePayload(subID uint64) []byte {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	buf.WriteByte(0)
	connection.WriteU64BE(buf, subID)
	return append([]byte(nil), buf.Bytes()...)
}

func scriptedLeaseWriteCount(trans *scriptedLeaseRestoreTransport) int {
	trans.mu.Lock()
	defer trans.mu.Unlock()
	return len(trans.written)
}

func waitForLeaseWrites(t *testing.T, trans *scriptedLeaseRestoreTransport, expected int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return scriptedLeaseWriteCount(trans) >= expected
	}, time.Second, 10*time.Millisecond)
}

// TestShouldEncodeLeaseAcquireRequest tests ACQUIRE operation encoding.
func TestShouldEncodeLeaseAcquireRequestGivenRouteAndTTLWhenPayloadWritten(t *testing.T) {
	t.Run("valid route and ttl", func(t *testing.T) {
		// Arrange
		route := "lease://acme/app/locks"
		ttlSecs := uint64(300)

		// Act
		payload, err := encodeLeaseAcquire(route, ttlSecs)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		require.Greater(t, len(payload), 4)
		// Verify route length prefix
		routeLen := binary.BigEndian.Uint32(payload[0:4])
		assert.Equal(t, uint32(len(route)), routeLen)
	})

	t.Run("zero ttl", func(t *testing.T) {
		// Arrange
		route := "lease://acme/app/locks"

		// Act
		payload, err := encodeLeaseAcquire(route, 0)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		// Zero is valid for TTL (server may use default)
	})

	t.Run("max ttl", func(t *testing.T) {
		// Arrange & Act
		payload, err := encodeLeaseAcquire("path", 0xFFFFFFFFFFFFFFFF)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		// Max uint64 should be accepted
	})
}

// TestShouldEncodeLeaseRenewRequest tests RENEW operation encoding.
func TestShouldEncodeLeaseRenewRequestGivenTokenAndTTLWhenPayloadWritten(t *testing.T) {
	t.Run("valid token and ttl", func(t *testing.T) {
		// Arrange
		token := uint64(0x0123456789ABCDEF)
		ttlSecs := uint64(600)

		// Act
		payload, err := encodeLeaseRenew("resource", token, ttlSecs)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		require.Greater(t, len(payload), 24) // Sufficient for all fields
	})

	t.Run("zero token", func(t *testing.T) {
		// Arrange & Act
		payload, err := encodeLeaseRenew("path", 0, 300) // Zero token (invalid, but tests encoding)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
	})
}

// TestShouldEncodeLeaseReleaseRequest tests RELEASE operation encoding.
func TestShouldEncodeLeaseReleaseRequestGivenTokenWhenPayloadWritten(t *testing.T) {
	t.Run("valid token", func(t *testing.T) {
		// Arrange
		token := uint64(0xFEDCBA9876543210)

		// Act
		payload, err := encodeLeaseRelease("resource", token)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		require.Greater(t, len(payload), 8)
	})

	t.Run("empty resource", func(t *testing.T) {
		// Arrange & Act
		payload, err := encodeLeaseRelease("", 12345)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		// Empty resource is encoded as 0-length string
	})
}

// TestShouldEncodeLeaseQueryRequest tests QUERY operation encoding.
func TestShouldEncodeLeaseQueryRequestGivenRouteWhenPayloadWritten(t *testing.T) {
	t.Run("query with route", func(t *testing.T) {
		// Arrange
		route := "lease://acme/app/locks/resource1"

		// Act
		payload, err := encodeLeaseQuery(route)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		require.Greater(t, len(payload), 4)
		// Verify route is encoded correctly
		routeLen := binary.BigEndian.Uint32(payload[0:4])
		assert.Equal(t, uint32(len(route)), routeLen)
	})

	t.Run("query with complex route", func(t *testing.T) {
		// Arrange
		route := "lease://org.example.com/system/distributed-locks"

		// Act
		payload, err := encodeLeaseQuery(route)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		routeLen := binary.BigEndian.Uint32(payload[0:4])
		assert.Equal(t, uint32(len(route)), routeLen)
	})
}

// TestShouldMapLeaseErrors tests error mapping.
func TestShouldMapLeaseErrorsGivenBrokerMessageWhenMapLeaseErrorCalled(t *testing.T) {
	t.Run("map held error", func(t *testing.T) {
		// Arrange
		errMsg := coreerrors.NewDomainError(coreerrors.LeaseHeld, "the lease is held by another owner")

		// Act
		mapped := mapLeaseError(errMsg)

		// Assert
		assert.Equal(t, ErrLeaseHeld, mapped)
	})

	t.Run("map invalid fence error", func(t *testing.T) {
		// Arrange
		errMsg := coreerrors.NewDomainError(coreerrors.LeaseInvalidFence, "invalid fencing token provided")

		// Act
		mapped := mapLeaseError(errMsg)

		// Assert
		assert.Equal(t, ErrInvalidFence, mapped)
	})

	t.Run("map expired error", func(t *testing.T) {
		// Arrange
		errMsg := coreerrors.NewDomainError(coreerrors.LeaseExpired, "lease has expired")

		// Act
		mapped := mapLeaseError(errMsg)

		// Assert
		assert.Equal(t, ErrLeaseExpired, mapped)
	})

	t.Run("map not found error", func(t *testing.T) {
		// Arrange
		errMsg := coreerrors.NewDomainError(coreerrors.LeaseNotFound, "resource not found")

		// Act
		mapped := mapLeaseError(errMsg)

		// Assert
		assert.Equal(t, ErrLeaseNotFound, mapped)
	})

	t.Run("unknown error returns wrapped message", func(t *testing.T) {
		// Arrange
		errMsg := errors.New("some unknown error condition")

		// Act
		mapped := mapLeaseError(errMsg)

		// Assert
		assert.Error(t, mapped)
		assert.Equal(t, errMsg, mapped)
	})
}

func TestShouldPreserveLocalSubscriptionsGivenRestoreFailureWithReusedSubIDWhenRestoreSubscriptionsCalled(t *testing.T) {
	trans := newScriptedLeaseRestoreTransport()
	conn := connection.New(trans, connection.Config{Token: "", ReadTimeout: time.Second})
	require.NoError(t, conn.Start(context.Background()))
	t.Cleanup(func() {
		_ = conn.Close()
	})
	baseWrites := scriptedLeaseWriteCount(trans)

	handlerAlpha := func(context.Context, ChangeNotification) error { return nil }
	handlerBravo := func(context.Context, ChangeNotification) error { return nil }
	c := &client{
		conn: conn,
		subscriptions: map[uint64]*Subscription{
			1: {subID: 1, route: "lease://realm/area/alpha", client: nil, handler: handlerAlpha},
			2: {subID: 2, route: "lease://realm/area/bravo", client: nil, handler: handlerBravo},
		},
	}

	go func() {
		waitForLeaseWrites(t, trans, baseWrites+1)
		trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseSubscribe, leaseSubscribeResponsePayload(1)))
		waitForLeaseWrites(t, trans, baseWrites+2)
		trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseSubscribe, []byte{}))
		waitForLeaseWrites(t, trans, baseWrites+3)
		trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseUnsubscribe, []byte{0}))
	}()

	err := c.RestoreSubscriptions(context.Background())
	require.Error(t, err)
	assert.Zero(t, conn.ActiveSubscriptions())

	c.mu.RLock()
	defer c.mu.RUnlock()
	alpha, ok := c.subscriptions[1]
	require.True(t, ok)
	assert.Equal(t, "lease://realm/area/alpha", alpha.route)
	assert.NotNil(t, alpha.handler)
	bravo, ok := c.subscriptions[2]
	require.True(t, ok)
	assert.Equal(t, "lease://realm/area/bravo", bravo.route)
	assert.NotNil(t, bravo.handler)
	assert.Len(t, c.subscriptions, 2)
}

func TestShouldParseLeaseQueryResponseGivenCanonicalPayloadWhenParseLeaseQueryResponseCalled(t *testing.T) {
	t.Run("free lease response", func(t *testing.T) {
		remaining := []byte{0x00, 0x00, 0x00, 0x00, 0x03}

		info, err := parseLeaseQueryResponse(remaining)

		require.NoError(t, err)
		require.NotNil(t, info)
		assert.False(t, info.Held)
		assert.Equal(t, uint32(3), info.PendingWaiters)
	})

	t.Run("held lease response", func(t *testing.T) {
		ownerID := []byte("owner-1")
		remaining := make([]byte, 1+4+len(ownerID)+8+4)
		remaining[0] = 0x01
		binary.BigEndian.PutUint32(remaining[1:5], uint32(len(ownerID)))
		copy(remaining[5:5+len(ownerID)], ownerID)
		offset := 5 + len(ownerID)
		binary.BigEndian.PutUint64(remaining[offset:offset+8], 60)
		offset += 8
		binary.BigEndian.PutUint32(remaining[offset:offset+4], 2)

		info, err := parseLeaseQueryResponse(remaining)

		require.NoError(t, err)
		require.NotNil(t, info)
		assert.True(t, info.Held)
		assert.Equal(t, "owner-1", info.OwnerID)
		assert.Equal(t, uint64(60), info.TTLRemainingSecs)
		assert.Equal(t, uint32(2), info.PendingWaiters)
	})
}

func TestShouldRejectMalformedLeaseQueryResponseGivenInvalidShapeWhenParseLeaseQueryResponseCalled(t *testing.T) {
	t.Run("invalid has_holder flag", func(t *testing.T) {
		info, err := parseLeaseQueryResponse([]byte{0x02, 0x00, 0x00, 0x00, 0x00})

		require.Error(t, err)
		require.Nil(t, info)
		require.ErrorContains(t, err, "invalid has_holder")
	})

	t.Run("free response with trailing bytes", func(t *testing.T) {
		info, err := parseLeaseQueryResponse([]byte{0x00, 0x00, 0x00, 0x00, 0x03, 0xFF})

		require.Error(t, err)
		require.Nil(t, info)
		require.ErrorContains(t, err, "malformed")
	})

	t.Run("held response missing pending_waiters", func(t *testing.T) {
		ownerID := []byte("owner-1")
		remaining := make([]byte, 1+4+len(ownerID)+8)
		remaining[0] = 0x01
		binary.BigEndian.PutUint32(remaining[1:5], uint32(len(ownerID)))
		copy(remaining[5:5+len(ownerID)], ownerID)
		offset := 5 + len(ownerID)
		binary.BigEndian.PutUint64(remaining[offset:offset+8], 60)

		info, err := parseLeaseQueryResponse(remaining)

		require.Error(t, err)
		require.Nil(t, info)
		require.ErrorContains(t, err, "missing pending_waiters")
	})
}

// Benchmarks

func BenchmarkEncodeLeaseAcquire(b *testing.B) {
	b.Run("short route", func(b *testing.B) {
		route := "lease://a/b/c"

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = encodeLeaseAcquire(route, 300)
		}
	})

	b.Run("long route", func(b *testing.B) {
		route := "lease://prod/locks/critical-section"

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = encodeLeaseAcquire(route, 300)
		}
	})
}

func BenchmarkEncodeLeaseRenew(b *testing.B) {
	b.Run("standard", func(b *testing.B) {
		token := uint64(0x123456789ABCDEF0)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = encodeLeaseRenew("resource", token, 600)
		}
	})
}

func BenchmarkEncodeLeaseRelease(b *testing.B) {
	token := uint64(0xFEDCBA9876543210)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = encodeLeaseRelease("resource", token)
	}
}

func BenchmarkEncodeLeaseQuery(b *testing.B) {
	route := "lease://acme/app/locks/resource"

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = encodeLeaseQuery(route)
	}
}

func BenchmarkParseLeaseAcquireResponse(b *testing.B) {
	// [status=0][response_type=0][u64 BE fencing_token]
	payload := make([]byte, 1+1+8)
	payload[0] = 0
	payload[1] = 0 // Acquired
	binary.BigEndian.PutUint64(payload[2:10], 0x123456789ABCDEF0)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		success, remaining, _ := connection.ParseStandardResponse(payload)
		if success && len(remaining) >= 9 {
			_ = remaining[0]
			_ = binary.BigEndian.Uint64(remaining[1:9])
		}
	}
}

func BenchmarkParseLeaseQueryResponse(b *testing.B) {
	// [status=0][has_holder=0][u32 pending_waiters=0]
	payload := make([]byte, 1+1+4)
	payload[0] = 0
	payload[1] = 0
	binary.BigEndian.PutUint32(payload[2:6], 0)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		success, remaining, _ := connection.ParseStandardResponse(payload)
		if success && len(remaining) >= 5 {
			_ = remaining[0]
			_ = binary.BigEndian.Uint32(remaining[1:5])
		}
	}
}
