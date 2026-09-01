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
		payload, err := encodeLeaseAcquire(route, "owner-1", ttlSecs, 15)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		require.Greater(t, len(payload), 4)
		// Verify route length prefix
		routeLen := binary.BigEndian.Uint32(payload[0:4])
		assert.Equal(t, uint32(len(route)), routeLen)
		pos := 4 + len(route)
		ownerLen := int(binary.BigEndian.Uint32(payload[pos : pos+4]))
		pos += 4
		assert.Equal(t, "owner-1", string(payload[pos:pos+ownerLen]))
		pos += ownerLen
		assert.Equal(t, ttlSecs, binary.BigEndian.Uint64(payload[pos:pos+8]))
		assert.Equal(t, uint32(15), binary.BigEndian.Uint32(payload[pos+8:pos+12]))
	})

	t.Run("zero ttl", func(t *testing.T) {
		// Arrange
		route := "lease://acme/app/locks"

		// Act
		payload, err := encodeLeaseAcquire(route, "owner-1", 0, 0)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		// Zero is valid for TTL (server may use default)
	})

	t.Run("max ttl", func(t *testing.T) {
		// Arrange & Act
		payload, err := encodeLeaseAcquire("path", "owner-1", 0xFFFFFFFFFFFFFFFF, 0)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		// Max uint64 should be accepted
	})
}

func TestShouldResolveQueuedAcquireGivenDeferredBrokerFrame(t *testing.T) {
	trans := newScriptedLeaseRestoreTransport()
	conn := connection.New(trans, connection.Config{Token: "", ReadTimeout: time.Second})
	require.NoError(t, conn.Start(context.Background()))
	t.Cleanup(func() { _ = conn.Close() })
	c := NewClient(conn)
	baseWrites := scriptedLeaseWriteCount(trans)

	go func() {
		waitForLeaseWrites(t, trans, baseWrites+1)
		queued := make([]byte, 10)
		queued[0], queued[1] = 0, 2
		trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseAcquire, queued))
		acquired := make([]byte, 10)
		acquired[0], acquired[1] = 0, 0
		binary.BigEndian.PutUint64(acquired[2:], 42)
		trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseAcquire, acquired))
	}()

	acquired, err := c.Acquire(context.Background(), "lease://realm/area/resource", 30, AcquireOptions{OwnerID: "worker-1", WaitSeconds: 5})
	require.NoError(t, err)
	assert.Equal(t, uint64(42), binary.BigEndian.Uint64(acquired.Token))
	assert.Equal(t, "worker-1", acquired.ownerID)
}

func TestShouldSerializeAcquireLifecycleGivenDeferredBrokerFrame(t *testing.T) {
	trans := newScriptedLeaseRestoreTransport()
	conn := connection.New(trans, connection.Config{Token: "", ReadTimeout: time.Second})
	require.NoError(t, conn.Start(context.Background()))
	t.Cleanup(func() { _ = conn.Close() })
	c := NewClient(conn)
	baseWrites := scriptedLeaseWriteCount(trans)

	firstResult := make(chan error, 1)
	go func() {
		_, err := c.Acquire(context.Background(), "lease://realm/area/first", 30, AcquireOptions{OwnerID: "worker-1", WaitSeconds: 5})
		firstResult <- err
	}()
	waitForLeaseWrites(t, trans, baseWrites+1)
	queued := make([]byte, 10)
	queued[0], queued[1] = 0, 2
	trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseAcquire, queued))

	secondResult := make(chan error, 1)
	go func() {
		_, err := c.Acquire(context.Background(), "lease://realm/area/second", 30, AcquireOptions{OwnerID: "worker-2"})
		secondResult <- err
	}()
	assert.Never(t, func() bool {
		return scriptedLeaseWriteCount(trans) > baseWrites+1
	}, 100*time.Millisecond, 10*time.Millisecond)

	acquired := make([]byte, 10)
	acquired[0], acquired[1] = 0, 0
	binary.BigEndian.PutUint64(acquired[2:], 42)
	trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseAcquire, acquired))
	require.NoError(t, <-firstResult)
	waitForLeaseWrites(t, trans, baseWrites+2)
	binary.BigEndian.PutUint64(acquired[2:], 43)
	trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseAcquire, acquired))
	require.NoError(t, <-secondResult)
}

// TestShouldEncodeLeaseRenewRequest tests RENEW operation encoding.
func TestShouldEncodeLeaseRenewRequestGivenTokenAndTTLWhenPayloadWritten(t *testing.T) {
	t.Run("valid token and ttl", func(t *testing.T) {
		// Arrange
		token := uint64(0x0123456789ABCDEF)
		ttlSecs := uint64(600)

		// Act
		payload, err := encodeLeaseRenew("resource", "owner-1", token, ttlSecs)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		require.Greater(t, len(payload), 24) // Sufficient for all fields
	})

	t.Run("zero token", func(t *testing.T) {
		// Arrange & Act
		payload, err := encodeLeaseRenew("path", "owner-1", 0, 300) // Zero token (invalid, but tests encoding)

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
		payload, err := encodeLeaseRelease("resource", "owner-1", token)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		require.Greater(t, len(payload), 8)
	})

	t.Run("empty resource", func(t *testing.T) {
		// Arrange & Act
		payload, err := encodeLeaseRelease("", "owner-1", 12345)

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

// --- SUBSCRIBE/UNSUBSCRIBE wildcard selector tests (msg_type 407/408) ---

func TestShouldAcceptWildcardSubscribeRouteGivenValidSelectorMatrixWhenSubscribeCalled(t *testing.T) {
	valid := []string{
		"lease://acme/renderers/*",
		"lease://acme/*/doc-1",
		"lease://*/*/*",
		"lease://acme/**",
		"lease://**",
		"lease://acme/renderers/resource",
		"lease://**/renderers/**", // non-adjacent ** occurrences are allowed
	}
	for _, route := range valid {
		t.Run(route, func(t *testing.T) {
			trans := newScriptedLeaseRestoreTransport()
			conn := connection.New(trans, connection.Config{Token: "", ReadTimeout: time.Second})
			require.NoError(t, conn.Start(context.Background()))
			t.Cleanup(func() { _ = conn.Close() })
			c := NewClient(conn)
			baseWrites := scriptedLeaseWriteCount(trans)

			go func() {
				waitForLeaseWrites(t, trans, baseWrites+1)
				trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseSubscribe, leaseSubscribeResponsePayload(7)))
			}()

			sub, err := c.Subscribe(context.Background(), route, func(context.Context, ChangeNotification) error { return nil })
			require.NoError(t, err)
			require.NotNil(t, sub)
		})
	}
}

func TestShouldRejectMalformedSubscribeRouteGivenInvalidGrammarWhenSubscribeCalled(t *testing.T) {
	invalid := []string{
		"lease://acme/renderers/lock*", // partial wildcard
		"notice://acme/x/y",            // wrong scheme
		"lease://acme/x",               // wrong segment count
		"lease://acme//x",              // empty segment
		"lease://acme/**/**",           // adjacent ** (only a single trailing ** alias allowed)
	}
	for _, route := range invalid {
		t.Run(route, func(t *testing.T) {
			trans := newScriptedLeaseRestoreTransport()
			conn := connection.New(trans, connection.Config{Token: "", ReadTimeout: time.Second})
			require.NoError(t, conn.Start(context.Background()))
			t.Cleanup(func() { _ = conn.Close() })
			c := NewClient(conn)
			baseWrites := scriptedLeaseWriteCount(trans)

			sub, err := c.Subscribe(context.Background(), route, func(context.Context, ChangeNotification) error { return nil })
			require.Error(t, err)
			require.Nil(t, sub)
			assert.Equal(t, baseWrites, scriptedLeaseWriteCount(trans), "malformed route must be rejected client-side without a wire round trip")
		})
	}
}

// --- LIST wire codec tests (msg_type 410) ---

func leaseListResponsePayload(t *testing.T, entries []LeaseEntry, next *leaseListCursor) []byte {
	t.Helper()
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	buf.WriteByte(0) // status = success
	connection.WriteU32BE(buf, uint32(len(entries)))
	for _, e := range entries {
		connection.WriteBytes(buf, []byte(e.Route))
		connection.WriteBytes(buf, []byte(e.OwnerID))
		connection.WriteU64BE(buf, e.HolderIncarnation)
		connection.WriteBytes(buf, []byte(e.AcquiredAt))
		connection.WriteU64BE(buf, e.ExpiresInSecs)
		connection.WriteU32BE(buf, e.Renewals)
	}
	if next == nil {
		buf.WriteByte(0)
	} else {
		buf.WriteByte(1)
		connection.WriteU64BE(buf, next.snapshotID)
		connection.WriteU32BE(buf, next.offset)
	}
	return append([]byte(nil), buf.Bytes()...)
}

func TestShouldEncodeLeaseListRequestGivenPatternAndCursorWhenPayloadWritten(t *testing.T) {
	t.Run("no cursor, default limit", func(t *testing.T) {
		payload, err := encodeLeaseList("lease://acme/renderers/*", nil, 0)
		require.NoError(t, err)

		pattern, offset, err := connection.ReadString(payload, 0)
		require.NoError(t, err)
		assert.Equal(t, "lease://acme/renderers/*", pattern)
		hasCursor, offset, err := connection.ReadU8(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, uint8(0), hasCursor)
		limit, offset, err := connection.ReadU32BE(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), limit)
		assert.Equal(t, len(payload), offset)
	})

	t.Run("with cursor and limit", func(t *testing.T) {
		cursor := &leaseListCursor{snapshotID: 99, offset: 200}
		payload, err := encodeLeaseList("lease://**", cursor, 50)
		require.NoError(t, err)

		_, offset, err := connection.ReadString(payload, 0)
		require.NoError(t, err)
		hasCursor, offset, err := connection.ReadU8(payload, offset)
		require.NoError(t, err)
		require.Equal(t, uint8(1), hasCursor)
		snapshotID, offset, err := connection.ReadU64BE(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, uint64(99), snapshotID)
		cursorOffset, offset, err := connection.ReadU32BE(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, uint32(200), cursorOffset)
		limit, offset, err := connection.ReadU32BE(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, uint32(50), limit)
		assert.Equal(t, len(payload), offset)
	})
}

func TestShouldParseLeaseListResponseGivenCanonicalPayloadWhenParseLeaseListResponseCalled(t *testing.T) {
	t.Run("single page, no more results", func(t *testing.T) {
		entries := []LeaseEntry{
			{Route: "lease://acme/renderers/a", OwnerID: "owner-1", HolderIncarnation: 7, AcquiredAt: "2026-08-29T00:00:00Z", ExpiresInSecs: 30, Renewals: 2},
			{Route: "lease://acme/renderers/b", OwnerID: "owner-2", HolderIncarnation: 8, AcquiredAt: "2026-08-29T00:00:01Z", ExpiresInSecs: 45, Renewals: 0},
		}
		payload := leaseListResponsePayload(t, entries, nil)

		success, remaining, err := connection.ParseStandardResponse(payload)
		require.NoError(t, err)
		require.True(t, success)

		items, next, err := parseLeaseListResponse(remaining)
		require.NoError(t, err)
		require.Nil(t, next)
		require.Equal(t, entries, items)
	})

	t.Run("page with cursor for continuation", func(t *testing.T) {
		entries := []LeaseEntry{{Route: "lease://acme/renderers/a", OwnerID: "owner-1", HolderIncarnation: 1, AcquiredAt: "t", ExpiresInSecs: 1, Renewals: 1}}
		next := &leaseListCursor{snapshotID: 42, offset: 100}
		payload := leaseListResponsePayload(t, entries, next)

		success, remaining, err := connection.ParseStandardResponse(payload)
		require.NoError(t, err)
		require.True(t, success)

		items, gotNext, err := parseLeaseListResponse(remaining)
		require.NoError(t, err)
		require.Equal(t, entries, items)
		require.NotNil(t, gotNext)
		assert.Equal(t, next.snapshotID, gotNext.snapshotID)
		assert.Equal(t, next.offset, gotNext.offset)
	})

	t.Run("empty result set", func(t *testing.T) {
		payload := leaseListResponsePayload(t, nil, nil)

		success, remaining, err := connection.ParseStandardResponse(payload)
		require.NoError(t, err)
		require.True(t, success)

		items, next, err := parseLeaseListResponse(remaining)
		require.NoError(t, err)
		require.Nil(t, next)
		require.Empty(t, items)
	})

	t.Run("invalid has_next flag", func(t *testing.T) {
		payload := leaseListResponsePayload(t, nil, nil)
		payload[len(payload)-1] = 2 // corrupt has_next flag

		_, next, err := parseLeaseListResponse(payload[1:])
		require.Error(t, err)
		require.Nil(t, next)
	})

	// A corrupted/malicious response claiming an enormous item_count with a
	// short body must fail cleanly (a parse error on the first truncated
	// item) rather than attempting a multi-gigabyte preallocation.
	t.Run("huge item_count with short body fails cleanly without huge allocation", func(t *testing.T) {
		buf := connection.GetBuffer()
		defer connection.PutBuffer(buf)
		connection.WriteU32BE(buf, 0xFFFFFFFF) // claimed item_count
		buf.WriteString("short")               // far too little data for even one item
		remaining := append([]byte(nil), buf.Bytes()...)

		done := make(chan struct{})
		var items []LeaseEntry
		var err error
		go func() {
			items, _, err = parseLeaseListResponse(remaining)
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("parseLeaseListResponse did not return promptly given a huge item_count and short body")
		}
		require.Error(t, err)
		require.Nil(t, items)
	})
}

// TestShouldBoundPreallocCapacityGivenItemCountAndBufferLenWhenComputed
// verifies leaseListPreallocCapacity never preallocates more capacity than
// the remaining buffer could plausibly hold, regardless of how large the
// wire-supplied item_count claims to be.
func TestShouldBoundPreallocCapacityGivenItemCountAndBufferLenWhenComputed(t *testing.T) {
	t.Run("huge item_count with short buffer is capped to what could fit", func(t *testing.T) {
		cap := leaseListPreallocCapacity(0xFFFFFFFF, 5)
		assert.Less(t, cap, 1000, "capacity must not scale with the untrusted item_count")
	})

	t.Run("small item_count with large buffer uses item_count", func(t *testing.T) {
		cap := leaseListPreallocCapacity(3, 10_000)
		assert.Equal(t, 3, cap)
	})

	t.Run("zero remaining bytes yields zero capacity", func(t *testing.T) {
		assert.Equal(t, 0, leaseListPreallocCapacity(1000, 0))
		assert.Equal(t, 0, leaseListPreallocCapacity(1000, -1))
	})
}

// --- List() iterator paging test ---

func TestShouldPageAcrossMultipleResponsesGivenScriptedBrokerWhenListIteratorConsumed(t *testing.T) {
	trans := newScriptedLeaseRestoreTransport()
	conn := connection.New(trans, connection.Config{Token: "", ReadTimeout: time.Second})
	require.NoError(t, conn.Start(context.Background()))
	t.Cleanup(func() { _ = conn.Close() })
	c := NewClient(conn)
	baseWrites := scriptedLeaseWriteCount(trans)

	page1 := []LeaseEntry{
		{Route: "lease://acme/renderers/a", OwnerID: "owner-1", HolderIncarnation: 1, AcquiredAt: "t1", ExpiresInSecs: 10, Renewals: 0},
		{Route: "lease://acme/renderers/b", OwnerID: "owner-2", HolderIncarnation: 2, AcquiredAt: "t2", ExpiresInSecs: 20, Renewals: 1},
	}
	page2 := []LeaseEntry{
		{Route: "lease://acme/renderers/c", OwnerID: "owner-3", HolderIncarnation: 3, AcquiredAt: "t3", ExpiresInSecs: 30, Renewals: 2},
	}
	cursor := &leaseListCursor{snapshotID: 5, offset: 2}

	go func() {
		waitForLeaseWrites(t, trans, baseWrites+1)
		trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseList, leaseListResponsePayload(t, page1, cursor)))
		waitForLeaseWrites(t, trans, baseWrites+2)
		trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseList, leaseListResponsePayload(t, page2, nil)))
	}()

	it, err := c.List(context.Background(), "lease://acme/renderers/*")
	require.NoError(t, err)
	require.NotNil(t, it)

	var got []LeaseEntry
	for it.Next() {
		got = append(got, it.Value())
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	assert.Equal(t, append(append([]LeaseEntry{}, page1...), page2...), got)
}

func TestShouldRejectMalformedListPatternGivenInvalidGrammarWhenListCalled(t *testing.T) {
	trans := newScriptedLeaseRestoreTransport()
	conn := connection.New(trans, connection.Config{Token: "", ReadTimeout: time.Second})
	require.NoError(t, conn.Start(context.Background()))
	t.Cleanup(func() { _ = conn.Close() })
	c := NewClient(conn)
	baseWrites := scriptedLeaseWriteCount(trans)

	it, err := c.List(context.Background(), "lease://acme/renderers/lock*")
	require.Error(t, err)
	require.Nil(t, it)
	assert.Equal(t, baseWrites, scriptedLeaseWriteCount(trans))
}

// --- New error code decoding tests (5011, 5012) ---

func TestShouldMapNewLeaseListErrorCodesGivenBrokerMessageWhenMapLeaseErrorCalled(t *testing.T) {
	t.Run("map invalid list cursor error", func(t *testing.T) {
		errMsg := coreerrors.NewDomainError(coreerrors.LeaseInvalidListCursor, "cursor unknown or evicted")

		mapped := mapLeaseError(errMsg)

		assert.ErrorIs(t, mapped, ErrInvalidListCursor)
	})

	t.Run("map invalid list pattern error", func(t *testing.T) {
		errMsg := coreerrors.NewDomainError(coreerrors.LeaseInvalidListPattern, "pattern is malformed")

		mapped := mapLeaseError(errMsg)

		assert.ErrorIs(t, mapped, ErrInvalidListPattern)
	})
}

// Benchmarks

func BenchmarkEncodeLeaseAcquire(b *testing.B) {
	b.Run("short route", func(b *testing.B) {
		route := "lease://a/b/c"

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = encodeLeaseAcquire(route, "owner-1", 300, 0)
		}
	})

	b.Run("long route", func(b *testing.B) {
		route := "lease://prod/locks/critical-section"

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = encodeLeaseAcquire(route, "owner-1", 300, 0)
		}
	})
}

func BenchmarkEncodeLeaseRenew(b *testing.B) {
	b.Run("standard", func(b *testing.B) {
		token := uint64(0x123456789ABCDEF0)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = encodeLeaseRenew("resource", "owner-1", token, 600)
		}
	})
}

func BenchmarkEncodeLeaseRelease(b *testing.B) {
	token := uint64(0xFEDCBA9876543210)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = encodeLeaseRelease("resource", "owner-1", token)
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
