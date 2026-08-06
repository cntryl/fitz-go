package transport

import (
	"bufio"
	"context"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cntryl/fitz-go/v2/internal/testkit"
)

func newTestWSTransport(conn *testkit.MockWSConn) *WebSocketTransport {
	addr := conn.RemoteAddrHost
	if addr == "" {
		addr = "ws://localhost:4090/ws"
	}
	return &WebSocketTransport{
		conn:   conn,
		reader: bufio.NewReader(conn),
		addr:   addr,
	}
}

func buildMaskedTestWSFrame(opcode byte, payload []byte) []byte {
	maskKey := [4]byte{0xAA, 0xBB, 0xCC, 0xDD}
	frame := make([]byte, 0, 2+4+len(payload))
	frame = append(frame, 0x80|opcode, 0x80|byte(len(payload)))
	frame = append(frame, maskKey[:]...)
	for i := range payload {
		frame = append(frame, payload[i]^maskKey[i%4])
	}
	return frame
}

// TestShouldWriteBinaryFrameGivenPayloadWhenWriteCalled tests WebSocket write framing.
func TestShouldWriteBinaryFrameGivenPayloadWhenWriteCalled(t *testing.T) {
	t.Run("valid payload", func(t *testing.T) {
		// Arrange
		conn := &testkit.MockWSConn{
			Messages: make([][]byte, 0),
		}
		transport := newTestWSTransport(conn)

		payload := []byte{0x01, 0x02, 0x03}

		// Act
		err := transport.Write(context.Background(), payload)

		// Assert
		require.NoError(t, err)
		assert.Len(t, conn.Messages, 1)
		assert.Equal(t, payload, conn.Messages[0])
	})

	t.Run("empty payload", func(t *testing.T) {
		// Arrange
		conn := &testkit.MockWSConn{
			Messages: make([][]byte, 0),
		}
		transport := newTestWSTransport(conn)

		payload := []byte{}

		// Act
		err := transport.Write(context.Background(), payload)

		// Assert
		require.NoError(t, err)
		assert.Len(t, conn.Messages, 1)
	})

	t.Run("large payload", func(t *testing.T) {
		// Arrange
		conn := &testkit.MockWSConn{
			Messages: make([][]byte, 0),
		}
		transport := newTestWSTransport(conn)

		payload := make([]byte, 65535)

		// Act
		err := transport.Write(context.Background(), payload)

		// Assert
		require.NoError(t, err)
		assert.Len(t, conn.Messages, 1)
	})
}

func TestShouldWriteCompleteBinaryFrameGivenShortWritesWhenWriteCalled(t *testing.T) {
	// Arrange
	conn := &testkit.MockWSConn{
		Messages:     make([][]byte, 0),
		MaxWriteSize: 3,
	}
	transport := newTestWSTransport(conn)
	payload := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06}

	// Act
	err := transport.Write(context.Background(), payload)

	// Assert
	require.NoError(t, err)
	assert.Len(t, conn.Messages, 1)
	assert.Equal(t, payload, conn.Messages[0])
}

// TestShouldReadBinaryFrameGivenBinaryMessageWhenReadCalled tests WebSocket read framing.
func TestShouldReadBinaryFrameGivenBinaryMessageWhenReadCalled(t *testing.T) {
	t.Run("binary message", func(t *testing.T) {
		// Arrange
		data := []byte{0x01, 0x02, 0x03, 0x04}
		conn := &testkit.MockWSConn{
			NextMessage: data,
			IsText:      false,
		}
		transport := newTestWSTransport(conn)

		// Act
		frame, err := transport.Read(context.Background())

		// Assert
		require.NoError(t, err)
		assert.Equal(t, data, frame)
	})

	t.Run("empty message", func(t *testing.T) {
		// Arrange
		conn := &testkit.MockWSConn{
			NextMessage: []byte{},
			IsText:      false,
		}
		transport := newTestWSTransport(conn)

		// Act
		frame, err := transport.Read(context.Background())

		// Assert
		require.NoError(t, err)
		assert.Equal(t, []byte{}, frame)
	})
}

// TestShouldRejectTextFrameGivenTextMessageWhenReadCalled tests text frame rejection.
func TestShouldRejectTextFrameGivenTextMessageWhenReadCalled(t *testing.T) {
	// Arrange
	conn := &testkit.MockWSConn{
		NextMessage: []byte("text message"),
		IsText:      true,
	}
	transport := &WebSocketTransport{
		conn:   conn,
		reader: bufio.NewReader(conn),
	}

	// Act
	_, err := transport.Read(context.Background())

	// Assert — text frames must be immediately rejected with a descriptive error,
	// not silently skipped (REQ-PROTO-003: binary-only transport).
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "text frame")
}

func TestShouldRejectMaskedServerFrameGivenMaskedBinaryPayloadWhenReadCalled(t *testing.T) {
	// Arrange
	conn := &testkit.MockWSConn{
		ReadBuf: buildMaskedTestWSFrame(opcodeBinary, []byte{0x01, 0x02, 0x03}),
	}
	transport := newTestWSTransport(conn)

	// Act
	_, err := transport.Read(context.Background())

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "masked")
}

func TestShouldRejectOversizeFrameGivenAnnouncedBinaryLengthWhenReadCalled(t *testing.T) {
	// Arrange
	conn := &testkit.MockWSConn{
		ReadBuf: []byte{0x82, 0x7f, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01},
	}
	transport := newTestWSTransport(conn)

	// Act
	_, err := transport.Read(context.Background())

	// Assert
	assert.ErrorIs(t, err, ErrFrameTooLarge)
}

func TestShouldRejectReservedBitsGivenNonZeroRSVWhenReadCalled(t *testing.T) {
	// Arrange
	conn := &testkit.MockWSConn{
		ReadBuf: []byte{0xC2, 0x00},
	}
	transport := newTestWSTransport(conn)

	// Act
	_, err := transport.Read(context.Background())

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reserved bits")
}

// TestShouldParseWSURLGivenURLStringWhenURLParsed tests URL parsing assumptions used by dialing.
func TestShouldParseWSURLGivenURLStringWhenURLParsed(t *testing.T) {
	t.Run("ws scheme", func(t *testing.T) {
		// Arrange

		// Act
		u, err := url.Parse("ws://localhost:4090/ws")

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "ws", u.Scheme)
	})

	t.Run("wss scheme", func(t *testing.T) {
		// Arrange

		// Act
		u, err := url.Parse("wss://localhost:4090/ws")

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "wss", u.Scheme)
	})

	t.Run("non-websocket scheme", func(t *testing.T) {
		// Arrange

		// Act
		_, err := DialWebSocket(context.Background(), "http://localhost:4090/ws")

		// Assert
		require.Error(t, err)
	})
}

// TestShouldTimeoutReadGivenShortDeadlineWhenWSReadCalled tests WebSocket read timeouts.
func TestShouldTimeoutReadGivenShortDeadlineWhenWSReadCalled(t *testing.T) {
	// Arrange
	conn := &testkit.MockWSConn{
		Blocked: true, // Will block forever
	}
	transport := newTestWSTransport(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// Act
	_, err := transport.Read(ctx)

	// Assert
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestShouldCancelReadGivenCanceledContextWithoutDeadlineWhenWSReadCalled(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})
	transport := &WebSocketTransport{
		conn:   clientConn,
		reader: bufio.NewReader(clientConn),
		addr:   "ws://pipe",
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := transport.Read(ctx)
		result <- err
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-result:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Read did not return after context cancellation")
	}
}

// TestShouldCloseGracefullyGivenOpenWSTransportWhenCloseCalled tests WebSocket close behavior.
func TestShouldCloseGracefullyGivenOpenWSTransportWhenCloseCalled(t *testing.T) {
	// Arrange
	conn := &testkit.MockWSConn{}
	transport := newTestWSTransport(conn)

	// Act
	err := transport.Close()

	// Assert
	require.NoError(t, err)
	assert.True(t, conn.Closed)
}

func TestShouldNotifyOnlyRegisteredPongWaitersGivenWaiterRemoved(t *testing.T) {
	transport := &WebSocketTransport{}
	removed := make(chan struct{})
	registered := make(chan struct{})

	transport.addPongWaiter(removed)
	transport.addPongWaiter(registered)
	transport.removePongWaiter(removed)
	transport.notifyPongWaiters()

	select {
	case <-registered:
	case <-time.After(time.Second):
		t.Fatal("registered pong waiter was not notified")
	}

	select {
	case <-removed:
		t.Fatal("removed pong waiter was notified")
	default:
	}

	require.Empty(t, transport.pongWaiters)
}

// TestShouldReturnRemoteAddrGivenWSTransportWhenRemoteAddrCalled tests WebSocket remote address reporting.
func TestShouldReturnRemoteAddrGivenWSTransportWhenRemoteAddrCalled(t *testing.T) {
	// Arrange
	conn := &testkit.MockWSConn{
		RemoteAddrHost: "ws://example.com:4090/ws",
	}
	transport := newTestWSTransport(conn)

	// Act
	addr := transport.RemoteAddr()

	// Assert
	assert.NotEmpty(t, addr)
}

// TestShouldRejectWriteGivenCanceledContextWhenWSWriteCalled tests context cancellation handling.
func TestShouldRejectWriteGivenCanceledContextWhenWSWriteCalled(t *testing.T) {
	// Arrange
	conn := &testkit.MockWSConn{}
	transport := newTestWSTransport(conn)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Act
	err := transport.Write(ctx, []byte{0x01})

	// Assert
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// Mock WebSocket connection is now provided by testkit.MockWSConn from internal/testkit package

// Benchmarks
func BenchmarkWriteWSFrame(b *testing.B) {
	b.Run("100 byte payload", func(b *testing.B) {
		conn := &testkit.MockWSConn{
			Messages: make([][]byte, 0),
		}
		transport := newTestWSTransport(conn)
		payload := make([]byte, 100)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_ = transport.Write(context.Background(), payload)
		}
	})

	b.Run("10KB payload", func(b *testing.B) {
		conn := &testkit.MockWSConn{
			Messages: make([][]byte, 0),
		}
		transport := newTestWSTransport(conn)
		payload := make([]byte, 10240)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_ = transport.Write(context.Background(), payload)
		}
	})
}

func BenchmarkReadWSFrame(b *testing.B) {
	frame := make([]byte, 100)
	conn := &testkit.MockWSConn{
		NextMessage: frame,
		IsText:      false,
	}
	transport := newTestWSTransport(conn)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = transport.Read(context.Background())
	}
}
