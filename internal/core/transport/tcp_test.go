package transport

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cntryl/fitz-go/internal/testkit"
)

func newTestTCPTransport(conn *testkit.MockTCPConn) *TCPTransport {
	addr := ""
	if conn.RemoteAddr() != nil {
		addr = conn.RemoteAddr().String()
	}
	return &TCPTransport{
		conn:   conn,
		addr:   addr,
		reader: bufio.NewReader(conn),
	}
}

// TestShouldReturnErrorGivenNoServerWhenDialTCPCalled tests TCP dial failure behavior.
func TestShouldReturnErrorGivenNoServerWhenDialTCPCalled(t *testing.T) {
	// Arrange
	addr := "localhost:9999"

	// Act
	_, err := DialTCP(context.Background(), addr)

	// Assert
	assert.Error(t, err)
}

// TestShouldWriteFrameWithLengthPrefixGivenPayloadWhenWriteCalled tests TCP write framing.
func TestShouldWriteFrameWithLengthPrefixGivenPayloadWhenWriteCalled(t *testing.T) {
	t.Run("valid payload", func(t *testing.T) {
		// Arrange
		conn := &testkit.MockTCPConn{
			Written: make([]byte, 0),
		}
		transport := newTestTCPTransport(conn)

		payload := []byte{0x01, 0x02, 0x03}

		// Act
		err := transport.Write(context.Background(), payload)

		// Assert
		require.NoError(t, err)
		require.Greater(t, len(conn.Written), 4) // At least 4 bytes for length prefix
	})

	t.Run("empty payload", func(t *testing.T) {
		// Arrange
		conn := &testkit.MockTCPConn{
			Written: make([]byte, 0),
		}
		transport := newTestTCPTransport(conn)

		payload := []byte{}

		// Act
		err := transport.Write(context.Background(), payload)

		// Assert
		require.NoError(t, err)
	})
}

func TestShouldWriteCompleteFrameGivenShortWritesWhenWriteCalled(t *testing.T) {
	// Arrange
	conn := &testkit.MockTCPConn{
		Written:      make([]byte, 0),
		MaxWriteSize: 2,
	}
	transport := newTestTCPTransport(conn)
	payload := []byte{0x01, 0x02, 0x03, 0x04, 0x05}

	// Act
	err := transport.Write(context.Background(), payload)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x05, 0x01, 0x02, 0x03, 0x04, 0x05}, conn.Written)
}

// TestShouldTimeoutWriteGivenShortDeadlineWhenWriteCalled tests write timeout handling.
func TestShouldTimeoutWriteGivenShortDeadlineWhenWriteCalled(t *testing.T) {
	// Arrange
	conn := &testkit.MockTCPConn{
		WriteDelay: 100 * time.Millisecond,
		Written:    make([]byte, 0),
	}
	transport := newTestTCPTransport(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// Act
	err := transport.Write(ctx, []byte{0x01})

	// Assert
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestShouldReadFrameWithLengthPrefixGivenFramedPayloadWhenReadCalled tests TCP read framing.
func TestShouldReadFrameWithLengthPrefixGivenFramedPayloadWhenReadCalled(t *testing.T) {
	t.Run("valid prefixed data", func(t *testing.T) {
		// Arrange
		frame := []byte{0x00, 0x00, 0x00, 0x05, 0x01, 0x02, 0x03, 0x04, 0x05}
		conn := &testkit.MockTCPConn{
			ToRead: frame,
		}
		transport := newTestTCPTransport(conn)

		// Act
		data, err := transport.Read(context.Background())

		// Assert
		require.NoError(t, err)
		assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x04, 0x05}, data)
	})

	t.Run("empty payload", func(t *testing.T) {
		// Arrange
		frame := []byte{0x00, 0x00, 0x00, 0x00}
		conn := &testkit.MockTCPConn{
			ToRead: frame,
		}
		transport := newTestTCPTransport(conn)

		// Act
		data, err := transport.Read(context.Background())

		// Assert
		require.NoError(t, err)
		assert.Equal(t, []byte{}, data)
	})
}

// TestShouldTimeoutReadGivenShortDeadlineWhenReadCalled tests read timeout handling.
func TestShouldTimeoutReadGivenShortDeadlineWhenReadCalled(t *testing.T) {
	// Arrange
	conn := &testkit.MockTCPConn{
		Blocked: true, // Block forever
	}
	transport := newTestTCPTransport(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// Act
	_, err := transport.Read(ctx)

	// Assert
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestShouldCancelReadGivenCanceledContextWithoutDeadlineWhenReadCalled(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})
	transport := &TCPTransport{
		conn:   clientConn,
		addr:   "pipe",
		reader: bufio.NewReader(clientConn),
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

// TestShouldCloseGracefullyGivenOpenTransportWhenCloseCalled tests TCP close behavior.
func TestShouldCloseGracefullyGivenOpenTransportWhenCloseCalled(t *testing.T) {
	// Arrange
	conn := &testkit.MockTCPConn{}
	transport := newTestTCPTransport(conn)

	// Act
	err := transport.Close()

	// Assert
	require.NoError(t, err)
	assert.True(t, conn.Closed)
}

// TestShouldReturnRemoteAddrGivenTransportWhenRemoteAddrCalled tests remote address reporting.
func TestShouldReturnRemoteAddrGivenTransportWhenRemoteAddrCalled(t *testing.T) {
	// Arrange
	conn := &testkit.MockTCPConn{
		RemoteAddrString: "127.0.0.1:4091",
	}
	transport := newTestTCPTransport(conn)

	// Act
	addr := transport.RemoteAddr()

	// Assert
	assert.Equal(t, "127.0.0.1:4091", addr)
}

// TestShouldRejectWriteGivenCanceledContextWhenWriteCalled tests context cancellation handling.
func TestShouldRejectWriteGivenCanceledContextWhenWriteCalled(t *testing.T) {
	// Arrange
	conn := &testkit.MockTCPConn{}
	transport := newTestTCPTransport(conn)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel before write

	// Act
	err := transport.Write(ctx, []byte{0x01})

	// Assert
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// Mock TCP connection is now provided by testkit.MockTCPConn from internal/testkit package

// Benchmarks
func BenchmarkWriteFrame(b *testing.B) {
	b.Run("100 byte payload", func(b *testing.B) {
		conn := &testkit.MockTCPConn{
			Written: make([]byte, 0),
		}
		transport := newTestTCPTransport(conn)
		payload := make([]byte, 100)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_ = transport.Write(context.Background(), payload)
		}
	})

	b.Run("10KB payload", func(b *testing.B) {
		conn := &testkit.MockTCPConn{
			Written: make([]byte, 0),
		}
		transport := newTestTCPTransport(conn)
		payload := make([]byte, 10240)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_ = transport.Write(context.Background(), payload)
		}
	})
}

func BenchmarkReadFrame(b *testing.B) {
	b.Run("100 byte payload", func(b *testing.B) {
		// Prepare frame with 100 byte payload
		frame := make([]byte, 4+100)
		frame[3] = 100 // Length = 100
		conn := &testkit.MockTCPConn{
			ToRead: frame,
		}
		transport := newTestTCPTransport(conn)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			conn.ReadPos = 0 // Reset for next iteration
			_, _ = transport.Read(context.Background())
		}
	})
}
