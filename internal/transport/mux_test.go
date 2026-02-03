package transport

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func readFrameWithTimeout(ch <-chan Frame, timeout time.Duration) (Frame, error) {
	select {
	case f, ok := <-ch:
		if !ok {
			return Frame{}, context.Canceled
		}
		return f, nil
	case <-time.After(timeout):
		return Frame{}, context.DeadlineExceeded
	}
}

// TestShouldDeliverSentFrameGivenMuxStartedWhenSendCalled
func TestShouldDeliverSentFrameGivenMuxStartedWhenSendCalled(t *testing.T) {
	// Arrange
	c1, c2 := net.Pipe()
	m1 := NewMux(c1)
	m2 := NewMux(c2)
	m1.Start()
	m2.Start()
	defer func() {
		_ = m1.Close()
		_ = m2.Close()
		c1.Close()
		c2.Close()
	}()
	f := Frame{Type: FrameTypeConnOpen, Flags: 0, Channel: 7, Body: []byte("payload")}

	// Act
	require.NoError(t, m1.Send(f))
	got, err := readFrameWithTimeout(m2.In(), 500*time.Millisecond)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, f.Channel, got.Channel)
	assert.Equal(t, f.Body, got.Body)
}

// TestShouldProduceHeartbeatGivenStartHeartbeatWhenRunning
func TestShouldProduceHeartbeatGivenStartHeartbeatWhenRunning(t *testing.T) {
	// Arrange
	c1, c2 := net.Pipe()
	m1 := NewMux(c1)
	m2 := NewMux(c2)
	m1.Start()
	m2.Start()
	defer func() { _ = m1.Close(); _ = m2.Close(); c1.Close(); c2.Close() }()

	// Act: start heartbeat on m1 (uses m1's context, runs until mux closes)
	go m1.StartHeartbeat(50 * time.Millisecond)

	// Assert: ensure we receive at least one heartbeat on m2
	got, err := readFrameWithTimeout(m2.In(), 500*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, FrameTypeHeartbeat, got.Type)
}

// fakeWS is a minimal in-process WebSocket-like connection used for testing.
// It exposes Write and ReadMessage so the mux will take the WebSocket path.
type fakeWS struct {
	messages chan []byte
}

func newFakeWS() *fakeWS {
	return &fakeWS{messages: make(chan []byte, 8)}
}

func (f *fakeWS) Write(p []byte) (int, error) {
	data := make([]byte, len(p))
	copy(data, p)
	select {
	case f.messages <- data:
		return len(p), nil
	default:
		return 0, io.ErrShortWrite
	}
}

func (f *fakeWS) ReadMessage() ([]byte, error) {
	msg, ok := <-f.messages
	if !ok {
		return nil, io.EOF
	}
	return msg, nil
}

func (f *fakeWS) Read(p []byte) (int, error) { return 0, io.EOF }
func (f *fakeWS) Close() error               { close(f.messages); return nil }

// TestMux_WebSocketEncodeDecode_HeaderIncluded verifies that WebSocket writes
// include the frame header (type|flags|channel) and CONNECT wraps token in TagToken TLV.
func TestMux_WebSocketEncodeDecode_HeaderIncluded(t *testing.T) {
	// Arrange
	fw := newFakeWS()
	m := NewMux(fw)

	// Act / Assert: non-CONNECT frame should roundtrip header+body
	f := Frame{Type: FrameTypeAck, Flags: 0, Channel: 6, Body: []byte{0x01, 0x02, 0x03}}
	require.NoError(t, m.encodeFrame(f))
	got, err := m.decodeFrame()
	require.NoError(t, err)
	assert.Equal(t, f.Type, got.Type)
	assert.Equal(t, f.Channel, got.Channel)
	assert.Equal(t, f.Body, got.Body)

	// CONNECT frame should wrap token in TagToken TLV
	conn := Frame{Type: FrameTypeConnOpen, Flags: 0, Channel: 0, Body: []byte("tok")}
	require.NoError(t, m.encodeFrame(conn))
	got2, err := m.decodeFrame()
	require.NoError(t, err)
	dec, derr := NewTLVDecoder(got2.Body)
	require.NoError(t, derr)
	assert.Equal(t, "tok", dec.GetString(TagToken))
}
