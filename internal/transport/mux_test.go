package transport

import (
	"context"
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
