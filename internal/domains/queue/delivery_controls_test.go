package queue

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/protocol"
	"github.com/cntryl/fitz-go/internal/testkit"
	"github.com/stretchr/testify/require"
)

func TestShouldEncodeQueueTTLExactlyLikeBrokerVector(t *testing.T) {
	// Arrange
	hours := uint32(2)
	var payload bytes.Buffer
	// Act
	enqueueWriterWithTTL("queue://acme/jobs/q", []byte("job"), enqueueConfig{ttlHours: &hours})(&payload)
	frame := protocol.EncodeFrameOwnedWithHeader(200, 4, 1, payload.Bytes())
	defer frame.Release()
	// Assert
	require.Equal(t, "00c80400000000000000010000002d0000001f0000001371756575653a2f2f61636d652f6a6f62732f71000000036a6f620000010000000400000002", hex.EncodeToString(frame.Bytes()))
}

func TestShouldCloseReservationAfterSuccessfulDelayedNack(t *testing.T) {
	// Arrange
	transport := testkit.NewWire2MockTransport()
	conn := connection.New(transport, connection.Config{})
	require.NoError(t, conn.Start(context.Background()))
	t.Cleanup(func() { _ = conn.Close() })
	item := &QueueItem{ID: 1, Token: 9, Route: "queue://acme/jobs/q", conn: conn}
	transport.SetReadFrames([][]byte{protocol.EncodeFrame(210, []byte{0})})
	// Act
	err := item.Nack(context.Background(), 60)
	// Assert
	require.NoError(t, err)
	frames := transport.GetWrittenFrames()
	frame, err := protocol.DecodeMessage(frames[1])
	require.NoError(t, err)
	require.Equal(t, uint16(210), frame.MessageType)
	require.Equal(t, byte(60), frame.Payload[len(frame.Payload)-1])
	require.ErrorIs(t, item.Complete(context.Background()), connection.ErrStaleHandle)
}
