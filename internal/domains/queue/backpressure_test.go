package queue

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestShouldPreserveCodedCapacityRejectionForPlainQueueOperation(t *testing.T) {
	// Arrange
	message := []byte("not accepted")
	payload := make([]byte, 9+len(message))
	payload[0] = 1
	binary.BigEndian.PutUint32(payload[1:5], 4005)
	binary.BigEndian.PutUint32(payload[5:9], uint32(len(message)))
	copy(payload[9:], message)
	// Act
	_, _, err := parsePlainQueueResponse(payload)
	// Assert
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected capacity rejection, got %v", err)
	}
}

func TestShouldPreserveLongPlainErrorWithoutInferringCapacity(t *testing.T) {
	// Arrange
	message := strings.Repeat("x", 4005)
	payload := make([]byte, 5+len(message))
	payload[0] = 1
	binary.BigEndian.PutUint32(payload[1:5], uint32(len(message)))
	copy(payload[5:], message)
	// Act
	_, _, err := parsePlainQueueResponse(payload)
	// Assert
	if err == nil || err.Error() != message || errors.Is(err, ErrQueueFull) {
		t.Fatalf("incorrect legacy error: %v", err)
	}
}

func TestShouldRejectMalformedCapacityEnvelopeWithoutRetryClassification(t *testing.T) {
	// Arrange
	payload := []byte{1, 0, 0, 15, 165, 0, 0, 0, 2, 'x'}
	// Act
	_, _, err := parsePlainQueueResponse(payload)
	// Assert
	if err == nil || errors.Is(err, ErrQueueFull) {
		t.Fatalf("malformed capacity response was classified: %v", err)
	}
}

func TestShouldInvalidateReservationGivenSuccessfulRetryWhenCompletionAcknowledged(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	trans := newScriptedRestoreTransport()
	conn := connection.New(trans, connection.Config{Token: "", ReadTimeout: time.Second})
	require.NoError(t, conn.Start(context.Background()))
	t.Cleanup(func() { _ = conn.Close() })
	baseWrites := restoreWriteCount(trans)
	item := &QueueItem{ID: 7, Token: 11, Route: "queue://realm/app/jobs", conn: conn}
	go func() {
		waitForRestoreWrites(t, trans, baseWrites+1)
		trans.enqueue(queueRestoreFrame(t, protocol.MessageTypeQueueComplete, []byte{1, 0, 0, 15, 165, 0, 0, 0, 0}))
		waitForRestoreWrites(t, trans, baseWrites+2)
		trans.enqueue(queueRestoreFrame(t, protocol.MessageTypeQueueComplete, []byte{0}))
	}()
	require.ErrorIs(t, item.Complete(ctx), ErrQueueFull)
	// Act
	require.NoError(t, item.Complete(ctx))
	// Assert
	require.ErrorIs(t, item.Complete(ctx), connection.ErrStaleHandle)
	require.Equal(t, baseWrites+2, restoreWriteCount(trans))
	trans.mu.Lock()
	defer trans.mu.Unlock()
	require.Equal(t, trans.written[baseWrites], trans.written[baseWrites+1])
}

func TestShouldRejectUnknownStatusWithoutClassifyingCapacity(t *testing.T) {
	// Arrange
	payload := []byte{2, 0, 0, 15, 165, 0, 0, 0, 0}
	// Act
	_, _, err := parseQueueResponse(payload)
	// Assert
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrQueueFull)
}

func TestShouldRejectMalformedAcknowledgementGivenTrailingBytesWhenCompleting(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	trans := newScriptedRestoreTransport()
	conn := connection.New(trans, connection.Config{Token: "", ReadTimeout: time.Second})
	require.NoError(t, conn.Start(ctx))
	t.Cleanup(func() { _ = conn.Close() })
	baseWrites := restoreWriteCount(trans)
	item := &QueueItem{ID: 7, Token: 11, Route: "queue://realm/app/jobs", conn: conn}
	go func() {
		waitForRestoreWrites(t, trans, baseWrites+1)
		trans.enqueue(queueRestoreFrame(t, protocol.MessageTypeQueueComplete, []byte{0, 120}))
	}()
	// Act
	err := item.Complete(ctx)
	// Assert
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrQueueFull)
}
