package testkit

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/protocol"
	"github.com/stretchr/testify/require"
)

func requestFrame(t *testing.T, kind uint16, correlation uint64) []byte {
	t.Helper()
	frame := protocol.EncodeFrameOwnedWithHeader(kind, 0, correlation, nil)
	require.NotNil(t, frame)
	defer frame.Release()
	return append([]byte(nil), frame.Bytes()...)
}

func TestShouldCorrelateAndCompleteSingleResponse(t *testing.T) {
	// Arrange
	request := requestFrame(t, protocol.MessageTypeKvBegin, 42)
	response := protocol.EncodeFrame(protocol.MessageTypeKvBegin, []byte{0})

	// Act
	decoded, err := protocol.DecodeMessage(CorrelateResponse(response, [][]byte{request}))

	// Assert
	require.NoError(t, err)
	require.Equal(t, uint64(42), decoded.CorrelationID)
	require.Equal(t, protocol.FlagResponseFinal, decoded.Flags)
}

func TestShouldKeepQueuedLeaseResponseOpen(t *testing.T) {
	// Arrange
	request := requestFrame(t, protocol.MessageTypeLeaseAcquire, 42)
	response := protocol.EncodeFrame(protocol.MessageTypeLeaseAcquire, []byte{0, 2})

	// Act
	decoded, err := protocol.DecodeMessage(CorrelateResponse(response, [][]byte{request}))

	// Assert
	require.NoError(t, err)
	require.Equal(t, uint64(42), decoded.CorrelationID)
	require.Zero(t, decoded.Flags&protocol.FlagResponseFinal)
}

func TestShouldPreserveExplicitIntermediateResponse(t *testing.T) {
	// Arrange
	request := requestFrame(t, protocol.MessageTypeKvBegin, 42)
	response := requestFrame(t, protocol.MessageTypeKvBegin, 99)

	// Act
	actual := CorrelateResponse(response, [][]byte{request})

	// Assert
	require.Equal(t, response, actual)
}

func TestShouldLeaveUnsolicitedNotificationUncorrelated(t *testing.T) {
	// Arrange
	request := requestFrame(t, protocol.MessageTypeKvNotify, 42)
	response := protocol.EncodeFrame(protocol.MessageTypeKvNotify, []byte{0})

	// Act
	actual := CorrelateResponse(response, [][]byte{request})

	// Assert
	require.Equal(t, response, actual)
}

func TestShouldCompleteQueuedMockResponseAfterMatchingRequest(t *testing.T) {
	// Arrange
	transport := NewWire2MockTransport()
	transport.SetReadFrames([][]byte{protocol.EncodeFrame(protocol.MessageTypeKvBegin, []byte{0})})
	request := requestFrame(t, protocol.MessageTypeKvBegin, 42)
	require.NoError(t, transport.Write(context.Background(), request))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Act
	response, err := transport.Read(ctx)

	// Assert
	require.NoError(t, err)
	decoded, err := protocol.DecodeMessage(response)
	require.NoError(t, err)
	require.Equal(t, uint64(42), decoded.CorrelationID)
	require.Equal(t, protocol.FlagResponseFinal, decoded.Flags)
}

func TestShouldKeepMockResponsePendingUntilMatchingRequest(t *testing.T) {
	// Arrange
	transport := NewWire2MockTransport()
	transport.SetReadFrames([][]byte{protocol.EncodeFrame(protocol.MessageTypeKvBegin, []byte{0})})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	// Act
	_, earlyErr := transport.Read(ctx)
	require.NoError(t, transport.Write(context.Background(), requestFrame(t, protocol.MessageTypeKvBegin, 42)))
	readyCtx, readyCancel := context.WithTimeout(context.Background(), time.Second)
	defer readyCancel()
	response, err := transport.Read(readyCtx)

	// Assert
	require.ErrorIs(t, earlyErr, context.DeadlineExceeded)
	require.NoError(t, err)
	decoded, err := protocol.DecodeMessage(response)
	require.NoError(t, err)
	require.Equal(t, uint64(42), decoded.CorrelationID)
	require.Equal(t, protocol.FlagResponseFinal, decoded.Flags)
}
