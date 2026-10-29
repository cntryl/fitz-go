package client

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/core/transport"
	"github.com/cntryl/fitz-go/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestShouldWaitForServerHelloBeyondAuthenticationSettlement(t *testing.T) {
	// Arrange
	scripted := newScriptedTransport()
	scripted.helloOnConnect = false
	originalDial := dialTCPTransport
	dialTCPTransport = func(context.Context, string) (transport.Transport, error) { return scripted, nil }
	t.Cleanup(func() { dialTCPTransport = originalDial })
	client := NewClient("localhost:4091", nil)
	client.config.AuthSettleDelay = time.Millisecond
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	connected := make(chan error, 1)

	// Act
	go func() { connected <- client.Connect(ctx) }()
	select {
	case <-connected:
		t.Fatal("Connect completed before capability negotiation")
	case <-time.After(25 * time.Millisecond):
	}
	scripted.PushReadFrame(protocol.EncodeFrame(protocol.MessageTypeServerHello, wire2HelloPayload()))

	// Assert
	require.NoError(t, <-connected)
	_, capabilities := client.currentConnection().ServerCapabilities()
	require.Equal(t, protocol.CapabilityRPCCancellation, capabilities)
}

func TestShouldSurfaceVersionMismatchWithoutRetryWhenConnecting(t *testing.T) {
	// Arrange
	originalDial := dialTCPTransport
	t.Cleanup(func() { dialTCPTransport = originalDial })
	attempts := 0
	dialTCPTransport = func(context.Context, string) (transport.Transport, error) {
		attempts++
		return versionMismatchTransport(), nil
	}
	client := NewClient("localhost:4091", nil)
	client.config.ReconnectBackoff = time.Millisecond
	client.config.ReconnectMaxDelay = time.Millisecond
	client.config.MaxReconnects = 3
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Act
	err := client.ConnectWhenReady(ctx)

	// Assert
	require.Error(t, err)
	require.ErrorIs(t, err, connection.ErrVersionMismatch)
	require.ErrorContains(t, err, "version mismatch")
	require.ErrorContains(t, err, "no common wire version")
	require.Equal(t, 1, attempts)
}

func TestShouldStopReconnectGivenVersionMismatch(t *testing.T) {
	// Arrange
	originalDial := dialTCPTransport
	t.Cleanup(func() { dialTCPTransport = originalDial })
	attempts := 0
	dialTCPTransport = func(context.Context, string) (transport.Transport, error) {
		attempts++
		return versionMismatchTransport(), nil
	}
	client := NewClient("localhost:4091", nil)
	client.config.ReconnectBackoff = time.Millisecond
	client.config.ReconnectMaxDelay = time.Millisecond
	client.config.MaxReconnects = 3
	t.Cleanup(func() { _ = client.Close() })

	// Act
	client.beginReconnect(connection.ErrConnectionClosed)

	// Assert
	require.Equal(t, 1, attempts)
}

func versionMismatchTransport() *scriptedTransport {
	scripted := newScriptedTransport()
	scripted.helloOnConnect = false
	message := []byte("no common wire version")
	payload := make([]byte, 6+len(message))
	binary.BigEndian.PutUint16(payload[:2], 50)
	binary.BigEndian.PutUint32(payload[2:6], uint32(len(message)))
	copy(payload[6:], message)
	scripted.PushReadFrame(protocol.EncodeFrame(protocol.MessageTypeConnectRejected, payload))
	return scripted
}

func TestShouldFailConnectionWithinCallerDeadlineWhenServerHelloIsAbsent(t *testing.T) {
	// Arrange
	scripted := newScriptedTransport()
	scripted.helloOnConnect = false
	originalDial := dialTCPTransport
	dialTCPTransport = func(context.Context, string) (transport.Transport, error) { return scripted, nil }
	t.Cleanup(func() { dialTCPTransport = originalDial })
	client := NewClient("localhost:4091", nil)
	client.config.AuthSettleDelay = time.Millisecond
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	// Act
	err := client.Connect(ctx)

	// Assert
	require.ErrorIs(t, err, context.DeadlineExceeded)
	select {
	case <-scripted.closed:
	default:
		t.Fatal("unnegotiated transport remained open")
	}
}
