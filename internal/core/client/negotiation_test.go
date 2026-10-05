package client

import (
	"context"
	"testing"
	"time"

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
	scripted.PushReadFrame(protocol.EncodeFrame(protocol.MessageTypeServerHello, []byte{0, 1, 0, 0, 0, byte(protocol.CapabilityRPCCancellation)}))

	// Assert
	require.NoError(t, <-connected)
	_, capabilities := client.currentConnection().ServerCapabilities()
	require.Equal(t, protocol.CapabilityRPCCancellation, capabilities)
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
