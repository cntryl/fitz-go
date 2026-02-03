package integration

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/cntryl-go/test/fixture"
)

// TestShouldConnectViaTCPGivenValidAddressWhenTCPTransportUsed verifies
// TCP transport connection establishment (AC-CONN-001).
func TestShouldConnectViaTCPGivenValidAddressWhenTCPTransportUsed(t *testing.T) {
	// Arrange
	f := fixture.NewTestFixture(t, fixture.TransportTCP)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Act
	err := f.Connect(ctx)

	// Assert
	if err != nil {
		t.Fatalf("TCP connection failed: %v (expected successful connection per AC-CONN-001)", err)
	}

	// Verify client is accessible and not nil.
	client := f.Client()
	if client == nil {
		t.Fatal("expected non-nil client after successful connection")
	}
}

// TestShouldConnectViaWebSocketGivenValidAddressWhenWebSocketTransportUsed
// verifies WebSocket transport connection establishment (AC-CONN-001).
// NOTE: Currently failing - Bug #001: WebSocket not implemented in broker.
func TestShouldConnectViaWebSocketGivenValidAddressWhenWebSocketTransportUsed(t *testing.T) {
	// Arrange
	f := fixture.NewTestFixture(t, fixture.TransportWebSocket)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Act
	err := f.Connect(ctx)

	// Assert
	if err != nil {
		t.Fatalf("WebSocket connection failed: %v (expected successful connection per AC-CONN-001)", err)
	}

	// Verify client is accessible and not nil.
	client := f.Client()
	if client == nil {
		t.Fatal("expected non-nil client after successful connection")
	}
}

// TestShouldProduceIdenticalBehaviorGivenSameOperationWhenDifferentTransports
// verifies protocol equivalence between TCP and WebSocket per CLIENT_SPEC.md.
// This tests AC-CONN-004: anonymous mode when FITZ_AUTH_REQUIRED=false.
func TestShouldProduceIdenticalBehaviorGivenSameOperationWhenDifferentTransports(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := f.Connect(ctx); err != nil {
			t.Fatalf("failed to connect: %v", err)
		}

		// Act - verify connection succeeds with empty JWT (anonymous mode)
		client := f.Client()

		// Assert
		if client == nil {
			t.Fatal("expected non-nil client in anonymous mode (AC-CONN-004)")
		}

		// TODO: Add domain operation tests once domain clients are implemented.
		// Per CLIENT_SPEC.md: "A client receiving the same payload over both transports MUST produce identical behavior."
	})
}

// TestShouldReconnectGivenConnectionLostWhenReconnectAttempted verifies
// reconnection behavior after connection loss.
func TestShouldReconnectGivenConnectionLostWhenReconnectAttempted(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := f.Connect(ctx); err != nil {
			t.Fatalf("initial connection failed: %v", err)
		}

		// Act & Assert
		t.Fatal("Reconnection logic not yet implemented")
	})
}

// TestShouldDropSessionStateGivenDisconnectWhenReconnect verifies session
// state (subscriptions, transactions) is lost on disconnect per CLIENT_SPEC.md.
func TestShouldDropSessionStateGivenDisconnectWhenReconnect(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := f.Connect(ctx); err != nil {
			t.Fatalf("failed to connect: %v", err)
		}

		// Act & Assert
		t.Fatal("Session state lifecycle not yet implemented")
	})
}

// TestShouldHandleFrameSizeLimitGivenLargePayloadWhenFrameExceedsMax verifies
// broker closes connection when frame exceeds max_frame_size.
func TestShouldHandleFrameSizeLimitGivenLargePayloadWhenFrameExceedsMax(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := f.Connect(ctx); err != nil {
			t.Fatalf("failed to connect: %v", err)
		}

		// Act & Assert
		t.Fatal("Frame size enforcement not yet implemented")
	})
}
