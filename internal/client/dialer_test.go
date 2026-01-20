package client

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

func TestShouldDialTCPGivenPlainHostPort(t *testing.T) {
	// Arrange
	dialer := &DefaultDialer{}
	addr := "localhost:9999" // Use unlikely port to avoid accidental connection

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	conn, err := dialer.Dial(ctx, addr)

	// Assert
	if conn != nil {
		conn.Close()
	}
	// We expect error since nothing is listening, but the dial attempt should be made
	if err == nil {
		t.Fatal("expected connection error to non-existent service")
	}
	// Verify it's a TCP dial attempt (not a parse error)
	if strings.Contains(err.Error(), "invalid address") {
		t.Fatalf("should not be a parse error: %v", err)
	}
}

func TestShouldDialTCPGivenTCPScheme(t *testing.T) {
	// Arrange
	dialer := &DefaultDialer{}
	addr := "tcp://localhost:9999"

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	conn, err := dialer.Dial(ctx, addr)

	// Assert
	if conn != nil {
		conn.Close()
	}
	// We expect error since nothing is listening
	if err == nil {
		t.Fatal("expected connection error to non-existent service")
	}
	// Verify it's a TCP dial attempt
	if strings.Contains(err.Error(), "invalid address") || strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("should attempt TCP dial, got: %v", err)
	}
}

func TestShouldReturnErrorGivenInvalidAddress(t *testing.T) {
	// Arrange
	dialer := &DefaultDialer{}
	addr := "://invalid"

	// Act
	ctx := context.Background()
	conn, err := dialer.Dial(ctx, addr)

	// Assert
	if conn != nil {
		conn.Close()
		t.Fatal("expected no connection for invalid address")
	}
	if err == nil {
		t.Fatal("expected error for invalid address")
	}
	if !strings.Contains(err.Error(), "invalid address") {
		t.Fatalf("expected 'invalid address' error, got: %v", err)
	}
}

func TestShouldReturnErrorGivenUnsupportedScheme(t *testing.T) {
	// Arrange
	dialer := &DefaultDialer{}
	addr := "http://localhost:8080"

	// Act
	ctx := context.Background()
	conn, err := dialer.Dial(ctx, addr)

	// Assert
	if conn != nil {
		conn.Close()
		t.Fatal("expected no connection for unsupported scheme")
	}
	if err == nil {
		t.Fatal("expected error for unsupported scheme")
	}
	if !strings.Contains(err.Error(), "unsupported transport scheme") {
		t.Fatalf("expected 'unsupported transport scheme' error, got: %v", err)
	}
}

func TestShouldRespectContextCancellationGivenTCPDial(t *testing.T) {
	// Arrange
	dialer := &DefaultDialer{}
	addr := "localhost:9999"
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Act
	conn, err := dialer.Dial(ctx, addr)

	// Assert
	if conn != nil {
		conn.Close()
		t.Fatal("expected no connection when context is cancelled")
	}
	if err == nil {
		t.Fatal("expected error when context is cancelled")
	}
}

// MockDialer is a test helper that implements the Dialer interface.
type MockDialer struct {
	dialFunc func(ctx context.Context, addr string) (net.Conn, error)
}

func (m *MockDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	if m.dialFunc != nil {
		return m.dialFunc(ctx, addr)
	}
	return nil, nil
}

func TestShouldUseMockDialerGivenCustomDialer(t *testing.T) {
	// Arrange
	dialCalled := false
	mockDialer := &MockDialer{
		dialFunc: func(ctx context.Context, addr string) (net.Conn, error) {
			dialCalled = true
			if addr != "test://addr" {
				t.Errorf("expected addr 'test://addr', got: %s", addr)
			}
			return nil, nil
		},
	}

	// Act
	_, _ = mockDialer.Dial(context.Background(), "test://addr")

	// Assert
	if !dialCalled {
		t.Fatal("expected dial to be called")
	}
}
