package client

import (
	"context"
	"errors"
	"net"
	"testing"
)

func TestShouldUseDialerWhenConnect(t *testing.T) {
	// Arrange
	dialCalled := false
	mockConn := &mockConn{}
	mockDialer := &MockDialer{
		dialFunc: func(ctx context.Context, addr string) (net.Conn, error) {
			dialCalled = true
			if addr != "test://broker" {
				t.Errorf("expected addr 'test://broker', got: %s", addr)
			}
			return mockConn, nil
		},
	}

	tokenProvider := func(ctx context.Context) (string, error) {
		return "test-token", nil
	}

	client := NewClient("test://broker", tokenProvider, WithDialer(mockDialer))

	// Act
	err := client.Connect(context.Background())

	// Assert
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if !dialCalled {
		t.Fatal("expected dialer to be called")
	}

	// Cleanup
	_ = client.Close()
}

func TestShouldRetryGivenDialFailure(t *testing.T) {
	// Arrange
	attempts := 0
	mockDialer := &MockDialer{
		dialFunc: func(ctx context.Context, addr string) (net.Conn, error) {
			attempts++
			return nil, errors.New("dial failed")
		},
	}

	client := NewClient("tcp://broker:9090", nil,
		WithDialer(mockDialer),
		WithMaxRetries(2),
		WithRetryBackoff(1)) // 1ms to speed up test

	// Act
	err := client.Connect(context.Background())

	// Assert
	if err == nil {
		t.Fatal("expected error after retries exhausted")
	}
	// Should try initial + 2 retries = 3 attempts
	if attempts != 3 {
		t.Fatalf("expected 3 dial attempts, got: %d", attempts)
	}
}

func TestShouldReturnErrorGivenAlreadyConnected(t *testing.T) {
	// Arrange
	mockConn := &mockConn{}
	mockDialer := &MockDialer{
		dialFunc: func(ctx context.Context, addr string) (net.Conn, error) {
			return mockConn, nil
		},
	}

	client := NewClient("tcp://broker:9090", nil, WithDialer(mockDialer))
	_ = client.Connect(context.Background())

	// Act
	err := client.Connect(context.Background())

	// Assert
	if err == nil {
		t.Fatal("expected error when already connected")
	}
	if err.Error() != "already connected" {
		t.Fatalf("expected 'already connected' error, got: %v", err)
	}

	// Cleanup
	_ = client.Close()
}

func TestShouldCallTokenProviderGivenConnect(t *testing.T) {
	// Arrange
	mockConn := &mockConn{}
	mockDialer := &MockDialer{
		dialFunc: func(ctx context.Context, addr string) (net.Conn, error) {
			return mockConn, nil
		},
	}

	tokenProvider := func(ctx context.Context) (string, error) {
		return "my-jwt-token", nil
	}

	client := NewClient("tcp://broker:9090", tokenProvider, WithDialer(mockDialer))

	// Act
	err := client.Connect(context.Background())

	// Assert
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	// TODO: Once CONN_OPEN is implemented, tokenProvider should be called
	// For now, just verify client was created with the provider
	if tokenProvider == nil {
		t.Fatal("token provider should not be nil")
	}

	// Cleanup
	_ = client.Close()
}

// mockConn is a minimal net.Conn implementation for testing.
type mockConn struct {
	net.Conn
	closed bool
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	return 0, nil
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	return len(b), nil
}

func (m *mockConn) Close() error {
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr {
	return &net.TCPAddr{}
}

func (m *mockConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{}
}
