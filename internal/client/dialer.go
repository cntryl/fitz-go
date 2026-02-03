package client

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Dialer creates network connections for different transport protocols.
// This interface allows for easy testing and transport abstraction.
type Dialer interface {
	// Dial establishes a connection based on the address scheme.
	Dial(ctx context.Context, addr string) (net.Conn, error)
}

// DefaultDialer implements the Dialer interface with support for
// TCP and WebSocket transports.
type DefaultDialer struct{}

// Dial establishes a network connection based on the address scheme.
// Supports:
//   - "host:port" or "tcp://host:port" for TCP connections
//   - "ws://host:port/path" for WebSocket connections
//   - "wss://host:port/path" for secure WebSocket connections
func (d *DefaultDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	// If no scheme, assume TCP
	if !strings.Contains(addr, "://") {
		return d.dialTCP(ctx, addr)
	}

	u, err := url.Parse(addr)
	if err != nil {
		return nil, fmt.Errorf("invalid address: %w", err)
	}

	switch u.Scheme {
	case "tcp":
		return d.dialTCP(ctx, u.Host)
	case "ws", "wss":
		return d.dialWebSocket(ctx, addr)
	default:
		return nil, fmt.Errorf("unsupported transport scheme: %s", u.Scheme)
	}
}

// dialTCP establishes a TCP connection.
func (d *DefaultDialer) dialTCP(ctx context.Context, addr string) (net.Conn, error) {
	var dialer net.Dialer
	return dialer.DialContext(ctx, "tcp", addr)
}

// dialWebSocket establishes a WebSocket connection and returns a net.Conn adapter.
// Uses gorilla/websocket for reliable WebSocket support. Returns a wrapper that
// implements net.Conn by translating WebSocket binary messages to Read/Write calls.
func (d *DefaultDialer) dialWebSocket(ctx context.Context, addr string) (net.Conn, error) {
	// Use gorilla websocket.DefaultDialer for standard WebSocket connection.
	dialer := websocket.DefaultDialer

	// Dial the WebSocket connection.
	conn, _, err := dialer.DialContext(ctx, addr, nil)
	if err != nil {
		return nil, fmt.Errorf("websocket dial failed: %w", err)
	}

	// Wrap the WebSocket connection to implement net.Conn.
	return &wsConnAdapter{conn: conn}, nil
}

// wsConnAdapter adapts a WebSocket connection to implement net.Conn.
// Per CLIENT_SPEC.md, WebSocket transport uses binary frames where "each binary frame = one complete TLV frame payload".
type wsConnAdapter struct {
	conn     *websocket.Conn
	readBuf  []byte
	readMu   sync.Mutex
	writeMu  sync.Mutex
	closeErr error
}

// Read implements net.Conn.Read by reading a WebSocket binary message.
func (w *wsConnAdapter) Read(p []byte) (n int, err error) {
	w.readMu.Lock()
	defer w.readMu.Unlock()

	// If we have buffered data from a previous message, return that first.
	if len(w.readBuf) > 0 {
		n = copy(p, w.readBuf)
		w.readBuf = w.readBuf[n:]
		return n, nil
	}

	// Read a new WebSocket message.
	messageType, data, err := w.conn.ReadMessage()
	if err != nil {
		return 0, err
	}

	if messageType != websocket.BinaryMessage {
		return 0, fmt.Errorf("expected binary message, got type %d", messageType)
	}

	// Copy what we can into p, buffer the rest.
	n = copy(p, data)
	if n < len(data) {
		w.readBuf = data[n:]
	}

	return n, nil
}

// ReadMessage returns the next WebSocket binary message as a single byte slice.
// This is used by transport.Mux to read whole binary frames (WebSocket path).
func (w *wsConnAdapter) ReadMessage() ([]byte, error) {
	for {
		messageType, data, err := w.conn.ReadMessage()
		if err != nil {
			return nil, err
		}
		if messageType != websocket.BinaryMessage {
			// Ignore non-binary frames and continue reading.
			continue
		}
		return data, nil
	}
}

// Write implements net.Conn.Write by sending a WebSocket binary message.
// Per CLIENT_SPEC.md: "Each binary frame = one complete TLV frame payload".
func (w *wsConnAdapter) Write(p []byte) (n int, err error) {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	if err := w.conn.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}

	return len(p), nil
}

// Close closes the WebSocket connection.
func (w *wsConnAdapter) Close() error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	// Send close message.
	err := w.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	if err != nil {
		w.closeErr = err
	}

	// Close underlying connection.
	return w.conn.Close()
}

// LocalAddr returns the local network address.
func (w *wsConnAdapter) LocalAddr() net.Addr {
	return w.conn.LocalAddr()
}

// RemoteAddr returns the remote network address.
func (w *wsConnAdapter) RemoteAddr() net.Addr {
	return w.conn.RemoteAddr()
}

// SetDeadline sets the read and write deadlines.
func (w *wsConnAdapter) SetDeadline(t time.Time) error {
	if err := w.conn.SetReadDeadline(t); err != nil {
		return err
	}
	return w.conn.SetWriteDeadline(t)
}

// SetReadDeadline sets the deadline for future Read calls.
func (w *wsConnAdapter) SetReadDeadline(t time.Time) error {
	return w.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the deadline for future Write calls.
func (w *wsConnAdapter) SetWriteDeadline(t time.Time) error {
	return w.conn.SetWriteDeadline(t)
}

var _ net.Conn = (*wsConnAdapter)(nil)
var _ io.Reader = (*wsConnAdapter)(nil)
var _ io.Writer = (*wsConnAdapter)(nil)
