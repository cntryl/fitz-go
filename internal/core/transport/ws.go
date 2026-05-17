//nolint:gosec,errcheck,gocritic
package transport

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// WebSocket opcodes (RFC 6455)
const (
	opcodeContinuation = 0x0
	opcodeText         = 0x1
	opcodeBinary       = 0x2
	opcodeClose        = 0x8
	opcodePing         = 0x9
	opcodePong         = 0xA
)

// WebSocket close status codes
const (
	statusNormalClosure = 1000
	statusGoingAway     = 1001
)

// WebSocketTransport implements Transport for WebSocket connections.
// Each WebSocket frame is a complete TLV message (no additional framing needed).
// Per CLIENT_SPEC.md: Binary frames only, one message per frame.
// Implements RFC 6455 WebSocket protocol.
type WebSocketTransport struct {
	conn   net.Conn
	reader *bufio.Reader
	addr   string
	closed atomic.Bool
	mu     sync.Mutex // Protects conn writes (WebSocket frames must be written atomically)
}

// DialWebSocket creates a WebSocket transport to the specified URL.
// URL should use ws:// or wss:// scheme.
// Performs HTTP upgrade handshake per RFC 6455.
func DialWebSocket(ctx context.Context, urlStr string) (Transport, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, newTransportError("dial", fmt.Errorf("parse url: %w", err))
	}

	// Determine host and port
	host := u.Host
	if !strings.Contains(host, ":") {
		if u.Scheme == "wss" {
			host += ":443"
		} else {
			host += ":80"
		}
	}

	// Dial TCP connection
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", host)
	if err != nil {
		return nil, newTransportError("dial", err)
	}

	// Perform WebSocket handshake
	reader, err := performHandshake(conn, u, ctx)
	if err != nil {
		conn.Close()
		return nil, newTransportError("handshake", err)
	}

	return &WebSocketTransport{
		conn:   conn,
		reader: reader,
		addr:   urlStr,
	}, nil
}

// performHandshake sends HTTP upgrade request and validates response.
// It returns the buffered reader so any bytes already read during the
// handshake remain available to the WebSocket frame parser.
func performHandshake(conn net.Conn, u *url.URL, ctx context.Context) (*bufio.Reader, error) {
	// Generate random WebSocket key (16 bytes base64 encoded)
	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)

	// Build HTTP upgrade request
	path := u.Path
	if path == "" {
		path = "/"
	}
	if u.RawQuery != "" {
		path += "?" + u.RawQuery
	}

	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return nil, fmt.Errorf("set deadline: %w", err)
		}
		defer conn.SetDeadline(time.Time{})
	}

	req := fmt.Sprintf(
		"GET %s HTTP/1.1\r\n"+
			"Host: %s\r\n"+
			"Upgrade: websocket\r\n"+
			"Connection: Upgrade\r\n"+
			"Sec-WebSocket-Key: %s\r\n"+
			"Sec-WebSocket-Version: 13\r\n"+
			"\r\n",
		path, u.Host, key,
	)

	// Send upgrade request
	if err := writeAll(conn, []byte(req)); err != nil {
		return nil, fmt.Errorf("write request: %w", err)
	}

	reader := bufio.NewReader(conn)
	tp := textproto.NewReader(reader)
	statusLine, err := tp.ReadLine()
	if err != nil {
		return nil, fmt.Errorf("read status line: %w", err)
	}

	if !strings.Contains(statusLine, "101") {
		return nil, fmt.Errorf("unexpected handshake status: %s", statusLine)
	}

	hdrs, err := tp.ReadMIMEHeader()
	if err != nil {
		return nil, fmt.Errorf("read response headers: %w", err)
	}

	// Validate Sec-WebSocket-Accept header
	expectedAccept := computeAccept(key)
	if hdrs.Get("Sec-WebSocket-Accept") != expectedAccept {
		return nil, errors.New("invalid Sec-WebSocket-Accept header")
	}

	return reader, nil
}

// computeAccept computes Sec-WebSocket-Accept value per RFC 6455.
func computeAccept(key string) string {
	const magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	h := sha1.New()
	h.Write([]byte(key))
	h.Write([]byte(magic))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// Write sends a complete TLV frame as a binary WebSocket message.
// Thread-safe (protected by mutex).
func (w *WebSocketTransport) Write(ctx context.Context, frame []byte) error {
	if w.closed.Load() {
		return ErrTransportClosed
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Set write deadline from context
	if deadline, ok := ctx.Deadline(); ok {
		w.conn.SetWriteDeadline(deadline)
	}
	defer w.conn.SetWriteDeadline(time.Time{})

	// Write binary frame with masking (client-to-server requires mask)
	if err := w.writeFrame(opcodeBinary, frame, true); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	return nil
}

// writeFrame writes a WebSocket frame with optional masking.
func (w *WebSocketTransport) writeFrame(opcode byte, payload []byte, mask bool) error {
	// Build frame header
	var headerBuf [14]byte
	header := headerBuf[:0]

	// Byte 0: FIN=1, RSV=0, opcode
	header = append(header, 0x80|opcode)

	// Byte 1+: Mask bit + payload length
	length := len(payload)
	if length < 126 {
		if mask {
			header = append(header, byte(length)|0x80)
		} else {
			header = append(header, byte(length))
		}
	} else if length <= 0xFFFF {
		if mask {
			header = append(header, 126|0x80)
		} else {
			header = append(header, 126)
		}
		header = append(header, byte(length>>8), byte(length))
	} else {
		if mask {
			header = append(header, 127|0x80)
		} else {
			header = append(header, 127)
		}
		header = append(header, 0, 0, 0, 0) // High 32 bits (always 0 for our use)
		header = append(header, byte(length>>24), byte(length>>16), byte(length>>8), byte(length))
	}

	// Add masking key if needed
	var maskKey [4]byte
	if mask {
		if _, err := rand.Read(maskKey[:]); err != nil {
			return fmt.Errorf("generate websocket mask: %w", err)
		}
		header = append(header, maskKey[:]...)
	}

	// Write header
	if err := writeAll(w.conn, header); err != nil {
		return err
	}

	// Write payload (masked if needed)
	if mask {
		var scratch [1024]byte
		for i := 0; i < len(payload); {
			chunkLen := len(payload) - i
			if chunkLen > len(scratch) {
				chunkLen = len(scratch)
			}
			for j := 0; j < chunkLen; j++ {
				scratch[j] = payload[i+j] ^ maskKey[(i+j)%4]
			}
			if err := writeAll(w.conn, scratch[:chunkLen]); err != nil {
				return err
			}
			i += chunkLen
		}
		return nil
	}

	return writeAll(w.conn, payload)
}

// Read blocks until next binary WebSocket message arrives.
// Returns immediately if context is canceled.
// Handles control frames (ping, pong, close) automatically.
func (w *WebSocketTransport) Read(ctx context.Context) ([]byte, error) {
	// Set read deadline from context
	if deadline, ok := ctx.Deadline(); ok {
		w.conn.SetReadDeadline(deadline)
	}
	defer w.conn.SetReadDeadline(time.Time{})

	for {
		// Check context cancellation
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		opcode, payload, err := w.readFrame()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, err
		}

		switch opcode {
		case opcodeBinary:
			// Validate frame size (safety check)
			if len(payload) > MaxFrameSize {
				return nil, ErrFrameTooLarge
			}
			return payload, nil

		case opcodeText:
			// Reject text frames — the protocol requires binary-only transport (CLIENT_SPEC.md §2).
			return nil, fmt.Errorf("fitz: received WebSocket text frame: protocol requires binary-only transport")

		case opcodePing:
			// Respond with pong
			w.mu.Lock()
			w.writeFrame(opcodePong, payload, true)
			w.mu.Unlock()
			continue

		case opcodePong:
			// Ignore pong frames
			continue

		case opcodeClose:
			// Server sent close frame
			return nil, io.EOF

		default:
			// Unknown opcode, ignore
			continue
		}
	}
}

// readFrame reads a single WebSocket frame.
func (w *WebSocketTransport) readFrame() (opcode byte, payload []byte, err error) {
	// Read first two bytes
	var header [2]byte
	if _, err := io.ReadFull(w.reader, header[:]); err != nil {
		return 0, nil, err
	}

	// Parse byte 0: FIN, RSV, opcode
	fin := header[0]&0x80 != 0
	if header[0]&0x70 != 0 {
		return 0, nil, errors.New("websocket reserved bits not supported")
	}
	opcode = header[0] & 0x0F
	switch opcode {
	case opcodeBinary, opcodeText, opcodeClose, opcodePing, opcodePong:
	case opcodeContinuation:
		return 0, nil, errors.New("websocket continuation frames not supported")
	default:
		return 0, nil, fmt.Errorf("unsupported websocket opcode: 0x%x", opcode)
	}
	isControl := opcode >= opcodeClose
	if isControl {
		if !fin {
			return 0, nil, errors.New("fragmented control frames not supported")
		}
	} else if !fin {
		return 0, nil, errors.New("fragmented frames not supported")
	}

	// Parse byte 1: mask bit, payload length
	masked := header[1]&0x80 != 0
	if masked {
		return 0, nil, errors.New("masked server frames not supported")
	}
	length := uint64(header[1] & 0x7F)

	// Read extended payload length if needed
	switch length {
	case 126:
		var extLen [2]byte
		if _, err := io.ReadFull(w.reader, extLen[:]); err != nil {
			return 0, nil, err
		}
		length = uint64(binary.BigEndian.Uint16(extLen[:]))
	case 127:
		var extLen [8]byte
		if _, err := io.ReadFull(w.reader, extLen[:]); err != nil {
			return 0, nil, err
		}
		length = binary.BigEndian.Uint64(extLen[:])
	}
	if isControl && length > 125 {
		return 0, nil, errors.New("control frame payload too large")
	}
	if !isControl && length > uint64(MaxFrameSize) {
		return 0, nil, ErrFrameTooLarge
	}

	// Read payload
	payload = make([]byte, length)
	if _, err := io.ReadFull(w.reader, payload); err != nil {
		return 0, nil, err
	}

	return opcode, payload, nil
}

// Close cleanly shuts down the WebSocket connection.
// Sends close frame with normal closure status.
func (w *WebSocketTransport) Close() error {
	if !w.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	// Send close frame
	w.mu.Lock()
	closePayload := make([]byte, 2)
	binary.BigEndian.PutUint16(closePayload, statusNormalClosure)
	w.writeFrame(opcodeClose, closePayload, true)
	w.mu.Unlock()

	// Close underlying connection
	return w.conn.Close()
}

// RemoteAddr returns the WebSocket URL.
func (w *WebSocketTransport) RemoteAddr() string {
	return w.addr
}
