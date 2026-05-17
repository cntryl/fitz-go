//nolint:gosec,gocritic,unparam
package testkit

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

// MockTransport is a controllable transport for unit tests.
// It allows tests to specify read responses and track written frames.
type MockTransport struct {
	mu          sync.Mutex
	readFrames  [][]byte // Frames to return on Read() calls
	readIndex   int
	writeFrames [][]byte // Frames written via Write()
	closed      bool
	remoteAddr  string
	readErr     error // Error to return on Read
	writeErr    error // Error to return on Write
}

// NewMockTransport creates a new mock transport.
func NewMockTransport() *MockTransport {
	return &MockTransport{
		remoteAddr: "mock://test",
	}
}

// SetReadFrames sets the frames to return on Read() calls.
func (m *MockTransport) SetReadFrames(frames [][]byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readFrames = make([][]byte, 0, len(frames))
	for _, frame := range frames {
		m.readFrames = append(m.readFrames, append([]byte(nil), frame...))
	}
	m.readIndex = 0
}

// SetReadError sets an error to return for all Read() calls.
func (m *MockTransport) SetReadError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readErr = err
}

// SetWriteError sets an error to return for all Write() calls.
func (m *MockTransport) SetWriteError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeErr = err
}

// GetWrittenFrames returns all frames written via Write().
func (m *MockTransport) GetWrittenFrames() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	frames := make([][]byte, 0, len(m.writeFrames))
	for _, frame := range m.writeFrames {
		frames = append(frames, append([]byte(nil), frame...))
	}
	return frames
}

// Write appends the frame to the written frames list.
func (m *MockTransport) Write(ctx context.Context, frame []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writeErr != nil {
		return m.writeErr
	}
	if m.closed {
		return errors.New("transport closed")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	m.writeFrames = append(m.writeFrames, append([]byte(nil), frame...))
	return nil
}

// Read returns the next queued read frame or an error.
func (m *MockTransport) Read(ctx context.Context) ([]byte, error) {
	m.mu.Lock()
	if m.readErr != nil {
		err := m.readErr
		m.mu.Unlock()
		return nil, err
	}
	if m.closed {
		m.mu.Unlock()
		return nil, errors.New("transport closed")
	}
	if m.readIndex >= len(m.readFrames) {
		m.mu.Unlock()
		// Block until context cancelled
		<-ctx.Done()
		return nil, ctx.Err()
	}
	frame := append([]byte(nil), m.readFrames[m.readIndex]...)
	m.readIndex++
	m.mu.Unlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return frame, nil
}

// Close marks the transport as closed.
func (m *MockTransport) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// RemoteAddr returns the remote address.
func (m *MockTransport) RemoteAddr() string {
	return m.remoteAddr
}

// MockTCPConn is a mock TCP connection for testing transport layer.
type MockTCPConn struct {
	ToRead           []byte
	Written          []byte
	ReadPos          int
	Blocked          bool
	Closed           bool
	ReadDelay        time.Duration
	WriteDelay       time.Duration
	MaxWriteSize     int
	RemoteAddrString string
	readDeadline     time.Time
	writeDeadline    time.Time
}

func (m *MockTCPConn) Read(b []byte) (int, error) {
	if m.Blocked {
		if !m.readDeadline.IsZero() {
			if time.Now().After(m.readDeadline) {
				return 0, context.DeadlineExceeded
			}
			time.Sleep(time.Until(m.readDeadline))
			return 0, context.DeadlineExceeded
		}
		select {}
	}
	time.Sleep(m.ReadDelay)
	if !m.readDeadline.IsZero() && time.Now().After(m.readDeadline) {
		return 0, context.DeadlineExceeded
	}
	if m.ReadPos >= len(m.ToRead) {
		return 0, io.EOF
	}
	n := copy(b, m.ToRead[m.ReadPos:])
	m.ReadPos += n
	return n, nil
}

func (m *MockTCPConn) Write(b []byte) (int, error) {
	if !m.writeDeadline.IsZero() {
		if time.Now().Add(m.WriteDelay).After(m.writeDeadline) {
			time.Sleep(time.Until(m.writeDeadline))
			return 0, context.DeadlineExceeded
		}
	}
	time.Sleep(m.WriteDelay)
	chunk := len(b)
	if m.MaxWriteSize > 0 && chunk > m.MaxWriteSize {
		chunk = m.MaxWriteSize
	}
	m.Written = append(m.Written, b[:chunk]...)
	return chunk, nil
}

func (m *MockTCPConn) Close() error {
	m.Closed = true
	return nil
}

func (m *MockTCPConn) LocalAddr() net.Addr {
	addr, _ := net.ResolveTCPAddr("tcp", "localhost:0")
	return addr
}

func (m *MockTCPConn) RemoteAddr() net.Addr {
	if m.RemoteAddrString != "" {
		addr, _ := net.ResolveTCPAddr("tcp", m.RemoteAddrString)
		return addr
	}
	addr, _ := net.ResolveTCPAddr("tcp", "localhost:4091")
	return addr
}

func (m *MockTCPConn) SetDeadline(t time.Time) error {
	m.readDeadline = t
	m.writeDeadline = t
	return nil
}

func (m *MockTCPConn) SetReadDeadline(t time.Time) error {
	m.readDeadline = t
	return nil
}

func (m *MockTCPConn) SetWriteDeadline(t time.Time) error {
	m.writeDeadline = t
	return nil
}

// MockWSConn is a mock WebSocket connection for testing.
type MockWSConn struct {
	NextMessage    []byte
	Messages       [][]byte
	IsText         bool
	Blocked        bool
	Closed         bool
	ReadBuf        []byte
	MaxWriteSize   int
	RemoteAddrHost string
	readBuf        []byte
	readPos        int
	writeBuf       []byte
	readDeadline   time.Time
	writeDeadline  time.Time
}

func (m *MockWSConn) Read(b []byte) (int, error) {
	if m.Closed {
		return 0, errors.New("connection closed")
	}
	if m.Blocked {
		if !m.readDeadline.IsZero() {
			if time.Now().After(m.readDeadline) {
				return 0, context.DeadlineExceeded
			}
			time.Sleep(time.Until(m.readDeadline))
			return 0, context.DeadlineExceeded
		}
		select {}
	}
	if m.ReadBuf != nil {
		m.readBuf = append(m.readBuf[:0], m.ReadBuf...)
		m.ReadBuf = nil
	}
	if m.readBuf == nil {
		opcode := byte(2)
		if m.IsText {
			opcode = 1
		}
		m.readBuf = buildWSFrame(opcode, m.NextMessage, false)
	}
	if m.readPos >= len(m.readBuf) {
		return 0, io.EOF
	}
	n := copy(b, m.readBuf[m.readPos:])
	m.readPos += n
	return n, nil
}

func (m *MockWSConn) Write(b []byte) (int, error) {
	if m.Closed {
		return 0, errors.New("connection closed")
	}
	if !m.writeDeadline.IsZero() && time.Now().After(m.writeDeadline) {
		return 0, context.DeadlineExceeded
	}
	chunk := len(b)
	if m.MaxWriteSize > 0 && chunk > m.MaxWriteSize {
		chunk = m.MaxWriteSize
	}
	m.writeBuf = append(m.writeBuf, b[:chunk]...)
	for {
		payload, remaining, ok, err := parseWSFrame(m.writeBuf)
		if err != nil {
			return 0, err
		}
		if !ok {
			break
		}
		m.Messages = append(m.Messages, payload)
		m.writeBuf = remaining
	}
	return chunk, nil
}

func (m *MockWSConn) Close() error {
	m.Closed = true
	return nil
}

func (m *MockWSConn) LocalAddr() net.Addr {
	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	return addr
}

func (m *MockWSConn) RemoteAddr() net.Addr {
	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:4090")
	return addr
}

func (m *MockWSConn) SetDeadline(t time.Time) error {
	m.readDeadline = t
	m.writeDeadline = t
	return nil
}

func (m *MockWSConn) SetReadDeadline(t time.Time) error {
	m.readDeadline = t
	return nil
}

func (m *MockWSConn) SetWriteDeadline(t time.Time) error {
	m.writeDeadline = t
	return nil
}

func buildWSFrame(opcode byte, payload []byte, mask bool) []byte {
	header := make([]byte, 0, 14)
	header = append(header, 0x80|opcode)
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
		header = append(header, 0, 0, 0, 0)
		header = append(header, byte(length>>24), byte(length>>16), byte(length>>8), byte(length))
	}
	if !mask {
		return append(header, payload...)
	}
	maskKey := [4]byte{0xAA, 0xBB, 0xCC, 0xDD}
	header = append(header, maskKey[:]...)
	masked := make([]byte, len(payload))
	for i := range payload {
		masked[i] = payload[i] ^ maskKey[i%4]
	}
	return append(header, masked...)
}

func parseWSFrame(buf []byte) ([]byte, []byte, bool, error) {
	if len(buf) < 2 {
		return nil, buf, false, nil
	}
	masked := buf[1]&0x80 != 0
	length := uint64(buf[1] & 0x7F)
	idx := 2
	switch length {
	case 126:
		if len(buf) < idx+2 {
			return nil, buf, false, nil
		}
		length = uint64(buf[idx])<<8 | uint64(buf[idx+1])
		idx += 2
	case 127:
		if len(buf) < idx+8 {
			return nil, buf, false, nil
		}
		length = uint64(buf[idx+4])<<24 | uint64(buf[idx+5])<<16 | uint64(buf[idx+6])<<8 | uint64(buf[idx+7])
		idx += 8
	}
	var maskKey [4]byte
	if masked {
		if len(buf) < idx+4 {
			return nil, buf, false, nil
		}
		copy(maskKey[:], buf[idx:idx+4])
		idx += 4
	}
	if len(buf) < idx+int(length) {
		return nil, buf, false, nil
	}
	payload := append([]byte(nil), buf[idx:idx+int(length)]...)
	if masked {
		for i := range payload {
			payload[i] ^= maskKey[i%4]
		}
	}
	return payload, buf[idx+int(length):], true, nil
}
