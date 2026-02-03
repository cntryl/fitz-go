package lease

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/cntryl/cntryl-go/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper: read a single TCP-style frame from conn
func readFrameConn(t *testing.T, conn net.Conn) ([]byte, error) {
	t.Helper()
	var lenBuf [4]byte
	if _, err := conn.Read(lenBuf[:]); err != nil {
		return nil, err
	}
	l := binary.BigEndian.Uint32(lenBuf[:])
	buf := make([]byte, l)
	if _, err := conn.Read(buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// Helper: write a TCP-style frame to conn
func writeFrameConn(t *testing.T, conn net.Conn, payload []byte) error {
	t.Helper()
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	if _, err := conn.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := conn.Write(payload); err != nil {
		return err
	}
	return nil
}

func TestShouldAcquireLeaseGivenAvailableLeaseWhenAcquireCalled(t *testing.T) {
	// Arrange: create connected pair
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	mux := transport.NewMux(c1)
	mux.Start()
	lc := NewClient(mux)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	// Simulate server: read request, validate, respond
	go func() {
		defer func() { errCh <- nil }()
		buf, err := readFrameConn(t, c2)
		if err != nil {
			errCh <- err
			return
		}
		// header: type(1) | flags(1) | channel(4)
		tp := buf[0]
		if tp != LeaseAcquire {
			errCh <- fmt.Errorf("expected Type LeaseAcquire (%d), got %d", LeaseAcquire, tp)
			return
		}
		ch := binary.BigEndian.Uint32(buf[2:6])
		if ch != transport.ChannelLease {
			errCh <- fmt.Errorf("expected channel %d, got %d", transport.ChannelLease, ch)
			return
		}
		// respond with FrameTypeResp, channel lease, TLV {TagLease: bytes, TagTTL: 30}
		e := transport.NewTLVEncoder()
		e.AddBytes(transport.TagLease, []byte{0xAA, 0xBB, 0xCC})
		e.AddUint32(transport.TagTTL, 30)
		// response payload: type | flags | channel(4) | body
		var chBuf [4]byte
		binary.BigEndian.PutUint32(chBuf[:], transport.ChannelLease)
		payload := append([]byte{transport.FrameTypeResp, 0}, chBuf[:]...)
		payload = append(payload, e.Encode()...)
		_ = writeFrameConn(t, c2, payload)
	}()

	// Act
	token, expires, held, err := lc.Acquire(ctx, "lease://r/a/res", 30)
	require.NoError(t, err)
	require.NoError(t, <-errCh)
	assert.True(t, held, "expected held=true")
	assert.Equal(t, []byte{0xAA, 0xBB, 0xCC}, token, "unexpected token")
	assert.Greater(t, expires, time.Now().Unix(), "unexpected expires timestamp")
}

func TestShouldRejectRenewGivenInvalidTokenWhenTokenMismatch(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	mux := transport.NewMux(c1)
	mux.Start()
	lc := NewClient(mux)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	// Simulate server sending an error frame on renew
	go func() {
		buf, err := readFrameConn(t, c2)
		if err != nil {
			errCh <- err
			return
		}
		// Expect Type == LeaseRenew
		if buf[0] != LeaseRenew {
			errCh <- fmt.Errorf("expected Type LeaseRenew (%d), got %d", LeaseRenew, buf[0])
			return
		}
		// send error frame
		e := transport.NewTLVEncoder()
		e.AddString(transport.TagErr, "invalid fencing token")
		var chBuf [4]byte
		binary.BigEndian.PutUint32(chBuf[:], transport.ChannelLease)
		payload := append([]byte{transport.FrameTypeErr, 0}, chBuf[:]...)
		payload = append(payload, e.Encode()...)
		_ = writeFrameConn(t, c2, payload)
		errCh <- nil
	}()

	// Act
	_, err := lc.Renew(ctx, "lease://r/a/res", []byte{0x01}, 30)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid fencing token")
	require.NoError(t, <-errCh)
}
