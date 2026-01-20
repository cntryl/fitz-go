package transport

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"time"
)

// Frame represents a single top-level Fitz TLV frame transported over the
// connection. This is intentionally simple: length (u32) is handled by the
// encoder/decoder below; payload is left opaque for domain layers to decode.
// See `CLIENT_SPEC.md` for authoritative frame layout and TLV tag meanings.
type Frame struct {
	Type    uint8
	Flags   uint8
	Channel uint32
	Body    []byte
}

// Mux multiplexes/demultiplexes Frame values over a single io.ReadWriteCloser
// (TCP or WebSocket binary). It exposes an incoming frames channel and a
// Send method for outbound frames. Mux runs two goroutines (read/write) and
// handles clean shutdowns via context cancellation.
type Mux struct {
	rw        io.ReadWriteCloser
	r         *bufio.Reader
	inCh      chan Frame
	outCh     chan Frame
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	writeMu   sync.Mutex // serialises writes
}

// NewMux constructs a Mux for the provided connection. It does not start the
// background loops; call Start() to begin processing.
func NewMux(rw io.ReadWriteCloser) *Mux {
	ctx, cancel := context.WithCancel(context.Background())
	return &Mux{
		rw:     rw,
		r:      bufio.NewReader(rw),
		inCh:   make(chan Frame, 128),
		outCh:  make(chan Frame, 128),
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start launches the read and write loops. Safe to call multiple times (idempotent).
func (m *Mux) Start() {
	go m.readLoop()
	go m.writeLoop()
}

// In returns a read-only channel of inbound frames.
func (m *Mux) In() <-chan Frame { return m.inCh }

// Send enqueues a frame for sending. It may return an error if the mux is
// closed. Send is safe for concurrent callers.
func (m *Mux) Send(f Frame) error {
	select {
	case <-m.ctx.Done():
		return errors.New("transport mux closed")
	default:
	}
	select {
	case m.outCh <- f:
		return nil
	case <-m.ctx.Done():
		return errors.New("transport mux closed")
	}
}

// Close stops the loops and closes the underlying connection. Safe to call
// multiple times (idempotent via sync.Once).
func (m *Mux) Close() error {
	var errClose error
	m.closeOnce.Do(func() {
		m.cancel()
		errClose = m.rw.Close()
		close(m.inCh)
		close(m.outCh)
	})
	return errClose
}

// Ctx returns the mux's context. Callers can use this to coordinate graceful
// shutdown (e.g., for goroutines that call StartHeartbeat).
func (m *Mux) Ctx() context.Context { return m.ctx }

// StartHeartbeat sends periodic heartbeat frames until the mux context is done.
// The actual frame Type and payload are left to callers; here we use a
// reserved Heartbeat frame type for convenience.
func (m *Mux) StartHeartbeat(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			if err := m.Send(Frame{Type: FrameTypeHeartbeat}); err != nil {
				return
			}
		}
	}
}

// FrameType constants (small set used by the transport scaffolding).
const (
	FrameTypeConnOpen  uint8 = 1
	FrameTypeAck       uint8 = 2
	FrameTypeErr       uint8 = 3
	FrameTypeHeartbeat uint8 = 0xFE
)

// readLoop decodes frames and publishes them onto m.inCh. Exits when context
// is done or on read error.
func (m *Mux) readLoop() {
	defer func() {
		// Ensure the channel is closed and mux is shut down on exit.
		_ = m.Close()
	}()
	for {
		select {
		case <-m.ctx.Done():
			return
		default:
		}
		f, err := m.decodeFrame()
		if err != nil {
			// on error (EOF/closed connection), exit gracefully
			return
		}
		select {
		case m.inCh <- f:
		case <-m.ctx.Done():
			return
		}
	}
}

// writeLoop encodes frames from m.outCh and writes them atomically.
// Exits when context is done or on write error.
func (m *Mux) writeLoop() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case f, ok := <-m.outCh:
			if !ok {
				return
			}
			m.writeMu.Lock()
			err := m.encodeFrame(f)
			m.writeMu.Unlock()
			if err != nil {
				_ = m.Close()
				return
			}
		}
	}
}

// encodeFrame writes a single frame in the canonical format.
// Format: length(u32 BE) = len(type+flags+channel+body), type(u8), flags(u8), channel(u32 BE), body...
func (m *Mux) encodeFrame(f Frame) error {
	bufLen := 1 + 1 + 4 + len(f.Body)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(bufLen))
	mParts := make([]byte, 0, 4+bufLen)
	mParts = append(mParts, lenBuf[:]...)
	mParts = append(mParts, f.Type)
	mParts = append(mParts, f.Flags)
	var chBuf [4]byte
	binary.BigEndian.PutUint32(chBuf[:], f.Channel)
	mParts = append(mParts, chBuf[:]...)
	mParts = append(mParts, f.Body...)
	_, err := m.rw.Write(mParts)
	return err
}

// decodeFrame reads and parses a single frame. Returns an error on EOF or
// protocol errors.
func (m *Mux) decodeFrame() (Frame, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(m.r, lenBuf[:]); err != nil {
		return Frame{}, err
	}
	l := binary.BigEndian.Uint32(lenBuf[:])
	if l < 6 { // type(1) + flags(1) + channel(4) == 6
		return Frame{}, errors.New("invalid frame length")
	}
	payload := make([]byte, l)
	if _, err := io.ReadFull(m.r, payload); err != nil {
		return Frame{}, err
	}
	f := Frame{}
	f.Type = payload[0]
	f.Flags = payload[1]
	f.Channel = binary.BigEndian.Uint32(payload[2:6])
	f.Body = append([]byte(nil), payload[6:]...)
	return f, nil
}
