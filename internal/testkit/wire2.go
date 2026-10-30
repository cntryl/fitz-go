package testkit

import (
	"bytes"
	"encoding/binary"
	"slices"

	"github.com/cntryl/fitz-go/internal/protocol"
)

// NewWire2MockTransport provides CONNECT/HELLO and request correlation for domain tests.
func NewWire2MockTransport() *MockTransport { m := NewMockTransport(); m.autoHello = true; return m }

// signal wakes a reader after fixture state changes. Caller holds mu.
func (m *MockTransport) signal() {
	if m.changed != nil {
		close(m.changed)
	}
	m.changed = make(chan struct{})
}

// HelloPayload advertises the Wire 2 base contract without optional capability bits.
func HelloPayload() []byte {
	p := make([]byte, 22)
	binary.BigEndian.PutUint16(p, 2)
	binary.BigEndian.PutUint32(p[6:], 1<<20)
	binary.BigEndian.PutUint32(p[10:], 64)
	binary.BigEndian.PutUint64(p[14:], 1)
	return p
}

// ResponseRequiresCorrelation excludes controls, live notifications and UUID-routed RPC deliveries.
func ResponseRequiresCorrelation(kind uint16) bool {
	switch kind {
	case 1, 2, 3, 4, 111, 209, 302, 303, 304, 305, 409, 504, 609, 705:
		return false
	}
	return true
}

// CorrelateResponse binds an implicit fixture reply to its latest matching request.
// A queued Lease acquisition is intermediate; other implicit replies are final.
// Explicitly correlated frames are left unchanged.
func CorrelateResponse(frame []byte, written [][]byte) []byte {
	frame = append([]byte(nil), frame...)
	r, e := protocol.DecodeMessage(frame)
	if e != nil || r.CorrelationID != 0 || !ResponseRequiresCorrelation(r.MessageType) {
		return frame
	}
	for _, v := range slices.Backward(written) {
		q, e := protocol.DecodeMessage(v)
		if e == nil && q.MessageType == r.MessageType && q.CorrelationID != 0 {
			binary.BigEndian.PutUint64(frame[3:11], q.CorrelationID)
			if r.MessageType != protocol.MessageTypeLeaseAcquire || !bytes.HasPrefix(r.Payload, []byte{0, 2}) {
				frame[2] |= protocol.FlagResponseFinal
			}
			break
		}
	}
	return frame
}

// WriteErrorCode writes the common code, class and effect fields for synthetic broker failures.
func WriteErrorCode(buf *bytes.Buffer, code uint32) {
	buf.WriteByte(byte(code >> 8))
	buf.WriteByte(byte(code))
	category := code % 1000
	if category <= 10 {
		buf.WriteByte(byte(category))
	} else {
		buf.WriteByte(0)
	}
	if category == 6 || category == 7 {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
}
