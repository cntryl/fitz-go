package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
)

const (
	HeaderLen                = 15
	MaxMessageSize           = 1 << 20
	MaxPayloadSize           = MaxMessageSize - HeaderLen
	FlagAckRequested   uint8 = 1 << 0
	validFlags               = FlagAckRequested
	pooledBufferMaxCap       = MaxMessageSize
)

const (
	MessageTypeServerHello = MessageTypeHello

	// Deprecated wire-1 control IDs. Wire 2 carries correlation in the header.
	MessageTypeCorrelate       uint16 = 0
	MessageTypeCorrelated      uint16 = 0
	MessageTypeSessionMetadata uint16 = 0

	// Wire 2 reserves capability bits; these names remain as zero-valued compatibility aliases.
	CapabilityCorrelation     uint32 = 0
	CapabilitySessionMetadata uint32 = 0
	CapabilityKVScanExclusive uint32 = 0
	CapabilityRPCCancellation uint32 = 0
)

var (
	bufferPool      = sync.Pool{New: func() any { return new(bytes.Buffer) }}
	frameBufferPool = sync.Pool{New: func() any { return &FrameBuffer{} }}
)

// FrameBuffer wraps a pooled frame buffer with explicit release.
type FrameBuffer struct{ buf *bytes.Buffer }

func (f *FrameBuffer) Bytes() []byte {
	if f == nil || f.buf == nil {
		return nil
	}
	return f.buf.Bytes()
}

func (f *FrameBuffer) Len() int {
	if f == nil || f.buf == nil {
		return 0
	}
	return f.buf.Len()
}

func (f *FrameBuffer) Release() {
	if f == nil || f.buf == nil {
		return
	}
	putBuffer(f.buf)
	f.buf = nil
	frameBufferPool.Put(f)
}

func getBuffer() *bytes.Buffer { return bufferPool.Get().(*bytes.Buffer) }

func putBuffer(buf *bytes.Buffer) {
	buf.Reset()
	if buf.Cap() <= pooledBufferMaxCap {
		bufferPool.Put(buf)
	}
}

type Frame struct {
	MessageType   uint16
	Flags         uint8
	CorrelationID uint64
	Payload       []byte
}

// EncodeFrame returns a complete wire-2 message with zero flags and correlation.
func EncodeFrame(msgType uint16, payload []byte) []byte {
	frame := EncodeFrameOwned(msgType, payload)
	if frame == nil {
		return nil
	}
	defer frame.Release()
	return append([]byte(nil), frame.Bytes()...)
}

func EncodeFrameOwned(msgType uint16, payload []byte) *FrameBuffer {
	return EncodeFrameOwnedWithHeader(msgType, 0, 0, payload)
}

func EncodeCorrelatedFrameOwned(correlationID uint64, msgType uint16, payload []byte) *FrameBuffer {
	if correlationID == 0 {
		return nil
	}
	return EncodeFrameOwnedWithHeader(msgType, 0, correlationID, payload)
}

func EncodeFrameOwnedWithHeader(msgType uint16, flags uint8, correlationID uint64, payload []byte) *FrameBuffer {
	if len(payload) > MaxPayloadSize || flags&^validFlags != 0 {
		return nil
	}
	buf := getBuffer()
	buf.Grow(HeaderLen + len(payload))
	writeU16BE(buf, msgType)
	buf.WriteByte(flags)
	var header [12]byte
	binary.BigEndian.PutUint64(header[:8], correlationID)
	binary.BigEndian.PutUint32(header[8:], uint32(len(payload)))
	buf.Write(header[:])
	buf.Write(payload)
	frame := frameBufferPool.Get().(*FrameBuffer)
	frame.buf = buf
	return frame
}

func EncodeFrameWithPayloadWriter(msgType uint16, writePayload func(*bytes.Buffer)) (*FrameBuffer, error) {
	return EncodeFrameWithHeaderAndPayloadWriter(msgType, 0, 0, writePayload)
}

func EncodeFrameWithHeaderAndPayloadWriter(msgType uint16, flags uint8, correlationID uint64, writePayload func(*bytes.Buffer)) (*FrameBuffer, error) {
	if writePayload == nil {
		return nil, errors.New("payload writer is nil")
	}
	if flags&^validFlags != 0 {
		return nil, fmt.Errorf("unsupported frame flags: %#x", flags)
	}
	buf := getBuffer()
	buf.Grow(HeaderLen)
	writeU16BE(buf, msgType)
	buf.WriteByte(flags)
	var zeroHeader [12]byte
	buf.Write(zeroHeader[:])
	payloadStart := buf.Len()
	writePayload(buf)
	payloadLen := buf.Len() - payloadStart
	if payloadLen > MaxPayloadSize {
		putBuffer(buf)
		return nil, fmt.Errorf("message too large: %d bytes (max %d)", payloadLen+HeaderLen, MaxMessageSize)
	}
	result := buf.Bytes()
	binary.BigEndian.PutUint64(result[3:11], correlationID)
	binary.BigEndian.PutUint32(result[11:15], uint32(payloadLen))
	frame := frameBufferPool.Get().(*FrameBuffer)
	frame.buf = buf
	return frame, nil
}

// DecodeMessage decodes exactly one complete wire-2 message.
func DecodeMessage(data []byte) (Frame, error) {
	if len(data) < HeaderLen {
		return Frame{}, fmt.Errorf("message header is truncated: got %d bytes", len(data))
	}
	flags := data[2]
	if flags&^validFlags != 0 {
		return Frame{}, fmt.Errorf("unsupported frame flags: %#x", flags)
	}
	length := uint64(binary.BigEndian.Uint32(data[11:15]))
	if length+HeaderLen != uint64(len(data)) {
		return Frame{}, fmt.Errorf("message length %d does not match frame size %d", length, len(data)-HeaderLen)
	}
	return Frame{
		MessageType:   binary.BigEndian.Uint16(data[:2]),
		Flags:         flags,
		CorrelationID: binary.BigEndian.Uint64(data[3:11]),
		Payload:       data[HeaderLen:],
	}, nil
}

// DecodeFrame is retained as a convenience for payload-only callers.
func DecodeFrame(data []byte) (uint16, []byte, error) {
	frame, err := DecodeMessage(data)
	return frame.MessageType, frame.Payload, err
}

// DecodeFrames preserves the old collection-shaped API; wire 2 permits one message per transport frame.
func DecodeFrames(data []byte) ([]Frame, error) {
	frame, err := DecodeMessage(data)
	if err != nil {
		return nil, err
	}
	return []Frame{frame}, nil
}

func EncodeTCPFrame(msgType uint16, payload []byte) []byte {
	frame := EncodeTCPFrameOwned(msgType, payload)
	if frame == nil {
		return nil
	}
	defer frame.Release()
	return append([]byte(nil), frame.Bytes()...)
}

func EncodeTCPFrameOwned(msgType uint16, payload []byte) *FrameBuffer {
	message := EncodeFrameOwned(msgType, payload)
	if message == nil {
		return nil
	}
	defer message.Release()
	buf := getBuffer()
	buf.Grow(4 + message.Len())
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(message.Len()))
	buf.Write(prefix[:])
	buf.Write(message.Bytes())
	frame := frameBufferPool.Get().(*FrameBuffer)
	frame.buf = buf
	return frame
}

func DecodeTCPFrameLength(lengthPrefix []byte) (uint32, error) {
	if len(lengthPrefix) < 4 {
		return 0, errors.New("insufficient data for TCP frame length")
	}
	return binary.BigEndian.Uint32(lengthPrefix), nil
}

func EncodeMessageType(msgType uint16) []byte {
	encoded := make([]byte, 2)
	binary.BigEndian.PutUint16(encoded, msgType)
	return encoded
}

func DecodeMessageType(data []byte) (uint16, int, error) {
	if len(data) < 2 {
		return 0, 0, errors.New("insufficient data for message type")
	}
	return binary.BigEndian.Uint16(data[:2]), 2, nil
}

func writeU16BE(buf *bytes.Buffer, value uint16) {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], value)
	buf.Write(encoded[:])
}
