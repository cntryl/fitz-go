package connection

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	coreerrors "github.com/cntryl/fitz-go/v2/internal/core/errors"
)

// ParseStandardResponse handles the common [u8 status] prefix per CLIENT_SPEC.md.
// Returns (success, remainingPayload, error).
// If status=0 (success), returns remaining payload after status byte.
// If status=1 (error), parses [u32 code][u32 msg_len][msg].
func ParseStandardResponse(payload []byte) (bool, []byte, error) {
	if len(payload) < 1 {
		return false, nil, errors.New("response too short")
	}

	status := payload[0]

	if status == 1 { // Error (per CLIENT_SPEC.md)
		code, message, err := parseTypedDomainError(payload[1:])
		if err != nil {
			return false, nil, fmt.Errorf("decode domain error: %w", err)
		}
		return false, nil, coreerrors.NewDomainError(code, message)
	}

	// Success (status=0) - return remaining payload
	return true, payload[1:], nil
}

// ParsePlainResponse handles domains whose error envelope is
// [status=1][u32 message length][message], without a numeric domain code.
func ParsePlainResponse(payload []byte) (bool, []byte, error) {
	if len(payload) < 1 {
		return false, nil, errors.New("response too short")
	}

	switch payload[0] {
	case 0:
		return true, payload[1:], nil
	case 1:
		message, offset, err := ReadString(payload, 1)
		if err != nil {
			return false, nil, fmt.Errorf("decode domain error: %w", err)
		}
		if offset != len(payload) {
			return false, nil, errors.New("trailing bytes after domain error")
		}
		return false, nil, errors.New(message)
	default:
		return false, nil, fmt.Errorf("unknown response status %d", payload[0])
	}
}

func parseTypedDomainError(payload []byte) (uint32, string, error) {
	if len(payload) < 8 {
		return 0, "", io.ErrUnexpectedEOF
	}

	code, offset, err := ReadU32BE(payload, 0)
	if err != nil {
		return 0, "", err
	}

	message, newOffset, err := ReadString(payload, offset)
	if err != nil {
		return 0, "", err
	}
	if newOffset != len(payload) {
		return 0, "", errors.New("trailing bytes after domain error")
	}

	return code, message, nil
}

// Read helpers with bounds checking (per CLIENT_SPEC.md encoding).
// All return (value, newOffset, error).

// ReadU8 reads a single byte.
func ReadU8(payload []byte, offset int) (uint8, int, error) {
	if offset+1 > len(payload) {
		return 0, offset, io.ErrUnexpectedEOF
	}
	return payload[offset], offset + 1, nil
}

// ReadU16BE reads a u16 in big-endian format.
func ReadU16BE(payload []byte, offset int) (uint16, int, error) {
	if offset+2 > len(payload) {
		return 0, offset, io.ErrUnexpectedEOF
	}
	val := binary.BigEndian.Uint16(payload[offset:])
	return val, offset + 2, nil
}

// ReadU32BE reads a u32 in big-endian format.
func ReadU32BE(payload []byte, offset int) (uint32, int, error) {
	if offset+4 > len(payload) {
		return 0, offset, io.ErrUnexpectedEOF
	}
	val := binary.BigEndian.Uint32(payload[offset:])
	return val, offset + 4, nil
}

// ReadU64BE reads a u64 in big-endian format.
func ReadU64BE(payload []byte, offset int) (uint64, int, error) {
	if offset+8 > len(payload) {
		return 0, offset, io.ErrUnexpectedEOF
	}
	val := binary.BigEndian.Uint64(payload[offset:])
	return val, offset + 8, nil
}

// ReadString reads [u32 BE length][bytes] per CLIENT_SPEC.md.
func ReadString(payload []byte, offset int) (string, int, error) {
	length, offset, err := ReadU32BE(payload, offset)
	if err != nil {
		return "", offset, err
	}

	if offset+int(length) > len(payload) {
		return "", offset, io.ErrUnexpectedEOF
	}

	str := string(payload[offset : offset+int(length)])
	return str, offset + int(length), nil
}

// ReadBytes reads [u32 BE length][bytes] per CLIENT_SPEC.md.
// Returns a slice referencing the original payload (no copy).
func ReadBytes(payload []byte, offset int) ([]byte, int, error) {
	length, offset, err := ReadU32BE(payload, offset)
	if err != nil {
		return nil, offset, err
	}

	if offset+int(length) > len(payload) {
		return nil, offset, io.ErrUnexpectedEOF
	}

	data := payload[offset : offset+int(length)]
	return data, offset + int(length), nil
}

// ReadUUID reads a fixed 16-byte UUID.
func ReadUUID(payload []byte, offset int) ([16]byte, int, error) {
	var uuid [16]byte
	if offset+16 > len(payload) {
		return uuid, offset, io.ErrUnexpectedEOF
	}
	copy(uuid[:], payload[offset:offset+16])
	return uuid, offset + 16, nil
}

// Write helpers for building request payloads.

// WriteU8 writes a single byte.
func WriteU8(buf *bytes.Buffer, val uint8) {
	buf.WriteByte(val)
}

// WriteU16BE writes a u16 in big-endian format.
func WriteU16BE(buf *bytes.Buffer, val uint16) {
	buf.Grow(2)
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], val)
	buf.Write(b[:])
}

// WriteU32BE writes a u32 in big-endian format.
func WriteU32BE(buf *bytes.Buffer, val uint32) {
	buf.Grow(4)
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], val)
	buf.Write(b[:])
}

// WriteU64BE writes a u64 in big-endian format.
func WriteU64BE(buf *bytes.Buffer, val uint64) {
	buf.Grow(8)
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], val)
	buf.Write(b[:])
}

// WriteString writes [u32 BE length][bytes] per CLIENT_SPEC.md.
func WriteString(buf *bytes.Buffer, s string) {
	buf.Grow(4 + len(s))
	WriteU32BE(buf, uint32(len(s)))
	buf.WriteString(s)
}

// WriteBytes writes [u32 BE length][bytes] per CLIENT_SPEC.md.
func WriteBytes(buf *bytes.Buffer, b []byte) {
	buf.Grow(4 + len(b))
	WriteU32BE(buf, uint32(len(b)))
	buf.Write(b)
}

// WriteUUID writes a fixed 16-byte UUID.
func WriteUUID(buf *bytes.Buffer, uuid [16]byte) {
	buf.Grow(16)
	buf.Write(uuid[:])
}

// Buffer pool for efficiency (reduces allocations).
var bufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

// GetBuffer gets a buffer from the pool.
// Call PuBuffer when done to return it to the pool.
func GetBuffer() *bytes.Buffer {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

// PutBuffer returns a buffer to the pool.
func PutBuffer(buf *bytes.Buffer) {
	// Don't return oversized buffers to pool (prevents memory leaks)
	if buf.Cap() > 64*1024 {
		return
	}
	bufferPool.Put(buf)
}
