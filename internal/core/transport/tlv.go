package transport

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// TLV layer invariants:
// - Tags are unique per frame. A duplicate tag in a frame is treated as a protocol error.
// - Each TLV value length is encoded as a 2-byte big-endian uint16, which limits a single
//   TLV value to 65535 bytes (64 KiB).
// - Unknown tags are accepted and preserved (forward-compatibility).

// MaxTLVValueLen is the maximum allowed length for a single TLV value (2-byte length field).
const MaxTLVValueLen uint16 = 65535

// TLV tag constants (canonical from CLIENT_SPEC.md).
const (
	TagToken          uint8 = 0x10
	TagOp             uint8 = 0x11 // operation tag used by request frames
	TagRoute          uint8 = 0x20
	TagID             uint8 = 0x21
	TagBody           uint8 = 0x22
	TagErr            uint8 = 0x3F
	TagKey            uint8 = 0x23
	TagValue          uint8 = 0x24
	TagStartKey       uint8 = 0x25
	TagEndKey         uint8 = 0x26
	TagLimit          uint8 = 0x27
	TagFrom           uint8 = 0x28
	TagExpectedOffset uint8 = 0x29
	TagLease          uint8 = 0x2A
	TagBatchSize      uint8 = 0x2B
	TagDeliveryToken  uint8 = 0x2C
	TagTTL            uint8 = 0x2D
	TagReplyRoute     uint8 = 0x2E
	TagSeq            uint8 = 0x30
	TagStreamEnd      uint8 = 0x31
	TagCron           uint8 = 0x32
)

// KV operation type constants (mapped in KV domain).
const (
	KVOpGet         uint8 = 1
	KVOpPut         uint8 = 2
	KVOpDelete      uint8 = 3
	KVOpScan        uint8 = 4
	KVOpDeleteRange uint8 = 5
	KVOpInsert      uint8 = 6
	KVOpCommit      uint8 = 7
	KVOpRollback    uint8 = 8
)

// TLVValue represents a single TLV (tag-length-value) entry.
type TLVValue struct {
	Tag   uint8
	Value []byte
}

// TLVEncoder accumulates TLV entries and can be marshaled to bytes.
type TLVEncoder struct {
	entries []TLVValue
}

// NewTLVEncoder creates a new TLV encoder.
func NewTLVEncoder() *TLVEncoder {
	return &TLVEncoder{
		entries: make([]TLVValue, 0),
	}
}

// AddTag appends a TLV entry with the given tag and raw value.
func (e *TLVEncoder) AddTag(tag uint8, value []byte) *TLVEncoder {
	e.entries = append(e.entries, TLVValue{
		Tag:   tag,
		Value: value,
	})
	return e
}

// AddString adds a string value with the given tag.
func (e *TLVEncoder) AddString(tag uint8, value string) *TLVEncoder {
	return e.AddTag(tag, []byte(value))
}

// AddBytes adds a byte slice value with the given tag.
func (e *TLVEncoder) AddBytes(tag uint8, value []byte) *TLVEncoder {
	return e.AddTag(tag, value)
}

// AddUint32 adds a u32 BE value with the given tag.
func (e *TLVEncoder) AddUint32(tag uint8, value uint32) *TLVEncoder {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, value)
	return e.AddTag(tag, b)
}

// AddUint64 adds a u64 BE value with the given tag.
func (e *TLVEncoder) AddUint64(tag uint8, value uint64) *TLVEncoder {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, value)
	return e.AddTag(tag, b)
}

// AddUint8 adds a single byte value with the given tag.
func (e *TLVEncoder) AddUint8(tag uint8, value uint8) *TLVEncoder {
	b := []byte{value}
	return e.AddTag(tag, b)
}

// Encode marshals all accumulated TLV entries into a single byte slice.
// Format: [tag(1)][len(2 BE)][value...]...
func (e *TLVEncoder) Encode() []byte {
	// Pre-calculate total size.
	totalSize := 0
	for _, entry := range e.entries {
		totalSize += 1 + 2 + len(entry.Value) // tag + len + value
	}

	result := make([]byte, 0, totalSize)
	var lenBuf [2]byte
	for _, entry := range e.entries {
		result = append(result, entry.Tag)
		binary.BigEndian.PutUint16(lenBuf[:], uint16(len(entry.Value)))
		result = append(result, lenBuf[:]...)
		result = append(result, entry.Value...)
	}
	return result
}

// TLVDecoder decodes a byte slice into TLV entries.
// It treats duplicate tags as a protocol error. Convenience getters (GetBytes/GetString/GetUint32/etc.)
// return the last value seen for a tag when present.
type TLVDecoder struct {
	entries map[uint8][][]byte
}

// NewTLVDecoder creates a decoder from raw TLV bytes.
func NewTLVDecoder(data []byte) (*TLVDecoder, error) {
	dec := &TLVDecoder{
		entries: make(map[uint8][][]byte),
	}
	if err := dec.parse(data); err != nil {
		return nil, err
	}
	return dec, nil
}

// parse decodes the raw TLV data.
func (d *TLVDecoder) parse(data []byte) error {
	offset := 0
	for offset < len(data) {
		if offset+3 > len(data) {
			return errors.New("truncated TLV: insufficient bytes for tag and length")
		}
		tag := data[offset]
		offset++
		length := binary.BigEndian.Uint16(data[offset : offset+2])
		offset += 2
		if offset+int(length) > len(data) {
			return errors.New("truncated TLV: value exceeds remaining bytes")
		}
		value := make([]byte, length)
		copy(value, data[offset:offset+int(length)])
		offset += int(length)
		// If a tag is already present, treat duplicates as a protocol error.
		if _, exists := d.entries[tag]; exists {
			return fmt.Errorf("duplicate TLV tag: %d", tag)
		}
		d.entries[tag] = [][]byte{value}
	}
	return nil
}

// GetBytes retrieves the raw byte value for a tag (last value seen), or nil if not present.
func (d *TLVDecoder) GetBytes(tag uint8) []byte {
	vals := d.entries[tag]
	if len(vals) == 0 {
		return nil
	}
	// Return the last value to preserve prior "last one wins" semantics.
	last := vals[len(vals)-1]
	return append([]byte(nil), last...)
}

// GetAll returns a copy of all values present for a tag in order of appearance.
func (d *TLVDecoder) GetAll(tag uint8) [][]byte {
	vals := d.entries[tag]
	out := make([][]byte, len(vals))
	for i := range vals {
		out[i] = append([]byte(nil), vals[i]...)
	}
	return out
}

// GetString retrieves the string value for a tag (last value), or empty string if not present.
func (d *TLVDecoder) GetString(tag uint8) string {
	b := d.GetBytes(tag)
	if b == nil {
		return ""
	}
	return string(b)
}

// GetUint32 retrieves a u32 BE value for a tag (last value), or 0 if not present or invalid.
func (d *TLVDecoder) GetUint32(tag uint8) (uint32, error) {
	value := d.GetBytes(tag)
	if value == nil {
		return 0, nil
	}
	if len(value) != 4 {
		return 0, fmt.Errorf("invalid u32 length for tag %d: expected 4, got %d", tag, len(value))
	}
	return binary.BigEndian.Uint32(value), nil
}

// GetUint64 retrieves a u64 BE value for a tag (last value), or 0 if not present or invalid.
func (d *TLVDecoder) GetUint64(tag uint8) (uint64, error) {
	value := d.GetBytes(tag)
	if value == nil {
		return 0, nil
	}
	if len(value) != 8 {
		return 0, fmt.Errorf("invalid u64 length for tag %d: expected 8, got %d", tag, len(value))
	}
	return binary.BigEndian.Uint64(value), nil
}

// Has checks if a tag is present in the decoded TLV.
func (d *TLVDecoder) Has(tag uint8) bool {
	_, ok := d.entries[tag]
	return ok
}

// All returns a copy of all decoded entries (useful for debugging).
// For tags with multiple values, the last value is used in the returned map.
func (d *TLVDecoder) All() map[uint8][]byte {
	result := make(map[uint8][]byte)
	for k, vals := range d.entries {
		if len(vals) == 0 {
			continue
		}
		last := vals[len(vals)-1]
		result[k] = append([]byte(nil), last...)
	}
	return result
}
