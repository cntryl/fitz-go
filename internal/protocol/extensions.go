package protocol

import (
	"encoding/binary"
	"errors"
)

const FlagExtensionsPresent uint8 = 4
const CapabilityQueueMessageTTL uint32 = 1
const CapabilityQueueNack uint32 = 2

type Extension struct {
	ID    uint16
	Value []byte
}

// EncodeExtensions leaves a core-only payload untouched. Fields must be sorted.
func EncodeExtensions(core []byte, fields []Extension) ([]byte, error) {
	if len(fields) == 0 {
		return core, nil
	}
	size := uint64(4 + len(core))
	var previous uint16
	for _, field := range fields {
		if field.ID <= previous {
			return nil, errors.New("extension IDs must be nonzero and ascending")
		}
		previous = field.ID
		size += 6 + uint64(len(field.Value))
	}
	if size > MaxPayloadSize {
		return nil, errors.New("extension payload exceeds message limit")
	}
	result := make([]byte, 4, int(size))
	binary.BigEndian.PutUint32(result, uint32(len(core)))
	result = append(result, core...)
	for _, field := range fields {
		var header [6]byte
		binary.BigEndian.PutUint16(header[:2], field.ID)
		binary.BigEndian.PutUint32(header[2:], uint32(len(field.Value)))
		result = append(result, header[:]...)
		result = append(result, field.Value...)
	}
	return result, nil
}

func ClientCapabilityList() []byte { return []byte{0, 2, 0, 0, 0, 1, 0, 0, 0, 2} }

// DecodeExtensions validates the complete envelope before exposing borrowed core bytes.
// Only HELLO has registered broker-to-client fields; unknown values are never copied.
func DecodeExtensions(frame Frame) ([]byte, []uint32, error) {
	handshake := frame.MessageType == MessageTypeHello
	if frame.Flags&FlagExtensionsPresent == 0 {
		if handshake {
			return nil, nil, errors.New("HELLO requires capability extensions")
		}
		return frame.Payload, nil, nil
	}
	payload := frame.Payload
	if len(payload) < 4 {
		return nil, nil, errors.New("truncated extension core length")
	}
	coreEnd := uint64(4) + uint64(binary.BigEndian.Uint32(payload[:4]))
	if coreEnd >= uint64(len(payload)) {
		return nil, nil, errors.New("core exceeds extension bounds or no fields")
	}
	cursor := int(coreEnd)
	var capabilities []uint32
	capabilitiesSeen := false
	for cursor < len(payload) {
		if len(payload)-cursor < 6 {
			return nil, nil, errors.New("truncated extension header")
		}
		id := binary.BigEndian.Uint16(payload[cursor:])
		size := uint64(binary.BigEndian.Uint32(payload[cursor+2:]))
		cursor += 6
		if id == 0 || size > uint64(len(payload)-cursor) {
			return nil, nil, errors.New("invalid extension ID or length")
		}
		if handshake && id == 1 {
			if capabilitiesSeen || size < 2 {
				return nil, nil, errors.New("duplicate or truncated capabilities")
			}
			capabilitiesSeen = true
			count := int(binary.BigEndian.Uint16(payload[cursor:]))
			if size != uint64(2+4*count) {
				return nil, nil, errors.New("capability length mismatch")
			}
			var previous uint32
			for index := range count {
				value := binary.BigEndian.Uint32(payload[cursor+2+4*index:])
				if value <= previous {
					return nil, nil, errors.New("capabilities must be nonzero and ascending")
				}
				previous = value
				if value == CapabilityQueueMessageTTL || value == CapabilityQueueNack {
					capabilities = append(capabilities, value)
				}
			}
		}
		cursor += int(size)
	}
	if handshake && !capabilitiesSeen {
		return nil, nil, errors.New("HELLO capability list is missing")
	}
	return payload[4:coreEnd], capabilities, nil
}
