package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldRoundTripFixedWidthTypeGivenWire2Header(t *testing.T) {
	// Arrange
	values := []uint16{0, 1, 100, 254, 255, 256, 500, 65535}
	// Act
	for _, kind := range values {
		encoded := EncodeMessageType(kind)
		decoded, n, err := DecodeMessageType(encoded)
		// Assert
		require.NoError(t, err)
		assert.Equal(t, kind, decoded)
		assert.Equal(t, 2, n)
		assert.Equal(t, []byte{byte(kind >> 8), byte(kind)}, encoded)
	}
}
func TestShouldRejectTruncatedTypeGivenWire2Encoding(t *testing.T) {
	// Arrange
	for _, input := range [][]byte{nil, {0}, {255}} {
		// Act
		_, _, err := DecodeMessageType(input)
		// Assert
		require.Error(t, err)
	}
}
func TestShouldRoundTripFrameGivenCorrelationAndFlags(t *testing.T) {
	// Arrange
	payload := []byte{0, 255, 1, 2}
	// Act
	flags := FlagAckRequested | FlagResponseFinal
	encoded := EncodeFrameOwnedWithHeader(65535, flags, 0x0102030405060708, payload)
	defer encoded.Release()
	decoded, err := DecodeMessage(encoded.Bytes())
	// Assert
	require.NoError(t, err)
	assert.Equal(t, uint16(65535), decoded.MessageType)
	assert.Equal(t, flags, decoded.Flags)
	assert.Equal(t, uint64(0x0102030405060708), decoded.CorrelationID)
	assert.Equal(t, payload, decoded.Payload)
	assert.Equal(t, uint32(4), binary.BigEndian.Uint32(encoded.Bytes()[11:15]))
}
func TestShouldRejectInvalidFrameGivenTruncatedHeaderPayloadOrTrailingData(t *testing.T) {
	// Arrange
	frame := EncodeFrame(100, []byte{1, 2})
	inputs := [][]byte{nil, frame[:1], frame[:14], frame[:16], append(append([]byte(nil), frame...), 0), append(append([]byte(nil), frame...), frame...)}
	// Act
	for _, input := range inputs {
		_, err := DecodeMessage(input)
		// Assert
		require.Error(t, err)
	}
}
func TestShouldRejectReservedFlagsGivenEncodedFrame(t *testing.T) {
	// Arrange
	frame := EncodeFrame(100, nil)
	frame[2] = 4
	// Act
	_, err := DecodeMessage(frame)
	// Assert
	require.Error(t, err)
}
func TestShouldHandleMaximumPayloadGivenCompleteMessageLimit(t *testing.T) {
	// Arrange
	payload := bytes.Repeat([]byte{42}, MaxPayloadSize)
	// Act
	frame := EncodeFrame(100, payload)
	decoded, err := DecodeMessage(frame)
	// Assert
	require.NoError(t, err)
	assert.Len(t, frame, MaxMessageSize)
	assert.Equal(t, payload, decoded.Payload)
	assert.Nil(t, EncodeFrame(100, make([]byte, MaxPayloadSize+1)))
}
func TestShouldPreserveBinaryDataGivenFullByteRange(t *testing.T) {
	// Arrange
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}
	// Act
	kind, body, err := DecodeFrame(EncodeFrame(500, payload))
	// Assert
	require.NoError(t, err)
	assert.Equal(t, uint16(500), kind)
	assert.Equal(t, payload, body)
}
func BenchmarkEncodeMessageType(b *testing.B) {
	b.Run("single byte", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_ = EncodeMessageType(100)
		}
	})

	b.Run("escaped type", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_ = EncodeMessageType(500)
		}
	})
}

func BenchmarkDecodeMessageType(b *testing.B) {
	b.Run("single byte", func(b *testing.B) {
		data := []byte{0x64}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _, _ = DecodeMessageType(data)
		}
	})

	b.Run("escaped type", func(b *testing.B) {
		data := []byte{0xFF, 0x01, 0xF4}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _, _ = DecodeMessageType(data)
		}
	})
}

func BenchmarkEncodeFrame(b *testing.B) {
	b.Run("100 byte payload", func(b *testing.B) {
		payload := make([]byte, 100)
		msgType := uint16(100)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_ = EncodeFrame(msgType, payload)
		}
	})

	b.Run("10KB payload", func(b *testing.B) {
		payload := make([]byte, 10240)
		msgType := uint16(100)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_ = EncodeFrame(msgType, payload)
		}
	})
}

func BenchmarkEncodeFrameWithPayloadWriter(b *testing.B) {
	b.Run("100 byte payload", func(b *testing.B) {
		payload := make([]byte, 100)
		msgType := uint16(100)
		writer := func(buf *bytes.Buffer) {
			buf.Write(payload)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			frame, err := EncodeFrameWithPayloadWriter(msgType, writer)
			if err == nil {
				frame.Release()
			}
		}
	})

	b.Run("10KB payload", func(b *testing.B) {
		payload := make([]byte, 10240)
		msgType := uint16(100)
		writer := func(buf *bytes.Buffer) {
			buf.Write(payload)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			frame, err := EncodeFrameWithPayloadWriter(msgType, writer)
			if err == nil {
				frame.Release()
			}
		}
	})
}

func BenchmarkDecodeFrame(b *testing.B) {
	b.Run("100 byte payload", func(b *testing.B) {
		payload := make([]byte, 100)
		frame := EncodeFrame(100, payload)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _, _ = DecodeFrame(frame)
		}
	})

	b.Run("10KB payload", func(b *testing.B) {
		payload := make([]byte, 10240)
		frame := EncodeFrame(100, payload)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _, _ = DecodeFrame(frame)
		}
	})
}

func TestShouldRouteDomainGivenMessageTypeWhenRouteDomainCalled(t *testing.T) {
	// Arrange
	tests := []struct {
		msgType uint16
		domain  string
	}{
		{100, "kv"},
		{108, "kv"},
		{200, "queue"},
		{204, "queue"},
		{300, "rpc"},
		{303, "rpc"},
		{400, "lease"},
		{403, "lease"},
		{500, "notice"},
		{504, "notice"},
		{600, "stream"},
		{603, "stream"},
		{700, "schedule"},
		{707, "schedule"},
		{999, "unknown"},
	}

	// Act / Assert
	for _, tt := range tests {
		domain := RouteDomain(tt.msgType)
		assert.Equal(t, tt.domain, domain)
	}
}
