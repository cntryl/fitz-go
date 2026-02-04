package queue

import (
	"testing"

	"github.com/cntryl/cntryl-go/internal/transport"
	"github.com/stretchr/testify/assert"
)

type mockMux struct {
	lastFrame transport.Frame
}

func (m *mockMux) Send(f transport.Frame) error { m.lastFrame = f; return nil }

func TestShouldSendEnqueueWhenCalled(t *testing.T) {
	m := &mockMux{}
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, "realm/area/resource")
	enc.AddBytes(transport.TagBody, []byte("payload"))
	frame := transport.Frame{Type: QueueEnqueue, Flags: 0, Channel: ChannelQueue, Body: enc.Encode()}
	_ = m.Send(frame)

	assert.Equal(t, QueueEnqueue, m.lastFrame.Type)
	dec, err := transport.NewTLVDecoder(m.lastFrame.Body)
	assert.NoError(t, err)
	assert.Equal(t, "realm/area/resource", dec.GetString(transport.TagRoute))
	assert.Equal(t, "payload", string(dec.GetBytes(transport.TagBody)))
}

func TestShouldSendReserveWhenCalled(t *testing.T) {
	m := &mockMux{}
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, "realm/area/resource")
	enc.AddUint32(transport.TagTTL, 10)
	enc.AddUint32(transport.TagBatchSize, 5)
	frame := transport.Frame{Type: QueueReserve, Flags: 0, Channel: ChannelQueue, Body: enc.Encode()}
	_ = m.Send(frame)

	assert.Equal(t, QueueReserve, m.lastFrame.Type)
	dec, err := transport.NewTLVDecoder(m.lastFrame.Body)
	assert.NoError(t, err)
	assert.Equal(t, "realm/area/resource", dec.GetString(transport.TagRoute))
}

func TestShouldSendCompleteWhenCalled(t *testing.T) {
	m := &mockMux{}
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, "realm/area/resource")
	enc.AddString(transport.TagToken, "msg-1")
	enc.AddUint64(transport.TagID, 123)
	frame := transport.Frame{Type: QueueComplete, Flags: 0, Channel: ChannelQueue, Body: enc.Encode()}
	_ = m.Send(frame)

	assert.Equal(t, QueueComplete, m.lastFrame.Type)
	dec, err := transport.NewTLVDecoder(m.lastFrame.Body)
	assert.NoError(t, err)
	assert.Equal(t, "msg-1", dec.GetString(transport.TagToken))
}
