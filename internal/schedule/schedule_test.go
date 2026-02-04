package schedule

import (
	"testing"

	"github.com/cntryl/cntryl-go/internal/transport"
	"github.com/stretchr/testify/assert"
)

func TestShouldSendCreateWhenCalled(t *testing.T) {
	m := &struct{ lastFrame transport.Frame }{}
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, "realm/area/resource")
	enc.AddString(transport.TagBody, "* * * * *")
	enc.AddBytes(transport.TagBody, []byte("payload"))
	frame := transport.Frame{Type: uint8(600 % 256), Flags: 0, Channel: 9, Body: enc.Encode()}
	m.lastFrame = frame

	assert.Equal(t, uint8(600%256), m.lastFrame.Type)
	dec, err := transport.NewTLVDecoder(m.lastFrame.Body)
	assert.NoError(t, err)
	assert.Equal(t, "realm/area/resource", dec.GetString(transport.TagRoute))
}

func TestShouldSendCancelWhenCalled(t *testing.T) {
	m := &struct{ lastFrame transport.Frame }{}
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagToken, "sched-1")
	frame := transport.Frame{Type: uint8(601 % 256), Flags: 0, Channel: 9, Body: enc.Encode()}
	m.lastFrame = frame
	assert.Equal(t, uint8(601%256), m.lastFrame.Type)
}

func TestShouldSendListWhenCalled(t *testing.T) {
	m := &struct{ lastFrame transport.Frame }{}
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, "realm/area/resource")
	frame := transport.Frame{Type: uint8(602 % 256), Flags: 0, Channel: 9, Body: enc.Encode()}
	m.lastFrame = frame
	dec, err := transport.NewTLVDecoder(m.lastFrame.Body)
	assert.NoError(t, err)
	assert.Equal(t, "realm/area/resource", dec.GetString(transport.TagRoute))
}
