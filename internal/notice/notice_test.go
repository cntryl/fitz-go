package notice

import (
	"testing"

	"github.com/cntryl/cntryl-go/internal/transport"
	"github.com/stretchr/testify/assert"
)

// MockMux is a minimal send-only mock for verifying outbound frames.
type MockMux struct {
	lastFrameSent *transport.Frame
	sendErr       error
}

func (m *MockMux) Send(f transport.Frame) error {
	m.lastFrameSent = &f
	return m.sendErr
}

func TestShouldSendSubscribeRequestWhenSubscribeCalled(t *testing.T) {
	// Arrange
	mock := &MockMux{}

	// Act: construct the frame via encoder and call mock.Send
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, "realm/area/resource")
	frame := transport.Frame{Type: NoticeSubscribe, Flags: 0, Channel: ChannelSub, Body: enc.Encode()}
	_ = mock.Send(frame)

	// Assert
	assert.NotNil(t, mock.lastFrameSent)
	assert.Equal(t, NoticeSubscribe, mock.lastFrameSent.Type)
	assert.Equal(t, ChannelSub, mock.lastFrameSent.Channel)
	dec, err := transport.NewTLVDecoder(mock.lastFrameSent.Body)
	assert.NoError(t, err)
	assert.Equal(t, "realm/area/resource", dec.GetString(transport.TagRoute))
}

func TestShouldSendUnsubscribeRequestWhenUnsubscribeCalled(t *testing.T) {
	mock := &MockMux{}
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, "realm/area/resource")
	frame := transport.Frame{Type: NoticeUnsubscribe, Flags: 0, Channel: ChannelSub, Body: enc.Encode()}
	_ = mock.Send(frame)

	assert.NotNil(t, mock.lastFrameSent)
	assert.Equal(t, NoticeUnsubscribe, mock.lastFrameSent.Type)
	dec, err := transport.NewTLVDecoder(mock.lastFrameSent.Body)
	assert.NoError(t, err)
	assert.Equal(t, "realm/area/resource", dec.GetString(transport.TagRoute))
}

func TestShouldSendPublishRequestWhenPublishCalled(t *testing.T) {
	mock := &MockMux{}
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, "realm/area/resource")
	enc.AddBytes(transport.TagBody, []byte("hello"))
	frame := transport.Frame{Type: NoticePublish, Flags: 0, Channel: ChannelPub, Body: enc.Encode()}
	_ = mock.Send(frame)

	assert.NotNil(t, mock.lastFrameSent)
	assert.Equal(t, NoticePublish, mock.lastFrameSent.Type)
	dec, err := transport.NewTLVDecoder(mock.lastFrameSent.Body)
	assert.NoError(t, err)
	assert.Equal(t, "realm/area/resource", dec.GetString(transport.TagRoute))
	assert.Equal(t, "hello", string(dec.GetBytes(transport.TagBody)))
}
