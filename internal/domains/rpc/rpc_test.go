package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/cntryl-go/internal/core/transport"
	"github.com/stretchr/testify/require"
)

// mockMux implements the muxProvider interface for tests.
type mockMux struct {
	sendCh chan transport.Frame
	inCh   chan transport.Frame
	ctx    context.Context
	cb     func()
}

func newMockMux() *mockMux {
	return &mockMux{sendCh: make(chan transport.Frame, 8), inCh: make(chan transport.Frame, 8), ctx: context.Background()}
}

func (m *mockMux) Send(f transport.Frame) error {
	m.sendCh <- f
	return nil
}

func (m *mockMux) In() <-chan transport.Frame { return m.inCh }
func (m *mockMux) Ctx() context.Context       { return m.ctx }
func (m *mockMux) OnReconnect(fn func())      { m.cb = fn }

func TestShouldReturnResponseGivenCallWhenServerReplies(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)

	route := "realm/area/resource"
	reqBody := []byte("hello")
	respBody := []byte("world")

	// Act: simulate broker replying when it sees request
	go func() {
		f := <-m.sendCh
		// ensure it's a request
		require.Equal(t, RPCRequest, f.Type)
		dec, err := transport.NewTLVDecoder(f.Body)
		require.NoError(t, err)
		id, err := dec.GetUint64(transport.TagID)
		require.NoError(t, err)
		// send a response with same id
		enc := transport.NewTLVEncoder()
		enc.AddUint64(transport.TagID, id)
		enc.AddBytes(transport.TagBody, respBody)
		m.inCh <- transport.Frame{Type: transport.FrameTypeResp, Channel: transport.ChannelRPC, Body: enc.Encode()}
	}()

	// Call
	res, err := c.Call(context.Background(), route, reqBody, time.Second)

	// Assert
	require.NoError(t, err)
	require.Equal(t, respBody, res)
}

func TestShouldReturnErrorGivenCallWhenServerSendsErr(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)

	route := "realm/area/resource"
	reqBody := []byte("hello")
	serverErr := "boom"

	go func() {
		f := <-m.sendCh
		require.Equal(t, RPCRequest, f.Type)
		dec, err := transport.NewTLVDecoder(f.Body)
		require.NoError(t, err)
		id, err := dec.GetUint64(transport.TagID)
		require.NoError(t, err)
		enc := transport.NewTLVEncoder()
		enc.AddUint64(transport.TagID, id)
		enc.AddString(transport.TagErr, serverErr)
		m.inCh <- transport.Frame{Type: transport.FrameTypeErr, Channel: transport.ChannelRPC, Body: enc.Encode()}
	}()

	// Act
	_, err := c.Call(context.Background(), route, reqBody, time.Second)

	// Assert
	require.Error(t, err)
	require.Contains(t, err.Error(), serverErr)
}

func TestShouldReturnTimeoutGivenCallWhenNoResponse(t *testing.T) {
	m := newMockMux()
	c := NewClient(m)

	route := "realm/area/resource"
	reqBody := []byte("x")

	// don't reply
	_, err := c.Call(context.Background(), route, reqBody, 10*time.Millisecond)

	require.Error(t, err)
	require.Equal(t, ErrRPCTimeout, err)
}

func TestShouldDispatchToWorkerGivenSubscribeWorker(t *testing.T) {
	m := newMockMux()
	c := NewClient(m)

	route := "realm/area/resource"
	reqBody := []byte("ping")
	respBody := []byte("pong")

	// Register worker
	sub, err := c.SubscribeWorker(context.Background(), route, func(ctx context.Context, r InboundRequest) ([]byte, error) {
		return respBody, nil
	})
	require.NoError(t, err)

	// drain the subscribe frame sent by SubscribeWorker
	<-m.sendCh

	// send a request frame
	enc := transport.NewTLVEncoder()
	enc.AddUint64(transport.TagID, 42)
	enc.AddString(transport.TagRoute, route)
	enc.AddBytes(transport.TagBody, reqBody)
	m.inCh <- transport.Frame{Type: RPCRequest, Channel: transport.ChannelRPC, Body: enc.Encode()}

	// expect a response to be sent via Send
	select {
	case f := <-m.sendCh:
		require.Equal(t, transport.FrameTypeResp, f.Type)
		dec, err := transport.NewTLVDecoder(f.Body)
		require.NoError(t, err)
		id, err := dec.GetUint64(transport.TagID)
		require.NoError(t, err)
		require.Equal(t, uint64(42), id)
		require.Equal(t, respBody, dec.GetBytes(transport.TagBody))
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for response")
	}

	// Unsubscribe and ensure no further handling occurs
	sub.Unsubscribe()
	enc2 := transport.NewTLVEncoder()
	enc2.AddUint64(transport.TagID, 43)
	enc2.AddString(transport.TagRoute, route)
	enc2.AddBytes(transport.TagBody, []byte("x"))
	m.inCh <- transport.Frame{Type: RPCRequest, Channel: transport.ChannelRPC, Body: enc2.Encode()}

	select {
	case f := <-m.sendCh:
		// if we get a frame it must not be a response for id 43 (handler shouldn't have run)
		dec, _ := transport.NewTLVDecoder(f.Body)
		id, _ := dec.GetUint64(transport.TagID)
		require.NotEqual(t, uint64(43), id)
	case <-time.After(50 * time.Millisecond):
		// no frame as expected
	}
}
