package connection

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/protocol"
	"github.com/cntryl/fitz-go/internal/testkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldAuthenticateAfterSilentConnectWindowGivenValidJWTWhenStartCalled(t *testing.T) {
	// Arrange
	transport := testkit.NewMockTransport()
	cfg := DefaultConfig()
	cfg.JWT = "token"
	cfg.AuthTimeout = 20 * time.Millisecond
	cfg.ReadTimeout = time.Second
	conn := New(transport, cfg)

	// Act
	err := conn.Start(context.Background())

	// Assert
	require.NoError(t, err)
	assert.True(t, conn.isAuthenticated())
	require.NoError(t, conn.Close())
}

func TestShouldReturnAuthenticationFailedGivenReadErrorWhenStartCalled(t *testing.T) {
	// Arrange
	transport := testkit.NewMockTransport()
	transport.SetReadError(io.EOF)
	cfg := DefaultConfig()
	cfg.JWT = "token"
	conn := New(transport, cfg)

	// Act
	err := conn.Start(context.Background())

	// Assert
	require.ErrorIs(t, err, ErrAuthenticationFailed)
}

func TestShouldConfirmAuthenticationGivenFirstValidResponseWhenStartCalled(t *testing.T) {
	// Arrange
	frame := protocol.EncodeFrameOwned(protocol.MessageTypeNoticePublish, []byte("payload"))
	defer frame.Release()
	transport := testkit.NewMockTransport()
	transport.SetReadFrames([][]byte{append([]byte(nil), frame.Bytes()...)})
	cfg := DefaultConfig()
	cfg.JWT = "token"
	cfg.ReadTimeout = time.Second
	conn := New(transport, cfg)

	// Act
	err := conn.Start(context.Background())

	// Assert
	require.NoError(t, err)
	assert.True(t, conn.isAuthenticated())
	require.NoError(t, conn.Close())
}

func TestShouldReturnConnectionClosedGivenCloseWhileRequestPendingWhenSendRequestCalled(t *testing.T) {
	// Arrange
	transport := testkit.NewMockTransport()
	cfg := DefaultConfig()
	cfg.JWT = ""
	cfg.ReadTimeout = time.Second
	conn := New(transport, cfg)
	require.NoError(t, conn.Start(context.Background()))

	result := make(chan error, 1)

	// Act
	go func() {
		_, err := conn.SendRequest(context.Background(), protocol.MessageTypeKvBegin, []byte("req"))
		result <- err
	}()
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, conn.Close())
	err := <-result

	// Assert
	require.ErrorIs(t, err, ErrConnectionClosed)
}

func TestShouldDispatchHandlersGivenIncomingNotifyFramesWhenStartCalled(t *testing.T) {
	// Arrange
	notifyPayload := func(subID uint64, route string, payload []byte) []byte {
		buf := GetBuffer()
		defer PutBuffer(buf)
		WriteU64BE(buf, subID)
		WriteString(buf, route)
		WriteBytes(buf, payload)
		return append([]byte(nil), buf.Bytes()...)
	}
	schedulePayload := func(subID uint64, payload []byte) []byte {
		buf := GetBuffer()
		defer PutBuffer(buf)
		WriteU64BE(buf, subID)
		WriteBytes(buf, payload)
		return append([]byte(nil), buf.Bytes()...)
	}
	noticeFrame := protocol.EncodeFrameOwned(protocol.MessageTypeNoticeNotify, notifyPayload(7, "notice://realm/area/resource", []byte("hello")))
	defer noticeFrame.Release()
	scheduleFrame := protocol.EncodeFrameOwned(protocol.MessageTypeScheduleNotify, schedulePayload(9, []byte("run")))
	defer scheduleFrame.Release()
	transport := testkit.NewMockTransport()
	transport.SetReadFrames([][]byte{
		append([]byte(nil), noticeFrame.Bytes()...),
		append([]byte(nil), scheduleFrame.Bytes()...),
	})
	conn := New(transport, Config{JWT: "", ReadTimeout: time.Second})
	noticeSeen := make(chan struct{}, 1)
	scheduleSeen := make(chan struct{}, 1)
	conn.RegisterNotifyHandler(protocol.MessageTypeNoticeNotify, func(subID uint64, route string, payload []byte) {
		if subID == 7 && route == "notice://realm/area/resource" && string(payload) == "hello" {
			noticeSeen <- struct{}{}
		}
	})
	conn.RegisterScheduleNotifyHandler(func(subID uint64, payload []byte) {
		if subID == 9 && string(payload) == "run" {
			scheduleSeen <- struct{}{}
		}
	})

	// Act
	require.NoError(t, conn.Start(context.Background()))

	// Assert
	select {
	case <-noticeSeen:
	case <-time.After(time.Second):
		t.Fatal("notice handler not called")
	}
	select {
	case <-scheduleSeen:
	case <-time.After(time.Second):
		t.Fatal("schedule handler not called")
	}
	require.NoError(t, conn.Close())
}
