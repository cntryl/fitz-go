package connection

import (
	"context"
	"io"
	"log/slog"
	"sync"
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
	cfg.Token = "token"
	cfg.AuthSettleDelay = 20 * time.Millisecond
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
	cfg.Token = "token"
	conn := New(transport, cfg)

	// Act
	err := conn.Start(context.Background())

	// Assert
	require.ErrorIs(t, err, ErrAuthenticationFailed)
}

func TestShouldLogReadErrorGivenLoggerWhenStartCalled(t *testing.T) {
	transport := testkit.NewMockTransport()
	transport.SetReadError(io.EOF)
	recorder := newLogRecorder()
	cfg := DefaultConfig()
	cfg.Token = "token"
	cfg.Logger = slog.New(recorder)
	conn := New(transport, cfg)

	err := conn.Start(context.Background())
	require.ErrorIs(t, err, ErrAuthenticationFailed)
	assertLogEntry(t, recorder.snapshot(), slog.LevelWarn, "read error")
}

func TestShouldLogDecodeFailureGivenLoggerWhenStartCalled(t *testing.T) {
	transport := testkit.NewMockTransport()
	transport.SetReadFrames([][]byte{{0xFF}})
	recorder := newLogRecorder()
	cfg := DefaultConfig()
	cfg.Token = "token"
	cfg.AuthSettleDelay = 20 * time.Millisecond
	cfg.Logger = slog.New(recorder)
	conn := New(transport, cfg)

	err := conn.Start(context.Background())
	require.Error(t, err)
	assertLogEntry(t, recorder.snapshot(), slog.LevelError, "decode frame failed")
}

func TestShouldConfirmAuthenticationGivenFirstValidResponseWhenStartCalled(t *testing.T) {
	// Arrange
	frame := protocol.EncodeFrameOwned(protocol.MessageTypeNoticePublish, []byte("payload"))
	defer frame.Release()
	transport := testkit.NewMockTransport()
	transport.SetReadFrames([][]byte{append([]byte(nil), frame.Bytes()...)})
	cfg := DefaultConfig()
	cfg.Token = "token"
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
	cfg.Token = ""
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
	conn := New(transport, Config{Token: "", ReadTimeout: time.Second})
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

func TestShouldReturnGivenCloseBeforeStartWhenCloseCalled(t *testing.T) {
	transport := testkit.NewMockTransport()
	conn := New(transport, DefaultConfig())

	closed := make(chan error, 1)
	go func() {
		closed <- conn.Close()
	}()

	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("close blocked before start")
	}

	require.ErrorIs(t, conn.Start(context.Background()), ErrConnectionClosed)
	assert.Equal(t, StateClosed, conn.State())
}

type logRecord struct {
	level   slog.Level
	message string
}

type logRecorder struct {
	mu      sync.Mutex
	entries []logRecord
}

func newLogRecorder() *logRecorder {
	return &logRecorder{}
}

func (r *logRecorder) Enabled(context.Context, slog.Level) bool {
	return true
}

func (r *logRecorder) Handle(_ context.Context, record slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, logRecord{level: record.Level, message: record.Message})
	return nil
}

func (r *logRecorder) WithAttrs([]slog.Attr) slog.Handler {
	return r
}

func (r *logRecorder) WithGroup(string) slog.Handler {
	return r
}

func (r *logRecorder) snapshot() []logRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	entries := make([]logRecord, len(r.entries))
	copy(entries, r.entries)
	return entries
}

func assertLogEntry(t *testing.T, entries []logRecord, level slog.Level, message string) {
	t.Helper()
	for _, entry := range entries {
		if entry.level == level && entry.message == message {
			return
		}
	}
	t.Fatalf("expected log entry level=%s message=%q, got %#v", level, message, entries)
}
