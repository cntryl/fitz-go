package connection

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/protocol"
	"github.com/cntryl/fitz-go/internal/testkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type heartbeatMockTransport struct {
	*testkit.MockTransport

	count atomic.Int32
	err   error
}

func (h *heartbeatMockTransport) SendHeartbeat(context.Context) error {
	h.count.Add(1)
	return h.err
}

type authCloseRaceTransport struct {
	wrote        chan struct{}
	closeStarted chan struct{}
	releaseRead  chan struct{}
	releaseClose chan struct{}
	writeOnce    sync.Once
	closeOnce    sync.Once
}

func newAuthCloseRaceTransport() *authCloseRaceTransport {
	return &authCloseRaceTransport{
		wrote:        make(chan struct{}),
		closeStarted: make(chan struct{}),
		releaseRead:  make(chan struct{}),
		releaseClose: make(chan struct{}),
	}
}

func (t *authCloseRaceTransport) Write(context.Context, []byte) error {
	t.writeOnce.Do(func() {
		close(t.wrote)
	})
	return nil
}

func (t *authCloseRaceTransport) Read(ctx context.Context) ([]byte, error) {
	<-t.releaseRead
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

func (t *authCloseRaceTransport) Close() error {
	t.closeOnce.Do(func() {
		close(t.closeStarted)
	})
	<-t.releaseClose
	return nil
}

func (t *authCloseRaceTransport) RemoteAddr() string {
	return "mock://auth-close-race"
}

func TestShouldConfirmAuthenticationOnlyOnceGivenConcurrentCalls(t *testing.T) {
	conn := New(testkit.NewMockTransport(), DefaultConfig())
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	panicCh := make(chan any, 128)
	var wg sync.WaitGroup
	for range 128 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if recovered := recover(); recovered != nil {
					panicCh <- recovered
				}
			}()
			conn.confirmAuthentication()
		}()
	}
	wg.Wait()
	close(panicCh)

	require.Empty(t, panicCh)
	select {
	case <-conn.authConfirmed:
	default:
		t.Fatal("authentication was not confirmed")
	}
	assert.Equal(t, StateAuthenticated, conn.State())
}

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

func TestShouldReturnConnectionClosedGivenCloseDuringSilentAuthSettleWindow(t *testing.T) {
	transport := newAuthCloseRaceTransport()
	cfg := DefaultConfig()
	cfg.Token = "token"
	cfg.AuthSettleDelay = 20 * time.Millisecond
	cfg.ReadTimeout = time.Second
	conn := New(transport, cfg)

	startResult := make(chan error, 1)
	go func() {
		startResult <- conn.Start(context.Background())
	}()

	select {
	case <-transport.wrote:
	case <-time.After(time.Second):
		t.Fatal("CONNECT was not written")
	}

	closeResult := make(chan error, 1)
	go func() {
		closeResult <- conn.Close()
	}()

	select {
	case <-transport.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("close did not start")
	}

	select {
	case err := <-startResult:
		require.ErrorIs(t, err, ErrConnectionClosed)
	case <-time.After(time.Second):
		t.Fatal("Start did not return after auth settle window")
	}
	assert.False(t, conn.isAuthenticated())

	close(transport.releaseRead)
	close(transport.releaseClose)

	select {
	case err := <-closeResult:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close did not finish")
	}
}

func TestShouldRemainOpenGivenIdleReadTimeoutWhenAuthenticated(t *testing.T) {
	transport := testkit.NewMockTransport()
	cfg := DefaultConfig()
	cfg.Token = ""
	cfg.ReadTimeout = 10 * time.Millisecond
	conn := New(transport, cfg)

	require.NoError(t, conn.Start(context.Background()))

	select {
	case <-conn.Done():
		t.Fatal("idle read timeout unexpectedly closed the connection")
	case <-time.After(40 * time.Millisecond):
	}

	assert.Equal(t, StateAuthenticated, conn.State())
	assert.NoError(t, conn.Err())
	require.NoError(t, conn.Close())
}

func TestShouldReturnQueueFullGivenRequestWaitQueueSaturated(t *testing.T) {
	conn := New(testkit.NewMockTransport(), Config{
		Token:               "",
		MaxInFlightRequests: 1,
		MaxRequestQueueSize: 1,
	})
	conn.requestSem <- struct{}{}
	conn.queuedRequests.Store(1)

	err := conn.AcquireRequestSlotErr(context.Background())

	require.ErrorIs(t, err, ErrRequestQueueFull)
	<-conn.requestSem
}

func TestShouldSendHeartbeatGivenIdleAuthenticatedConnection(t *testing.T) {
	transport := &heartbeatMockTransport{MockTransport: testkit.NewMockTransport()}
	cfg := DefaultConfig()
	cfg.Token = ""
	cfg.ReadTimeout = 5 * time.Millisecond
	cfg.HeartbeatInterval = 10 * time.Millisecond
	cfg.HeartbeatTimeout = 20 * time.Millisecond
	conn := New(transport, cfg)
	require.NoError(t, conn.Start(context.Background()))
	defer func() {
		_ = conn.Close()
	}()

	require.Eventually(t, func() bool {
		return transport.count.Load() > 0
	}, time.Second, 5*time.Millisecond)
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

func TestShouldCloseLifecycleGivenDecodeFailureAfterStartWhenDispatchLoopExits(t *testing.T) {
	transport := testkit.NewMockTransport()
	transport.SetReadFrames([][]byte{{0xFF}})
	conn := New(transport, Config{Token: "", ReadTimeout: time.Second})

	require.NoError(t, conn.Start(context.Background()))

	select {
	case <-conn.Done():
	case <-time.After(time.Second):
		t.Fatal("dispatch loop did not exit after decode failure")
	}

	assert.Equal(t, StateClosed, conn.State())
	require.Error(t, conn.Err())
	assert.Contains(t, conn.Err().Error(), "decode frame")

	select {
	case <-conn.LifecycleContext().Done():
	case <-time.After(time.Second):
		t.Fatal("lifecycle context was not canceled after dispatch failure")
	}
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
