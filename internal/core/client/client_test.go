package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/core/transport"
	"github.com/cntryl/fitz-go/internal/domains/notice"
	"github.com/cntryl/fitz-go/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldCreateClientGivenAddressWhenNewClientCalled(t *testing.T) {
	// Arrange
	tokenProvider := func(context.Context) (string, error) { return "", nil }

	// Act
	c := NewClient("localhost:4091", tokenProvider)

	// Assert
	require.NotNil(t, c)
	assert.Equal(t, "localhost:4091", c.addr)
	assert.NotNil(t, c.config)
}

func TestShouldDetectWebSocketGivenWebSocketURLWhenDetectTransportCalled(t *testing.T) {
	// Arrange
	url := "ws://localhost:4090/ws"

	// Act
	transportType := detectTransport(url)

	// Assert
	assert.Equal(t, TransportWebSocket, transportType)
}

func TestShouldDetectTCPGivenHostPortWhenDetectTransportCalled(t *testing.T) {
	// Arrange
	url := "localhost:4091"

	// Act
	transportType := detectTransport(url)

	// Assert
	assert.Equal(t, TransportTCP, transportType)
}

func TestShouldReturnErrorGivenMissingURLWhenDialCalled(t *testing.T) {
	// Arrange
	ctx := context.Background()

	// Act
	c, err := Dial(ctx, "", nil)

	// Assert
	require.Error(t, err)
	assert.Nil(t, c)
	assert.Contains(t, err.Error(), "URL is required")
}

func TestShouldReturnErrorGivenTokenProviderFailureWhenConnectCalled(t *testing.T) {
	// Arrange
	expected := errors.New("token unavailable")
	c := NewClient("localhost:4091", func(context.Context) (string, error) {
		return "", expected
	})

	// Act
	err := c.Connect(context.Background())

	// Assert
	require.Error(t, err)
	assert.ErrorIs(t, err, expected)
}

func TestShouldReturnErrorGivenInvalidConfigWhenConnectCalled(t *testing.T) {
	// Arrange
	c := NewClient("", func(context.Context) (string, error) {
		return "", nil
	})

	// Act
	err := c.Connect(context.Background())

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "URL is required")
}

func TestShouldReturnNilGivenNoConnectionWhenCloseCalledTwice(t *testing.T) {
	// Arrange
	c := NewClient("localhost:4091", nil)

	// Act
	err1 := c.Close()
	err2 := c.Close()

	// Assert
	require.NoError(t, err1)
	require.NoError(t, err2)
}

func TestShouldValidateTransportLabelGivenKnownValueWhenTransportTypeStringCalled(t *testing.T) {
	// Arrange / Act / Assert
	assert.Equal(t, "websocket", transportTypeString(TransportWebSocket))
	assert.Equal(t, "tcp", transportTypeString(TransportTCP))
	assert.Equal(t, "auto", transportTypeString(TransportAuto))
}

func TestShouldRefreshTokenAndRestoreNoticeSubscriptionGivenConnectionLossWhenReconnectEnabled(t *testing.T) {
	// Arrange
	firstTransport := newScriptedTransport()
	secondTransport := newScriptedTransport()

	originalDialTCP := dialTCPTransport
	originalDialWS := dialWebSocketTransport
	defer func() {
		dialTCPTransport = originalDialTCP
		dialWebSocketTransport = originalDialWS
	}()

	transports := []transport.Transport{firstTransport, secondTransport}
	dialTCPTransport = func(context.Context, string) (transport.Transport, error) {
		if len(transports) == 0 {
			return nil, errors.New("no transports remaining")
		}
		next := transports[0]
		transports = transports[1:]
		return next, nil
	}

	tokenCalls := 0
	c := NewClient("localhost:4091", func(context.Context) (string, error) {
		tokenCalls++
		return fmt.Sprintf("token-%d", tokenCalls), nil
	})
	c.config.AuthSettleDelay = 20 * time.Millisecond
	c.config.ReconnectEnabled = true
	c.config.ReconnectBackoff = 10 * time.Millisecond
	c.config.MaxReconnects = 1

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go pushResponseAfterWrite(firstTransport, scheduleListResponseFrame(t), 2)
	require.NoError(t, c.Connect(ctx))

	go pushResponseAfterWrite(firstTransport, noticeSubscribeResponseFrame(t, 11), 3)
	_, err := c.Notice().Subscribe(ctx, "notice://realm/area/resource", func(context.Context, notice.NoticeMsg) error {
		return nil
	})
	require.NoError(t, err)

	initialConn := c.currentConnection()
	go pushResponseAfterWrite(secondTransport, scheduleListResponseFrame(t), 2)
	go pushResponseAfterWrite(secondTransport, noticeSubscribeResponseFrame(t, 22), 3)

	// Act
	require.NoError(t, initialConn.Close())

	// Assert
	require.Eventually(t, func() bool {
		return c.currentConnection() != nil && c.currentConnection() != initialConn
	}, time.Second, 20*time.Millisecond)
	assert.Equal(t, 2, tokenCalls)

	writtenFrames := secondTransport.WrittenFrames()
	require.Len(t, writtenFrames, 3)
	connectType, connectPayload, err := protocol.DecodeFrame(writtenFrames[0])
	require.NoError(t, err)
	assert.Equal(t, protocol.MessageTypeConnect, connectType)
	assert.Equal(t, []byte("token-2"), connectPayload)

	probeType, _, err := protocol.DecodeFrame(writtenFrames[1])
	require.NoError(t, err)
	assert.Equal(t, protocol.MessageTypeScheduleList, probeType)

	subscribeType, _, err := protocol.DecodeFrame(writtenFrames[2])
	require.NoError(t, err)
	assert.Equal(t, protocol.MessageTypeNoticeSubscribe, subscribeType)
}

func noticeSubscribeResponseFrame(t *testing.T, subID uint64) []byte {
	t.Helper()

	// Arrange
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	buf.WriteByte(0)
	buf.WriteByte(1)
	connection.WriteU64BE(buf, subID)
	frame := protocol.EncodeFrameOwned(protocol.MessageTypeNoticeSubscribe, append([]byte(nil), buf.Bytes()...))
	defer frame.Release()

	// Act
	encoded := append([]byte(nil), frame.Bytes()...)

	// Assert
	return encoded
}

func scheduleListResponseFrame(t *testing.T) []byte {
	t.Helper()

	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	buf.WriteByte(0)
	connection.WriteU64BE(buf, 0)
	frame := protocol.EncodeFrameOwned(protocol.MessageTypeScheduleList, append([]byte(nil), buf.Bytes()...))
	defer frame.Release()
	return append([]byte(nil), frame.Bytes()...)
}

type scriptedTransport struct {
	mu      sync.Mutex
	written [][]byte
	readCh  chan []byte
	closed  chan struct{}
}

func newScriptedTransport() *scriptedTransport {
	return &scriptedTransport{
		readCh: make(chan []byte, 8),
		closed: make(chan struct{}),
	}
}

func (s *scriptedTransport) Write(ctx context.Context, frame []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closed:
		return transport.ErrTransportClosed
	default:
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.written = append(s.written, append([]byte(nil), frame...))
	return nil
}

func (s *scriptedTransport) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.closed:
		return nil, io.EOF
	case frame := <-s.readCh:
		return append([]byte(nil), frame...), nil
	}
}

func (s *scriptedTransport) Close() error {
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
	return nil
}

func (s *scriptedTransport) RemoteAddr() string {
	return "scripted://transport"
}

func (s *scriptedTransport) PushReadFrame(frame []byte) {
	s.readCh <- append([]byte(nil), frame...)
}

func (s *scriptedTransport) WrittenFrames() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.written))
	for idx := range s.written {
		out[idx] = append([]byte(nil), s.written[idx]...)
	}
	return out
}

func pushResponseAfterWrite(trans *scriptedTransport, frame []byte, expectedWrites int) {
	for {
		if len(trans.WrittenFrames()) >= expectedWrites {
			trans.PushReadFrame(frame)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}
