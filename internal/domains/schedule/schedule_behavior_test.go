package schedule

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testIOTimeout = 3 * time.Second

type scriptedTransport struct {
	mu      sync.Mutex
	writes  chan []byte
	reads   chan []byte
	closed  bool
	address string
}

func newScriptedTransport() *scriptedTransport {
	return &scriptedTransport{
		writes:  make(chan []byte, 8),
		reads:   make(chan []byte, 8),
		address: "scripted://transport",
	}
}

func (t *scriptedTransport) Write(ctx context.Context, frame []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case t.writes <- append([]byte(nil), frame...):
		return nil
	}
}

func (t *scriptedTransport) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case frame := <-t.reads:
		return frame, nil
	}
}

func (t *scriptedTransport) Close() error {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	return nil
}

func (t *scriptedTransport) RemoteAddr() string {
	return t.address
}

func newStartedScheduleClient(t *testing.T) (*client, *scriptedTransport) {
	t.Helper()
	transport := newScriptedTransport()
	conn := connection.New(transport, connection.Config{Token: "", ReadTimeout: testIOTimeout})
	require.NoError(t, conn.Start(context.Background()))
	select {
	case frame := <-transport.writes:
		msgType, _, err := protocol.DecodeFrame(frame)
		require.NoError(t, err)
		require.Equal(t, protocol.MessageTypeConnect, msgType)
	case <-time.After(testIOTimeout):
		t.Fatal("timed out waiting for connect frame")
	}
	t.Cleanup(func() {
		_ = conn.Close()
	})
	return NewClient(conn).(*client), transport
}

func scheduleSuccessFrame(msgType uint16, payload []byte) []byte {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	connection.WriteU8(buf, 0)
	buf.Write(payload)
	frame := protocol.EncodeFrameOwned(msgType, append([]byte(nil), buf.Bytes()...))
	defer frame.Release()
	return append([]byte(nil), frame.Bytes()...)
}

func respondOnNextWrite(t *testing.T, transport *scriptedTransport, msgType uint16, payload []byte) {
	t.Helper()
	go func() {
		select {
		case <-transport.writes:
			transport.reads <- scheduleSuccessFrame(msgType, payload)
		case <-time.After(testIOTimeout):
			t.Error("timed out waiting for request write")
		}
	}()
}

func TestShouldReturnServerScheduleIDGivenPresentWhenCreateCalled(t *testing.T) {
	// Arrange
	client, transport := newStartedScheduleClient(t)
	buf := connection.GetBuffer()
	connection.WriteU8(buf, 1)
	connection.WriteString(buf, "schedule-id")
	payload := append([]byte(nil), buf.Bytes()...)
	connection.PutBuffer(buf)
	respondOnNextWrite(t, transport, protocol.MessageTypeScheduleCreate, payload)

	// Act
	id, err := client.Create(context.Background(), "schedule://realm/area/resource", "0 0 * * *", []byte("payload"))

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "schedule-id", id)
}

func TestShouldReturnRouteGivenNoServerScheduleIDWhenCreateCalled(t *testing.T) {
	// Arrange
	client, transport := newStartedScheduleClient(t)
	respondOnNextWrite(t, transport, protocol.MessageTypeScheduleCreate, []byte{0})
	route := "schedule://realm/area/resource"

	// Act
	id, err := client.Create(context.Background(), route, "0 0 * * *", []byte("payload"))

	// Assert
	require.NoError(t, err)
	assert.Equal(t, route, id)
}

func TestShouldParseEntriesGivenValidListResponseWhenListCalled(t *testing.T) {
	// Arrange
	client, transport := newStartedScheduleClient(t)
	buf := connection.GetBuffer()
	connection.WriteU64BE(buf, 2)
	connection.WriteU8(buf, 1)
	connection.WriteString(buf, "schedule://realm/area/one")
	connection.WriteString(buf, "0 0 * * *")
	connection.WriteBytes(buf, []byte("first"))
	connection.WriteU8(buf, 1)
	connection.WriteString(buf, "schedule://realm/area/two")
	connection.WriteString(buf, "*/5 * * * *")
	connection.WriteBytes(buf, []byte("second"))
	connection.WriteU8(buf, 0)
	payload := append([]byte(nil), buf.Bytes()...)
	connection.PutBuffer(buf)
	respondOnNextWrite(t, transport, protocol.MessageTypeScheduleList, payload)

	// Act
	entries, totalCount, err := client.List(context.Background(), 0, 100)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, uint64(2), totalCount)
	require.Len(t, entries, 2)
	assert.Equal(t, "schedule://realm/area/one", entries[0].Route)
	assert.Equal(t, []byte("second"), entries[1].Payload)
}

func TestShouldReturnErrorGivenShortListResponseWhenListCalled(t *testing.T) {
	// Arrange
	client, transport := newStartedScheduleClient(t)
	respondOnNextWrite(t, transport, protocol.MessageTypeScheduleList, []byte{1, 2, 3})

	// Act
	entries, totalCount, err := client.List(context.Background(), 0, 100)

	// Assert
	require.Error(t, err)
	assert.Nil(t, entries)
	assert.Equal(t, uint64(0), totalCount)
}

func TestShouldUseServerSubscriptionIDGivenPresentWhenSubscribeCalled(t *testing.T) {
	// Arrange
	client, transport := newStartedScheduleClient(t)
	buf := connection.GetBuffer()
	connection.WriteU8(buf, 1)
	connection.WriteU64BE(buf, 42)
	payload := append([]byte(nil), buf.Bytes()...)
	connection.PutBuffer(buf)
	respondOnNextWrite(t, transport, protocol.MessageTypeScheduleSubscribe, payload)

	// Act
	sub, err := client.Subscribe(context.Background(), "schedule://realm/area/*", func(context.Context, Notification) error {
		return nil
	})

	// Assert
	require.NoError(t, err)
	assert.Equal(t, uint64(42), sub.subID)
}

func TestShouldReturnErrorGivenMissingServerSubscriptionIDWhenSubscribeCalled(t *testing.T) {
	// Arrange
	client, transport := newStartedScheduleClient(t)
	respondOnNextWrite(t, transport, protocol.MessageTypeScheduleSubscribe, nil)

	// Act
	sub, err := client.Subscribe(context.Background(), "schedule://realm/area/*", func(context.Context, Notification) error {
		return nil
	})

	// Assert
	require.Error(t, err)
	assert.Nil(t, sub)
}

func TestShouldDispatchNotificationGivenMatchingSubscriptionWhenHandleScheduleNotifyCalled(t *testing.T) {
	// Arrange
	client, _ := newStartedScheduleClient(t)
	received := make(chan Notification, 1)
	_, _, err := client.subscriptions.Subscribe("schedule://realm/area/*", func(_ context.Context, n Notification) error {
		received <- n
		return nil
	}, func(string) (uint64, error) {
		return 7, nil
	})
	require.NoError(t, err)

	// Act
	client.handleScheduleNotify(7, []byte("run"))

	// Assert
	select {
	case msg := <-received:
		assert.Equal(t, []byte("run"), msg.Payload)
	case <-time.After(time.Second):
		t.Fatal("schedule notification not delivered")
	}
}

func TestShouldFanOutHandlersGivenDuplicatePatternWhenSubscribeCalled(t *testing.T) {
	client, transport := newStartedScheduleClient(t)
	buf := connection.GetBuffer()
	connection.WriteU8(buf, 1)
	connection.WriteU64BE(buf, 42)
	payload := append([]byte(nil), buf.Bytes()...)
	connection.PutBuffer(buf)
	respondOnNextWrite(t, transport, protocol.MessageTypeScheduleSubscribe, payload)

	first := make(chan Notification, 1)
	second := make(chan Notification, 1)

	sub1, err := client.Subscribe(context.Background(), "schedule://realm/area/*", func(_ context.Context, n Notification) error {
		first <- n
		return nil
	})
	require.NoError(t, err)

	sub2, err := client.Subscribe(context.Background(), "schedule://realm/area/*", func(_ context.Context, n Notification) error {
		second <- n
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, sub1.subID, sub2.subID)

	client.handleScheduleNotify(42, []byte("fire"))

	select {
	case msg := <-first:
		assert.Equal(t, []byte("fire"), msg.Payload)
	case <-time.After(time.Second):
		t.Fatal("first schedule handler not notified")
	}

	select {
	case msg := <-second:
		assert.Equal(t, []byte("fire"), msg.Payload)
	case <-time.After(time.Second):
		t.Fatal("second schedule handler not notified")
	}
}

func TestShouldContinueFanOutGivenHandlerErrorWhenHandleScheduleNotifyCalled(t *testing.T) {
	client, _ := newStartedScheduleClient(t)

	_, _, err := client.subscriptions.Subscribe("schedule://realm/area/*", func(_ context.Context, _ Notification) error {
		return assert.AnError
	}, func(string) (uint64, error) {
		return 77, nil
	})
	require.NoError(t, err)

	received := make(chan Notification, 1)
	_, _, err = client.subscriptions.Subscribe("schedule://realm/area/*", func(_ context.Context, n Notification) error {
		received <- n
		return nil
	}, func(string) (uint64, error) {
		return 77, nil
	})
	require.NoError(t, err)

	client.handleScheduleNotify(77, []byte("still-delivered"))

	select {
	case msg := <-received:
		assert.Equal(t, []byte("still-delivered"), msg.Payload)
	case <-time.After(time.Second):
		t.Fatal("schedule notify fan-out stalled after handler error")
	}
}

func TestShouldReturnErrorGivenInvalidRouteWhenCancelCalled(t *testing.T) {
	// Arrange
	client, _ := newStartedScheduleClient(t)

	// Act
	err := client.Cancel(context.Background(), "schedule://realm/area")

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid route")
}
