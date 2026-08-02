package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/core/iter"
	"github.com/cntryl/fitz-go/internal/core/transport"
	"github.com/cntryl/fitz-go/internal/domains/kv"
	"github.com/cntryl/fitz-go/internal/domains/notice"
	"github.com/cntryl/fitz-go/internal/domains/rpc"
	"github.com/cntryl/fitz-go/internal/protocol"
	"github.com/cntryl/fitz-go/internal/testkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type cleanupRPCClient struct {
	once     sync.Once
	called   chan struct{}
	callErr  error
	callIter iter.Iterator[rpc.ResponseFrame]
}

func (c *cleanupRPCClient) RegisterWorker(context.Context, string, rpc.RPCHandler) (*rpc.Subscription, error) {
	return nil, nil
}

func (c *cleanupRPCClient) Call(context.Context, string, []byte) (iter.Iterator[rpc.ResponseFrame], error) {
	return c.callIter, c.callErr
}

func (c *cleanupRPCClient) ClosePendingRPCs() {
	c.once.Do(func() {
		close(c.called)
	})
}

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
	assert.Contains(t, err.Error(), "url is required")
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
	assert.Contains(t, err.Error(), "url is required")
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

func TestShouldClosePendingRPCsGivenConnectionLossWhenMonitorConnectionEnds(t *testing.T) {
	transport := testkit.NewMockTransport()
	conn := connection.New(transport, connection.Config{Token: "", ReadTimeout: time.Second})
	require.NoError(t, conn.Start(context.Background()))
	t.Cleanup(func() {
		_ = conn.Close()
	})

	called := make(chan struct{})
	c := &Client{
		config:    &Config{ReconnectEnabled: false},
		rpcClient: &cleanupRPCClient{called: called},
	}
	c.conn.Store(conn)

	done := make(chan struct{})
	go func() {
		c.monitorConnection(conn)
		close(done)
	}()

	require.NoError(t, conn.Close())

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("expected pending RPC cleanup on connection loss")
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("monitorConnection did not return")
	}
}

func TestShouldIgnoreStaleConnectionGivenReplacementWhenMonitorConnectionEnds(t *testing.T) {
	oldTransport := testkit.NewMockTransport()
	oldConn := connection.New(oldTransport, connection.Config{Token: "", ReadTimeout: time.Second})
	require.NoError(t, oldConn.Start(context.Background()))
	t.Cleanup(func() {
		_ = oldConn.Close()
	})

	newTransport := testkit.NewMockTransport()
	newConn := connection.New(newTransport, connection.Config{Token: "", ReadTimeout: time.Second})
	require.NoError(t, newConn.Start(context.Background()))
	t.Cleanup(func() {
		_ = newConn.Close()
	})

	called := make(chan struct{})
	c := &Client{
		config:    &Config{ReconnectEnabled: true},
		rpcClient: &cleanupRPCClient{called: called},
	}
	c.conn.Store(newConn)

	done := make(chan struct{})
	go func() {
		c.monitorConnection(oldConn)
		close(done)
	}()

	require.NoError(t, oldConn.Close())

	select {
	case <-called:
		t.Fatal("stale connection unexpectedly triggered pending RPC cleanup")
	case <-time.After(100 * time.Millisecond):
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("monitorConnection did not return for stale connection")
	}
}

func TestShouldClosePendingRPCsGivenClientShutdownWhenMonitorConnectionEnds(t *testing.T) {
	transport := testkit.NewMockTransport()
	conn := connection.New(transport, connection.Config{Token: "", ReadTimeout: time.Second})
	require.NoError(t, conn.Start(context.Background()))

	called := make(chan struct{})
	c := &Client{
		config:    &Config{ReconnectEnabled: true},
		rpcClient: &cleanupRPCClient{called: called},
	}
	c.conn.Store(conn)

	done := make(chan struct{})
	go func() {
		c.monitorConnection(conn)
		close(done)
	}()

	require.NoError(t, c.Close())

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("expected pending RPC cleanup during client shutdown")
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("monitorConnection did not return during client shutdown")
	}
}

func TestShouldStopReconnectGivenCloseDuringBackoffWhenCloseCalled(t *testing.T) {
	originalDialTCP := dialTCPTransport
	originalDialWS := dialWebSocketTransport
	defer func() {
		dialTCPTransport = originalDialTCP
		dialWebSocketTransport = originalDialWS
	}()

	firstAttempt := make(chan struct{})
	var attempts atomic.Int32
	dialTCPTransport = func(context.Context, string) (transport.Transport, error) {
		if attempts.Add(1) == 1 {
			close(firstAttempt)
		}
		return nil, errors.New("broker unavailable")
	}

	c := NewClient("localhost:4091", nil)
	c.config.ReconnectBackoff = time.Second
	c.config.ReconnectMaxDelay = time.Second
	c.config.MaxReconnects = 0
	done := make(chan struct{})
	go func() {
		c.beginReconnect(connection.ErrConnectionClosed)
		close(done)
	}()

	select {
	case <-firstAttempt:
	case <-time.After(time.Second):
		t.Fatal("first reconnect attempt did not start")
	}
	require.NoError(t, c.Close())

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reconnect loop did not stop after close")
	}
	assert.Equal(t, int32(1), attempts.Load())
	assert.False(t, c.IsReconnecting())
}

func TestShouldAllowManualReconnectGivenConnectionLossWhenReconnectDisabled(t *testing.T) {
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

	c := NewClient("localhost:4091", func(context.Context) (string, error) {
		return "token-1", nil
	})
	c.config.AuthSettleDelay = 20 * time.Millisecond
	c.config.ReconnectEnabled = false

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, c.Connect(ctx))
	defer func() {
		_ = c.Close()
	}()

	initialConn := c.currentConnection()
	require.NotNil(t, initialConn)

	require.NoError(t, initialConn.Close())
	require.Eventually(t, func() bool {
		return c.currentConnection() == nil
	}, time.Second, 20*time.Millisecond)

	require.NoError(t, c.Connect(ctx))
	require.NotNil(t, c.currentConnection())
	assert.NotEqual(t, initialConn, c.currentConnection())

	writtenFrames := secondTransport.WrittenFrames()
	require.Len(t, writtenFrames, 1)
	connectType, connectPayload, err := protocol.DecodeFrame(writtenFrames[0])
	require.NoError(t, err)
	assert.Equal(t, protocol.MessageTypeConnect, connectType)
	assert.Equal(t, []byte("token-1"), connectPayload)
}

func TestShouldConnectGivenDelayedStartupWhenConnectWhenReadyCalled(t *testing.T) {
	// Arrange
	originalDialTCP := dialTCPTransport
	defer func() { dialTCPTransport = originalDialTCP }()
	readyTransport := newScriptedTransport()
	var attempts atomic.Int32
	dialTCPTransport = func(context.Context, string) (transport.Transport, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("broker unavailable")
		}
		return readyTransport, nil
	}
	client := NewClient("localhost:4091", nil)
	client.config.ReconnectBackoff = time.Millisecond
	client.config.ReconnectMaxDelay = time.Millisecond
	client.config.MaxReconnects = 3
	client.config.AuthSettleDelay = time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Act
	err := client.ConnectWhenReady(ctx)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, int32(2), attempts.Load())
	require.NoError(t, client.Close())
}

func TestShouldReturnNilWithoutRedialGivenExistingConnectionWhenConnectCalled(t *testing.T) {
	originalDialTCP := dialTCPTransport
	defer func() {
		dialTCPTransport = originalDialTCP
	}()

	scripted := newScriptedTransport()
	dialCalls := 0
	dialTCPTransport = func(context.Context, string) (transport.Transport, error) {
		dialCalls++
		return scripted, nil
	}

	c := NewClient("localhost:4091", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	require.NoError(t, c.Connect(ctx))
	defer closeQuietly(c)

	err := c.Connect(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, dialCalls)
}

func TestShouldValidateTransportLabelGivenKnownValueWhenTransportTypeStringCalled(t *testing.T) {
	// Arrange / Act / Assert
	assert.Equal(t, "websocket", transportTypeString(TransportWebSocket))
	assert.Equal(t, "tcp", transportTypeString(TransportTCP))
	assert.Equal(t, "auto", transportTypeString(TransportAuto))
}

func TestShouldUseDefaultAsyncHandlerTimeoutGivenNoOptionWhenNewClientCreated(t *testing.T) {
	// Act
	c := NewClient("localhost:4091", nil)

	// Assert
	require.NotNil(t, c.config)
	assert.Equal(t, 30*time.Second, c.config.AsyncHandlerTimeout)
}

func TestShouldApplyAsyncHandlerTimeoutOptionGivenOverrideWhenNewClientWithOptionsCalled(t *testing.T) {
	// Arrange
	customTimeout := 5 * time.Second

	// Act
	c := NewClientWithOptions("localhost:4091", nil, WithAsyncHandlerTimeout(customTimeout))

	// Assert
	require.NotNil(t, c.config)
	assert.Equal(t, customTimeout, c.config.AsyncHandlerTimeout)
}

func TestShouldUseDefaultAsyncHandlerMaxConcurrencyGivenNoOptionWhenNewClientCreated(t *testing.T) {
	// Act
	c := NewClient("localhost:4091", nil)

	// Assert
	require.NotNil(t, c.config)
	assert.Equal(t, 256, c.config.AsyncHandlerMaxConcurrency)
}

func TestShouldUseDefaultMaxInFlightRequestsGivenNoOptionWhenNewClientCreated(t *testing.T) {
	// Act
	c := NewClient("localhost:4091", nil)

	// Assert
	require.NotNil(t, c.config)
	assert.Equal(t, 256, c.config.MaxInFlightRequests)
}

func TestShouldUseTSStyleResilienceDefaultsGivenNoOptionsWhenNewClientCreated(t *testing.T) {
	c := NewClient("localhost:4091", nil)

	require.NotNil(t, c.config)
	assert.True(t, c.config.ReconnectEnabled)
	assert.Equal(t, 0, c.config.MaxReconnects)
	assert.Equal(t, 250*time.Millisecond, c.config.ReconnectBackoff)
	assert.Equal(t, 5*time.Second, c.config.ReconnectMaxDelay)
	assert.True(t, c.config.RetryEnabled)
	assert.Equal(t, 3, c.config.RetryMaxAttempts)
	assert.Equal(t, 100*time.Millisecond, c.config.RetryBackoff)
	assert.Equal(t, time.Second, c.config.RetryMaxDelay)
	assert.True(t, c.config.HeartbeatEnabled)
	assert.Equal(t, 10*time.Second, c.config.HeartbeatInterval)
	assert.Equal(t, 30*time.Second, c.config.HeartbeatTimeout)
	assert.Equal(t, 1024, c.config.MaxRequestQueueSize)
}

func TestShouldApplyAsyncHandlerMaxConcurrencyOptionGivenOverrideWhenNewClientWithOptionsCalled(t *testing.T) {
	// Arrange
	customMax := 64

	// Act
	c := NewClientWithOptions("localhost:4091", nil, WithAsyncHandlerMaxConcurrency(customMax))

	// Assert
	require.NotNil(t, c.config)
	assert.Equal(t, customMax, c.config.AsyncHandlerMaxConcurrency)
}

func TestShouldEmitLifecycleEventsGivenConnectAndCloseWhenHandlerConfigured(t *testing.T) {
	firstTransport := newScriptedTransport()

	originalDialTCP := dialTCPTransport
	originalDialWS := dialWebSocketTransport
	defer func() {
		dialTCPTransport = originalDialTCP
		dialWebSocketTransport = originalDialWS
	}()
	dialTCPTransport = func(context.Context, string) (transport.Transport, error) {
		return firstTransport, nil
	}

	var events []string
	c := NewClientWithOptions("localhost:4091", nil, WithLifecycleHandler(func(event LifecycleEvent) {
		events = append(events, event.Event)
	}))
	c.config.AuthSettleDelay = 20 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, c.Connect(ctx))
	require.NoError(t, c.Close())

	assert.Contains(t, events, "connect_start")
	assert.Contains(t, events, "auth_start")
	assert.Contains(t, events, "connect_succeeded")
	assert.Contains(t, events, "closed")
}

func TestShouldApplyMaxInFlightRequestsOptionGivenOverrideWhenNewClientWithOptionsCalled(t *testing.T) {
	// Arrange
	customMax := 7

	// Act
	c := NewClientWithOptions("localhost:4091", nil, WithMaxInFlightRequests(customMax))

	// Assert
	require.NotNil(t, c.config)
	assert.Equal(t, customMax, c.config.MaxInFlightRequests)
}

func TestShouldApplyMeterOptionGivenOverrideWhenNewClientWithOptionsCalled(t *testing.T) {
	// Arrange
	meter := metricnoop.NewMeterProvider().Meter("fitz-go-client-test")

	// Act
	c := NewClientWithOptions("localhost:4091", nil, WithMeter(meter))

	// Assert
	require.NotNil(t, c.config)
	require.NotNil(t, c.config.Meter)
	assert.Equal(t, meter, c.config.Meter)
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
	require.NoError(t, c.Connect(ctx))

	go pushResponseAfterWrite(firstTransport, noticeSubscribeResponseFrame(t, 11), 2)
	_, err := c.Notice().Subscribe(ctx, "notice://realm/area/resource", func(context.Context, notice.NoticeMsg) error {
		return nil
	})
	require.NoError(t, err)

	initialConn := c.currentConnection()
	go pushResponseAfterWrite(secondTransport, noticeSubscribeResponseFrame(t, 22), 2)

	// Act
	require.NoError(t, initialConn.Close())

	// Assert
	require.Eventually(t, func() bool {
		return c.currentConnection() != nil && c.currentConnection() != initialConn
	}, time.Second, 20*time.Millisecond)
	assert.Equal(t, 2, tokenCalls)

	require.Eventually(t, func() bool {
		return len(secondTransport.WrittenFrames()) >= 2
	}, time.Second, 10*time.Millisecond)

	writtenFrames := secondTransport.WrittenFrames()
	require.GreaterOrEqual(t, len(writtenFrames), 2)
	connectType, connectPayload, err := protocol.DecodeFrame(writtenFrames[0])
	require.NoError(t, err)
	assert.Equal(t, protocol.MessageTypeConnect, connectType)
	assert.Equal(t, []byte("token-2"), connectPayload)

	subscribeType, _, err := protocol.DecodeFrame(writtenFrames[1])
	require.NoError(t, err)
	assert.Equal(t, protocol.MessageTypeNoticeSubscribe, subscribeType)
}

func TestShouldEmitReconnectMetricsGivenReconnectSuccessWhenConnectionRestored(t *testing.T) {
	// Arrange
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
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

	c := NewClientWithOptions(
		"localhost:4091",
		func(context.Context) (string, error) { return "token-1", nil },
		WithMeter(meterProvider.Meter("fitz-go-client-test")),
	)
	c.config.AuthSettleDelay = 20 * time.Millisecond
	c.config.ReconnectEnabled = true
	c.config.ReconnectBackoff = 10 * time.Millisecond
	c.config.MaxReconnects = 1

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, c.Connect(ctx))

	go pushResponseAfterWrite(firstTransport, noticeSubscribeResponseFrame(t, 11), 2)
	_, err := c.Notice().Subscribe(ctx, "notice://realm/area/resource", func(context.Context, notice.NoticeMsg) error {
		return nil
	})
	require.NoError(t, err)

	initialConn := c.currentConnection()
	go pushResponseAfterWrite(secondTransport, noticeSubscribeResponseFrame(t, 22), 2)

	// Act
	require.NoError(t, initialConn.Close())
	require.Eventually(t, func() bool {
		return c.currentConnection() != nil && c.currentConnection() != initialConn
	}, time.Second, 20*time.Millisecond)

	require.Eventually(t, func() bool {
		var metrics metricdata.ResourceMetrics
		if err := reader.Collect(ctx, &metrics); err != nil {
			return false
		}
		foundCount, foundDuration := reconnectMetricPresence(metrics)
		return foundCount && foundDuration
	}, time.Second, 20*time.Millisecond)
	assert.NoError(t, c.Close())
}

func TestShouldRebindKVClientGivenReconnectWhenExistingDomainHandleUsed(t *testing.T) {
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

	c := NewClient("localhost:4091", func(context.Context) (string, error) {
		return "token-1", nil
	})
	c.config.AuthSettleDelay = 20 * time.Millisecond
	c.config.ReconnectEnabled = true
	c.config.ReconnectBackoff = 10 * time.Millisecond
	c.config.MaxReconnects = 1

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, c.Connect(ctx))
	t.Cleanup(func() {
		_ = c.Close()
	})

	kvClient := c.KV()
	initialConn := c.currentConnection()
	go pushResponseAfterWrite(secondTransport, kvBeginResponseFrame(t, 44), 2)

	// Act
	require.NoError(t, initialConn.Close())
	require.Eventually(t, func() bool {
		return c.currentConnection() != nil && c.currentConnection() != initialConn
	}, time.Second, 20*time.Millisecond)

	tx, err := kvClient.Begin(ctx, "kv://realm/area/resource", kv.DurabilityBuffered)

	// Assert
	require.NoError(t, err)
	require.NotNil(t, tx)
	assert.Len(t, firstTransport.WrittenFrames(), 1)
	require.Eventually(t, func() bool {
		return len(secondTransport.WrittenFrames()) >= 2
	}, time.Second, 10*time.Millisecond)

	writtenFrames := secondTransport.WrittenFrames()
	require.GreaterOrEqual(t, len(writtenFrames), 2)
	connectType, _, err := protocol.DecodeFrame(writtenFrames[0])
	require.NoError(t, err)
	assert.Equal(t, protocol.MessageTypeConnect, connectType)

	beginType, _, err := protocol.DecodeFrame(writtenFrames[1])
	require.NoError(t, err)
	assert.Equal(t, protocol.MessageTypeKvBegin, beginType)
}

func reconnectMetricPresence(metrics metricdata.ResourceMetrics) (bool, bool) {
	foundCount := false
	foundDuration := false
	for _, scopeMetrics := range metrics.ScopeMetrics {
		for _, collected := range scopeMetrics.Metrics {
			switch data := collected.Data.(type) {
			case metricdata.Sum[int64]:
				if collected.Name != "fitz.reconnect.attempts" {
					continue
				}
				for _, point := range data.DataPoints {
					attrs := point.Attributes.ToSlice()
					if hasStringAttr(attrs, "fitz.outcome", "success") && hasStringAttr(attrs, "fitz.transport", "tcp") && point.Value >= 1 {
						foundCount = true
					}
				}
			case metricdata.Histogram[int64]:
				if collected.Name != "fitz.reconnect.attempt_duration_ms" {
					continue
				}
				for _, point := range data.DataPoints {
					attrs := point.Attributes.ToSlice()
					if hasStringAttr(attrs, "fitz.outcome", "success") && hasStringAttr(attrs, "fitz.transport", "tcp") && point.Count >= 1 {
						foundDuration = true
					}
				}
			}
		}
	}
	return foundCount, foundDuration
}

func hasStringAttr(attrs []attribute.KeyValue, key string, expected string) bool {
	for _, attr := range attrs {
		if string(attr.Key) == key && attr.Value.Type() == attribute.STRING && attr.Value.AsString() == expected {
			return true
		}
	}
	return false
}

func TestShouldSendOnlyConnectFrameGivenAuthenticatedTokenWhenConnectCalled(t *testing.T) {
	firstTransport := newScriptedTransport()

	originalDialTCP := dialTCPTransport
	originalDialWS := dialWebSocketTransport
	defer func() {
		dialTCPTransport = originalDialTCP
		dialWebSocketTransport = originalDialWS
	}()

	dialTCPTransport = func(context.Context, string) (transport.Transport, error) {
		return firstTransport, nil
	}

	c := NewClient("localhost:4091", func(context.Context) (string, error) {
		return "token-1", nil
	})
	c.config.AuthSettleDelay = 20 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	require.NoError(t, c.Connect(ctx))
	t.Cleanup(func() {
		_ = c.Close()
	})

	writtenFrames := firstTransport.WrittenFrames()
	require.Len(t, writtenFrames, 1)
	connectType, connectPayload, err := protocol.DecodeFrame(writtenFrames[0])
	require.NoError(t, err)
	assert.Equal(t, protocol.MessageTypeConnect, connectType)
	assert.Equal(t, []byte("token-1"), connectPayload)
}

func TestShouldLogClientLifecycleGivenReconnectLoggerWhenConnectionRestored(t *testing.T) {
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

	recorder := newLogRecorder()
	c := NewClient("localhost:4091", func(context.Context) (string, error) {
		return "token-1", nil
	})
	c.config.Logger = slog.New(recorder)
	c.config.AuthSettleDelay = 20 * time.Millisecond
	c.config.ReconnectEnabled = true
	c.config.ReconnectBackoff = 10 * time.Millisecond
	c.config.MaxReconnects = 1

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, c.Connect(ctx))
	t.Cleanup(func() {
		_ = c.Close()
	})

	go pushResponseAfterWrite(firstTransport, noticeSubscribeResponseFrame(t, 11), 2)
	_, err := c.Notice().Subscribe(ctx, "notice://realm/area/resource", func(context.Context, notice.NoticeMsg) error {
		return nil
	})
	require.NoError(t, err)

	initialConn := c.currentConnection()
	go pushResponseAfterWrite(secondTransport, noticeSubscribeResponseFrame(t, 22), 2)
	require.NoError(t, firstTransport.Close())
	require.Eventually(t, func() bool {
		return c.currentConnection() != nil && c.currentConnection() != initialConn
	}, time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool {
		entries := recorder.snapshot()
		for _, entry := range entries {
			if entry.level == slog.LevelInfo && entry.message == "reconnect success" {
				return true
			}
		}
		return false
	}, time.Second, 20*time.Millisecond)
	require.NoError(t, c.Close())

	entries := recorder.snapshot()
	assertLogEntry(t, entries, slog.LevelInfo, "connect started")
	assertLogEntry(t, entries, slog.LevelInfo, "connect success")
	assertLogEntry(t, entries, slog.LevelInfo, "connection authenticating")
	assertLogEntry(t, entries, slog.LevelInfo, "connection authenticated after silent CONNECT window")
	assertLogEntry(t, entries, slog.LevelWarn, "reconnect attempt")
	assertLogEntry(t, entries, slog.LevelInfo, "reconnect success")
	assertLogEntry(t, entries, slog.LevelInfo, "client close")
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

func kvBeginResponseFrame(t *testing.T, txID uint64) []byte {
	t.Helper()

	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	buf.WriteByte(0)
	connection.WriteU64BE(buf, txID)
	frame := protocol.EncodeFrameOwned(protocol.MessageTypeKvBegin, append([]byte(nil), buf.Bytes()...))
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
