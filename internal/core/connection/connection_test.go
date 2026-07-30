package connection_test

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/core/connection"
	coreerrors "github.com/cntryl/fitz-go/internal/core/errors"
	"github.com/cntryl/fitz-go/internal/core/transport"
	"github.com/cntryl/fitz-go/internal/protocol"
	"github.com/cntryl/fitz-go/internal/testkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

// Mock transport is now provided by testkit.MockTransport from internal/testkit package

type writeReorderingTransport struct {
	frames    chan []byte
	closeOnce sync.Once
	closed    chan struct{}
}

func newWriteReorderingTransport() *writeReorderingTransport {
	return &writeReorderingTransport{
		frames: make(chan []byte, 8),
		closed: make(chan struct{}),
	}
}

func (t *writeReorderingTransport) Write(ctx context.Context, frame []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.closed:
		return io.EOF
	default:
	}

	msgType, payload, err := protocol.DecodeFrame(frame)
	if err != nil {
		return err
	}
	if msgType == protocol.MessageTypeConnect {
		return nil
	}

	if string(payload) == "first" {
		time.Sleep(50 * time.Millisecond)
	}

	resp := protocol.EncodeFrame(msgType, []byte("resp:"+string(payload)))
	select {
	case t.frames <- resp:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-t.closed:
		return io.EOF
	}
}

func (t *writeReorderingTransport) Read(ctx context.Context) ([]byte, error) {
	select {
	case frame := <-t.frames:
		return frame, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.closed:
		return nil, io.EOF
	}
}

func (t *writeReorderingTransport) Close() error {
	t.closeOnce.Do(func() {
		close(t.closed)
	})
	return nil
}

func (t *writeReorderingTransport) RemoteAddr() string {
	return "mock://reordering"
}

// TestShouldCreateConnectionGivenValidConfig tests basic connection creation.
func TestShouldCreateConnectionGivenValidConfigWhenNewCalled(t *testing.T) {
	// Arrange
	transport := &testkit.MockTransport{}
	cfg := connection.DefaultConfig()

	// Act
	conn := connection.New(transport, cfg)

	// Assert
	require.NotNil(t, conn)
}

func TestShouldReturnDefaultAsyncHandlerTimeoutGivenUnsetConfigWhenNewCalled(t *testing.T) {
	// Arrange
	transport := &testkit.MockTransport{}
	cfg := connection.DefaultConfig()
	cfg.AsyncHandlerTimeout = 0

	// Act
	conn := connection.New(transport, cfg)

	// Assert
	require.NotNil(t, conn)
	assert.Equal(t, 30*time.Second, conn.AsyncHandlerTimeout())
}

func TestShouldReturnConfiguredAsyncHandlerTimeoutGivenConfigWhenNewCalled(t *testing.T) {
	// Arrange
	transport := &testkit.MockTransport{}
	cfg := connection.DefaultConfig()
	cfg.AsyncHandlerTimeout = 2 * time.Second

	// Act
	conn := connection.New(transport, cfg)

	// Assert
	require.NotNil(t, conn)
	assert.Equal(t, 2*time.Second, conn.AsyncHandlerTimeout())
}

func TestShouldReturnDefaultAsyncHandlerMaxConcurrencyGivenUnsetConfigWhenNewCalled(t *testing.T) {
	// Arrange
	transport := &testkit.MockTransport{}
	cfg := connection.DefaultConfig()
	cfg.AsyncHandlerMaxConcurrency = 0

	// Act
	conn := connection.New(transport, cfg)

	// Assert
	require.NotNil(t, conn)
	assert.Equal(t, 256, conn.AsyncHandlerMaxConcurrency())
}

func TestShouldReturnConfiguredAsyncHandlerMaxConcurrencyGivenConfigWhenNewCalled(t *testing.T) {
	// Arrange
	transport := &testkit.MockTransport{}
	cfg := connection.DefaultConfig()
	cfg.AsyncHandlerMaxConcurrency = 8

	// Act
	conn := connection.New(transport, cfg)

	// Assert
	require.NotNil(t, conn)
	assert.Equal(t, 8, conn.AsyncHandlerMaxConcurrency())
}

func TestShouldUseConfiguredMeterGivenConfigWhenNewCalled(t *testing.T) {
	// Arrange
	transport := &testkit.MockTransport{}
	cfg := connection.DefaultConfig()
	meter := metricnoop.NewMeterProvider().Meter("fitz-go-connection-test")
	cfg.Meter = meter

	// Act
	conn := connection.New(transport, cfg)

	// Assert
	require.NotNil(t, conn)
	require.NotNil(t, conn.Meter())
	assert.Equal(t, meter, conn.Meter())
}

func TestShouldBoundConcurrentSlotsGivenMaxOneWhenAcquireAsyncHandlerSlotCalled(t *testing.T) {
	// Arrange
	transport := &testkit.MockTransport{}
	cfg := connection.DefaultConfig()
	cfg.AsyncHandlerMaxConcurrency = 1
	conn := connection.New(transport, cfg)
	require.NotNil(t, conn)

	// Act
	release1, ok1 := conn.AcquireAsyncHandlerSlot(context.Background())
	require.True(t, ok1)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, ok2 := conn.AcquireAsyncHandlerSlot(ctx)

	// Assert
	assert.False(t, ok2)

	release1()
	release3, ok3 := conn.AcquireAsyncHandlerSlot(context.Background())
	assert.True(t, ok3)
	release3()
}

type admissionBlockingTransport struct {
	mu             sync.Mutex
	requestWrites  int
	responseCh     chan []byte
	closed         chan struct{}
	requestWriteCh chan struct{}
}

func newAdmissionBlockingTransport() *admissionBlockingTransport {
	return &admissionBlockingTransport{
		responseCh:     make(chan []byte, 8),
		closed:         make(chan struct{}),
		requestWriteCh: make(chan struct{}, 8),
	}
}

func (t *admissionBlockingTransport) Write(ctx context.Context, frame []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.closed:
		return io.EOF
	default:
	}

	msgType, _, err := protocol.DecodeFrame(frame)
	if err != nil {
		return err
	}

	t.mu.Lock()
	if msgType != protocol.MessageTypeConnect {
		t.requestWrites++
		select {
		case t.requestWriteCh <- struct{}{}:
		default:
		}
	}
	t.mu.Unlock()
	return nil
}

func (t *admissionBlockingTransport) Read(ctx context.Context) ([]byte, error) {
	select {
	case frame := <-t.responseCh:
		return append([]byte(nil), frame...), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.closed:
		return nil, io.EOF
	}
}

func (t *admissionBlockingTransport) Close() error {
	select {
	case <-t.closed:
	default:
		close(t.closed)
	}
	return nil
}

func (t *admissionBlockingTransport) RemoteAddr() string {
	return "admission-blocking://transport"
}

func (transport *admissionBlockingTransport) waitForRequestWrite(t *testing.T, expected int) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		transport.mu.Lock()
		count := transport.requestWrites
		transport.mu.Unlock()
		if count >= expected {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for request write %d", expected)
		case <-transport.requestWriteCh:
		}
	}
}

func (t *admissionBlockingTransport) pushResponse(msgType uint16, payload []byte) {
	frame := protocol.EncodeFrame(msgType, payload)
	t.responseCh <- append([]byte(nil), frame...)
}

func (t *admissionBlockingTransport) requestWriteCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.requestWrites
}

func TestShouldBoundConcurrentOutboundRequestsGivenMaxOneWhenSecondRequestStarts(t *testing.T) {
	transport := newAdmissionBlockingTransport()
	cfg := connection.DefaultConfig()
	cfg.Token = ""
	cfg.AuthSettleDelay = 20 * time.Millisecond
	cfg.ReadTimeout = time.Second
	cfg.MaxInFlightRequests = 1
	conn := connection.New(transport, cfg)
	require.NoError(t, conn.Start(context.Background()))

	firstResult := make(chan error, 1)
	go func() {
		_, err := conn.SendRequest(context.Background(), protocol.MessageTypeKvBegin, []byte("first"))
		firstResult <- err
	}()

	transport.waitForRequestWrite(t, 1)

	secondCtx, cancelSecond := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancelSecond()
	secondResult := make(chan error, 1)
	go func() {
		_, err := conn.SendRequest(secondCtx, protocol.MessageTypeKvBegin, []byte("second"))
		secondResult <- err
	}()

	secondErr := <-secondResult
	require.ErrorIs(t, secondErr, context.DeadlineExceeded)
	assert.Equal(t, 1, transport.requestWriteCount())

	transport.pushResponse(protocol.MessageTypeKvBegin, []byte("ok"))

	firstErr := <-firstResult
	require.NoError(t, firstErr)
	require.NoError(t, conn.Close())
}

func TestShouldNotBlockFireAndForgetGivenPendingRequestWhenAdmissionPoolsAreSeparated(t *testing.T) {
	transport := newAdmissionBlockingTransport()
	cfg := connection.DefaultConfig()
	cfg.Token = ""
	cfg.AuthSettleDelay = 20 * time.Millisecond
	cfg.ReadTimeout = time.Second
	cfg.MaxInFlightRequests = 1
	conn := connection.New(transport, cfg)
	require.NoError(t, conn.Start(context.Background()))
	defer func() {
		_ = conn.Close()
	}()

	firstResult := make(chan error, 1)
	go func() {
		_, err := conn.SendRequest(context.Background(), protocol.MessageTypeKvBegin, []byte("first"))
		firstResult <- err
	}()

	transport.waitForRequestWrite(t, 1)

	fireAndForgetResult := make(chan error, 1)
	go func() {
		fireAndForgetResult <- conn.SendFireAndForget(context.Background(), protocol.MessageTypeNoticePublish, []byte("fire-and-forget"))
	}()

	select {
	case err := <-fireAndForgetResult:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("fire-and-forget blocked behind the synchronous request slot")
	}

	assert.Equal(t, 2, transport.requestWriteCount())
	transport.pushResponse(protocol.MessageTypeKvBegin, []byte("ok"))

	require.NoError(t, <-firstResult)
}

// TestShouldParseStandardResponseGivenSuccessStatus tests success response parsing.
func TestShouldParseStandardResponseGivenSuccessStatusWhenParseStandardResponseCalled(t *testing.T) {
	// Arrange - Success response: [status=0][remaining data]
	payload := []byte{0x00, 0x01, 0x02, 0x03}

	// Act
	success, remaining, err := connection.ParseStandardResponse(payload)

	// Assert
	require.NoError(t, err)
	assert.True(t, success)
	assert.Equal(t, []byte{0x01, 0x02, 0x03}, remaining)
}

// TestShouldParseStandardResponseGivenErrorStatus tests error response parsing.
func TestShouldParseStandardResponseGivenErrorStatusWhenParseStandardResponseCalled(t *testing.T) {
	// Arrange - Error response: [status=1][u32 BE len][error message]
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteU8(buf, 1) // Error status
	connection.WriteString(buf, "test error message")
	payload := buf.Bytes()

	// Act
	success, _, err := connection.ParseStandardResponse(payload)

	// Assert
	require.Error(t, err)
	assert.False(t, success)
	assert.Contains(t, err.Error(), "test error message")
}

// TestShouldRejectParseGivenEmptyPayload tests edge case of empty payload.
func TestShouldRejectParseGivenEmptyPayloadWhenParseStandardResponseCalled(t *testing.T) {
	_, _, err := connection.ParseStandardResponse([]byte{})

	require.Error(t, err)
}

// TestShouldEncodeDecodeU32BE tests U32BE encoding/decoding.
func TestShouldEncodeDecodeU32BEGivenValueWhenHelpersCalled(t *testing.T) {
	t.Run("value 0", func(t *testing.T) {
		buf := connection.GetBuffer()
		defer connection.PutBuffer(buf)

		connection.WriteU32BE(buf, 0)
		actual, _, err := connection.ReadU32BE(buf.Bytes(), 0)

		require.NoError(t, err)
		assert.Equal(t, uint32(0), actual)
	})

	t.Run("value max uint32", func(t *testing.T) {
		buf := connection.GetBuffer()
		defer connection.PutBuffer(buf)

		connection.WriteU32BE(buf, 0xFFFFFFFF)
		actual, _, err := connection.ReadU32BE(buf.Bytes(), 0)

		require.NoError(t, err)
		assert.Equal(t, uint32(0xFFFFFFFF), actual)
	})
}

// TestShouldEncodeDecodeU64BE tests U64BE encoding/decoding.
func TestShouldEncodeDecodeU64BEGivenValueWhenHelpersCalled(t *testing.T) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	expectedValue := uint64(0x123456789ABCDEF0)

	connection.WriteU64BE(buf, expectedValue)
	actual, _, err := connection.ReadU64BE(buf.Bytes(), 0)

	require.NoError(t, err)
	assert.Equal(t, expectedValue, actual)
}

// TestShouldEncodeDecodeU8 tests U8 encoding/decoding.
func TestShouldEncodeDecodeU8GivenValueWhenHelpersCalled(t *testing.T) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteU8(buf, 42)
	actual, _, err := connection.ReadU8(buf.Bytes(), 0)

	require.NoError(t, err)
	assert.Equal(t, uint8(42), actual)
}

// TestShouldEncodeDecodeString tests string encoding/decoding.
func TestShouldEncodeDecodeStringGivenValueWhenHelpersCalled(t *testing.T) {
	t.Run("simple ASCII", func(t *testing.T) {
		buf := connection.GetBuffer()
		defer connection.PutBuffer(buf)

		expectedString := "hello world"

		connection.WriteString(buf, expectedString)
		actual, _, err := connection.ReadString(buf.Bytes(), 0)

		require.NoError(t, err)
		assert.Equal(t, expectedString, actual)
	})

	t.Run("UTF-8 with special characters", func(t *testing.T) {
		buf := connection.GetBuffer()
		defer connection.PutBuffer(buf)

		expectedString := "test string with special chars: ñ 测试"

		connection.WriteString(buf, expectedString)
		actual, _, err := connection.ReadString(buf.Bytes(), 0)

		require.NoError(t, err)
		assert.Equal(t, expectedString, actual)
	})

	t.Run("empty string", func(t *testing.T) {
		buf := connection.GetBuffer()
		defer connection.PutBuffer(buf)

		connection.WriteString(buf, "")
		actual, _, err := connection.ReadString(buf.Bytes(), 0)

		require.NoError(t, err)
		assert.Empty(t, actual)
	})
}

// TestShouldMatchResponsesInFIFOOrder tests multiplexer FIFO ordering.
func TestShouldMatchResponsesInFIFOOrderGivenSharedMessageTypeWhenDispatchCalled(t *testing.T) {
	// Arrange
	mux := connection.NewMultiplexer()
	defer closeQuietly(mux)

	// Register 3 requests for same MessageType
	resp1 := make(chan []byte, 1)
	resp2 := make(chan []byte, 1)
	resp3 := make(chan []byte, 1)

	mux.RegisterRequest(100, resp1, nil)
	mux.RegisterRequest(100, resp2, nil)
	mux.RegisterRequest(100, resp3, nil)

	// Act - Dispatch responses
	mux.Dispatch(100, []byte("response_1"))
	mux.Dispatch(100, []byte("response_2"))
	mux.Dispatch(100, []byte("response_3"))

	// Assert - Verify FIFO order
	assert.Equal(t, []byte("response_1"), <-resp1)
	assert.Equal(t, []byte("response_2"), <-resp2)
	assert.Equal(t, []byte("response_3"), <-resp3)
}

// TestShouldReturnMetricsGivenMultiplexer tests metrics collection.
func TestShouldReturnMetricsGivenRegisteredRequestWhenMetricsCalled(t *testing.T) {
	// Arrange
	mux := connection.NewMultiplexer()
	defer closeQuietly(mux)

	respChan := make(chan []byte, 1)
	mux.RegisterRequest(100, respChan, nil)

	// Act
	metrics := mux.Metrics()

	// Assert
	assert.Equal(t, int64(1), metrics.RequestsInFlight)
	assert.Equal(t, uint64(1), metrics.RequestsTotal)
}

// TestShouldDispatchToCorrectChannel tests response routing.
func TestShouldDispatchToCorrectChannelGivenMatchingMessageTypeWhenDispatchCalled(t *testing.T) {
	mux := connection.NewMultiplexer()
	defer closeQuietly(mux)

	resp := make(chan []byte, 1)
	mux.RegisterRequest(100, resp, nil)

	mux.Dispatch(100, []byte("test response"))

	select {
	case data := <-resp:
		assert.Equal(t, []byte("test response"), data)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("response not received")
	}
}

func TestShouldPreserveResponseRoutingGivenConcurrentSameTypeRequestsWhenSendRequestCalled(t *testing.T) {
	transport := newWriteReorderingTransport()
	cfg := connection.DefaultConfig()
	cfg.Token = ""
	conn := connection.New(transport, cfg)

	ctx := t.Context()
	defer func() {
		_ = conn.Close()
	}()

	require.NoError(t, conn.Start(ctx))

	type result struct {
		name string
		resp string
		err  error
	}

	results := make(chan result, 2)

	go func() {
		resp, err := conn.SendRequest(ctx, 100, []byte("first"))
		results <- result{name: "first", resp: string(resp), err: err}
	}()

	time.Sleep(10 * time.Millisecond)

	go func() {
		resp, err := conn.SendRequest(ctx, 100, []byte("second"))
		results <- result{name: "second", resp: string(resp), err: err}
	}()

	got := make(map[string]result, 2)
	for range 2 {
		select {
		case res := <-results:
			got[res.name] = res
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for request results")
		}
	}

	require.Len(t, got, 2)
	require.NoError(t, got["first"].err)
	require.NoError(t, got["second"].err)
	assert.Equal(t, "resp:first", got["first"].resp)
	assert.Equal(t, "resp:second", got["second"].resp)
}

func TestShouldClassifyRetryableGivenDeadlineExceededWhenRetryPolicyEvaluated(t *testing.T) {
	assert.True(t, connection.IsTransientRetryable(context.DeadlineExceeded))
}

func TestShouldClassifyRetryableGivenConnectionClosedWhenRetryPolicyEvaluated(t *testing.T) {
	assert.True(t, connection.IsTransientRetryable(connection.ErrConnectionClosed))
}

func TestShouldClassifyRetryableGivenTransportErrorWhenRetryPolicyEvaluated(t *testing.T) {
	transportErr := &transport.TransportError{Op: "read", Cause: errors.New("socket")}

	assert.True(t, connection.IsTransientRetryable(transportErr))
}

func TestShouldClassifyRetryableGivenIsolationConflictWhenRetryPolicyEvaluated(t *testing.T) {
	err := coreerrors.NewDomainError(coreerrors.KvIsolationConflict, "isolation conflict")

	assert.True(t, connection.IsTransientRetryable(err))
}

func TestShouldClassifyFatalGivenInvalidModeWhenRetryPolicyEvaluated(t *testing.T) {
	err := coreerrors.NewDomainError(coreerrors.KvInvalidMode, "invalid mode")

	assert.False(t, connection.IsTransientRetryable(err))
}

func TestShouldClassifyFatalGivenNilErrorWhenRetryPolicyEvaluated(t *testing.T) {
	assert.False(t, connection.IsTransientRetryable(nil))
}

func TestShouldStopRetryingGivenNonRetryableErrorWhenRunWithRetryCalled(t *testing.T) {
	transport := &testkit.MockTransport{}
	cfg := connection.DefaultConfig()
	cfg.RetryEnabled = true
	cfg.RetryMaxAttempts = 3
	cfg.RetryBackoff = time.Millisecond
	cfg.RetryMaxBackoff = 2 * time.Millisecond

	conn := connection.New(transport, cfg)
	attempts := 0
	err := conn.RunWithRetry(context.Background(), func() error {
		attempts++
		return errors.New("non-retryable")
	}, nil)

	require.Error(t, err)
	assert.Equal(t, 1, attempts)
}

func TestShouldRespectMaxAttemptsGivenRetryableErrorWhenRunWithRetryCalled(t *testing.T) {
	transport := &testkit.MockTransport{}
	cfg := connection.DefaultConfig()
	cfg.RetryEnabled = true
	cfg.RetryMaxAttempts = 2
	cfg.RetryBackoff = time.Millisecond
	cfg.RetryMaxBackoff = 2 * time.Millisecond

	conn := connection.New(transport, cfg)
	attempts := 0
	err := conn.RunWithRetry(context.Background(), func() error {
		attempts++
		return connection.ErrConnectionClosed
	}, nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, connection.ErrConnectionClosed)
	assert.Equal(t, 2, attempts)
}

func TestShouldReturnStaleHandleGivenClosedConnectionWhenCheckLiveHandleCalled(t *testing.T) {
	transport := &testkit.MockTransport{}
	cfg := connection.DefaultConfig()
	cfg.Token = ""
	conn := connection.New(transport, cfg)
	require.NoError(t, conn.Close())

	err := conn.CheckLiveHandle()
	require.Error(t, err)
	assert.ErrorIs(t, err, connection.ErrStaleHandle)
}

// Benchmarks

func BenchmarkEncodeDecodeU32BE(b *testing.B) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		buf.Reset()
		connection.WriteU32BE(buf, 0x12345678)
		_, _, _ = connection.ReadU32BE(buf.Bytes(), 0)
	}
}

func BenchmarkEncodeDecodeU64BE(b *testing.B) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		buf.Reset()
		connection.WriteU64BE(buf, 0x123456789ABCDEF0)
		_, _, _ = connection.ReadU64BE(buf.Bytes(), 0)
	}
}

func BenchmarkEncodeDecodeString(b *testing.B) {
	b.Run("small string", func(b *testing.B) {
		testString := "hello world"
		buf := connection.GetBuffer()
		defer connection.PutBuffer(buf)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			buf.Reset()
			connection.WriteString(buf, testString)
			_, _, _ = connection.ReadString(buf.Bytes(), 0)
		}
	})

	b.Run("large string", func(b *testing.B) {
		testString := "this is a much longer test string that might be used in real KV operations"
		buf := connection.GetBuffer()
		defer connection.PutBuffer(buf)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			buf.Reset()
			connection.WriteString(buf, testString)
			_, _, _ = connection.ReadString(buf.Bytes(), 0)
		}
	})
}

func BenchmarkDispatchResponse(b *testing.B) {
	mux := connection.NewMultiplexer()
	defer closeQuietly(mux)

	// Pre-register channels to avoid registration overhead in benchmark
	channels := make([]chan []byte, 100)
	for i := range 100 {
		ch := make(chan []byte, 1)
		channels[i] = ch
		mux.RegisterRequest(uint16(100+i), ch, nil)
	}

	payload := []byte("test response")

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		mux.Dispatch(uint16(100+i%100), payload)
	}
}

func BenchmarkParseStandardResponseSuccess(b *testing.B) {
	// [status=0][1K remaining]
	remaining := make([]byte, 1024)
	payload := make([]byte, 1+len(remaining))
	payload[0] = 0
	copy(payload[1:], remaining)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _, _ = connection.ParseStandardResponse(payload)
	}
}

func BenchmarkParseStandardResponseError(b *testing.B) {
	// [status=1][u32 len][error message]
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	connection.WriteU8(buf, 1)
	connection.WriteString(buf, "test error message")
	payload := make([]byte, buf.Len())
	copy(payload, buf.Bytes())
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _, _ = connection.ParseStandardResponse(payload)
	}
}

func BenchmarkGetPutBuffer(b *testing.B) {
	// Warm up the pool
	for range 10 {
		buf := connection.GetBuffer()
		connection.PutBuffer(buf)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		buf := connection.GetBuffer()
		connection.PutBuffer(buf)
	}
}
