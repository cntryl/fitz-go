package connection_test

import (
	"sync"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/stretchr/testify/assert"
)

// TestShouldTrackRequestsGivenRegisteredRequestsWhenMetricsRead tests request registration accounting.
func TestShouldTrackRequestsGivenRegisteredRequestsWhenMetricsRead(t *testing.T) {
	t.Run("incremental IDs", func(t *testing.T) {
		// Arrange
		mux := connection.NewMultiplexer()
		defer mux.Close()

		// Act
		for i := 0; i < 3; i++ {
			ch := make(chan []byte, 1)
			mux.RegisterRequest(uint16(100+i), ch, nil)
		}
		metrics := mux.Metrics()

		// Assert
		assert.Equal(t, int64(3), metrics.RequestsInFlight)
	})
}

// TestShouldRouteResponseGivenMatchingMessageTypeWhenDispatchCalled tests response routing by message type.
func TestShouldRouteResponseGivenMatchingMessageTypeWhenDispatchCalled(t *testing.T) {
	t.Run("matching ID receives response", func(t *testing.T) {
		// Arrange
		mux := connection.NewMultiplexer()
		defer mux.Close()

		ch1 := make(chan []byte, 1)
		ch2 := make(chan []byte, 1)

		mux.RegisterRequest(100, ch1, nil)
		mux.RegisterRequest(200, ch2, nil)

		// Act
		mux.Dispatch(100, []byte("response1"))
		mux.Dispatch(200, []byte("response2"))

		// Assert
		assert.Equal(t, []byte("response1"), <-ch1)
		assert.Equal(t, []byte("response2"), <-ch2)
	})

	t.Run("unregistered ID discards response", func(t *testing.T) {
		// Arrange
		mux := connection.NewMultiplexer()
		defer mux.Close()

		ch := make(chan []byte, 1)
		mux.RegisterRequest(100, ch, nil)

		// Act
		mux.Dispatch(999, []byte("orphaned"))

		// Assert
		select {
		case <-ch:
			t.Fatal("response should not be received on ch")
		case <-time.After(50 * time.Millisecond):
			// Expected - timeout waiting for unregistered dispatch
		}
	})
}

// TestShouldUnblockWaiterGivenPendingRequestWhenDispatchCalled tests waiter wake-up on dispatch.
func TestShouldUnblockWaiterGivenPendingRequestWhenDispatchCalled(t *testing.T) {
	// Arrange
	mux := connection.NewMultiplexer()
	defer mux.Close()

	ch := make(chan []byte, 1)
	done := make(chan bool, 1)

	mux.RegisterRequest(100, ch, nil)

	// Goroutine waiting for response
	go func() {
		select {
		case <-ch:
			done <- true
		case <-time.After(1 * time.Second):
			done <- false
		}
	}()

	// Act
	time.Sleep(50 * time.Millisecond)
	mux.Dispatch(100, []byte("response"))

	// Assert
	success := <-done
	assert.True(t, success)
}

// TestShouldHandleConcurrentRequestsGivenManyRegisteredRequestsWhenDispatchCalled tests concurrent dispatch behavior.
func TestShouldHandleConcurrentRequestsGivenManyRegisteredRequestsWhenDispatchCalled(t *testing.T) {
	t.Run("10 concurrent requests", func(t *testing.T) {
		// Arrange
		mux := connection.NewMultiplexer()
		defer mux.Close()

		numRequests := 10
		responses := make([]chan []byte, numRequests)
		for i := 0; i < numRequests; i++ {
			ch := make(chan []byte, 1)
			responses[i] = ch
			mux.RegisterRequest(uint16(100+i), ch, nil)
		}

		// Act
		for i := 0; i < numRequests; i++ {
			mux.Dispatch(uint16(100+i), []byte("response"+string(rune(i))))
		}

		// Assert
		for i := 0; i < numRequests; i++ {
			resp := <-responses[i]
			assert.NotNil(t, resp)
		}

		metrics := mux.Metrics()
		assert.Equal(t, int64(0), metrics.RequestsInFlight) // All dispatched and consumed
	})

	t.Run("100 concurrent requests", func(t *testing.T) {
		// Arrange
		mux := connection.NewMultiplexer()
		defer mux.Close()

		numRequests := 100
		responses := make([]chan []byte, numRequests)
		for i := 0; i < numRequests; i++ {
			ch := make(chan []byte, 1)
			responses[i] = ch
			mux.RegisterRequest(uint16(i), ch, nil)
		}

		// Act
		var wg sync.WaitGroup
		for i := 0; i < numRequests; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				mux.Dispatch(uint16(idx), []byte("resp"))
			}(i)
		}
		wg.Wait()

		// Assert
		for i := 0; i < numRequests; i++ {
			resp := <-responses[i]
			assert.NotNil(t, resp)
		}
	})
}

// TestShouldMaintainFIFOOrderGivenSharedMessageTypeWhenDispatchCalled tests FIFO response ordering.
func TestShouldMaintainFIFOOrderGivenSharedMessageTypeWhenDispatchCalled(t *testing.T) {
	// Arrange
	mux := connection.NewMultiplexer()
	defer mux.Close()

	// Register multiple requests for same message type (FIFO queue)
	ch1 := make(chan []byte, 1)
	ch2 := make(chan []byte, 1)
	ch3 := make(chan []byte, 1)

	mux.RegisterRequest(100, ch1, nil)
	mux.RegisterRequest(100, ch2, nil)
	mux.RegisterRequest(100, ch3, nil)

	// Act
	mux.Dispatch(100, []byte("first"))
	mux.Dispatch(100, []byte("second"))
	mux.Dispatch(100, []byte("third"))

	// Assert
	assert.Equal(t, []byte("first"), <-ch1)
	assert.Equal(t, []byte("second"), <-ch2)
	assert.Equal(t, []byte("third"), <-ch3)
}

// TestShouldCloseGracefullyGivenRegisteredRequestWhenCloseCalledTwice tests idempotent close behavior.
func TestShouldCloseGracefullyGivenRegisteredRequestWhenCloseCalledTwice(t *testing.T) {
	// Arrange
	mux := connection.NewMultiplexer()
	ch := make(chan []byte, 1)
	mux.RegisterRequest(100, ch, nil)

	// Act
	mux.Close()
	mux.Close()

	// Assert
}

// TestShouldCancelPendingRequestsGivenCancelFuncsWhenCloseCalled tests that pending cancel callbacks run on shutdown.
func TestShouldCancelPendingRequestsGivenCancelFuncsWhenCloseCalled(t *testing.T) {
	// Arrange
	mux := connection.NewMultiplexer()
	var cancelCount int
	ch := make(chan []byte, 1)
	mux.RegisterRequest(100, ch, func() { cancelCount++ })

	// Act
	err := mux.Close()

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 1, cancelCount)
	metrics := mux.Metrics()
	assert.Equal(t, int64(0), metrics.RequestsInFlight)
}

// TestShouldReportMetricsGivenRequestLifecycleWhenMetricsRead tests metric updates across dispatch.
func TestShouldReportMetricsGivenRequestLifecycleWhenMetricsRead(t *testing.T) {
	// Arrange
	mux := connection.NewMultiplexer()
	defer mux.Close()

	// Act
	metrics := mux.Metrics()

	// Assert
	assert.Zero(t, metrics.RequestsInFlight)
	assert.Zero(t, metrics.RequestsTotal)

	// Arrange
	ch := make(chan []byte, 1)
	mux.RegisterRequest(100, ch, nil)

	// Act
	metrics = mux.Metrics()

	// Assert
	assert.Equal(t, int64(1), metrics.RequestsInFlight)
	assert.Equal(t, uint64(1), metrics.RequestsTotal)

	// Act
	mux.Dispatch(100, []byte("resp"))
	<-ch

	// Assert
	metrics = mux.Metrics()
	assert.Equal(t, int64(0), metrics.RequestsInFlight)
	assert.Equal(t, uint64(1), metrics.RequestsTotal)
}

// Benchmarks

func BenchmarkRegisterRequest(b *testing.B) {
	mux := connection.NewMultiplexer()
	defer mux.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch := make(chan []byte, 1)
		mux.RegisterRequest(uint16(i%1000), ch, nil)
	}
}

func BenchmarkMuxDispatchResponse(b *testing.B) {
	mux := connection.NewMultiplexer()
	defer mux.Close()

	// Pre-register 1000 requests
	for i := 0; i < 1000; i++ {
		ch := make(chan []byte, 1)
		mux.RegisterRequest(uint16(i), ch, nil)
	}

	payload := []byte("test response payload")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mux.Dispatch(uint16(i%1000), payload)
	}
}

func BenchmarkConcurrentDispatch(b *testing.B) {
	mux := connection.NewMultiplexer()
	defer mux.Close()

	// Pre-register 1000 requests
	for i := 0; i < 1000; i++ {
		ch := make(chan []byte, 1)
		mux.RegisterRequest(uint16(i), ch, nil)
	}

	payload := []byte("test response")

	b.ReportAllocs()
	b.ResetTimer()
	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			mux.Dispatch(uint16(idx%1000), payload)
		}(i)
	}
	wg.Wait()
}
