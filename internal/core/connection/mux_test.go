package connection_test

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/protocol"
	"github.com/stretchr/testify/assert"
)

// TestShouldTrackRequestsGivenRegisteredRequestsWhenMetricsRead tests request registration accounting.
func TestShouldTrackRequestsGivenRegisteredRequestsWhenMetricsRead(t *testing.T) {
	t.Run("incremental IDs", func(t *testing.T) {
		// Arrange
		mux := connection.NewMultiplexer()
		defer closeQuietly(mux)

		// Act
		for i := range 3 {
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
		defer closeQuietly(mux)

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
		defer closeQuietly(mux)

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
	defer closeQuietly(mux)

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

func TestShouldReturnPromptlyGivenSlowConsumerWhenDispatchCalled(t *testing.T) {
	// Arrange
	mux := connection.NewMultiplexer()
	defer closeQuietly(mux)

	ch := make(chan []byte)
	mux.RegisterRequest(100, ch, nil)

	received := make(chan []byte, 1)
	go func() {
		time.Sleep(25 * time.Millisecond)
		received <- <-ch
	}()

	done := make(chan struct{})
	start := time.Now()
	go func() {
		mux.Dispatch(100, []byte("response"))
		close(done)
	}()

	// Assert dispatch returns quickly instead of parking the loop for the slow consumer window.
	select {
	case <-done:
	case <-time.After(20 * time.Millisecond):
		t.Fatalf("dispatch blocked for %s", time.Since(start))
	}

	select {
	case resp := <-received:
		assert.Equal(t, []byte("response"), resp)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("slow consumer did not receive response")
	}
}

// TestShouldHandleConcurrentRequestsGivenManyRegisteredRequestsWhenDispatchCalled tests concurrent dispatch behavior.
func TestShouldHandleConcurrentRequestsGivenManyRegisteredRequestsWhenDispatchCalled(t *testing.T) {
	t.Run("10 concurrent requests", func(t *testing.T) {
		// Arrange
		mux := connection.NewMultiplexer()
		defer closeQuietly(mux)

		numRequests := 10
		responses := make([]chan []byte, numRequests)
		for i := range numRequests {
			ch := make(chan []byte, 1)
			responses[i] = ch
			mux.RegisterRequest(uint16(100+i), ch, nil)
		}

		// Act
		for i := range numRequests {
			mux.Dispatch(uint16(100+i), []byte("response"+string(rune(i))))
		}

		// Assert
		for i := range numRequests {
			resp := <-responses[i]
			assert.NotNil(t, resp)
		}

		metrics := mux.Metrics()
		assert.Equal(t, int64(0), metrics.RequestsInFlight) // All dispatched and consumed
	})

	t.Run("100 concurrent requests", func(t *testing.T) {
		// Arrange
		mux := connection.NewMultiplexer()
		defer closeQuietly(mux)

		numRequests := 100
		responses := make([]chan []byte, numRequests)
		for i := range numRequests {
			ch := make(chan []byte, 1)
			responses[i] = ch
			mux.RegisterRequest(uint16(i), ch, nil)
		}

		// Act
		var wg sync.WaitGroup
		for i := range numRequests {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				mux.Dispatch(uint16(idx), []byte("resp"))
			}(i)
		}
		wg.Wait()

		// Assert
		for i := range numRequests {
			resp := <-responses[i]
			assert.NotNil(t, resp)
		}
	})
}

func TestShouldAllowConcurrentHandlerReplacementGivenNotifyDispatchWhenSetNotifyHandlerAndDispatchCalled(t *testing.T) {
	mux := connection.NewMultiplexer()
	defer closeQuietly(mux)

	payload := func() []byte {
		buf := make([]byte, 0, 8+4+20+4+4)
		subID := make([]byte, 8)
		binary.BigEndian.PutUint64(subID, 7)
		buf = append(buf, subID...)
		route := []byte("notice://realm/area/x")
		routeLen := make([]byte, 4)
		binary.BigEndian.PutUint32(routeLen, uint32(len(route)))
		buf = append(buf, routeLen...)
		buf = append(buf, route...)
		body := []byte("ping")
		bodyLen := make([]byte, 4)
		binary.BigEndian.PutUint32(bodyLen, uint32(len(body)))
		buf = append(buf, bodyLen...)
		buf = append(buf, body...)
		return buf
	}()

	var delivered atomic.Int64
	done := make(chan struct{})
	var wg sync.WaitGroup

	wg.Go(func() {
		for range 500 {
			mux.SetNotifyHandler(protocol.MessageTypeNoticeNotify, func(subID uint64, route string, body []byte) {
				delivered.Add(1)
			})
		}
		close(done)
	})

	wg.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
				mux.Dispatch(protocol.MessageTypeNoticeNotify, payload)
			}
		}
	})

	wg.Wait()
	assert.Positive(t, delivered.Load())
}

// TestShouldMaintainFIFOOrderGivenSharedMessageTypeWhenDispatchCalled tests FIFO response ordering.
func TestShouldMaintainFIFOOrderGivenSharedMessageTypeWhenDispatchCalled(t *testing.T) {
	// Arrange
	mux := connection.NewMultiplexer()
	defer closeQuietly(mux)

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

func TestShouldDropResponseAfterRequesterClosesGivenPendingRequest(t *testing.T) {
	mux := connection.NewMultiplexer()
	defer closeQuietly(mux)

	ch := make(chan []byte, 1)
	mux.RegisterRequest(100, ch, nil)

	assert.NoError(t, mux.Close())
	mux.Dispatch(100, []byte("stale"))

	select {
	case _, ok := <-ch:
		assert.False(t, ok)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("closed request should complete without response data")
	}

	assert.Equal(t, uint64(1), mux.Metrics().ResponsesDropped)
}

// TestShouldCloseGracefullyGivenRegisteredRequestWhenCloseCalledTwice tests idempotent close behavior.
func TestShouldCloseGracefullyGivenRegisteredRequestWhenCloseCalledTwice(t *testing.T) {
	// Arrange
	mux := connection.NewMultiplexer()
	ch := make(chan []byte, 1)
	mux.RegisterRequest(100, ch, nil)

	// Act
	closeQuietly(mux)
	closeQuietly(mux)

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
	err := func() error {
		closeQuietly(mux)
		return nil
	}()

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
	defer closeQuietly(mux)

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

// TestShouldDispatchQueueNotifyGivenQueuePayloadWhenNotifyHandlerRegistered verifies queue watch payload parsing.
func TestShouldDispatchQueueNotifyGivenQueuePayloadWhenNotifyHandlerRegistered(t *testing.T) {
	mux := connection.NewMultiplexer()
	defer closeQuietly(mux)

	got := make(chan struct {
		subID uint64
		route string
		body  []byte
	}, 1)

	mux.SetNotifyHandler(protocol.MessageTypeQueueNotify, func(subID uint64, route string, payload []byte) {
		copied := append([]byte(nil), payload...)
		got <- struct {
			subID uint64
			route string
			body  []byte
		}{subID: subID, route: route, body: copied}
	})

	route := "queue://realm/area/resource/ready"
	payload := make([]byte, 8+4+len(route)+24)
	offset := 0
	binary.BigEndian.PutUint64(payload[offset:offset+8], 99)
	offset += 8
	binary.BigEndian.PutUint32(payload[offset:offset+4], uint32(len(route)))
	offset += 4
	copy(payload[offset:offset+len(route)], []byte(route))
	offset += len(route)
	binary.BigEndian.PutUint64(payload[offset:offset+8], 3)
	offset += 8
	binary.BigEndian.PutUint64(payload[offset:offset+8], 0)
	offset += 8
	binary.BigEndian.PutUint64(payload[offset:offset+8], 0)

	mux.Dispatch(protocol.MessageTypeQueueNotify, payload)

	select {
	case delivered := <-got:
		assert.Equal(t, uint64(99), delivered.subID)
		assert.Equal(t, route, delivered.route)
		assert.Equal(t, payload[8+4+len(route):], delivered.body)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected queue notify delivery")
	}
}

// Benchmarks

func BenchmarkRegisterRequest(b *testing.B) {
	mux := connection.NewMultiplexer()
	defer closeQuietly(mux)

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		ch := make(chan []byte, 1)
		mux.RegisterRequest(uint16(i%1000), ch, nil)
	}
}

func BenchmarkMuxDispatchResponse(b *testing.B) {
	mux := connection.NewMultiplexer()
	defer closeQuietly(mux)

	// Pre-register 1000 requests
	for i := range 1000 {
		ch := make(chan []byte, 1)
		mux.RegisterRequest(uint16(i), ch, nil)
	}

	payload := []byte("test response payload")

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		mux.Dispatch(uint16(i%1000), payload)
	}
}

func BenchmarkConcurrentDispatch(b *testing.B) {
	mux := connection.NewMultiplexer()
	defer closeQuietly(mux)

	// Pre-register 1000 requests
	for i := range 1000 {
		ch := make(chan []byte, 1)
		mux.RegisterRequest(uint16(i), ch, nil)
	}

	payload := []byte("test response")

	b.ReportAllocs()
	b.ResetTimer()
	var wg sync.WaitGroup
	for i := range b.N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			mux.Dispatch(uint16(idx%1000), payload)
		}(i)
	}
	wg.Wait()
}
