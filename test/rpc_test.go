package integration

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/cntryl-go/test/fixture"
	"github.com/stretchr/testify/require"
)

// TestShouldRouteRequestToWorkerGivenRegisteredWorkerWhenRequestCalled
// verifies the basic RPC lifecycle: SUBSCRIBE_WORKER â†’ REQUEST â†’ RESPONSE.
func TestShouldRouteRequestToWorkerGivenRegisteredWorkerWhenRequestCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		// Act & Assert
		t.Fatal("RPC SUBSCRIBE_WORKER/REQUEST/RESPONSE not yet implemented")
	})
}

// TestShouldReassembleStreamingResponseGivenMultiFrameResponseWhenSequenced
// verifies streaming RPC responses are reassembled in sequence order.
func TestShouldReassembleStreamingResponseGivenMultiFrameResponseWhenSequenced(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		// Act & Assert
		t.Fatal("RPC streaming responses not yet implemented")
	})
}

// TestShouldReturnTimeoutGivenNoWorkerResponseWhenRequestTimeout verifies
// REQUEST operation times out when worker doesn't respond.
func TestShouldReturnTimeoutGivenNoWorkerResponseWhenRequestTimeout(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		// Act & Assert
		t.Fatal("RPC request timeout not yet implemented")
	})
}

// TestShouldLoadBalanceGivenMultipleWorkersWhenConcurrentRequests verifies
// multiple workers on same route handle requests.
func TestShouldLoadBalanceGivenMultipleWorkersWhenConcurrentRequests(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		// Act & Assert
		t.Fatal("RPC load balancing not yet implemented")
	})
}

// TestShouldCorrelateResponseGivenCorrectCorrelationIDWhenMultipleRequests
// verifies correlation_id links requests to responses.
func TestShouldCorrelateResponseGivenCorrectCorrelationIDWhenMultipleRequests(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		// Act & Assert
		t.Fatal("RPC correlation not yet implemented")
	})
}

// TestShouldUnregisterWorkerGivenActiveRegistrationWhenUnsubscribeWorkerCalled
// verifies UNSUBSCRIBE_WORKER stops receiving requests.
func TestShouldUnregisterWorkerGivenActiveRegistrationWhenUnsubscribeWorkerCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		// Act & Assert
		t.Fatal("RPC UNSUBSCRIBE_WORKER not yet implemented")
	})
}

// TestShouldReturnBackpressureErrorGivenFullQueueWhenRequestSent verifies
// ERR_RPC_BACKPRESSURE error when outbound queue is full.
func TestShouldReturnBackpressureErrorGivenFullQueueWhenRequestSent(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, f.Connect(ctx))

		// Act & Assert
		t.Fatal("RPC backpressure not yet implemented")
	})
}
