package fitz

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/v2/internal/core/connection"
	internallease "github.com/cntryl/fitz-go/v2/internal/domains/lease"
	"github.com/cntryl/fitz-go/v2/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type scriptedManagedLeaseTransport struct {
	mu      sync.Mutex
	written [][]byte
	readCh  chan []byte
	closed  chan struct{}
	once    sync.Once
}

const managedLeaseTestTimeout = 3 * time.Second

func newScriptedManagedLeaseTransport() *scriptedManagedLeaseTransport {
	return &scriptedManagedLeaseTransport{
		readCh: make(chan []byte, 8),
		closed: make(chan struct{}),
	}
}

func (s *scriptedManagedLeaseTransport) Write(ctx context.Context, frame []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closed:
		return connection.ErrConnectionClosed
	default:
	}
	s.mu.Lock()
	s.written = append(s.written, append([]byte(nil), frame...))
	s.mu.Unlock()
	return nil
}

func (s *scriptedManagedLeaseTransport) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.closed:
		return nil, connection.ErrConnectionClosed
	case frame := <-s.readCh:
		return append([]byte(nil), frame...), nil
	}
}

func (s *scriptedManagedLeaseTransport) Close() error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

func (s *scriptedManagedLeaseTransport) RemoteAddr() string {
	return "scripted://managed-lease"
}

func (s *scriptedManagedLeaseTransport) enqueue(ctx context.Context, frame []byte) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-s.closed:
		return connection.ErrConnectionClosed
	case s.readCh <- append([]byte(nil), frame...):
		return nil
	}
}

func (s *scriptedManagedLeaseTransport) writeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.written)
}

func (s *scriptedManagedLeaseTransport) writtenFrame(index int) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.written[index]...)
}

func newManagedLeaseTestClient(t *testing.T) (*leaseClient, *scriptedManagedLeaseTransport, int) {
	t.Helper()
	transport := newScriptedManagedLeaseTransport()
	conn := connection.New(transport, connection.Config{Token: "", ReadTimeout: time.Second})
	require.NoError(t, conn.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return &leaseClient{inner: internallease.NewClient(conn)}, transport, transport.writeCount()
}

func waitForManagedLeaseWrites(ctx context.Context, transport *scriptedManagedLeaseTransport, expected int) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if transport.writeCount() >= expected {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for %d managed lease writes: %w", expected, context.Cause(ctx))
		case <-ticker.C:
		}
	}
}

type managedLeaseResponseStep struct {
	expectedWrites int
	responses      [][]byte
	notify         chan struct{}
}

func startManagedLeaseResponder(
	ctx context.Context,
	transport *scriptedManagedLeaseTransport,
	steps ...managedLeaseResponseStep,
) <-chan error {
	done := make(chan error, 1)
	go func() {
		defer close(done)
		for _, step := range steps {
			if err := waitForManagedLeaseWrites(ctx, transport, step.expectedWrites); err != nil {
				done <- err
				return
			}
			for _, response := range step.responses {
				if err := transport.enqueue(ctx, response); err != nil {
					done <- err
					return
				}
			}
			if step.notify != nil {
				close(step.notify)
			}
		}
		done <- nil
	}()
	return done
}

func joinManagedLeaseResponder(done <-chan error) error {
	result := <-done
	for range done {
	}
	return result
}

func managedLeaseFrame(t *testing.T, messageType uint16, payload []byte) []byte {
	t.Helper()
	frame := protocol.EncodeFrameOwned(messageType, payload)
	require.NotNil(t, frame)
	defer frame.Release()
	return append([]byte(nil), frame.Bytes()...)
}

func managedLeaseAcquireResponse(t *testing.T, responseType byte, token uint64) []byte {
	t.Helper()
	payload := make([]byte, 10)
	payload[0] = 0
	payload[1] = responseType
	binary.BigEndian.PutUint64(payload[2:], token)
	return managedLeaseFrame(t, protocol.MessageTypeLeaseAcquire, payload)
}

func managedLeaseRenewResponse(t *testing.T, token uint64) []byte {
	t.Helper()
	payload := make([]byte, 9)
	payload[0] = 0
	binary.BigEndian.PutUint64(payload[1:], token)
	return managedLeaseFrame(t, protocol.MessageTypeLeaseRenew, payload)
}

func managedLeaseReleaseResponse(t *testing.T) []byte {
	t.Helper()
	return managedLeaseFrame(t, protocol.MessageTypeLeaseRelease, []byte{0})
}

func managedLeaseAcquireErrorResponse(t *testing.T, code uint32, message string) []byte {
	t.Helper()
	var payload bytes.Buffer
	payload.WriteByte(1)
	connection.WriteU32BE(&payload, code)
	connection.WriteString(&payload, message)
	return managedLeaseFrame(t, protocol.MessageTypeLeaseAcquire, payload.Bytes())
}

func managedLeaseCredentialFromWrittenFrame(t *testing.T, frame []byte, expectedType uint16) uint64 {
	t.Helper()
	messageType, payload, err := protocol.DecodeFrame(frame)
	require.NoError(t, err)
	require.Equal(t, expectedType, messageType)
	_, offset, err := connection.ReadString(payload, 0)
	require.NoError(t, err)
	_, offset, err = connection.ReadString(payload, offset)
	require.NoError(t, err)
	token, _, err := connection.ReadU64BE(payload, offset)
	require.NoError(t, err)
	return token
}

func TestShouldExposeAdmissionAuthorityGivenImmediateOrAlreadyHeldAcquire(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		responseType byte
	}{
		{name: "immediate", responseType: 0},
		{name: "already held", responseType: 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			client, transport, baseWrites := newManagedLeaseTestClient(t)
			const admissionToken = uint64(0x0102_0304_0506_0708)
			acquireResponse := managedLeaseAcquireResponse(t, testCase.responseType, admissionToken)
			releaseResponse := managedLeaseReleaseResponse(t)
			ctx, cancel := context.WithTimeout(context.Background(), managedLeaseTestTimeout)
			defer cancel()
			responder := startManagedLeaseResponder(ctx, transport,
				managedLeaseResponseStep{expectedWrites: baseWrites + 1, responses: [][]byte{acquireResponse}},
				managedLeaseResponseStep{expectedWrites: baseWrites + 2, responses: [][]byte{releaseResponse}},
			)

			var authority LeaseAuthority
			var found bool
			err := client.WithLease(ctx, "lease://realm/area/resource", 30, func(callbackCtx context.Context) error {
				authority, found = LeaseAuthorityFromContext(callbackCtx)
				return nil
			}, WithLeaseOwnerID("worker-1"))

			require.NoError(t, joinManagedLeaseResponder(responder))
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, admissionToken, authority.FencingToken)
		})
	}
}

func TestShouldExposeFinalAuthorityGivenQueuedAcquire(t *testing.T) {
	client, transport, baseWrites := newManagedLeaseTestClient(t)
	const provisionalToken = uint64(11)
	const grantedToken = uint64(42)
	queuedResponse := managedLeaseAcquireResponse(t, 2, provisionalToken)
	grantedResponse := managedLeaseAcquireResponse(t, 0, grantedToken)
	releaseResponse := managedLeaseReleaseResponse(t)
	ctx, cancel := context.WithTimeout(context.Background(), managedLeaseTestTimeout)
	defer cancel()
	responder := startManagedLeaseResponder(ctx, transport,
		managedLeaseResponseStep{expectedWrites: baseWrites + 1, responses: [][]byte{queuedResponse, grantedResponse}},
		managedLeaseResponseStep{expectedWrites: baseWrites + 2, responses: [][]byte{releaseResponse}},
	)

	var authority LeaseAuthority
	err := client.WithLease(ctx, "lease://realm/area/resource", 30, func(callbackCtx context.Context) error {
		authority, _ = LeaseAuthorityFromContext(callbackCtx)
		return nil
	}, WithLeaseOwnerID("worker-1"), WithLeaseWaitSeconds(5))

	require.NoError(t, joinManagedLeaseResponder(responder))
	require.NoError(t, err)
	assert.Equal(t, grantedToken, authority.FencingToken)
	assert.NotEqual(t, provisionalToken, authority.FencingToken)
}

func TestShouldKeepAdmissionAuthorityStableGivenSuccessfulRenewals(t *testing.T) {
	client, transport, baseWrites := newManagedLeaseTestClient(t)
	const admissionToken = uint64(41)
	const firstRenewalToken = uint64(42)
	const secondRenewalToken = uint64(43)
	allowCallbackReturn := make(chan struct{})
	acquireResponse := managedLeaseAcquireResponse(t, 0, admissionToken)
	firstRenewalResponse := managedLeaseRenewResponse(t, firstRenewalToken)
	secondRenewalResponse := managedLeaseRenewResponse(t, secondRenewalToken)
	releaseResponse := managedLeaseReleaseResponse(t)
	ctx, cancel := context.WithTimeout(context.Background(), managedLeaseTestTimeout)
	defer cancel()
	responder := startManagedLeaseResponder(ctx, transport,
		managedLeaseResponseStep{expectedWrites: baseWrites + 1, responses: [][]byte{acquireResponse}},
		managedLeaseResponseStep{expectedWrites: baseWrites + 2, responses: [][]byte{firstRenewalResponse}},
		managedLeaseResponseStep{expectedWrites: baseWrites + 3, responses: [][]byte{secondRenewalResponse}, notify: allowCallbackReturn},
		managedLeaseResponseStep{expectedWrites: baseWrites + 4, responses: [][]byte{releaseResponse}},
	)

	var authority LeaseAuthority
	err := client.WithLease(ctx, "lease://realm/area/resource", 1, func(callbackCtx context.Context) error {
		authority, _ = LeaseAuthorityFromContext(callbackCtx)
		select {
		case <-allowCallbackReturn:
			return nil
		case <-callbackCtx.Done():
			return context.Cause(callbackCtx)
		}
	}, WithLeaseOwnerID("worker-1"))

	require.NoError(t, joinManagedLeaseResponder(responder))
	require.NoError(t, err)
	assert.Equal(t, admissionToken, authority.FencingToken)
	assert.Equal(t, admissionToken, managedLeaseCredentialFromWrittenFrame(t, transport.writtenFrame(baseWrites+1), protocol.MessageTypeLeaseRenew))
	assert.Equal(t, firstRenewalToken, managedLeaseCredentialFromWrittenFrame(t, transport.writtenFrame(baseWrites+2), protocol.MessageTypeLeaseRenew))
	assert.Equal(t, secondRenewalToken, managedLeaseCredentialFromWrittenFrame(t, transport.writtenFrame(baseWrites+3), protocol.MessageTypeLeaseRelease))
}

func TestShouldNotInvokeCallbackGivenAcquireFailure(t *testing.T) {
	client, transport, baseWrites := newManagedLeaseTestClient(t)
	errorResponse := managedLeaseAcquireErrorResponse(t, 5001, "lease held by another owner")
	ctx, cancel := context.WithTimeout(context.Background(), managedLeaseTestTimeout)
	defer cancel()
	responder := startManagedLeaseResponder(ctx, transport,
		managedLeaseResponseStep{expectedWrites: baseWrites + 1, responses: [][]byte{errorResponse}},
	)
	var invoked atomic.Bool

	err := client.WithLease(ctx, "lease://realm/area/resource", 30, func(context.Context) error {
		invoked.Store(true)
		return nil
	}, WithLeaseOwnerID("worker-1"))

	require.NoError(t, joinManagedLeaseResponder(responder))
	require.Error(t, err)
	assert.False(t, invoked.Load())
}

func TestShouldNotInvokeCallbackGivenQueuedAcquireTimeout(t *testing.T) {
	client, transport, baseWrites := newManagedLeaseTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	queuedResponse := managedLeaseAcquireResponse(t, 2, 11)
	responder := startManagedLeaseResponder(ctx, transport,
		managedLeaseResponseStep{expectedWrites: baseWrites + 1, responses: [][]byte{queuedResponse}},
	)
	var invoked atomic.Bool

	err := client.WithLease(ctx, "lease://realm/area/resource", 30, func(context.Context) error {
		invoked.Store(true)
		return nil
	}, WithLeaseOwnerID("worker-1"), WithLeaseWaitSeconds(5))

	require.NoError(t, joinManagedLeaseResponder(responder))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.False(t, invoked.Load())
}

func TestShouldNotInvokeCallbackGivenCanceledAcquire(t *testing.T) {
	client, _, _ := newManagedLeaseTestClient(t)
	deadlineCtx, deadlineCancel := context.WithTimeout(context.Background(), managedLeaseTestTimeout)
	defer deadlineCancel()
	ctx, cancel := context.WithCancel(deadlineCtx)
	cancel()
	var invoked atomic.Bool

	err := client.WithLease(ctx, "lease://realm/area/resource", 30, func(context.Context) error {
		invoked.Store(true)
		return nil
	}, WithLeaseOwnerID("worker-1"))

	require.ErrorIs(t, err, context.Canceled)
	assert.False(t, invoked.Load())
}

func TestShouldPreserveOneArgumentCallbackGivenManagedLease(t *testing.T) {
	client, transport, baseWrites := newManagedLeaseTestClient(t)
	acquireResponse := managedLeaseAcquireResponse(t, 0, 42)
	releaseResponse := managedLeaseReleaseResponse(t)
	ctx, cancel := context.WithTimeout(context.Background(), managedLeaseTestTimeout)
	defer cancel()
	responder := startManagedLeaseResponder(ctx, transport,
		managedLeaseResponseStep{expectedWrites: baseWrites + 1, responses: [][]byte{acquireResponse}},
		managedLeaseResponseStep{expectedWrites: baseWrites + 2, responses: [][]byte{releaseResponse}},
	)
	var invoked atomic.Bool

	err := client.WithLease(ctx, "lease://realm/area/resource", 30, func(context.Context) error {
		invoked.Store(true)
		return nil
	}, WithLeaseOwnerID("worker-1"))

	require.NoError(t, joinManagedLeaseResponder(responder))
	require.NoError(t, err)
	assert.True(t, invoked.Load())
}

func TestShouldOmitLeaseAuthorityGivenOrdinaryContext(t *testing.T) {
	authority, found := LeaseAuthorityFromContext(context.Background())

	assert.False(t, found)
	assert.Equal(t, LeaseAuthority{}, authority)
}
