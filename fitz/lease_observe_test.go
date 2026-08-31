package fitz

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/v2/internal/core/connection"
	internallease "github.com/cntryl/fitz-go/v2/internal/domains/lease"
	"github.com/cntryl/fitz-go/v2/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newObserveTestClient(t *testing.T) (*leaseClient, *scriptedManagedLeaseTransport, int) {
	t.Helper()
	transport := newScriptedManagedLeaseTransport()
	conn := connection.New(transport, connection.Config{Token: "", ReadTimeout: time.Second})
	require.NoError(t, conn.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return &leaseClient{inner: internallease.NewClient(conn)}, transport, transport.writeCount()
}

func observeSubscribeResponse(t *testing.T, subID uint64) []byte {
	t.Helper()
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	buf.WriteByte(0)
	connection.WriteU64BE(buf, subID)
	return managedLeaseFrame(t, protocol.MessageTypeLeaseSubscribe, append([]byte(nil), buf.Bytes()...))
}

func observeListResponse(t *testing.T, routes []string) []byte {
	t.Helper()
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	buf.WriteByte(0)
	connection.WriteU32BE(buf, uint32(len(routes)))
	for _, route := range routes {
		connection.WriteBytes(buf, []byte(route))
		connection.WriteBytes(buf, []byte("worker-1"))
		connection.WriteU64BE(buf, 1)
		connection.WriteBytes(buf, []byte("2026-08-29T00:00:00Z"))
		connection.WriteU64BE(buf, 30)
		connection.WriteU32BE(buf, 0)
	}
	buf.WriteByte(0) // has_next = false
	return managedLeaseFrame(t, protocol.MessageTypeLeaseList, append([]byte(nil), buf.Bytes()...))
}

func observeUnsubscribeResponse(t *testing.T) []byte {
	t.Helper()
	return managedLeaseFrame(t, protocol.MessageTypeLeaseUnsubscribe, []byte{0})
}

// TestShouldExposeLeaseObserveThroughPublicClientGivenExternalConsumerUsage
// proves that an external consumer of the canonical fitz package (using only
// the public LeaseClient interface, ObserveOption/With* options, LeaseEntry,
// and InventoryObserver types) can compile and drive
// client.Lease().Observe(...) end to end: subscribe-before-list bootstrap,
// the resulting view, readiness, and Close all forward correctly through the
// public adapter to the internal domain client.
func TestShouldExposeLeaseObserveThroughPublicClientGivenExternalConsumerUsage(t *testing.T) {
	client, transport, base := newObserveTestClient(t)

	var observer *InventoryObserver
	obsErr := make(chan error, 1)
	go func() {
		var err error
		observer, err = client.Observe(context.Background(), "lease://acme/renderers/*",
			WithObserveReconcileInterval(time.Hour),
			WithObserveReconcileJitter(0))
		obsErr <- err
	}()

	require.NoError(t, waitForManagedLeaseWrites(context.Background(), transport, base+1))
	require.NoError(t, transport.enqueue(context.Background(), observeSubscribeResponse(t, 55)))
	require.NoError(t, waitForManagedLeaseWrites(context.Background(), transport, base+2))
	require.NoError(t, transport.enqueue(context.Background(), observeListResponse(t, []string{"lease://acme/renderers/doc-1"})))

	select {
	case err := <-obsErr:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Observe did not complete bootstrap in time")
	}
	require.NotNil(t, observer)

	assert.True(t, observer.Ready())
	view := observer.View()
	require.Len(t, view, 1)
	assert.Equal(t, "lease://acme/renderers/doc-1", view[0].Route)
	assert.Equal(t, "worker-1", view[0].OwnerID)

	closeDone := make(chan error, 1)
	go func() { closeDone <- observer.Close() }()
	require.NoError(t, waitForManagedLeaseWrites(context.Background(), transport, base+3))
	require.NoError(t, transport.enqueue(context.Background(), observeUnsubscribeResponse(t)))

	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("observer Close did not return in time")
	}
}
