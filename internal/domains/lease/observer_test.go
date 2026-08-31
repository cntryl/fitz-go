package lease

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/v2/internal/core/connection"
	"github.com/cntryl/fitz-go/v2/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const observeTestTimeout = 3 * time.Second

func newObserverTestClient(t *testing.T) (*client, *scriptedLeaseRestoreTransport) {
	t.Helper()
	trans := newScriptedLeaseRestoreTransport()
	conn := connection.New(trans, connection.Config{Token: "", ReadTimeout: time.Second})
	require.NoError(t, conn.Start(context.Background()))
	t.Cleanup(func() { _ = conn.Close() })
	raw := NewClient(conn)
	c, ok := raw.(*client)
	require.True(t, ok)
	autoAnswerUnsubscribe(t, trans)
	return c, trans
}

// autoAnswerUnsubscribe answers any UNSUBSCRIBE write that a test itself did
// not explicitly script a response for (e.g. one issued by an observer's
// Close during t.Cleanup, after the test's own scripted exchanges are done),
// so InventoryObserver.Close's best-effort UNSUBSCRIBE round trip never hangs
// the test waiting for a response nobody sends.
func autoAnswerUnsubscribe(t *testing.T, trans *scriptedLeaseRestoreTransport) {
	t.Helper()
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go func() {
		answered := 0
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
			n := scriptedLeaseWriteCount(trans)
			for ; answered < n; answered++ {
				frame := scriptedLeaseWrittenFrame(trans, answered)
				msgType, _, err := protocol.DecodeFrame(frame)
				if err == nil && msgType == protocol.MessageTypeLeaseUnsubscribe {
					select {
					case <-trans.closed:
					default:
						trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseUnsubscribe, []byte{0}))
					}
				}
			}
		}
	}()
}

// --- (a) bootstrap ordering: subscribe before list, buffered notifications
// applied via a relist after the first list installs the view. ---

func TestShouldSubscribeBeforeListAndDrainBufferedNotificationsWhenObserveBootstraps(t *testing.T) {
	c, trans := newObserverTestClient(t)
	base := scriptedLeaseWriteCount(trans)
	pattern := "lease://acme/renderers/*"

	var observed *InventoryObserver
	obsErr := make(chan error, 1)
	go func() {
		o, err := c.Observe(context.Background(), pattern)
		observed = o
		obsErr <- err
	}()

	// Step 1: first wire write must be SUBSCRIBE, not LIST.
	waitForLeaseWrites(t, trans, base+1)
	assertWrittenFrameType(t, trans, base, protocol.MessageTypeLeaseSubscribe)
	trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseSubscribe, leaseSubscribeResponsePayload(42)))

	// Step 2: next write is the initial LIST call.
	waitForLeaseWrites(t, trans, base+2)
	assertWrittenFrameType(t, trans, base+1, protocol.MessageTypeLeaseList)

	// Race a notification in during the bootstrap window (before the LIST
	// response is delivered): it must be buffered, not applied via QUERY.
	c.handleNotify(42, "lease://acme/renderers/doc-2", nil)

	trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseList, leaseListResponsePayload(t, []LeaseEntry{
		{Route: "lease://acme/renderers/doc-1", OwnerID: "worker-1", ExpiresInSecs: 30},
	}, nil)))

	// Because a notification was buffered during bootstrap, the observer must
	// issue exactly one more full LIST pass (never a QUERY) to drain it.
	waitForLeaseWrites(t, trans, base+3)
	assertWrittenFrameType(t, trans, base+2, protocol.MessageTypeLeaseList)
	trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseList, leaseListResponsePayload(t, []LeaseEntry{
		{Route: "lease://acme/renderers/doc-1", OwnerID: "worker-1", ExpiresInSecs: 30},
		{Route: "lease://acme/renderers/doc-2", OwnerID: "worker-2", ExpiresInSecs: 15},
	}, nil)))

	select {
	case err := <-obsErr:
		require.NoError(t, err)
	case <-time.After(observeTestTimeout):
		t.Fatal("Observe did not complete bootstrap in time")
	}
	require.NotNil(t, observed)
	t.Cleanup(func() { _ = observed.Close() })

	require.True(t, observed.Ready())
	view := observed.View()
	assert.Len(t, view, 2)

	// No QUERY (403) write should ever have happened: bootstrap notifications
	// are drained via relist, not per-route Query.
	assert.Equal(t, base+3, scriptedLeaseWriteCount(trans), "expected exactly SUBSCRIBE, LIST, LIST and nothing else")
}

// --- (b) steady-state: complete relist on notification after bootstrap. ---

func TestShouldRelistCompleteItemsGivenSteadyStateNotificationWhenObserving(t *testing.T) {
	c, trans := newObserverTestClient(t)
	base := scriptedLeaseWriteCount(trans)
	pattern := "lease://acme/renderers/*"

	var observed *InventoryObserver
	obsErr := make(chan error, 1)
	go func() {
		o, err := c.Observe(context.Background(), pattern)
		observed = o
		obsErr <- err
	}()

	waitForLeaseWrites(t, trans, base+1)
	trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseSubscribe, leaseSubscribeResponsePayload(7)))
	waitForLeaseWrites(t, trans, base+2)
	trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseList, leaseListResponsePayload(t, nil, nil)))

	select {
	case err := <-obsErr:
		require.NoError(t, err)
	case <-time.After(observeTestTimeout):
		t.Fatal("Observe did not complete bootstrap in time")
	}
	require.NotNil(t, observed)
	t.Cleanup(func() { _ = observed.Close() })
	require.True(t, observed.Ready())
	require.Empty(t, observed.View())

	// A steady-state notification triggers LIST so fields unavailable from
	// QUERY remain complete in the view.
	c.handleNotify(7, "lease://acme/renderers/doc-9", nil)
	waitForLeaseWrites(t, trans, base+3)
	assertWrittenFrameType(t, trans, base+2, protocol.MessageTypeLeaseList)
	trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseList, leaseListResponsePayload(t, []LeaseEntry{{
		Route: "lease://acme/renderers/doc-9", OwnerID: "worker-9", HolderIncarnation: 77,
		AcquiredAt: "2026-08-31T00:00:00Z", ExpiresInSecs: 45, Renewals: 4,
	}}, nil)))

	require.Eventually(t, func() bool {
		view := observed.View()
		for _, e := range view {
			if e.Route == "lease://acme/renderers/doc-9" && e.OwnerID == "worker-9" &&
				e.HolderIncarnation == 77 && e.Renewals == 4 {
				return true
			}
		}
		return false
	}, observeTestTimeout, 5*time.Millisecond)

	// A subsequent notification for the same (now free) route removes it.
	c.handleNotify(7, "lease://acme/renderers/doc-9", nil)
	waitForLeaseWrites(t, trans, base+4)
	assertWrittenFrameType(t, trans, base+3, protocol.MessageTypeLeaseList)
	trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseList, leaseListResponsePayload(t, nil, nil)))

	require.Eventually(t, func() bool {
		return len(observed.View()) == 0
	}, observeTestTimeout, 5*time.Millisecond)
}

// --- (c) periodic reconciliation fires on its configured interval. ---

func TestShouldPeriodicallyReconcileGivenShortIntervalWhenObserving(t *testing.T) {
	c, trans := newObserverTestClient(t)
	base := scriptedLeaseWriteCount(trans)
	pattern := "lease://acme/renderers/*"

	var observed *InventoryObserver
	obsErr := make(chan error, 1)
	go func() {
		o, err := c.Observe(context.Background(), pattern,
			WithObserveReconcileInterval(15*time.Millisecond),
			WithObserveReconcileJitter(0))
		observed = o
		obsErr <- err
	}()

	waitForLeaseWrites(t, trans, base+1)
	trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseSubscribe, leaseSubscribeResponsePayload(3)))
	waitForLeaseWrites(t, trans, base+2)
	trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseList, leaseListResponsePayload(t, nil, nil)))

	select {
	case err := <-obsErr:
		require.NoError(t, err)
	case <-time.After(observeTestTimeout):
		t.Fatal("Observe did not complete bootstrap in time")
	}
	require.NotNil(t, observed)
	t.Cleanup(func() { _ = observed.Close() })

	// The periodic reconciler must fire a full LIST pass without any
	// notification and without sleeping for the real ~60s default.
	waitForLeaseWrites(t, trans, base+3)
	assertWrittenFrameType(t, trans, base+2, protocol.MessageTypeLeaseList)
	trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseList, leaseListResponsePayload(t, []LeaseEntry{
		{Route: "lease://acme/renderers/doc-5", OwnerID: "worker-5", ExpiresInSecs: 5},
	}, nil)))

	require.Eventually(t, func() bool {
		return len(observed.View()) == 1
	}, observeTestTimeout, 5*time.Millisecond)
}

// --- (d) reconnect triggers a fresh bootstrap via RestoreSubscriptions. ---

func TestShouldReBootstrapGivenReconnectSignalWhenRestoreSubscriptionsCalled(t *testing.T) {
	c, trans := newObserverTestClient(t)
	base := scriptedLeaseWriteCount(trans)
	pattern := "lease://acme/renderers/*"

	var observed *InventoryObserver
	obsErr := make(chan error, 1)
	go func() {
		o, err := c.Observe(context.Background(), pattern)
		observed = o
		obsErr <- err
	}()

	waitForLeaseWrites(t, trans, base+1)
	trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseSubscribe, leaseSubscribeResponsePayload(11)))
	waitForLeaseWrites(t, trans, base+2)
	trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseList, leaseListResponsePayload(t, []LeaseEntry{
		{Route: "lease://acme/renderers/doc-1", OwnerID: "worker-1", ExpiresInSecs: 30},
	}, nil)))

	select {
	case err := <-obsErr:
		require.NoError(t, err)
	case <-time.After(observeTestTimeout):
		t.Fatal("Observe did not complete bootstrap in time")
	}
	require.NotNil(t, observed)
	t.Cleanup(func() { _ = observed.Close() })
	require.True(t, observed.Ready())
	require.Len(t, observed.View(), 1)

	// Simulate reconnect: RestoreSubscriptions re-issues SUBSCRIBE for the
	// observer's own subscription (subID may change), which must then
	// trigger the observer's own re-bootstrap (a fresh LIST), not a bare
	// resubscribe.
	restoreErr := make(chan error, 1)
	go func() {
		restoreErr <- c.RestoreSubscriptions(context.Background())
	}()

	waitForLeaseWrites(t, trans, base+3)
	assertWrittenFrameType(t, trans, base+2, protocol.MessageTypeLeaseSubscribe)
	trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseSubscribe, leaseSubscribeResponsePayload(99)))

	select {
	case err := <-restoreErr:
		require.NoError(t, err)
	case <-time.After(observeTestTimeout):
		t.Fatal("RestoreSubscriptions did not complete in time")
	}

	// The observer must have been marked not-ready as soon as reconnect fired.
	require.Eventually(t, func() bool { return !observed.Ready() }, observeTestTimeout, time.Millisecond)

	waitForLeaseWrites(t, trans, base+4)
	assertWrittenFrameType(t, trans, base+3, protocol.MessageTypeLeaseList)
	trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseList, leaseListResponsePayload(t, []LeaseEntry{
		{Route: "lease://acme/renderers/doc-1", OwnerID: "worker-1", ExpiresInSecs: 30},
		{Route: "lease://acme/renderers/doc-2", OwnerID: "worker-2", ExpiresInSecs: 20},
	}, nil)))

	require.Eventually(t, func() bool {
		return observed.Ready() && len(observed.View()) == 2
	}, observeTestTimeout, 5*time.Millisecond)

	// A notification against the new subID (post-restore) must still be
	// recognized as steady state, proving the observer's subscription
	// tracking survived the subID change.
	c.handleNotify(99, "lease://acme/renderers/doc-1", nil)
	waitForLeaseWrites(t, trans, base+5)
	assertWrittenFrameType(t, trans, base+4, protocol.MessageTypeLeaseList)
	trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseList, leaseListResponsePayload(t, []LeaseEntry{
		{Route: "lease://acme/renderers/doc-2", OwnerID: "worker-2", ExpiresInSecs: 20},
	}, nil)))
	require.Eventually(t, func() bool {
		return len(observed.View()) == 1
	}, observeTestTimeout, 5*time.Millisecond)
}

// --- (e) Close stops the background goroutine and unsubscribes, no leak. ---

func TestShouldStopBackgroundGoroutineAndUnsubscribeWhenCloseCalled(t *testing.T) {
	c, trans := newObserverTestClient(t)
	base := scriptedLeaseWriteCount(trans)
	pattern := "lease://acme/renderers/*"

	var observed *InventoryObserver
	obsErr := make(chan error, 1)
	go func() {
		o, err := c.Observe(context.Background(), pattern,
			WithObserveReconcileInterval(10*time.Millisecond),
			WithObserveReconcileJitter(0))
		observed = o
		obsErr <- err
	}()

	waitForLeaseWrites(t, trans, base+1)
	trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseSubscribe, leaseSubscribeResponsePayload(21)))
	waitForLeaseWrites(t, trans, base+2)
	trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseList, leaseListResponsePayload(t, nil, nil)))

	select {
	case err := <-obsErr:
		require.NoError(t, err)
	case <-time.After(observeTestTimeout):
		t.Fatal("Observe did not complete bootstrap in time")
	}
	require.NotNil(t, observed)

	// Let at least one periodic reconciliation round-trip happen so the
	// background goroutine is definitely alive and looping.
	waitForLeaseWrites(t, trans, base+3)
	trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseList, leaseListResponsePayload(t, nil, nil)))

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- observed.Close()
	}()

	// Close triggers an UNSUBSCRIBE; answer it so Close's synchronous
	// unsubscribe path (best-effort response read) doesn't hang.
	waitForLeaseWrites(t, trans, base+4)
	assertWrittenFrameType(t, trans, base+3, protocol.MessageTypeLeaseUnsubscribe)
	trans.enqueue(scriptedLeaseFrame(t, protocol.MessageTypeLeaseUnsubscribe, []byte{0}))

	select {
	case err := <-closeDone:
		require.NoError(t, err, "Close must return (proving its background goroutine exited) without hanging")
	case <-time.After(observeTestTimeout):
		t.Fatal("Close did not return in time: background goroutine leaked")
	}

	// After Close, the periodic reconciler must no longer be running: no
	// further LIST writes should occur even after waiting past the interval.
	writesAtClose := scriptedLeaseWriteCount(trans)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, writesAtClose, scriptedLeaseWriteCount(trans), "no further wire activity after Close: background goroutine must have exited")
}

func scriptedLeaseWrittenFrame(trans *scriptedLeaseRestoreTransport, index int) []byte {
	trans.mu.Lock()
	defer trans.mu.Unlock()
	return append([]byte(nil), trans.written[index]...)
}

func assertWrittenFrameType(t *testing.T, trans *scriptedLeaseRestoreTransport, index int, expected uint16) {
	t.Helper()
	frame := scriptedLeaseWrittenFrame(trans, index)
	msgType, _, err := protocol.DecodeFrame(frame)
	require.NoError(t, err)
	assert.Equal(t, expected, msgType, "unexpected frame type at write index %d", index)
}
