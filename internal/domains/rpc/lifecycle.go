package rpc

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/protocol"
)

func decodeRPCRequestBudget(extension []byte) (*uint32, error) {
	if len(extension) == 0 {
		return nil, nil
	}
	if len(extension) != 6 || extension[0] != 1 || extension[1] != 1 {
		return nil, errors.New("invalid RPC request budget extension")
	}
	remaining := binary.BigEndian.Uint32(extension[2:])
	if remaining > 86_400_000 {
		return nil, errors.New("RPC request budget exceeds one day")
	}
	return &remaining, nil
}

func (c *client) workerCancellationRequested(correlationID [16]byte, invocation *activeRPCInvocation) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.activeInvocations[correlationID] == invocation && invocation.cancellationRequested
}

func (c *client) finishWorkerInvocation(correlationID [16]byte, invocation *activeRPCInvocation) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.activeInvocations[correlationID] != invocation {
		return invocation.cancellationRequested
	}
	delete(c.activeInvocations, correlationID)
	return invocation.cancellationRequested
}

func (c *client) sendWorkerCleanupAck(correlationID [16]byte) {
	if !c.supportsRPCCancellation() {
		return
	}
	_ = c.conn.SendFireAndForgetWithWriter(
		c.conn.LifecycleContext(),
		protocol.MessageTypeRpcCancellation,
		rpcWorkerCleanupAckPayloadWriter(correlationID),
	)
}

func (c *client) supportsRPCCancellation() bool {
	_, capabilities := c.conn.ServerCapabilities()
	return capabilities&protocol.CapabilityRPCCancellation != 0
}

func (c *client) handleRPCLifecycle(payload []byte) {
	if len(payload) != 18 {
		return
	}
	kind := payload[0]
	var correlationID [16]byte
	copy(correlationID[:], payload[1:17])
	value := payload[17]
	if kind == 2 && value >= 1 && value <= 4 {
		c.mu.Lock()
		invocation := c.activeInvocations[correlationID]
		var cancel context.CancelFunc
		buffered := false
		if invocation != nil {
			invocation.cancellationRequested = true
			cancel = invocation.cancel
			buffered = cancel == nil
			if buffered {
				delete(c.activeInvocations, correlationID)
			}
		}
		c.mu.Unlock()
		if cancel != nil {
			cancel()
		} else if buffered {
			// No application handler has started, so cleanup is already complete.
			// The buffered callback observes cancellation and never invokes it.
			c.sendWorkerCleanupAck(correlationID)
		}
		return
	}
	if kind != 4 {
		return
	}
	outcomes := map[byte]CancellationOutcome{
		1: CancellationQueuedRemoved,
		2: CancellationForwarded,
		3: CancellationWorkerUnsupported,
		4: CancellationAlreadyTerminal,
		5: CancellationUnknown,
		6: CancellationForwardingFailed,
	}
	if outcome, ok := outcomes[value]; ok {
		c.finishCancellation(correlationID, outcome)
	}
}

// ClosePendingRPCs fails all in-flight RPC call iterators with connection.ErrConnectionClosed.
func (c *client) ClosePendingRPCs() {
	c.mu.Lock()
	if len(c.pendingRPCs) == 0 && len(c.calls) == 0 && len(c.activeInvocations) == 0 {
		c.mu.Unlock()
		return
	}
	pending := c.pendingRPCs
	calls := c.calls
	timers := c.cancelTimers
	activeInvocations := c.activeInvocations
	c.pendingRPCs = make(map[[16]byte]*responseStream, len(pending))
	c.calls = make(map[[16]byte]*rpcIterator, len(calls))
	c.cancelRequested = make(map[[16]byte]bool)
	c.cancelTimers = make(map[[16]byte]*time.Timer)
	c.activeInvocations = make(map[[16]byte]*activeRPCInvocation)
	activeCancels := make([]context.CancelFunc, 0, len(activeInvocations))
	for _, invocation := range activeInvocations {
		invocation.cancellationRequested = true
		if invocation.cancel != nil {
			activeCancels = append(activeCancels, invocation.cancel)
		}
	}
	c.mu.Unlock()
	for _, cancel := range activeCancels {
		cancel()
	}
	for _, timer := range timers {
		timer.Stop()
	}
	for _, call := range calls {
		call.complete(CancellationConnectionClosed)
	}

	for _, stream := range pending {
		stream.fail(connection.ErrConnectionClosed)
	}
}

func (c *client) finishCall(correlationID [16]byte, outcome CancellationOutcome) {
	c.mu.Lock()
	delete(c.pendingRPCs, correlationID)
	call := c.calls[correlationID]
	if c.cancelRequested[correlationID] {
		c.mu.Unlock()
		return
	}
	delete(c.calls, correlationID)
	delete(c.cancelRequested, correlationID)
	timer := c.cancelTimers[correlationID]
	delete(c.cancelTimers, correlationID)
	c.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
	if call != nil {
		call.complete(outcome)
	}
}

func (c *client) cancelPendingRPC(correlationID [16]byte, reason byte) {
	c.mu.Lock()
	call := c.calls[correlationID]
	if call == nil {
		stream := c.pendingRPCs[correlationID]
		delete(c.pendingRPCs, correlationID)
		c.mu.Unlock()
		if stream != nil {
			stream.close()
		}
		return
	}
	if c.cancelRequested[correlationID] {
		c.mu.Unlock()
		return
	}
	if c.cancelRequested == nil {
		c.cancelRequested = make(map[[16]byte]bool)
	}
	c.cancelRequested[correlationID] = true
	stream := c.pendingRPCs[correlationID]
	c.mu.Unlock()

	_, capabilities := c.conn.ServerCapabilities()
	if capabilities&protocol.CapabilityRPCCancellation == 0 {
		c.finishCancellation(correlationID, CancellationUnsupported)
		return
	}
	timer := time.AfterFunc(5*time.Second, func() {
		c.finishCancellation(correlationID, CancellationUnconfirmed)
	})
	c.mu.Lock()
	if c.calls[correlationID] != call {
		c.mu.Unlock()
		timer.Stop()
		return
	}
	if c.cancelTimers == nil {
		c.cancelTimers = make(map[[16]byte]*time.Timer)
	}
	c.cancelTimers[correlationID] = timer
	c.mu.Unlock()

	err := c.conn.SendFireAndForgetWithWriter(
		c.conn.LifecycleContext(),
		protocol.MessageTypeRpcCancellation,
		rpcCallerCancellationPayloadWriter(correlationID, reason),
	)
	if err != nil {
		outcome := CancellationUnconfirmed
		if errors.Is(err, connection.ErrConnectionClosed) {
			outcome = CancellationConnectionClosed
		}
		c.finishCancellation(correlationID, outcome)
		return
	}
	if stream != nil {
		stream.close()
	}
}

func (c *client) finishCancellation(correlationID [16]byte, outcome CancellationOutcome) {
	c.mu.Lock()
	call := c.calls[correlationID]
	stream := c.pendingRPCs[correlationID]
	delete(c.calls, correlationID)
	delete(c.pendingRPCs, correlationID)
	delete(c.cancelRequested, correlationID)
	timer := c.cancelTimers[correlationID]
	delete(c.cancelTimers, correlationID)
	c.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
	if stream != nil {
		stream.close()
	}
	if call != nil {
		call.complete(outcome)
	}
}

func (c *client) ReplaceConnection(conn *connection.Connection) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn = conn
	if c.initialized {
		c.conn.RegisterRPCResponseHandler(c.handleRPCResponse)
		c.conn.RegisterRPCRequestHandler(c.handleRPCRequest)
		c.conn.RegisterRawPushHandler(protocol.MessageTypeRpcLifecycle, c.handleRPCLifecycle)
	}
}
