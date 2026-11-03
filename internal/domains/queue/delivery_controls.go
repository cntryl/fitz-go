package queue

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/protocol"
)

// WithTTLHours overrides the broker's message TTL with a positive lifetime in hours.
func WithTTLHours(hours uint32) EnqueueOption {
	return func(cfg *enqueueConfig) { cfg.ttlHours = &hours }
}

func (q *QueueItem) Nack(ctx context.Context, visibilitySeconds uint64) error {
	if err := q.conn.CheckLiveHandle(); err != nil {
		return err
	}
	if !q.conn.SupportsCapability(protocol.CapabilityQueueNack) {
		return errors.New("queue NACK capability was not negotiated")
	}
	if !q.state.CompareAndSwap(0, 1) {
		return connection.ErrStaleHandle
	}
	defer q.state.CompareAndSwap(1, 0)
	resp, err := q.conn.SendRequestWithWriter(ctx, protocol.MessageTypeQueueNack, extendPayloadWriter(q.Route, q.ID, q.Token, visibilitySeconds))
	if err != nil {
		return err
	}
	success, _, err := parseQueueAcknowledgementResponse(resp)
	if err != nil {
		return err
	}
	if !success {
		return errors.New("nack failed: unexpected status")
	}
	q.state.Store(2)
	return nil
}

func enqueueWriterWithTTL(route string, body []byte, cfg enqueueConfig) func(*bytes.Buffer) {
	coreWriter := enqueuePayloadWriter(route, body, cfg.delaySeconds)
	return func(buf *bytes.Buffer) {
		start := buf.Len()
		buf.Write([]byte{0, 0, 0, 0})
		coreWriter(buf)
		binary.BigEndian.PutUint32(buf.Bytes()[start:start+4], uint32(buf.Len()-start-4))
		buf.Write([]byte{0, 1, 0, 0, 0, 4})
		var ttl [4]byte
		binary.BigEndian.PutUint32(ttl[:], *cfg.ttlHours)
		buf.Write(ttl[:])
	}
}
