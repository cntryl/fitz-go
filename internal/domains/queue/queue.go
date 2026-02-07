package queue

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/cntryl/cntryl-go/internal/core/transport"
	"github.com/cntryl/cntryl-go/internal/core/types"
)

// Client is the API for the Queue domain.
type Client interface {
	Enqueue(ctx context.Context, route string, body []byte) (string, error)
	Reserve(ctx context.Context, route string, leaseSecs uint32, batchSize uint32) ([]QueueItem, error)
	Extend(ctx context.Context, route string, id string, token uint64, leaseSecs uint32) error
	Complete(ctx context.Context, route string, id string, token uint64) error
}

// QueueItem is a reserved message returned from Reserve.
type QueueItem struct {
	ID    string
	Body  []byte
	Token uint64
}

type client struct {
	mux transport.MuxProvider
}

// NewClient creates a new Queue domain client backed by the transport mux.
func NewClient(mux transport.MuxProvider) Client {
	return &client{mux: mux}
}

// Enqueue sends a message to the queue for the given route and returns a message ID.
func (c *client) Enqueue(ctx context.Context, route string, body []byte) (string, error) {
	if err := types.ValidateRoute(route, "queue"); err != nil {
		return "", err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(QueueEnqueue)
	enc.AddString(transport.TagRoute, route)
	enc.AddBytes(transport.TagBody, body)
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelQueue, Body: enc.Encode()}

	resp, err := transport.SendRecv(ctx, c.mux, frame, mapQueueError)
	if err != nil {
		return "", err
	}

	dec, derr := transport.NewTLVDecoder(resp.Body)
	if derr != nil {
		return "", fmt.Errorf("invalid TLV in response: %w", derr)
	}
	id, _ := dec.GetUint64(transport.TagID)
	if id == 0 {
		return "", nil
	}
	return fmt.Sprintf("%d", id), nil
}

// Reserve requests messages from the queue with lease semantics.
func (c *client) Reserve(ctx context.Context, route string, leaseSecs uint32, batchSize uint32) ([]QueueItem, error) {
	if err := types.ValidateRoute(route, "queue"); err != nil {
		return nil, err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(QueueReserve)
	enc.AddString(transport.TagRoute, route)
	enc.AddUint32(transport.TagTTL, leaseSecs)
	if batchSize > 0 {
		enc.AddUint32(transport.TagBatchSize, batchSize)
	}
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelQueue, Body: enc.Encode()}

	resp, err := transport.SendRecv(ctx, c.mux, frame, mapQueueError)
	if err != nil {
		return nil, err
	}

	dec, derr := transport.NewTLVDecoder(resp.Body)
	if derr != nil {
		return nil, fmt.Errorf("invalid TLV in response: %w", derr)
	}
	ids := dec.GetAll(transport.TagID)
	leases := dec.GetAll(transport.TagLease)
	bodies := dec.GetAll(transport.TagBody)
	count := len(ids)
	items := make([]QueueItem, 0, count)
	for i := 0; i < count; i++ {
		var id uint64
		if len(ids[i]) == 8 {
			id = binary.BigEndian.Uint64(ids[i])
		}
		var lease uint64
		if i < len(leases) && len(leases[i]) == 8 {
			lease = binary.BigEndian.Uint64(leases[i])
		}
		var body []byte
		if i < len(bodies) {
			body = bodies[i]
		}
		items = append(items, QueueItem{ID: fmt.Sprintf("%d", id), Body: body, Token: lease})
	}
	return items, nil
}

// Extend extends the lease for a reserved message.
func (c *client) Extend(ctx context.Context, route string, id string, token uint64, leaseSecs uint32) error {
	if err := types.ValidateRoute(route, "queue"); err != nil {
		return err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(QueueExtend)
	enc.AddString(transport.TagRoute, route)
	if id == "" {
		return fmt.Errorf("queue extend: id cannot be empty")
	}
	v, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return fmt.Errorf("queue extend: invalid id %q: %w", id, err)
	}
	enc.AddUint64(transport.TagID, v)
	enc.AddUint64(transport.TagLease, token)
	enc.AddUint32(transport.TagTTL, leaseSecs)
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelQueue, Body: enc.Encode()}

	_, err = transport.SendRecv(ctx, c.mux, frame, mapQueueError)
	return err
}

// Complete acknowledges completion of a reserved message.
func (c *client) Complete(ctx context.Context, route string, id string, token uint64) error {
	if err := types.ValidateRoute(route, "queue"); err != nil {
		return err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(QueueComplete)
	enc.AddString(transport.TagRoute, route)
	if id == "" {
		return fmt.Errorf("queue complete: id cannot be empty")
	}
	v, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return fmt.Errorf("queue complete: invalid id %q: %w", id, err)
	}
	enc.AddUint64(transport.TagID, v)
	enc.AddUint64(transport.TagLease, token)
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelQueue, Body: enc.Encode()}

	_, err = transport.SendRecv(ctx, c.mux, frame, mapQueueError)
	return err
}
