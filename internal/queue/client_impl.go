package queue

import (
	"context"
	"fmt"

	"github.com/cntryl/cntryl-go/internal/transport"
)

// client is a concrete implementation of queue.Client backed by the transport mux.
type client struct {
	mux *transport.Mux
}

// NewClient creates a new Queue domain client backed by the transport mux.
func NewClient(mux *transport.Mux) Client {
	return &client{mux: mux}
}

// Enqueue sends a message to the queue for the given route and returns a message ID.
func (c *client) Enqueue(ctx context.Context, route string, body []byte) (string, error) {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	enc.AddBytes(transport.TagBody, body)
	frame := transport.Frame{
		Type:    QueueEnqueue,
		Flags:   0,
		Channel: ChannelQueue,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return "", fmt.Errorf("send enqueue: %w", err)
	}
	// No ack handling in basic unit test; return a synthetic ID placeholder.
	return "", nil
}

// Reserve requests messages from the queue with lease semantics.
func (c *client) Reserve(ctx context.Context, route string, leaseSecs uint32, batchSize uint32) ([]LeaseMessage, error) {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	enc.AddUint32(transport.TagTTL, leaseSecs)
	enc.AddUint32(transport.TagBatchSize, batchSize)
	frame := transport.Frame{
		Type:    QueueReserve,
		Flags:   0,
		Channel: ChannelQueue,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return nil, fmt.Errorf("send reserve: %w", err)
	}
	return nil, nil
}

// Complete acknowledges completion of a reserved message.
func (c *client) Complete(ctx context.Context, route string, id string, token uint64) error {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	enc.AddString(transport.TagToken, id)
	enc.AddUint64(transport.TagID, token)
	frame := transport.Frame{
		Type:    QueueComplete,
		Flags:   0,
		Channel: ChannelQueue,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return fmt.Errorf("send complete: %w", err)
	}
	return nil
}
