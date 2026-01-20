package queue

import "context"

// Client is the API for the Queue domain.
type Client interface {
	Enqueue(ctx context.Context, route string, body []byte) (string, error)
	Reserve(ctx context.Context, route string, leaseSecs uint32, batchSize uint32) ([]LeaseMessage, error)
	Complete(ctx context.Context, route string, id string, token uint64) error
}

// LeaseMessage is a reserved message returned from Reserve.
type LeaseMessage struct {
	ID    string
	Body  []byte
	Token uint64
}
