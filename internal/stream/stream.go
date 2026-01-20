package stream

import "context"

// Client is the API for the Stream domain.
type Client interface {
	Append(ctx context.Context, route string, body []byte, expectedOffset *uint64) (uint64, error)
	ReadResource(ctx context.Context, route string, from uint64, limit uint32) ([]StreamRecord, error)
}

// StreamRecord is a minimal record returned by stream reads.
type StreamRecord struct {
	Offset uint64
	Body   []byte
}
