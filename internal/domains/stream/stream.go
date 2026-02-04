package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/cntryl/cntryl-go/internal/core/transport"
)

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

// client is a concrete implementation of stream.Client backed by the transport mux.
type client struct {
	mux *transport.Mux
}

// NewClient creates a new Stream domain client backed by the transport mux.
func NewClient(mux *transport.Mux) Client {
	return &client{mux: mux}
}

// Append appends a record and returns the new offset assigned by the broker.
func (c *client) Append(ctx context.Context, route string, body []byte, expectedOffset *uint64) (uint64, error) {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	enc.AddBytes(transport.TagBody, body)
	if expectedOffset != nil {
		enc.AddUint64(transport.TagExpectedOffset, *expectedOffset)
	}
	frame := transport.Frame{
		Type:    StreamAppend,
		Flags:   0,
		Channel: ChannelStream,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return 0, fmt.Errorf("send append: %w", err)
	}

	// Wait for response with assigned offset.
	ackCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		select {
		case <-ackCtx.Done():
			return 0, ackCtx.Err()
		case f, ok := <-c.mux.In():
			if !ok {
				return 0, fmt.Errorf("mux closed")
			}
			if f.Channel != ChannelStream {
				continue
			}
			dec, err := transport.NewTLVDecoder(f.Body)
			if err != nil {
				continue
			}
			if dec.Has(transport.TagErr) {
				return 0, fmt.Errorf("broker error: %s", dec.GetString(transport.TagErr))
			}
			seq, _ := dec.GetUint64(transport.TagSeq)
			if seq != 0 {
				return seq, nil
			}
		}
	}
}

// ReadResource reads records from a stream starting at `from` up to `limit` items.
func (c *client) ReadResource(ctx context.Context, route string, from uint64, limit uint32) ([]StreamRecord, error) {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	enc.AddUint64(transport.TagSeq, from)
	enc.AddUint32(transport.TagLimit, limit)
	frame := transport.Frame{
		Type:    StreamRead,
		Flags:   0,
		Channel: ChannelStream,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return nil, fmt.Errorf("send read: %w", err)
	}

	// Collect records until stream end or context done.
	var results []StreamRecord
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case f, ok := <-c.mux.In():
			if !ok {
				return nil, fmt.Errorf("mux closed")
			}
			if f.Channel != ChannelStream {
				continue
			}
			dec, err := transport.NewTLVDecoder(f.Body)
			if err != nil {
				continue
			}
			if dec.Has(transport.TagErr) {
				return nil, fmt.Errorf("broker error: %s", dec.GetString(transport.TagErr))
			}
			if dec.Has(transport.TagStreamEnd) {
				return results, nil
			}
			seq, _ := dec.GetUint64(transport.TagSeq)
			body := dec.GetBytes(transport.TagBody)
			results = append(results, StreamRecord{Offset: seq, Body: body})
		}
	}
}
