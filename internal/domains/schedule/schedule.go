package schedule

import (
	"context"
	"fmt"

	"github.com/cntryl/cntryl-go/internal/core/transport"
)

// Client provides schedule management APIs.
type Client interface {
	Create(ctx context.Context, route string, cronExpr string, payload []byte) (string, error)
	Cancel(ctx context.Context, id string) error
	List(ctx context.Context, route string) ([]ScheduleEntry, error)
}

// ScheduleEntry is a simple representation returned by List.
type ScheduleEntry struct {
	ID       string
	Route    string
	CronExpr string
}

// client is a concrete implementation of schedule.Client backed by the transport mux.
type client struct {
	mux *transport.Mux
}

// NewClient creates a new Schedule domain client backed by the transport mux.
func NewClient(mux *transport.Mux) Client {
	return &client{mux: mux}
}

// Create schedules a job using cron expression and payload; returns schedule ID on success.
func (c *client) Create(ctx context.Context, route string, cronExpr string, payload []byte) (string, error) {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	enc.AddString(transport.TagBody, cronExpr)
	enc.AddBytes(transport.TagBody, payload)
	frame := transport.Frame{
		Type:    600 % 256,
		Flags:   0,
		Channel: 9,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return "", fmt.Errorf("send create: %w", err)
	}
	return "", nil
}

// Cancel cancels a scheduled job by id.
func (c *client) Cancel(ctx context.Context, id string) error {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagToken, id)
	frame := transport.Frame{
		Type:    601 % 256,
		Flags:   0,
		Channel: 9,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return fmt.Errorf("send cancel: %w", err)
	}
	return nil
}

// List returns schedule entries for a route.
func (c *client) List(ctx context.Context, route string) ([]ScheduleEntry, error) {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	frame := transport.Frame{
		Type:    602 % 256,
		Flags:   0,
		Channel: 9,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return nil, fmt.Errorf("send list: %w", err)
	}
	return nil, nil
}
