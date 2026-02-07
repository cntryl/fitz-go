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

type client struct {
	mux transport.MuxProvider
}

// NewClient creates a new Schedule domain client backed by the transport mux.
func NewClient(mux transport.MuxProvider) Client {
	return &client{mux: mux}
}

// Create schedules a job; returns the schedule ID assigned by the broker.
func (c *client) Create(ctx context.Context, route string, cronExpr string, payload []byte) (string, error) {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	enc.AddString(transport.TagCron, cronExpr)
	if len(payload) > 0 {
		enc.AddBytes(transport.TagBody, payload)
	}
	frame := transport.Frame{Type: ScheduleCreate, Channel: transport.ChannelSchedule, Body: enc.Encode()}

	resp, err := transport.SendRecv(ctx, c.mux, frame, mapScheduleError)
	if err != nil {
		return "", err
	}
	dec, derr := transport.NewTLVDecoder(resp.Body)
	if derr != nil {
		return "", fmt.Errorf("invalid TLV in response: %w", derr)
	}
	id, _ := dec.GetUint64(transport.TagID)
	return fmt.Sprintf("%d", id), nil
}

// Cancel cancels a scheduled job by id.
func (c *client) Cancel(ctx context.Context, id string) error {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagToken, id)
	frame := transport.Frame{Type: ScheduleCancel, Channel: transport.ChannelSchedule, Body: enc.Encode()}

	_, err := transport.SendRecv(ctx, c.mux, frame, mapScheduleError)
	return err
}

// List returns schedule entries for a route.
func (c *client) List(ctx context.Context, route string) ([]ScheduleEntry, error) {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	frame := transport.Frame{Type: ScheduleList, Channel: transport.ChannelSchedule, Body: enc.Encode()}

	resp, err := transport.SendRecv(ctx, c.mux, frame, mapScheduleError)
	if err != nil {
		return nil, err
	}
	dec, derr := transport.NewTLVDecoder(resp.Body)
	if derr != nil {
		return nil, fmt.Errorf("invalid TLV in response: %w", derr)
	}
	// Entries are encoded as repeated TagID/TagRoute/TagBody triples.
	ids := dec.GetAll(transport.TagID)
	routes := dec.GetAll(transport.TagRoute)
	crons := dec.GetAll(transport.TagBody)
	entries := make([]ScheduleEntry, 0, len(ids))
	for i := range ids {
		e := ScheduleEntry{ID: string(ids[i])}
		if i < len(routes) {
			e.Route = string(routes[i])
		}
		if i < len(crons) {
			e.CronExpr = string(crons[i])
		}
		entries = append(entries, e)
	}
	return entries, nil
}
