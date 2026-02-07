package schedule

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/cntryl/cntryl-go/internal/core/transport"
	"github.com/cntryl/cntryl-go/internal/core/types"
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
	if err := types.ValidateRoute(route, "schedule"); err != nil {
		return "", err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(ScheduleCreate)
	enc.AddString(transport.TagRoute, route)
	enc.AddString(transport.TagCron, cronExpr)
	if len(payload) > 0 {
		enc.AddBytes(transport.TagBody, payload)
	}
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelSchedule, Body: enc.Encode()}

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
	enc.AddOp(ScheduleCancel)
	enc.AddString(transport.TagID, id)
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelSchedule, Body: enc.Encode()}

	_, err := transport.SendRecv(ctx, c.mux, frame, mapScheduleError)
	return err
}

// List returns schedule entries for a route.
func (c *client) List(ctx context.Context, route string) ([]ScheduleEntry, error) {
	if err := types.ValidateRoute(route, "schedule"); err != nil {
		return nil, err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(ScheduleList)
	enc.AddString(transport.TagRoute, route)
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelSchedule, Body: enc.Encode()}

	resp, err := transport.SendRecv(ctx, c.mux, frame, mapScheduleError)
	if err != nil {
		return nil, err
	}
	dec, derr := transport.NewTLVDecoder(resp.Body)
	if derr != nil {
		return nil, fmt.Errorf("invalid TLV in response: %w", derr)
	}
	// Entries are encoded as repeated TagID/TagRoute/TagCron triples.
	ids := dec.GetAll(transport.TagID)
	routes := dec.GetAll(transport.TagRoute)
	crons := dec.GetAll(transport.TagCron)
	entries := make([]ScheduleEntry, 0, len(ids))
	for i := range ids {
		// IDs are encoded as u64 BE values; format as decimal string.
		idstr := ""
		if len(ids[i]) == 8 {
			id := binary.BigEndian.Uint64(ids[i])
			idstr = fmt.Sprintf("%d", id)
		} else {
			// Fallback: if broker sent a non-u64 representation, preserve raw bytes as string.
			idstr = string(ids[i])
		}
		e := ScheduleEntry{ID: idstr}
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
