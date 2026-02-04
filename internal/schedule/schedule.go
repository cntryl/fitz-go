package schedule

import "context"

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
