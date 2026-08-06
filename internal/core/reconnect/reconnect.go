package reconnect

import (
	"context"

	"github.com/cntryl/fitz-go/v2/internal/core/connection"
)

// DomainRestorer is implemented by subscription-based domain clients that can
// bind to a replacement connection and replay restorable session state after
// reconnect.
type DomainRestorer interface {
	ReplaceConnection(conn *connection.Connection)
	RestoreSubscriptions(ctx context.Context) error
}
