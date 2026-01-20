package notice

import "context"

// Client is the API for the Notice domain.
type Client interface {
	Subscribe(ctx context.Context, route string) error
	Unsubscribe(ctx context.Context, route string) error
	Publish(ctx context.Context, route string, body []byte) error
}
