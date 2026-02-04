package notice

import "context"

// Client is the API for the Notice domain.
// Subscribe remains for simple use; SubscribeChan returns a channel that receives
// notifications for the subscribed route (supports wildcards '*' and '**').
type Client interface {
	Subscribe(ctx context.Context, route string) error
	SubscribeChan(ctx context.Context, route string) (<-chan []byte, error)
	Unsubscribe(ctx context.Context, route string) error
	UnsubscribeAll(ctx context.Context) error
	Publish(ctx context.Context, route string, body []byte) error
}
