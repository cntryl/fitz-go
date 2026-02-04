package notice

import (
	"context"
	"fmt"

	"github.com/cntryl/cntryl-go/internal/transport"
)

// client is a concrete implementation of notice.Client backed by the transport mux.
type client struct {
	mux *transport.Mux
}

// NewClient creates a new Notice domain client backed by the transport mux.
func NewClient(mux *transport.Mux) Client {
	return &client{mux: mux}
}

// Subscribe registers interest in notifications matching the route. Best-effort send.
func (c *client) Subscribe(ctx context.Context, route string) error {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	frame := transport.Frame{
		Type:    NoticeSubscribe,
		Flags:   0,
		Channel: ChannelSub,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return fmt.Errorf("send subscribe: %w", err)
	}
	return nil
}

// Unsubscribe removes a subscription for the route.
func (c *client) Unsubscribe(ctx context.Context, route string) error {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	frame := transport.Frame{
		Type:    NoticeUnsubscribe,
		Flags:   0,
		Channel: ChannelSub,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return fmt.Errorf("send unsubscribe: %w", err)
	}
	return nil
}

// Publish sends a notification to the given route with body bytes.
func (c *client) Publish(ctx context.Context, route string, body []byte) error {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	enc.AddBytes(transport.TagBody, body)
	frame := transport.Frame{
		Type:    NoticePublish,
		Flags:   0,
		Channel: ChannelPub,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return fmt.Errorf("send publish: %w", err)
	}
	return nil
}
