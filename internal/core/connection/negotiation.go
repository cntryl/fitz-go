package connection

import (
	"context"
	"time"
)

// WaitForServerHello completes capability negotiation before a Client publishes
// its domain handles. A parsed zero capability HELLO is a supported legacy peer.
// The caller deadline and configured read timeout bound a missing advertisement.
func (c *Connection) WaitForServerHello(ctx context.Context) error {
	timeout := c.cfg.ReadTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	negotiationCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case <-c.serverHello:
		return nil
	case <-c.done:
		if err := c.getConnError(); err != nil {
			return err
		}
		return ErrConnectionClosed
	case <-negotiationCtx.Done():
		return negotiationCtx.Err()
	}
}
