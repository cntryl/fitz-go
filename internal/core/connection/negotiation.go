package connection

import (
	"context"
	"time"
)

// WaitForServerHello confirms that wire-2 limits were parsed before domain use.
// The caller deadline and configured read timeout bound a missing HELLO.
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
