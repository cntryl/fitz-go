package lease

import "context"

// Client provides ephemeral exclusive lease primitives.
type Client interface {
	Acquire(ctx context.Context, route string, ttlSecs uint32) (token []byte, expiresAt int64, held bool, err error)
	Renew(ctx context.Context, route string, token []byte, ttlSecs uint32) (expiresAt int64, err error)
	Release(ctx context.Context, route string, token []byte) error
}
