package integration

import "context"

func closeQuietly[T interface{ Close() error }](value T) {
	_ = value.Close()
}

func rollbackQuietly[T interface{ Rollback(context.Context) error }](ctx context.Context, value T) {
	_ = value.Rollback(ctx)
}

func releaseQuietly[T interface{ Release(context.Context) error }](ctx context.Context, value T) {
	_ = value.Release(ctx)
}
