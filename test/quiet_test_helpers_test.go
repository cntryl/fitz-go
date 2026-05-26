//go:build integration

package integration

import (
	"context"

	"github.com/cntryl/fitz-go/internal/testkit"
)

func closeQuietly[T interface{ Close() error }](value T) {
	testkit.CloseQuietly(value)
}

func rollbackQuietly[T interface{ Rollback(context.Context) error }](ctx context.Context, value T) {
	testkit.RollbackQuietly(ctx, value)
}

func releaseQuietly[T interface{ Release(context.Context) error }](ctx context.Context, value T) {
	testkit.ReleaseQuietly(ctx, value)
}
