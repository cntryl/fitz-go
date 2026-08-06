//go:build integration

package conformance

import "github.com/cntryl/fitz-go/v2/internal/testkit"

func closeQuietly[T interface{ Close() error }](value T) {
	testkit.CloseQuietly(value)
}
