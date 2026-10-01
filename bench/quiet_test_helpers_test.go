package bench

import "github.com/cntryl/fitz-go/internal/testkit"

func closeQuietly[T interface{ Close() error }](value T) {
	testkit.CloseQuietly(value)
}
