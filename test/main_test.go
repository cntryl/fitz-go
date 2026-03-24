package integration

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	// goleak.VerifyTestMain ensures no goroutines are leaked between tests.
	// Filters for well-known background goroutines that are acceptable.
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("testing.(*M).Run"),
		goleak.IgnoreTopFunction("os/signal.signal_recv"),
	)
}
