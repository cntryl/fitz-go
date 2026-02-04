# cntryl-go

Go SDK for cntryl.

See `README_TEMPLATE.md` at the repository root for guidelines. This folder is a template for the `cntryl-go` repository.

## Running integration tests against a real broker

By default the test fixture will start an in-process simulator when `FITZ_BROKER_ADDR` is not set. To validate behavior against a real broker, set `FITZ_BROKER_ADDR` to your broker endpoint. If you want tests to fail when a real broker address is not provided, set `FITZ_REQUIRE_REAL_BROKER=true` as well.

Examples:

- Linux/macOS:

  FITZ_BROKER_ADDR=localhost:4091 FITZ_REQUIRE_REAL_BROKER=true go test ./test -run TestShouldOpenAndCommitTransactionGivenValidRouteWhenBeginCalled -v

- PowerShell (Windows):

  $env:FITZ_BROKER_ADDR = 'localhost:4091'; $env:FITZ_REQUIRE_REAL_BROKER = 'true'; go test ./test -run TestShouldOpenAndCommitTransactionGivenValidRouteWhenBeginCalled -v

This allows you to exercise the full broker implementation during development and CI when a broker is available.
