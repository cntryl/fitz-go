# fitz-go

Go client for Fitz.

This release is the breaking cleanup pass for the Go SDK. The supported public
API is the canonical `github.com/cntryl/fitz-go/fitz` package with
token-provider auth, `Connect`/`Close`, `State`, and spec-facing domain verbs.

## Public API

The canonical public package is `github.com/cntryl/fitz-go/fitz`.

```go
package main

import (
	"context"
	"time"

	"github.com/cntryl/fitz-go/fitz"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := fitz.NewClient("ws://localhost:4090/ws", func(context.Context) (string, error) {
		return "your-jwt-token", nil
	}, fitz.WithReconnect(true, 250*time.Millisecond, 5))

	if err := client.Connect(ctx); err != nil {
		panic(err)
	}
	defer client.Close()

	tx, err := client.KV().Begin(ctx, "kv://realm/area/users")
	if err != nil {
		panic(err)
	}
	defer tx.Rollback(ctx)

	if err := tx.Put(ctx, []byte("user-1"), []byte(`{"name":"Alice"}`)); err != nil {
		panic(err)
	}

	value, err := tx.Get(ctx, []byte("user-1"))
	if err != nil {
		panic(err)
	}
	if value.Found {
		println(string(value.Value))
	}

	if err := tx.Commit(ctx); err != nil {
		panic(err)
	}
}
```

## Canonical usage patterns

Use one control plane for request lifetime: `context.Context`.

- RPC calls use context deadlines/cancellation only.
- Schedule/Notice/Queue/Lease/Stream subscription handlers return `error`.
- Streaming iterators should be closed when no longer needed.

RPC timeout pattern:

```go
callCtx, cancelCall := context.WithTimeout(ctx, 2*time.Second)
defer cancelCall()

iter, err := client.RPC().Call(callCtx, "rpc://realm/area/echo", []byte("ping"))
if err != nil {
	panic(err)
}
defer iter.Close()

for iter.Next() {
	_ = iter.Value()
}
if err := iter.Err(); err != nil {
	panic(err)
}
```

Schedule subscription handler pattern:

```go
sub, err := client.Schedule().Subscribe(ctx, "schedule://realm/area/*", func(ctx context.Context, n fitz.ScheduleNotification) error {
	_ = n
	return nil
})
if err != nil {
	panic(err)
}
defer sub.Unsubscribe()
```

## Architecture

- `fitz/`: public client, public domain wrappers, public types
- `internal/core/client`: top-level client implementation
- `internal/core/connection`: CONNECT lifecycle, request correlation, notify dispatch
- `internal/core/transport`: TCP and WebSocket transports
- `internal/protocol`: frame encoding and message type constants
- `internal/domains/*`: spec-aligned domain clients
- `test/`: broker-backed integration coverage

## Broker-backed tests

Integration tests target a running Fitz broker and are part of the default
verification bar for this repo.

Use the local compose stack in [compose.yml](/D:/repos/cntryl/fitz/fitz-go/compose.yml):

```bash
docker compose -f compose.yml up -d
```

That starts:

- `fitz-auth` on `localhost:4091` and `ws://localhost:4090/ws`
- `fitz-anon` on `localhost:4191` and `ws://localhost:4190/ws`

Anonymous broker example:

```bash
export FITZ_BROKER_TCP_ADDR=localhost:4191
export FITZ_BROKER_WS_ADDR=ws://localhost:4190/ws
go test ./...
```

Auth-required broker example:

```bash
export FITZ_BROKER_TCP_ADDR=localhost:4091
export FITZ_BROKER_WS_ADDR=ws://localhost:4090/ws
export FITZ_BROKER_AUTH_REQUIRED=true
export FITZ_BROKER_JWT_HMAC_SECRET=test-secret-key
export FITZ_BROKER_JWT_AUDIENCE=fitz
go test ./...
```

Run the full suite with:

```bash
go test ./test/...
go test ./...
```

Run the repo-local spec-compliance conformance suite with:

```bash
go test -v -timeout 120s ./test/conformance/... -run TestConformanceSuite
```

## Local performance workflow

Use direct `go test` and `go tool pprof` commands while optimizing hot paths.

Run hotpath micro-benchmarks:

```bash
go test -run=^$ -bench='Benchmark(EncodeFrame|DecodeFrame|EncodeFrameWithPayloadWriter|MuxDispatchResponse|RegisterRequest|ConcurrentDispatch|WriteFrame|ReadFrame|WriteWSFrame|ReadWSFrame|WriteU64|WriteU32|WriteString|WriteBytes)' -benchmem -count=5 -benchtime=2s ./internal/protocol ./internal/core/encoding ./internal/core/connection ./internal/core/transport
```

Run domain-level benchmarks:

```bash
go test -run=^$ -bench=Benchmark -benchmem -count=5 -benchtime=2s ./internal/domains/...
```

Collect CPU and memory profiles for one benchmark target:

```bash
go test -run=^$ -bench=BenchmarkMuxDispatchResponse -benchmem -count=1 -cpuprofile=cpu.prof -memprofile=mem.prof ./internal/core/connection
go tool pprof -top cpu.prof
go tool pprof -top mem.prof
```

Save benchmark outputs directly when comparing changes:

```bash
go test -run=^$ -bench='Benchmark(EncodeFrame|DecodeFrame|EncodeFrameWithPayloadWriter|MuxDispatchResponse|RegisterRequest|ConcurrentDispatch|WriteFrame|ReadFrame|WriteWSFrame|ReadWSFrame|WriteU64|WriteU32|WriteString|WriteBytes)' -benchmem -count=5 ./internal/protocol ./internal/core/encoding ./internal/core/connection ./internal/core/transport > old.txt
go test -run=^$ -bench='Benchmark(EncodeFrame|DecodeFrame|EncodeFrameWithPayloadWriter|MuxDispatchResponse|RegisterRequest|ConcurrentDispatch|WriteFrame|ReadFrame|WriteWSFrame|ReadWSFrame|WriteU64|WriteU32|WriteString|WriteBytes)' -benchmem -count=5 ./internal/protocol ./internal/core/encoding ./internal/core/connection ./internal/core/transport > new.txt
benchstat old.txt new.txt
```

Install `benchstat` once if needed with `go install golang.org/x/perf/cmd/benchstat@latest`.

## Protocol source of truth

This repo does not maintain an independent copy of the protocol specification.
Use the canonical server-owned docs referenced from:

- `docs/CLIENT_SPEC.md`
- `docs/CLIENT_ACCEPTANCE_CRITERIA.md`
