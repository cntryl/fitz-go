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

Connection lifecycle is part of the stable public API:

- `Disconnected`: before `Connect` and after an unrecoverable disconnect
- `Connecting`: transport dial/handshake in progress
- `Connected`: transport established, auth not yet settled
- `Authenticating`: CONNECT/auth exchange in progress
- `Authenticated`: ready for domain traffic
- `Reconnecting`: automatic reconnect loop is actively retrying
- `Closed`: client has been closed and will not reconnect

Reconnect guarantees:

- Automatic reconnect only runs when configured with `fitz.WithReconnect(...)`.
- Notice, stream, lease, queue, and schedule subscriptions are restored after reconnect.
- RPC worker registrations are restored after reconnect.
- `Close()` is idempotent and permanently ends reconnect activity.

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
sub, err := client.Schedule().Subscribe(ctx, "schedule://realm/area/resource/run", func(ctx context.Context, n fitz.ScheduleNotification) error {
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

Use the local compose stack in [compose.yml](compose.yml):

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

Error-path coverage in the broker-backed suite now includes unauthorized operations across all 7 domains, plus invalid KV range and invalid cron cases.

Benchmark thresholds and evidence policy are documented in [docs/PERF_RESULTS.md](docs/PERF_RESULTS.md); treat that report as the source of truth for hot-path gates.

Run the full suite with:

```bash
go test ./test/...
go test ./...
```

Or use the repo-local verification script:

```powershell
./scripts/verify.ps1
```

Run the repo-local spec-compliance conformance suite with:

```bash
go test -v -timeout 120s ./test/conformance/... -run TestConformanceSuite
```

## Local performance workflow

Use direct `go test` and `go tool pprof` commands while optimizing hot paths.

The benchmark gates and evidence policy are summarized in [docs/PERF_RESULTS.md](docs/PERF_RESULTS.md).

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
# Compare against the checked-in baseline
go test -run=^$ -bench=. -benchmem -count=3 \
  github.com/cntryl/fitz-go/internal/protocol \
  github.com/cntryl/fitz-go/internal/core/connection \
  github.com/cntryl/fitz-go/internal/core/encoding \
  github.com/cntryl/fitz-go/internal/core/transport \
  github.com/cntryl/fitz-go/internal/domains/rpc \
  github.com/cntryl/fitz-go/internal/domains/stream \
  github.com/cntryl/fitz-go/internal/domains/kv \
  github.com/cntryl/fitz-go/internal/domains/notice \
  github.com/cntryl/fitz-go/internal/domains/schedule > new.txt
benchstat benchmarks/baseline.txt new.txt
```

A committed baseline covering all 9 benchmark packages lives in
`benchmarks/baseline.txt` (Intel Xeon E-2224G, Windows/amd64, Go 1.22).
Re-capture it after deliberate performance work with the same command redirected
to `benchmarks/baseline.txt`.

Install `benchstat` once if needed with `go install golang.org/x/perf/cmd/benchstat@latest`.

## Protocol source of truth

This repo does not maintain an independent copy of the protocol specification.
Use the canonical server-owned docs referenced from:

- [docs/CLIENT_SPEC.md](docs/CLIENT_SPEC.md)
- [docs/CLIENT_ACCEPTANCE_CRITERIA.md](docs/CLIENT_ACCEPTANCE_CRITERIA.md)

## Documentation

- [docs/README.md](docs/README.md)
- [docs/CLIENT_SPEC.md](docs/CLIENT_SPEC.md)
- [docs/CLIENT_ACCEPTANCE_CRITERIA.md](docs/CLIENT_ACCEPTANCE_CRITERIA.md)

## Message type and conformance coverage map

This is a lightweight map from implemented message-type ranges to conformance
scenario coverage in `test/conformance`.

| Domain | Message type range | Key message types (examples) | Conformance scenarios |
| --- | --- | --- | --- |
| Control | 1 | CONNECT | CS-001, CS-002 |
| KV | 100-108 | BEGIN, COMMIT, GET, PUT, INSERT, SCAN | CS-001, CS-003, CS-005, CS-006, CS-014, CS-015 |
| Queue | 200, 202-204, 207-209 | ENQUEUE, RESERVE, EXTEND, COMPLETE, SUBSCRIBE | CS-016 (enqueue/reserve/complete lifecycle) |
| RPC | 300-304 | SUBSCRIBE_WORKER, REQUEST, RESPONSE | CS-004, CS-006, CS-007, CS-008, CS-009 |
| Lease | 400-403, 407-409 | ACQUIRE, RENEW, RELEASE, QUERY, NOTIFY | CS-017 (acquire/contention/release lifecycle) |
| Notice | 500-504 | PUBLISH, SUBSCRIBE, UNSUBSCRIBE, NOTIFY | CS-018 (subscribe/publish/deliver/unsubscribe) |
| Stream | 600-609 | BEGIN, APPEND, COMMIT, READ, SUBSCRIBE | CS-011, CS-012, CS-013 |
| Schedule | 700-705 | CREATE, CANCEL, LIST, SUBSCRIBE, NOTIFY | CS-019 (create/subscribe/cancel lifecycle) |

Notes:

- Queue `201` (ENQUEUE_BATCH) is reserved by spec and intentionally not implemented.
- CS-016–CS-019 are Go client additions beyond the 15-scenario cross-language spec, closing
  coverage gaps for the four subscribe/notify-pattern domains.
- Schedule fire delivery (actual cron trigger) is covered in the integration suite
  (`TestShouldDeliverScheduleNotificationGivenLiveBrokerWhenScheduleFires`), which
  requires up to 90 s for the next `* * * * *` tick.
- Conformance scenarios focus on cross-language semantic parity; integration tests
  provide additional domain-specific operation coverage.
