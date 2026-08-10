# fitz-go

Reference Go client for Fitz.

The supported public API is the canonical `github.com/cntryl/fitz-go/v2/fitz`
package: token-provider auth, `Connect`/`Close`, `State`, and
spec-facing domain verbs.

## Public API

Import `github.com/cntryl/fitz-go/v2/fitz` for the public API.

```go
package main

import (
	"context"
	"time"

	"github.com/cntryl/fitz-go/v2/fitz"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := fitz.NewClient("ws://localhost:4090/ws", func(context.Context) (string, error) {
		return "your-jwt-token", nil
	})

	if err := client.Connect(ctx); err != nil {
		panic(err)
	}
	defer client.Close()

	tx, err := client.KV().Begin(ctx, "kv://realm/area/users", fitz.KVDurabilitySync)
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
- KV/Schedule/Notice/Queue/Lease/Stream subscription handlers return `error`.
- Streaming iterators should be closed when no longer needed.
- Clients validate route shape locally: scheme, segment count, empty segments,
  and method-specific wildcard placement. Route existence, permissions,
  authorization, realm semantics, and resource names remain broker-owned.

Connection lifecycle is part of the stable public API:

- `Disconnected`: before `Connect` and after an unrecoverable disconnect
- `Connecting`: transport dial/handshake in progress
- `Connected`: transport established, auth not yet settled
- `Authenticating`: CONNECT/auth exchange in progress
- `Authenticated`: ready for domain traffic
- `Reconnecting`: automatic reconnect loop is actively retrying
- `Closed`: client has been closed and will not reconnect

Reconnect guarantees:

- Automatic reconnect is enabled by default after the first successful
  `Connect`; `fitz.WithReconnect(false, ..., ...)` disables it.
- `fitz.WithReconnect(..., maxAttempts=0)` means unlimited reconnect attempts.
- KV, notice, stream, lease, queue, and schedule subscriptions are restored after reconnect.
- RPC worker registrations are restored after reconnect.
- `Close()` is idempotent and permanently ends reconnect activity.

Production defaults also include heartbeat (`10s` interval, `30s` timeout),
safe automatic retries for replayable reads, and a bounded outbound request
queue of `1024`. See [docs/PUBLIC_CONTRACT.md](docs/PUBLIC_CONTRACT.md).

The broker-backed test suite verifies those guarantees through a live disconnect proxy rather than by closing one client and creating another.

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

Stream replay pattern:

```go
filter := fitz.StreamFilterSet{Clauses: []fitz.StreamFilterClause{{Kind: fitz.StreamFilterEquals, Value: "proj.alpha"}}}

records, err := client.Stream().Read(ctx, "stream://realm/area/events", 0, 100, fitz.WithStreamFilter(filter))
if err != nil {
	panic(err)
}
defer records.Close()

page, err := client.Stream().ReadPage(ctx, "stream://realm/area/events", 0, 100, fitz.WithStreamFilter(filter))
if err != nil {
	panic(err)
}

// Read is the event-only projection of ReadPage.
// ReadPage exposes filtered markers plus cursor progression across hidden offsets.
_ = page.Cursor.LastResourceOffset
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

Integration tests target a running Fitz broker and are opt-in via the
`integration` build tag.

Use the local compose stack in [compose.yml](compose.yml):

```bash
docker compose up -d
```

That starts:

- `fitz-auth` on loopback-only `localhost:4091` and `ws://localhost:4090/ws`
- `fitz-anon` on loopback-only `localhost:4191` and `ws://localhost:4190/ws`

Both services use `ghcr.io/cntryl/fitz:latest` and local storage volumes. Host
ports are configurable through the `FITZ_AUTH_HOST_*` and
`FITZ_ANON_HOST_*` variables.

Anonymous broker example:

```bash
export FITZ_BROKER_TCP_ADDR=localhost:4191
export FITZ_BROKER_WS_ADDR=ws://localhost:4190/ws
go test ./...
```

Run the broker-backed acceptance suite explicitly when you want the full
end-to-end matrix:

```bash
go test -tags=integration ./test ./test/conformance/...
```

Auth-required broker example:

```bash
export FITZ_BROKER_TCP_ADDR=localhost:4091
export FITZ_BROKER_WS_ADDR=ws://localhost:4090/ws
export FITZ_BROKER_AUTH_REQUIRED=true
export FITZ_BROKER_JWT_HMAC_SECRET=dev-test-secret
export FITZ_BROKER_JWT_AUDIENCE=fitz
go test -tags=integration ./test
```

Error-path coverage in the broker-backed suite includes unauthorized operations across all 7 domains, plus invalid KV range and invalid cron cases.

Focused reconnect validation is usually more useful than a blanket `go test -tags=integration ./test` run in this repo. The high-signal reconnect slices are:

```bash
go test -tags=integration ./test -run "TestShould(RestoreNoticeSubscriptionGivenLiveDisconnectWhenReconnectEnabled|RestoreWorkerRegistrationGivenLiveDisconnectWhenReconnectEnabled|RestoreAvailabilitySubscriptionGivenLiveDisconnectWhenReconnectEnabled|RestoreCommitSubscriptionGivenLiveDisconnectWhenReconnectEnabled|RestoreLeaseSubscriptionGivenLiveDisconnectWhenReconnectEnabled)"
go test -tags=integration ./test -run "TestShouldRestoreScheduleSubscriptionGivenLiveDisconnectWhenReconnectEnabled"
go test -tags=integration ./test/conformance/... -run "TestConformanceSuite/(CS-009_disconnect_during_request|CS-010_reconnect_behavior)"
```

Those tests use the shared live-disconnect seam in `test/fixture/proxy.go` to exercise real disconnect, reconnect, and restore behavior against a running broker.

Benchmark thresholds and evidence policy are documented in [docs/PERF_RESULTS.md](docs/PERF_RESULTS.md); treat that report as the source of truth for hot-path gates.

Run the full suite with:

```bash
go test ./...
go test -tags=integration ./test ./test/conformance/...
```

Run pedantic lint and style checks directly with golangci-lint v2:

```bash
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
$(go env GOPATH)/bin/golangci-lint version
$(go env GOPATH)/bin/golangci-lint run --config .golangci.yml
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

Run the public hotpath suite:

```bash
go test -run=^$ -bench='Benchmark(HandleRPCResponseHotPath|QueueReserveHotPath|QueueCompleteHotPath|StreamBeginHotPath|StreamAppendHotPath|ScheduleCreateHotPath|ScheduleCancelHotPath|SubscriptionRegistryRestore|KVTransactionLoopback|NoticePublishHotPath|FrameEncode|RPCCorrelation1KInFlight)' -benchmem -count=5 -benchtime=2s ./bench ./internal/domains/rpc
```

`bench/hotpath_bench_test.go` holds the cross-domain hotpaths, and `internal/domains/rpc/rpc_test.go` keeps the RPC dispatch benchmark next to the implementation.

Collect CPU and memory profiles for one benchmark target:

```bash
go test -run=^$ -bench=BenchmarkMuxDispatchResponse -benchmem -count=1 -cpuprofile=cpu.prof -memprofile=mem.prof ./internal/core/connection
go tool pprof -top cpu.prof
go tool pprof -top mem.prof
```

When you need a regression diff, capture two local benchmark runs and compare them with `benchstat`:

```bash
go test -run=^$ -bench=. -benchmem -count=3 \
	github.com/cntryl/fitz-go/v2/bench \
	github.com/cntryl/fitz-go/v2/internal/domains/rpc \
	github.com/cntryl/fitz-go/v2/internal/protocol \
	github.com/cntryl/fitz-go/v2/internal/core/connection \
	github.com/cntryl/fitz-go/v2/internal/core/encoding \
	github.com/cntryl/fitz-go/v2/internal/core/transport \
	github.com/cntryl/fitz-go/v2/internal/domains/stream \
	github.com/cntryl/fitz-go/v2/internal/domains/kv \
	github.com/cntryl/fitz-go/v2/internal/domains/notice \
	github.com/cntryl/fitz-go/v2/internal/domains/schedule > before.txt
go test -run=^$ -bench=. -benchmem -count=3 \
	github.com/cntryl/fitz-go/v2/bench \
	github.com/cntryl/fitz-go/v2/internal/domains/rpc \
	github.com/cntryl/fitz-go/v2/internal/protocol \
	github.com/cntryl/fitz-go/v2/internal/core/connection \
	github.com/cntryl/fitz-go/v2/internal/core/encoding \
	github.com/cntryl/fitz-go/v2/internal/core/transport \
	github.com/cntryl/fitz-go/v2/internal/domains/stream \
	github.com/cntryl/fitz-go/v2/internal/domains/kv \
	github.com/cntryl/fitz-go/v2/internal/domains/notice \
	github.com/cntryl/fitz-go/v2/internal/domains/schedule > after.txt
benchstat before.txt after.txt
```

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
| Queue | 200, 202-204, 207-209 | ENQUEUE, RESERVE, EXTEND, COMPLETE, SUBSCRIBE | CS-018, CS-022 |
| RPC | 300-303 | SUBSCRIBE_WORKER, UNSUBSCRIBE_WORKER, REQUEST, RESPONSE | CS-004, CS-006, CS-007, CS-008, CS-009, CS-017 |
| Lease | 400-403, 407-409 | ACQUIRE, RENEW, RELEASE, QUERY, NOTIFY | CS-019 |
| Notice | 500-504 | PUBLISH, SUBSCRIBE, UNSUBSCRIBE, NOTIFY | CS-010, CS-020 |
| Stream | 600-609 | BEGIN, APPEND, COMMIT, READ, SUBSCRIBE | CS-011, CS-012, CS-013, CS-016 |
| Schedule | 700-705 | CREATE, CANCEL, LIST, SUBSCRIBE, NOTIFY | CS-021 |

Notes:

- Queue `201` (ENQUEUE_BATCH) is reserved by spec and intentionally not implemented.
- Delayed queue visibility is available through `EnqueueWithOptions` and
  `WithQueueEnqueueDelaySeconds`; `Enqueue` remains the immediate shorthand.
- CS-001–CS-017 are the shared cross-language suite. CS-018–CS-022 are Go
  additions for Queue, Lease, Notice, Schedule, and queue reconnect coverage.
- Schedule fire delivery (actual cron trigger) is covered in the integration suite
  (`TestShouldDeliverScheduleNotificationGivenLiveBrokerWhenScheduleFires`), which
  requires up to 90 s for the next `* * * * *` tick.
- Conformance scenarios focus on cross-language semantic parity; integration tests
  provide additional domain-specific operation coverage.

## Managed leases

`client.Lease().WithLease(ctx, route, ttlSecs, callback)` owns acquisition, renewal,
callback cancellation, and release. Add `fitz.WithLeaseWaitSeconds(30)` to use the
broker's bounded FIFO acquisition queue.
The callback must honor its context promptly; `context.Cause` reports caller cancellation
or `fitz.ErrLeaseLost`. Low-level lease methods remain available and serialize token rotation.

Use `fitz.LeaseAuthorityFromContext` inside the callback to obtain the broker-issued
admission fence from the successful ACQUIRE that started that invocation:

```go
err := client.Lease().WithLease(ctx, route, 30, func(callbackCtx context.Context) error {
	if authority, ok := fitz.LeaseAuthorityFromContext(callbackCtx); ok {
		return runRole(callbackCtx, authority.FencingToken)
	}
	return errors.New("managed lease authority is missing")
}, fitz.WithLeaseOwnerID("worker-1"))
```

The authority is an immutable value snapshot. A queued acquisition exposes the final
granted token, and managed renewal does not update the callback snapshot even when it
rotates the live lease credential. Treat it as an admission fencing epoch, not a live
renewal credential. Tokens are ordered only across successive ownership of the same lease
route. An external store enforces fencing by atomically retaining its greatest accepted
value and rejecting lower values; do not compare the admission snapshot for equality with
a later live broker token.
