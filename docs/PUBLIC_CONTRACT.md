# fitz-go Public Contract

This document summarizes the production-facing contract of the public
`github.com/cntryl/fitz-go/fitz` package.

## Routes

The Go client validates route shape for ergonomics: scheme, path segment count,
empty segments, and wildcard placement allowed for the called method. It does
not validate route existence, permissions, authorization, realm semantics,
resource names, or auth claims, and it does not normalize route strings.

KV, Queue, Stream, Notice, RPC worker, and Schedule registrations accept exact
routes plus whole-segment `*` and `**` patterns, including wildcard realms.
KV, Queue, and Stream patterns must be capable of matching three segments;
Schedule patterns must match four; Notice and RPC have flexible depth. The
broker permits 128 wildcard registrations per domain and session; exact
registrations do not consume that quota. Lease subscriptions are different:
they accept only an exact `lease://realm/area/resource` route.

Notifications always expose the exact concrete delivery route. Queue
availability notifications additionally expose ready, delayed, and inflight
message counts. Duplicate local registrations for the same original string
share one wire registration and reconnect restores active registrations.

## Lifecycle And Resilience

- `Connect(ctx)` is one-shot for the initial connection. Concurrent or repeated
  calls while connected return nil without creating a second transport.
- Automatic reconnect is enabled by default after the first successful connect.
  `WithReconnect(false, ..., ...)` disables it; `maxAttempts=0` means unlimited.
- Heartbeat is enabled by default. WebSocket uses ping/pong; TCP uses socket
  keepalive and never fabricates Fitz protocol frames.
- Automatic retries are enabled by default for the narrow replay-safe set:
  KV `Get`/`Scan`, Stream `Read`/`ReadPage`/`Peek`/`Metadata`, Lease `Query`,
  and Queue `Enqueue` only after an explicit retryable broker rejection.
- The outbound request queue is bounded. When saturated, operations fail with
  `ErrRequestQueueFull`.

## Handles And Wake Helpers

Stateful handles are bound to the connection that created them. `QueueItem`,
`Lease`, `KVTx`, and `StreamSession` handles from a previous connection fail
fast with `ErrStaleHandle` after reconnect or close.

`NewWakeGate`, `ReserveWhenAvailable`, `ReadWhenCommitted`, and
`WaitForNotifications` support Go-native consumer loops. Subscriptions are wake
signals only for queue and stream helpers; reserve/read calls remain
authoritative.

## Verification

Fast local verification must pass without Docker:

```bash
go test ./...
```

Broker-backed acceptance and conformance are opt-in:

```bash
docker compose up -d
go test -tags=integration ./test ./test/conformance/...
```
