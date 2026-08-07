# Changelog

All notable changes to this project are documented in this file.

## Unreleased

### Changed

- Breaking: Stream READ and SUBSCRIBE now accept only concrete resource, area (`realm/area/*`), realm (`realm/*/*`), or global (`stream://**`) selectors. Global continuation reuses the returned fingerprint and watermark pair, and Stream LAST is concrete-route only.
- Breaking: Schedule listing uses canonical message 702 with offset/limit pages and `TotalCount`.
- Breaking: Lease acquisition requires explicit owner and wait options; RPC worker registration exposes `maxConcurrent`.
- Queue reserve, global Stream records, optional zero values, read-only commit, and backend error classification now follow the canonical wire contracts.
- Breaking: `RPCWorkerRegistration.Deregister` now returns broker and transport errors instead of silently treating deregistration as best-effort.
- KV commit attempts are terminal even when rejected or interrupted; subsequent rollback is an idempotent no-op.
- Breaking: Queue reserve, Stream read, and Stream last wire items now require their concrete matched route. `QueueItem`, `StreamReadItem`, and `StreamRecord` expose it.
- Breaking: domain errors require the typed code-and-message envelope; Queue's one-byte and string-only error decoders were removed.
- Added `Client.ConnectWhenReady(ctx)` for context-bounded startup retry.
- Breaking: `ScheduleDeliveryMode` is now a distinct public type. Convert internal or numeric values explicitly when crossing package boundaries.
- Exported the existing Notice, Stream, and RPC sentinel errors for `errors.Is`.
- Made overlapping local RPC worker selection deterministic: exact routes win,
  followed by wildcard specificity and a lexical final tie-breaker.
- Unified Queue, Stream, and Schedule iterator cancellation, wake, polling, and
  cleanup internals without changing exported signatures or polling semantics.
- Split managed Lease execution into acquisition, renewal supervision, and
  release/error composition while preserving context causes and panic behavior.
- Moved the broker-backed acceptance suite behind the `integration` build tag and split the release gate so the default `go test ./...` path stays fast while the broker matrix remains opt-in.

### Removed

- Removed the unused `ConnectOrSkip` fixture alias.
- Deleted the obsolete bug-tracking placeholder in `bugs/README.md`.
- Removed placeholder example stubs from `fitz/` that did not add value beyond package godoc.

## v1.0.0 - 2026-03-24

### Breaking Changes

- Renamed `RPCWorkerRegistration.Unsubscribe()` to `RPCWorkerRegistration.Deregister()` to better reflect worker lifecycle semantics.

### Added

- Public error API enhancements:
  - `fitz.IsRetryable(err error) bool` for retry classification.
  - Exported numeric error code constants (`fitz.ErrCode*`) for all domains.
  - `fitz.TransportError` typed transport-layer error.
- Package-level godoc in `fitz/doc.go`.
- Benchmark suite in `bench/hotpath_bench_test.go`:
  - `BenchmarkKVRoundTrip`
  - `BenchmarkFrameEncode`
  - `BenchmarkRPCCorrelation`
  - `BenchmarkNoticeDispatch`
  - `BenchmarkConnectionSendRequest`
- Example documentation in:
  - `fitz/example_kv_test.go`
  - `fitz/example_rpc_test.go`
  - `fitz/example_errors_test.go`
- Error-path integration coverage in `test/error_test.go`.
- `goleak` wiring in `test/main_test.go` to catch leaked goroutines.

### Changed

- Reconnect logic now uses exponential backoff with configurable max delay (`WithReconnectMaxDelay`).
- Added missing OpenTelemetry metrics:
  - `fitz.request.duration`
  - `fitz.request.errors`
  - `fitz.connection.state`
  - `fitz.subscriptions.active`
- Tracer and meter defaults now use direct no-op providers when none are configured.
- WebSocket text frames are rejected with an explicit error (protocol compliance).
- RPC iterator cancellation race fixed for deterministic context cancellation behavior.
- Transport dial/handshake failures are wrapped as typed transport errors.

### Test and Stability

- Conformance suite status updated to all passing scenarios (P0/P1 pass conditions met).
- Added canonical error-code registry assertion tests.
- Strengthened cancellation and schedule integration test stability under race runs.
