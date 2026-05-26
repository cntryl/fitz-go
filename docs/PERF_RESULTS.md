# Performance Results (World-Class Validation)

Date: 2026-05-24  
Platform: Windows amd64, Intel(R) Xeon(R) E-2224G CPU @ 3.50GHz

## Commands

```powershell
go test ./bench ./internal/domains/rpc ./internal/core/subscriptions -run ^$ -bench "Benchmark(ConnectionSendRequest|HandleRPCResponseHotPath|QueueReserveHotPath|QueueCompleteHotPath|StreamBeginHotPath|StreamAppendHotPath|ScheduleCreateHotPath|ScheduleCancelHotPath|SubscriptionRegistryRestore|KVTransactionLoopback|NoticePublishHotPath|FrameEncode|RPCCorrelation1KInFlight)" -benchmem -count=3
```

The loopback benchmarks in `bench/hotpath_bench_test.go` now set `ReadTimeout` and `WriteTimeout` to `-1` so they measure the request path itself rather than constructor-default timeout timers.

## Release Gates

These measurements define the release gate for the hot-path benchmark suite:

- KV transaction loopback stays under 500 us/op.
- Frame encode stays under 500 ns/op.
- RPC correlation lookup at 1K in-flight stays under 2 us/op.
- Notice publish hot path stays above 50,000 ops/sec.

All benchmark runs use `-benchmem` so allocation regressions remain visible. This report is the source of truth for the release bar. It intentionally does not claim zero-allocation or zero-copy steady-state parsing. If a future change wants that stronger claim, it needs a dedicated proof that is measured separately from these gates.

## Results Summary

- `BenchmarkKVTransactionLoopback`: ~6.8-7.3 us/op, 12 B/op, 3 allocs/op
  - Target (REQ-PERF-004): < 500 us p99 loopback
  - Result: PASS by large margin on this benchmark environment

- `BenchmarkFrameEncode`: ~43.8-47.4 ns/op, 0 B/op, 0 allocs/op
  - Target (REQ-PERF-005): < 500 ns/frame
  - Result: PASS by large margin

- `BenchmarkRPCCorrelation1KInFlight`: ~389.3-400.8 ns/op with 1024 in-flight registrations, 0 B/op, 0 allocs/op
  - Target (REQ-PERF-006): < 2 us lookup with 1,000+ in-flight
  - Result: PASS by large margin

- `BenchmarkNoticePublishHotPath`: ~845-882 ns/op, 8 B/op, 1 allocs/op
  - Throughput equivalent: ~1.13M to ~1.18M ops/sec
  - Target (REQ-PERF-007): > 50,000 ops/sec single goroutine loopback
  - Result: PASS by large margin

## Broader Hotpaths

These measurements are exploratory evidence for the public client surface and reconnect-adjacent replay paths. They are not release gates, but they give us numbers for the remaining high-traffic areas in the SDK.

- `BenchmarkHandleRPCResponseHotPath`: ~73.8-91.8 ns/op, 0 B/op, 0 allocs/op
  - RPC response dispatch now queues internally before iterator delivery, and the steady-state single-frame response path is zero-allocation in this benchmark.

- `BenchmarkConnectionSendRequest`: ~1.93-1.97 us/op, 4 B/op, 1 alloc/op
  - This is the shared request/response root under the queue, stream, and schedule client hot paths.
  - The last remaining allocation in this loopback benchmark is the synthetic echo transport's response `EncodeFrame`, not the client-side request waiter path.

- `BenchmarkQueueReserveHotPath`: ~1.94-2.00 us/op, 4 B/op, 1 alloc/op
- `BenchmarkQueueCompleteHotPath`: ~2.01-2.13 us/op, 4 B/op, 1 alloc/op
- `BenchmarkStreamBeginHotPath`: ~2.06-2.16 us/op, 8 B/op, 1 alloc/op
- `BenchmarkStreamAppendHotPath`: ~2.04-2.08 us/op, 8 B/op, 1 alloc/op
- `BenchmarkScheduleCreateHotPath`: ~2.02-2.10 us/op, 8 B/op, 1 alloc/op
- `BenchmarkScheduleCancelHotPath`: ~2.05-2.22 us/op, 8 B/op, 1 alloc/op
- `BenchmarkSubscriptionRegistryRestore`: ~1.39-1.48 us/op, 0 B/op, 0 allocs/op
  - `BenchmarkRestoreHighFanout`: ~507-597 ns/op, 0 B/op, 0 allocs/op
  - This reconnect/resubscribe replay path now reuses restore scratch state rather than allocating fresh slices and maps on each restore.

## Comparing Changes

Capture two local benchmark runs and compare them with `benchstat` when you need a regression diff. The repo does not maintain a committed benchmark baseline file; the numeric gates above are the authoritative release bar.

Install `benchstat` once if needed with `go install golang.org/x/perf/cmd/benchstat@latest`.
