# Performance Results (World-Class Validation)

Date: 2026-04-22  
Platform: Windows amd64, 12th Gen Intel(R) Core(TM) i9-12900HK

## Commands

```powershell
go test ./bench ./internal/domains/rpc -run ^$ -bench "Benchmark(HandleRPCResponseHotPath|QueueReserveHotPath|QueueCompleteHotPath|StreamBeginHotPath|StreamAppendHotPath|ScheduleCreateHotPath|ScheduleCancelHotPath|SubscriptionRegistryRestore|KVTransactionLoopback|NoticePublishHotPath|FrameEncode|RPCCorrelation1KInFlight)" -benchmem -count=3
```

## Release Gates

These measurements define the release gate for the hot-path benchmark suite:

- KV transaction loopback stays under 500 us/op.
- Frame encode stays under 500 ns/op.
- RPC correlation lookup at 1K in-flight stays under 2 us/op.
- Notice publish hot path stays above 50,000 ops/sec.

All benchmark runs use `-benchmem` so allocation regressions remain visible. This report is the source of truth for the release bar. It intentionally does not claim zero-allocation or zero-copy steady-state parsing. If a future change wants that stronger claim, it needs a dedicated proof that is measured separately from these gates.

## Results Summary

- `BenchmarkKVTransactionLoopback`: ~11.5-15.5 us/op, 4914 B/op, 87 allocs/op
  - Target (REQ-PERF-004): < 500 us p99 loopback
  - Result: PASS by large margin on this benchmark environment

- `BenchmarkFrameEncode`: ~34.6-38.4 ns/op, 8 B/op, 1 allocs/op
  - Target (REQ-PERF-005): < 500 ns/frame
  - Result: PASS by large margin

- `BenchmarkRPCCorrelation1KInFlight`: ~321.8-349.1 ns/op with 1024 in-flight registrations, 344 B/op, 5 allocs/op
  - Target (REQ-PERF-006): < 2 us lookup with 1,000+ in-flight
  - Result: PASS by large margin

- `BenchmarkNoticePublishHotPath`: ~1163-1820 ns/op, 945 B/op, 17 allocs/op
  - Throughput equivalent: ~549k to ~860k ops/sec
  - Target (REQ-PERF-007): > 50,000 ops/sec single goroutine loopback
  - Result: PASS by large margin

## Broader Hotpaths

These measurements are exploratory evidence for the public client surface and reconnect-adjacent replay paths. They are not release gates, but they give us numbers for the remaining high-traffic areas in the SDK.

- `BenchmarkHandleRPCResponseHotPath`: ~1.1-1.2 us/op, 208 B/op, 6 allocs/op
  - RPC response dispatch now queues internally before iterator delivery, so a stalled consumer does not block the connection loop.

- `BenchmarkQueueReserveHotPath`: ~4.8 us/op, 1404 B/op, 27 allocs/op
- `BenchmarkQueueCompleteHotPath`: ~4.8 us/op, 1404 B/op, 27 allocs/op
- `BenchmarkStreamBeginHotPath`: ~17.0 us/op, 1408 B/op, 27 allocs/op
- `BenchmarkStreamAppendHotPath`: ~13.4 us/op, 1408 B/op, 27 allocs/op
- `BenchmarkScheduleCreateHotPath`: ~18.3 us/op, 1408 B/op, 27 allocs/op
- `BenchmarkScheduleCancelHotPath`: ~5.2 us/op, 1408 B/op, 27 allocs/op
- `BenchmarkSubscriptionRegistryRestore`: ~7.2 us/op, 7448 B/op, 60 allocs/op
  - This is the reconnect/resubscribe replay proxy for subscription-heavy clients.

## Comparing Changes

Capture two local benchmark runs and compare them with `benchstat` when you need a regression diff. The repo does not maintain a committed benchmark baseline file; the numeric gates above are the authoritative release bar.

Install `benchstat` once if needed with `go install golang.org/x/perf/cmd/benchstat@latest`.
