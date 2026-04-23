# Performance Results (World-Class Validation)

Date: 2026-04-22  
Platform: Windows amd64, 12th Gen Intel(R) Core(TM) i9-12900HK

## Commands

```powershell
go test ./bench -run ^$ -bench "Benchmark(KVTransactionLoopback|NoticePublishHotPath|FrameEncode|RPCCorrelation1KInFlight)" -benchmem -count=3
```

## Release Gates

These measurements are used as the release gate for the hot-path benchmark suite:

- KV transaction loopback stays under 500 us/op.
- Frame encode stays under 500 ns/op.
- RPC correlation lookup at 1K in-flight stays under 2 us/op.
- Notice publish hot path stays above 50,000 ops/sec.

All benchmark runs use `-benchmem` so allocation regressions remain visible. This report intentionally does not claim zero-allocation or zero-copy behavior unless a dedicated proof is added later.

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

## Allocation Tracking

The benchmark command includes `-benchmem`, and the current output records allocation counts for each hot path. The measurements remain throughput-positive, but they do not justify any zero-allocation or zero-copy claim, so this report keeps those claims out of scope.
