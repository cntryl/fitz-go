# Fitz Go World-Class TODO

You are working in fitz-go. This repository is already production-grade, but a few audit items still prevent it from being fully world-class. Close only the remaining gaps and do not churn working protocol paths.

## Canonical Sources

- [../fitz/docs/clients/client-requirements.md](../fitz/docs/clients/client-requirements.md)
- [../fitz/docs/clients/client-acceptance-criteria.md](../fitz/docs/clients/client-acceptance-criteria.md)
- [../fitz/docs/clients/client-spec.md](../fitz/docs/clients/client-spec.md)
- [docs/GRADING.md](docs/GRADING.md)
- [docs/PERF_RESULTS.md](docs/PERF_RESULTS.md)
- [README.md](README.md)
- [test/conformance/conformance_test.go](test/conformance/conformance_test.go)
- [benchmarks/baseline.txt](benchmarks/baseline.txt)

## What Is Still Missing

- The grading report still has partials around log-level policy verification and systematic error-path coverage.
- Benchmark evidence exists, but the world-class threshold language is not fully codified.
- The repo docs and audit notes still need one final sync pass so the status story is consistent everywhere.
- The additional coverage added for Queue, Lease, Notice, and Schedule must stay documented and visible in the verification story.

## Work In Order

1. Tighten observability and error reporting.
   - Verify and document the per-error and connection-event log-level policy against the Fitz contract.
   - Make any remaining logging policy gaps testable, not just descriptive.
2. Formalize performance targets.
   - Turn benchmark results into named release thresholds.
   - Re-run and refresh `benchmarks/baseline.txt` only when a deliberate performance change lands.
   - Keep the hot-path benchmark story reproducible.
3. Expand systematic error-path coverage.
   - Fill the remaining integration gaps for unauthorized operations, invalid cron, inverted scan ranges, and any other contract failures that are still only partially exercised.
   - Keep the extra scenario coverage for Queue, Lease, Notice, and Schedule intact.
4. Keep conformance and docs synchronized.
   - Ensure `test/conformance` remains aligned with the shared cross-language suite.
   - Update `docs/GRADING.md`, `docs/PERF_RESULTS.md`, `CHANGELOG.md`, and the README whenever behavior or evidence changes.

## Concrete Gap Checklist

- `docs/GRADING.md`: resolve the remaining partials for `REQ-ERR-004`, `REQ-ERR-009`, `REQ-OBS-002`, `REQ-PERF-001`, `REQ-PERF-002`, and `REQ-TEST-008`.
- `docs/PERF_RESULTS.md`: make the benchmark thresholds and evidence policy explicit enough to support a world-class release gate.
- `test/conformance/conformance_test.go`: keep the shared suite aligned with the canonical runner contract and the added Queue/Lease/Notice/Schedule coverage.
- `README.md`: keep the verification commands and public behavior description in sync with the code and the docs.

## Definition Of Done

- Every remaining partial in `docs/GRADING.md` is either resolved or explicitly justified as non-blocking.
- Benchmark thresholds are named, reproducible, and documented.
- CI, conformance, and docs all agree on the current release bar.

## Constraints

- Do not redesign the public API just to chase documentation cleanliness.
- Keep protocol semantics faithful to the Fitz contract.
- Prefer additive changes and targeted tests over broad rewrites.
- Keep the existing conformance additions intact while closing the remaining audit gaps.