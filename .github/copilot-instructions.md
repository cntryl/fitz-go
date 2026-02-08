# GitHub Copilot / AI Agent Instructions for cntryl-go ✅

## Quick summary
- This repo is a Go SDK for the Fitz/cntryl wire protocol. The canonical protocol and client expectations live in `CLIENT_SPEC.md` — read that first. 📚
- The library surface is organized as domain clients under `internal/domains/<domain>`: `kv`, `lease`, `notice`, `queue`, `rpc`, `schedule`, `stream`. Each domain is a flat package containing the `Client` interface, concrete implementation, protocol constants, and unit tests (e.g., `internal/domains/kv/kv.go`). 🔧

## Big picture (how things fit together) 💡
- Top-level package `fitz` (`client.go`) exposes a `Client` interface that returns domain clients: `Notice()`, `Stream()`, `Queue()`, `RPC()`, `KV()`, `Lease()`.
- Domain-specific behaviour and wire-level expectations are defined in `CLIENT_SPEC.md` (TLV framing, tags, error codes, domain semantics). Treat `CLIENT_SPEC.md` as authoritative when implementing protocols. 🧭
- The codebase intentionally keeps domain clients as minimal interfaces; implementations (including no-op test scaffolds) live under the same `internal/<domain>` package.

## Local conventions & patterns (essential) 🔍
- **Each domain is a flat package.** Interface, implementation, protocol constants, and unit tests all live in the same `internal/domains/<domain>` package. Each domain exports a `Client` interface, a `NewClient(mux)` constructor, domain types, and wire constants. The concrete struct is unexported (e.g., `client`).
- **No `impl/` sub-packages.** Do not split interface and implementation into separate packages. This is idiomatic Go for internal packages — accept interfaces, return structs.
- Shared core infrastructure (transport, types, iter) lives under `internal/core/`.

## Tests & build (commands you will use frequently) ▶️
- Run unit + integration tests: `go test ./...` (runs packages under `internal/*` and root tests).
- Run integration package only: `go test ./test -run TestIntegration_DoSomething`.
- Run a single package tests: `go test ./internal/kv -run TestBeginReturnsTx`.
- Standard Go tooling applies: `go build`, `go vet`, `gofmt` / `goimports`.

## Implementation notes for contributors ✍️
- When implementing a domain client:
  - Add the concrete struct (unexported, e.g., `client`) and `NewClient(mux transport.MuxProvider) Client` constructor directly in the domain package.
  - Follow the file layout convention: `<domain>.go` (interface + types), `protocol.go` (wire constants + encoding), `client.go` (implementation), `<domain>_test.go` (unit tests).
  - Unit tests use a `mockMux` test helper within the same package (see `queue/queue_test.go` for the canonical example).
- Respect names from the spec: `realm`, `area`, `resource`, `operation` (these terms are intentionally enforced in `CLIENT_SPEC.md`).
- Wire protocol & TLV details in `CLIENT_SPEC.md` are normative — ensure encoding/decoding aligns with its tags and message types.

## Tests & compatibility expectations ✅
- Acceptance and domain semantics belong in `CLIENT_SPEC.md` (e.g., heartbeats, error codes, append semantics for streams). Update the spec if protocol changes.
- Keep tests readable and domain-focused. Domain unit tests live alongside their implementation in the domain package. Integration tests under `test/` exercise the overall client behaviour against a running broker.

## Testing conventions (important) ✅
- Tests MUST follow the repo's behavioral naming convention. Prefer the template: `TestShould{Behavior}Given{State}When{Action}`. Simpler, readable variants (e.g., `Test{Behavior}Given{State}`) are allowed only when clarity is preserved.
  - Examples: `TestShouldReturnTxGivenNoopWhenBeginCalled`, `TestShouldOpenConnectionWhenValidTokenProvided`.
- Each test MUST assert a single behavior. If you find yourself asserting independent behaviors in one test, split it into multiple tests.
- Tests MUST include explicit sections using comments: `// Arrange`, `// Act`, `// Assert` (in that order). Keep each section focused and concise.
- Domain unit tests use a `mockMux` to test implementation behaviour in isolation (see existing tests in `queue`, `stream`, `lease`, `schedule` for examples).
- Integration tests that intentionally cover multiple behaviours belong under `test/` and must document why they are broader (name and top comment).

### Assertion style (require/assert)
- Use `github.com/stretchr/testify/require` and `github.com/stretchr/testify/assert` consistently.
  - **require** for fatal preconditions (e.g., setup errors, required responses) — use when the test cannot continue.
  - **assert** for non-fatal, behavior-verification checks (e.g., value equality, boolean conditions) so multiple assertions can run in a single test.
- Avoid calling `t.Fatal`/`t.Fatalf` from background goroutines. Instead:
  - Use an `errCh := make(chan error, 1)` and send errors from goroutines back to the main test goroutine, then `require.NoError(t, <-errCh)` in the main test body.
  - Prefer `require` for checking the channel result immediately after the operation.
- Use `require` for preconditions and `assert` for postconditions in a consistent order.

### Reviewer checklist (use in PRs) 🧾
- [ ] Test names follow the naming convention and are descriptive.
- [ ] Each test focuses on a single behavior.
- [ ] Tests include `// Arrange`, `// Act`, `// Assert` sections.
- [ ] No hidden shared state between tests (use t.Cleanup when needed).
- [ ] Implementation package tests assert no-op behaviour where applicable (e.g., `ErrNotImplemented` + non-nil stubs).

### Example (copyable) 🔧
// Example test that belongs in a concrete implementation package (not `internal/*`):
func TestShouldReturnTxGivenNoopWhenBeginCalled(t *testing.T) {
    // Arrange
    // c := NewImplementationNoop() // provided by the implementation package

    // Act
    // tx, err := c.Begin(context.Background(), "route://realm/area/resource")

    // Assert
    // if err == nil {
    //     t.Fatalf("expected ErrNotImplemented, got nil")
    // }
    // if tx == nil {
    //     t.Fatalf("expected non-nil tx even when unimplemented")
    // }
}

- Enforcement: PR reviewers should request changes when tests violate these rules. For accepted deviations (rare), document the reason in the test comment or PR description.

## Places to look first (important files) 🔎
- `docs/CLIENT_SPEC.md` — protocol and domain behaviours (required reading)
- `client.go` — top-level `Client` surface
- `internal/domains/*` — each domain's flat package (interface + impl + protocol + tests)
- `internal/core/transport/` — TLV encoding/decoding, mux, framing
- `test/` — integration tests against a running broker

---

If anything is unclear or you want more detail (examples of implementing a domain, wire-level TLV snippets, or test harness templates), tell me which section to expand and I’ll iterate. 🚀
