# GitHub Copilot / AI Agent Instructions for cntryl-go ✅

## Quick summary
- This repo is a Go SDK for the Fitz/cntryl wire protocol. The canonical protocol and client expectations live in `CLIENT_SPEC.md` — read that first. 📚
- The library surface is organized as thin domain clients under `internal/<domain>`: `kv`, `lease`, `notice`, `queue`, `rpc`, `stream`. Each domain exposes a small `Client` interface (e.g., `internal/kv/kv.go`). 🔧

## Big picture (how things fit together) 💡
- Top-level package `fitz` (`client.go`) exposes a `Client` interface that returns domain clients: `Notice()`, `Stream()`, `Queue()`, `RPC()`, `KV()`, `Lease()`.
- Domain-specific behaviour and wire-level expectations are defined in `CLIENT_SPEC.md` (TLV framing, tags, error codes, domain semantics). Treat `CLIENT_SPEC.md` as authoritative when implementing protocols. 🧭
- The codebase intentionally keeps domain clients as minimal interfaces; implementations (including no-op test scaffolds) live under the same `internal/<domain>` package.

## Local conventions & patterns (essential) 🔍
- **Domain packages under `internal/` are interface-only.** Each domain package should export only the minimal `Client` interface and domain types; do not add implementations or domain tests here. Implementations (and their unit tests) belong in separate packages or a different module.
- Shared domain-level constants and types (e.g., commonly used error values) may remain in `internal/common` where appropriate. Do not add implementation helpers (constructors, no-op clients) to the interface-only packages.

## Tests & build (commands you will use frequently) ▶️
- Run unit + integration tests: `go test ./...` (runs packages under `internal/*` and root tests).
- Run integration package only: `go test ./test -run TestIntegration_DoSomething`.
- Run a single package tests: `go test ./internal/kv -run TestBeginReturnsTx`.
- Standard Go tooling applies: `go build`, `go vet`, `gofmt` / `goimports`.

## Implementation notes for contributors ✍️
- When implementing a domain client (in an implementation package):
  - Implement the `Client` interface in a separate package (e.g., `internal/<domain>/impl` or a sibling package).
  - Provide a constructor (e.g., `New(conn ...)`) in the implementation package and keep any test helpers (e.g., `NewNoop()`) there — **do not** add constructors or no-op helpers to the interface-only package.
  - Preserve test contracts in the implementation package (for no-op helpers assert both `ErrNotImplemented` and non-nil stubs when applicable).
- Respect names from the spec: `realm`, `area`, `resource`, `operation` (these terms are intentionally enforced in `CLIENT_SPEC.md`).
- Wire protocol & TLV details in `CLIENT_SPEC.md` are normative — ensure encoding/decoding aligns with its tags and message types.

## Tests & compatibility expectations ✅
- Acceptance and domain semantics belong in `CLIENT_SPEC.md` (e.g., heartbeats, error codes, append semantics for streams). Update the spec if protocol changes.
- Keep tests readable and domain-focused. Domain/package unit tests should live with their concrete implementations (not under `internal/*` interface packages). The source tree may include integration tests under `test/` that exercise the overall client behaviour.

## Testing conventions (important) ✅
- Tests MUST follow the repo's behavioral naming convention. Prefer the template: `TestShould{Behavior}Given{State}When{Action}`. Simpler, readable variants (e.g., `Test{Behavior}Given{State}`) are allowed only when clarity is preserved.
  - Examples: `TestShouldReturnTxGivenNoopWhenBeginCalled`, `TestShouldOpenConnectionWhenValidTokenProvided`.
- Each test MUST assert a single behavior. If you find yourself asserting independent behaviors in one test, split it into multiple tests.
- Tests MUST include explicit sections using comments: `// Arrange`, `// Act`, `// Assert` (in that order). Keep each section focused and concise.
- Domain interface packages should not assert implementation behavior. When adding tests for a concrete implementation (in an implementation package), follow the testing conventions below and prefer the `NewNoop()` pattern in the implementation if a no-op is useful for test scaffolding.
- Integration tests that intentionally cover multiple behaviours belong under `test/` and must document why they are broader (name and top comment).

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
- `CLIENT_SPEC.md` — protocol and domain behaviours (required reading)
- `client.go` — top-level `Client` surface
- `internal/common/common.go` — shared constants (e.g., `ErrNotImplemented`)
- `internal/*` — each domain's `Client` interface (interface-only packages; implementations live elsewhere)
- `test/integration_test.go` — example integration test

---

If anything is unclear or you want more detail (examples of implementing a domain, wire-level TLV snippets, or test harness templates), tell me which section to expand and I’ll iterate. 🚀
