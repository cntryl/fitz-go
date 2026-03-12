# GitHub Copilot / AI Agent Instructions for fitz-go

## Quick summary

- This repo is the Go client for Fitz.
- The canonical public package is `fitz/`.
- The server-owned client docs referenced from `docs/CLIENT_SPEC.md` and
  `docs/CLIENT_ACCEPTANCE_CRITERIA.md` are the source of truth for protocol and
  behavior.

## Current architecture

- `fitz/client.go`: public `Client` interface
- `internal/core/client`: concrete top-level client implementation
- `internal/core/connection`: auth lifecycle, correlation, notify/RPC dispatch
- `internal/core/transport`: TCP and WebSocket I/O only
- `internal/protocol`: frame codec and message type constants
- `internal/domains/<domain>`: domain interfaces, implementation, protocol helpers, tests

## Contributor rules

- Do not reintroduce alternate public entrypoints at the repo root.
- Do not add legacy transport mux or channel-based routing layers.
- Keep transport logic in `internal/core/transport` and protocol framing in
  `internal/protocol`.
- Domain implementations should be connection-based and constructed with
  `NewClient(conn *connection.Connection)`.
- Keep tests domain-focused in-package, with integration coverage under `test/`.

## Useful commands

- `go test ./...`
- `go test ./test`
- `go test ./internal/domains/...`

