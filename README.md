# fitz-go

Go client for Fitz.

## Public API

The canonical public package is `github.com/cntryl/fitz-go/fitz`.

The concrete client implementation lives under `internal/core/client` and is
returned through the `fitz.Client` interface.

## Architecture

- `fitz/`: public client interface
- `internal/core/client`: top-level client implementation
- `internal/core/connection`: CONNECT lifecycle, request correlation, notify dispatch
- `internal/core/transport`: TCP and WebSocket transports
- `internal/protocol`: frame encoding and message type constants
- `internal/domains/*`: spec-aligned domain clients
- `test/`: broker-backed integration coverage

## Broker-backed tests

Integration tests target a running Fitz broker.

- `FITZ_BROKER_TCP_ADDR` defaults to `localhost:4091`
- `FITZ_BROKER_WS_ADDR` defaults to `ws://localhost:4090/ws`

Run the full suite with:

```bash
go test ./...
```

## Protocol source of truth

This repo does not maintain an independent copy of the protocol specification.
Use the canonical server-owned docs referenced from:

- `docs/CLIENT_SPEC.md`
- `docs/CLIENT_ACCEPTANCE_CRITERIA.md`

