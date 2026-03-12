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

Use the local compose stack in [compose.yml](/D:/repos/cntryl/fitz/fitz-go/compose.yml):

```bash
docker compose -f compose.yml up -d
```

That starts:

- `fitz-auth` on `localhost:4091` and `ws://localhost:4090/ws`
- `fitz-anon` on `localhost:4191` and `ws://localhost:4190/ws`

Broker-backed tests only run when you point them at a broker explicitly.

Anonymous broker example:

```bash
export FITZ_BROKER_TCP_ADDR=localhost:4191
export FITZ_BROKER_WS_ADDR=ws://localhost:4190/ws
go test ./...
```

Auth-required broker example:

```bash
export FITZ_BROKER_TCP_ADDR=localhost:4091
export FITZ_BROKER_WS_ADDR=ws://localhost:4090/ws
export FITZ_BROKER_AUTH_REQUIRED=true
export FITZ_BROKER_JWT_HMAC_SECRET=test-secret-key
export FITZ_BROKER_JWT_AUDIENCE=fitz
go test ./...
```

Run the full suite with:

```bash
go test ./...
```

## Protocol source of truth

This repo does not maintain an independent copy of the protocol specification.
Use the canonical server-owned docs referenced from:

- `docs/CLIENT_SPEC.md`
- `docs/CLIENT_ACCEPTANCE_CRITERIA.md`
