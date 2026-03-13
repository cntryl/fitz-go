# fitz-go

Go client for Fitz.

This release is the breaking cleanup pass for the Go SDK. The supported public
API is the canonical `github.com/cntryl/fitz-go/fitz` package with
token-provider auth, `Connect`/`Close`, `State`, and spec-facing domain verbs.

## Public API

The canonical public package is `github.com/cntryl/fitz-go/fitz`.

```go
package main

import (
	"context"
	"time"

	"github.com/cntryl/fitz-go/fitz"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := fitz.NewClient("ws://localhost:4090/ws", func(context.Context) (string, error) {
		return "your-jwt-token", nil
	}, fitz.WithReconnect(true, 250*time.Millisecond, 5))

	if err := client.Connect(ctx); err != nil {
		panic(err)
	}
	defer client.Close()

	tx, err := client.KV().Begin(ctx, "kv://realm/area/users")
	if err != nil {
		panic(err)
	}
	defer tx.Rollback(ctx)

	if err := tx.Put(ctx, []byte("user-1"), []byte(`{"name":"Alice"}`)); err != nil {
		panic(err)
	}

	value, err := tx.Get(ctx, []byte("user-1"))
	if err != nil {
		panic(err)
	}
	if value.Found {
		println(string(value.Value))
	}

	if err := tx.Commit(ctx); err != nil {
		panic(err)
	}
}
```

## Architecture

- `fitz/`: public client, public domain wrappers, public types
- `internal/core/client`: top-level client implementation
- `internal/core/connection`: CONNECT lifecycle, request correlation, notify dispatch
- `internal/core/transport`: TCP and WebSocket transports
- `internal/protocol`: frame encoding and message type constants
- `internal/domains/*`: spec-aligned domain clients
- `test/`: broker-backed integration coverage

## Broker-backed tests

Integration tests target a running Fitz broker and are part of the default
verification bar for this repo.

Use the local compose stack in [compose.yml](/D:/repos/cntryl/fitz/fitz-go/compose.yml):

```bash
docker compose -f compose.yml up -d
```

That starts:

- `fitz-auth` on `localhost:4091` and `ws://localhost:4090/ws`
- `fitz-anon` on `localhost:4191` and `ws://localhost:4190/ws`

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
go test ./test/...
go test ./...
```

## Protocol source of truth

This repo does not maintain an independent copy of the protocol specification.
Use the canonical server-owned docs referenced from:

- `docs/CLIENT_SPEC.md`
- `docs/CLIENT_ACCEPTANCE_CRITERIA.md`
