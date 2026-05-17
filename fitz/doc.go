// Package fitz provides a Go client for the Fitz messaging broker.
//
// Fitz is a single-broker messaging platform exposing seven domain subsystems
// over a binary TLV protocol on WebSocket (port 4090) and TCP (port 4091).
//
// # Usage
//
// Construct a connected client with [Dial], or build one step-by-step with
// [NewClient] followed by [Client.Connect]:
//
//	client, err := fitz.Dial(ctx, "ws://localhost:4090/ws",
//	    func(ctx context.Context) (string, error) {
//	        return myTokenSource.Token(ctx)
//	    },
//	    fitz.WithReconnect(true, 250*time.Millisecond, 10),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
// # Domain clients
//
// Access each subsystem through the dedicated accessor on [Client]:
//
//   - [Client.KV]       — key-value transactions with MVCC isolation
//   - [Client.Queue]    — durable queues with at-least-once delivery
//   - [Client.Notice]   — fan-out pub/sub notifications
//   - [Client.RPC]      — bidirectional streaming RPC
//   - [Client.Lease]    — distributed leader-election leases
//   - [Client.Stream]   — ordered append-only event streams
//   - [Client.Schedule] — cron-based scheduling
//
// # Error handling
//
// Server errors are returned as [*DomainError], which carries a numeric [ErrCode*]
// constant and a human-readable message. Use [IsRetryable] to test whether a
// transient error (backpressure, contention, transient routing failure) is
// worth retrying.
//
//	var de *fitz.DomainError
//	if errors.As(err, &de) && de.Code == fitz.ErrCodeKvKeyNotFound {
//	    // key does not exist
//	}
//	if fitz.IsRetryable(err) {
//	    // back off and retry
//	}
//
// # Iterators
//
// Streaming domain operations (RPC calls, stream subscriptions) return an
// [Iterator]. Always call [Iterator.Close] when done — especially on early
// exit — to release broker-side resources promptly.
//
//	iter, err := client.RPC().Call(ctx, "my.route", payload)
//	if err != nil { ... }
//	defer iter.Close()
//	for iter.Next() {
//	    frame := iter.Value()
//	    _ = frame.Body
//	}
//	if err := iter.Err(); err != nil { ... }
//
// # Lifecycle and cancellation
//
// [Client.Connect] and every domain operation accept a [context.Context]. In
// the Go SDK, context is the single control plane for deadlines and
// cancellation:
//
//   - canceling Connect aborts the full dial + auth attempt
//   - canceling a unary domain call aborts the in-flight request
//   - canceling a streaming RPC call stops the returned iterator; observe the
//     terminal cause via [Iterator.Err]
//
// [Client.State] exposes the connection lifecycle:
//
//   - [ConnectionStateDisconnected] before Connect and after unrecoverable loss
//   - [ConnectionStateConnecting] while establishing the transport
//   - [ConnectionStateConnected] after the transport is up, before auth settles
//   - [ConnectionStateAuthenticating] while CONNECT / auth is in progress
//   - [ConnectionStateAuthenticated] once the client is ready for domain traffic
//   - [ConnectionStateReconnecting] while automatic reconnect is actively retrying
//   - [ConnectionStateClosed] after [Client.Close]
//
// When reconnect is enabled, domain subscriptions and RPC worker registrations
// are restored automatically after the transport is re-established. [Client.Close]
// is idempotent and permanently transitions the client to Closed; closed clients
// are not reusable.
package fitz
