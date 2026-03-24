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
package fitz
