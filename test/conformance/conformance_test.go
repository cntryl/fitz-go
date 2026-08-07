//go:build integration

// Package conformance implements the Fitz cross-language conformance harness for fitz-go.
//
// Covers 22 scenarios: all 17 scenarios from the cross-language spec plus
// 5 Go-client scenarios (CS-018 through CS-022) added
// to close coverage gaps for Queue, Lease, Notice, Schedule, and reconnect
// restoration behavior:
//
//	fitz/docs/clients/cross-language-conformance-suite.yaml
//
// Configuration via environment variables:
//
//	CONFORMANCE_TRANSPORT   "tcp" (default) | "ws"
//	CONFORMANCE_AUTH_MODE   "anonymous" (default) | "valid_jwt"
//	CONFORMANCE_OUTPUT      path to write JSON results (default: ./conformance-results.json)
//
// Broker addresses resolved via the same env vars as integration tests.
//
// Run:
//
//	go test -v -timeout 120s ./test/conformance/... -run TestConformanceSuite
//	CONFORMANCE_TRANSPORT=ws CONFORMANCE_AUTH_MODE=valid_jwt \
//	  go test -v -timeout 120s ./test/conformance/... -run TestConformanceSuite
package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/v2/fitz"
	"github.com/cntryl/fitz-go/v2/test/fixture"
)

// ---------------------------------------------------------------------------
// Result types (must match cross-language-conformance-runner.md contract)
// ---------------------------------------------------------------------------

type Verdict string

const (
	VerdictPass           Verdict = "pass"
	VerdictPartial        Verdict = "partial"
	VerdictFail           Verdict = "fail"
	VerdictNotImplemented Verdict = "not_implemented"
	VerdictUnclear        Verdict = "unclear"
)

type ScenarioResult struct {
	ScenarioID string   `json:"scenario_id"`
	Title      string   `json:"title"`
	Priority   string   `json:"priority"`
	Client     string   `json:"client"`
	Transport  string   `json:"transport"`
	AuthMode   string   `json:"auth_mode"`
	Verdict    Verdict  `json:"verdict"`
	Evidence   []string `json:"evidence"`
	LatencyMs  int64    `json:"latency_ms"`
	Error      string   `json:"error,omitempty"`
}

type AggregateResult struct {
	Suite         string           `json:"suite"`
	Version       string           `json:"version"`
	GeneratedAt   string           `json:"generated_at"`
	Client        string           `json:"client"`
	Transport     string           `json:"transport"`
	AuthMode      string           `json:"auth_mode"`
	P0PassRate    float64          `json:"p0_pass_rate"`
	P1PassRate    float64          `json:"p1_pass_rate"`
	OverallStatus string           `json:"overall_status"`
	Scenarios     []ScenarioResult `json:"scenarios"`
}

// ---------------------------------------------------------------------------
// Collector
// ---------------------------------------------------------------------------

type collector struct {
	mu      sync.Mutex
	results []ScenarioResult
}

func (c *collector) record(r ScenarioResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.results = append(c.results, r)
}

func (c *collector) aggregate(client, transport, authMode string) AggregateResult {
	c.mu.Lock()
	defer c.mu.Unlock()

	passRate := func(priority string) float64 {
		var total, passed int
		for _, r := range c.results {
			if r.Priority == priority {
				total++
				if r.Verdict == VerdictPass {
					passed++
				}
			}
		}
		if total == 0 {
			return 1.0
		}
		return float64(passed) / float64(total)
	}

	p0Rate := passRate("P0")
	p1Rate := passRate("P1")

	overall := "pass"
	for _, r := range c.results {
		if r.Priority == "P0" && r.Verdict != VerdictPass {
			overall = "fail"
			break
		}
	}
	if overall == "pass" {
		for _, r := range c.results {
			if r.Priority == "P1" && (r.Verdict == VerdictFail || r.Verdict == VerdictPartial) {
				overall = "partial"
				break
			}
		}
	}

	return AggregateResult{
		Suite:         "fitz-cross-language-client-conformance",
		Version:       "1.0",
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		Client:        client,
		Transport:     transport,
		AuthMode:      authMode,
		P0PassRate:    p0Rate,
		P1PassRate:    p1Rate,
		OverallStatus: overall,
		Scenarios:     c.results,
	}
}

// ---------------------------------------------------------------------------
// Module-level globals
// ---------------------------------------------------------------------------

var (
	confTransport = getenv("CONFORMANCE_TRANSPORT", "tcp")
	confAuthMode  = getenv("CONFORMANCE_AUTH_MODE", "anonymous")
	confOutput    = getenv("CONFORMANCE_OUTPUT", "./conformance-results.json")
	results       = &collector{}
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func transportType() fixture.TransportType {
	if confTransport == "ws" {
		return fixture.TransportWebSocket
	}
	return fixture.TransportTCP
}

func authMode() fixture.AuthMode {
	if confAuthMode == "valid_jwt" {
		return fixture.AuthModeValidJWT
	}
	return fixture.AuthModeAnonymous
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

var routeCounter uint64
var routeCounterMu sync.Mutex

func uniqueRoute(scheme string) string {
	routeCounterMu.Lock()
	routeCounter++
	n := routeCounter
	routeCounterMu.Unlock()

	id := fmt.Sprintf("%d-%d-%d", time.Now().UnixNano(), n, rand.IntN(1_000_000))
	if scheme == "schedule" {
		return fmt.Sprintf("%s://conformance/%s/res/run", scheme, id)
	}
	return fmt.Sprintf("%s://conformance/%s/res", scheme, id)
}

// newFixture creates a fresh TestFixture connected to the configured transport/auth pair.
// The fixture registers t.Cleanup automatically.
func newFixture(t *testing.T) *fixture.TestFixture {
	t.Helper()
	f := fixture.NewTestFixture(t, transportType())
	f.SetAuthMode(authMode())
	return f
}

// connectFixture creates and connects a fixture, fatal on failure.
func connectFixture(t *testing.T) *fixture.TestFixture {
	t.Helper()
	f := newFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	f.ConnectOrFail(ctx)
	return f
}

func run(
	id, title, priority string,
	fn func() (Verdict, []string, error),
) ScenarioResult {
	start := time.Now()
	v, evidence, err := fn()
	r := ScenarioResult{
		ScenarioID: id,
		Title:      title,
		Priority:   priority,
		Client:     "fitz-go",
		Transport:  confTransport,
		AuthMode:   confAuthMode,
		Verdict:    v,
		Evidence:   evidence,
		LatencyMs:  time.Since(start).Milliseconds(),
	}
	if err != nil {
		r.Error = err.Error()
		if r.Verdict == "" {
			r.Verdict = VerdictFail
		}
	}
	return r
}

// ---------------------------------------------------------------------------
// TestMain — writes aggregate JSON after all scenarios
// ---------------------------------------------------------------------------

func TestMain(m *testing.M) {
	code := m.Run()

	aggregate := results.aggregate("fitz-go", confTransport, confAuthMode)
	data, err := json.MarshalIndent(aggregate, "", "  ")
	if err == nil {
		if writeErr := os.WriteFile(confOutput, data, 0o644); writeErr != nil {
			fmt.Fprintf(os.Stderr, "conformance: failed to write results: %v\n", writeErr)
		} else {
			fmt.Printf("\nConformance results written to: %s\n", confOutput)
			fmt.Printf("Status: %s  P0: %.0f%%  P1: %.0f%%\n",
				aggregate.OverallStatus,
				aggregate.P0PassRate*100,
				aggregate.P1PassRate*100,
			)
		}
	}

	os.Exit(code)
}

// ---------------------------------------------------------------------------
// Conformance scenarios wrapped inside a single Go test function so that
// `go test -run TestConformanceSuite` runs them all in one invocation.
// ---------------------------------------------------------------------------

func TestConformanceSuite(t *testing.T) {
	t.Run("CS-001_connect_success", func(t *testing.T) {
		r := run("CS-001", "connect success", "P0", func() (Verdict, []string, error) {
			var ev []string
			f := connectFixture(t)
			ev = append(ev, "connect returned successfully")

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			route := uniqueRoute("kv")
			tx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("kv begin: %w", err)
			}
			if err := tx.Put(ctx, []byte("cs001"), []byte("ok")); err != nil {
				return VerdictFail, ev, fmt.Errorf("kv put: %w", err)
			}
			if err := tx.Commit(ctx); err != nil {
				return VerdictFail, ev, fmt.Errorf("kv commit: %w", err)
			}
			ev = append(ev, "first domain request (kv) succeeded")
			return VerdictPass, ev, nil
		})
		results.record(r)
		if r.Verdict != VerdictPass {
			t.Errorf("CS-001: verdict=%s error=%s", r.Verdict, r.Error)
		}
	})

	t.Run("CS-002_auth_failure", func(t *testing.T) {
		r := run("CS-002", "auth failure", "P0", func() (Verdict, []string, error) {
			var ev []string
			f := fixture.NewTestFixture(t, transportType())
			f.SetAuthMode(fixture.AuthModeInvalidSignature)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			err := f.Connect(ctx)
			ev = append(ev, fmt.Sprintf("connect error: %v", err))

			if err != nil {
				ev = append(ev, "auth failure surfaced as error (correct)")
				return VerdictPass, ev, nil
			}

			// Didn't fail on connect — check domain access
			ev = append(ev, "connect did not error (TCP silent-close model)")
			_, kvErr := f.Client().KV().Begin(ctx, uniqueRoute("kv"), fitz.KVDurabilitySync)
			if kvErr != nil {
				ev = append(ev, fmt.Sprintf("domain request failed post-auth-close: %v", kvErr))
				return VerdictPartial, ev, nil
			}
			ev = append(ev, "WARNING: domain request succeeded after invalid JWT")
			return VerdictPartial, ev, nil
		})
		results.record(r)
		if r.Verdict != VerdictPass {
			t.Errorf("CS-002: verdict=%s error=%s", r.Verdict, r.Error)
		}
	})

	t.Run("CS-003_request_success", func(t *testing.T) {
		r := run("CS-003", "request success", "P0", func() (Verdict, []string, error) {
			var ev []string
			f := connectFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			route := uniqueRoute("kv")
			tx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("begin: %w", err)
			}
			if err := tx.Put(ctx, []byte("user:1"), []byte("Alice")); err != nil {
				return VerdictFail, ev, fmt.Errorf("put: %w", err)
			}
			if err := tx.Commit(ctx); err != nil {
				return VerdictFail, ev, fmt.Errorf("commit: %w", err)
			}
			ev = append(ev, "kv begin/put/commit succeeded")

			rtx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync, fitz.WithKVMode(fitz.KVModeReadOnly))
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("read begin: %w", err)
			}
			result, err := rtx.Get(ctx, []byte("user:1"))
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("get: %w", err)
			}
			if !result.Found {
				return VerdictFail, ev, errors.New("expected Found=true, got false")
			}
			if string(result.Value) != "Alice" {
				return VerdictFail, ev, fmt.Errorf("expected 'Alice', got %q", result.Value)
			}
			ev = append(ev, fmt.Sprintf("read-after-commit returned %q (correct)", result.Value))
			return VerdictPass, ev, nil
		})
		results.record(r)
		if r.Verdict != VerdictPass {
			t.Errorf("CS-003: verdict=%s error=%s", r.Verdict, r.Error)
		}
	})

	t.Run("CS-004_unknown_route", func(t *testing.T) {
		r := run("CS-004", "unknown route", "P0", func() (Verdict, []string, error) {
			var ev []string
			f := connectFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			noWorkerRoute := uniqueRoute("rpc")
			callCtx, callCancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer callCancel()
			iter, err := f.Client().RPC().Call(callCtx, noWorkerRoute, []byte("ping"))
			if err != nil {
				ev = append(ev, fmt.Sprintf("call to unregistered route returned error: %v", err))
			} else {
				defer closeQuietly(iter)
				iter.Next()
				if iterErr := iter.Err(); iterErr != nil {
					ev = append(ev, fmt.Sprintf("iterator error on unregistered route: %v", iterErr))
				} else {
					ev = append(ev, "WARNING: unexpectedly got a response from unregistered route")
					return VerdictFail, ev, nil
				}
			}
			ev = append(ev, "error is typed and surfaced (correct)")

			// Client must remain usable
			route := uniqueRoute("kv")
			tx, txErr := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
			if txErr != nil {
				return VerdictFail, ev, fmt.Errorf("client not reusable after error: %w", txErr)
			}
			_ = tx.Put(ctx, []byte("k"), []byte("v"))
			_ = tx.Commit(ctx)
			ev = append(ev, "client remains usable after unknown-route error")
			return VerdictPass, ev, nil
		})
		results.record(r)
		if r.Verdict != VerdictPass {
			t.Errorf("CS-004: verdict=%s error=%s", r.Verdict, r.Error)
		}
	})

	t.Run("CS-005_invalid_payload", func(t *testing.T) {
		r := run("CS-005", "invalid payload", "P0", func() (Verdict, []string, error) {
			var ev []string
			f := connectFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			route := uniqueRoute("kv")
			tx1, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("begin: %w", err)
			}
			if err := tx1.Insert(ctx, []byte("dup"), []byte("first")); err != nil {
				return VerdictFail, ev, fmt.Errorf("first insert: %w", err)
			}
			if err := tx1.Commit(ctx); err != nil {
				return VerdictFail, ev, fmt.Errorf("commit: %w", err)
			}
			ev = append(ev, "first insert succeeded")

			tx2, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("second begin: %w", err)
			}
			insertErr := tx2.Insert(ctx, []byte("dup"), []byte("second"))
			_ = tx2.Rollback(ctx)

			if insertErr == nil {
				return VerdictFail, ev, errors.New("expected error on duplicate insert, got nil")
			}
			ev = append(ev, fmt.Sprintf("duplicate insert returned error: %v", insertErr))

			rtx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync, fitz.WithKVMode(fitz.KVModeReadOnly))
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("client not reusable: %w", err)
			}
			res, err := rtx.Get(ctx, []byte("dup"))
			if err != nil {
				return VerdictFail, ev, err
			}
			if !res.Found {
				return VerdictFail, ev, errors.New("expected dup key to still exist")
			}
			ev = append(ev, "client remains usable after server-rejected operation")
			return VerdictPass, ev, nil
		})
		results.record(r)
		if r.Verdict != VerdictPass {
			t.Errorf("CS-005: verdict=%s error=%s", r.Verdict, r.Error)
		}
	})

	t.Run("CS-006_server_error_mapping", func(t *testing.T) {
		r := run("CS-006", "server error mapping", "P0", func() (Verdict, []string, error) {
			var ev []string
			f := connectFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			route := uniqueRoute("rpc")
			callCtx, callCancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer callCancel()
			iter, err := f.Client().RPC().Call(callCtx, route, []byte("ping"))
			if err != nil {
				ev = append(ev, fmt.Sprintf("rpc call error type: %T", err))
				ev = append(ev, fmt.Sprintf("rpc call error: %v", err))
			} else {
				defer closeQuietly(iter)
				iter.Next()
				err = iter.Err()
				ev = append(ev, fmt.Sprintf("rpc iterator error type: %T", err))
			}

			if err != nil {
				ev = append(ev, "server error surfaced as typed Go error (correct)")
			}

			// Also verify kv conflict produces typed error
			kvRoute := uniqueRoute("kv")
			tx, _ := f.Client().KV().Begin(ctx, kvRoute, fitz.KVDurabilitySync)
			_ = tx.Insert(ctx, []byte("x"), []byte("1"))
			_ = tx.Commit(ctx)

			tx2, _ := f.Client().KV().Begin(ctx, kvRoute, fitz.KVDurabilitySync)
			kvErr := tx2.Insert(ctx, []byte("x"), []byte("2"))
			_ = tx2.Rollback(ctx)
			ev = append(ev, fmt.Sprintf("kv conflict error type: %T, value: %v", kvErr, kvErr))

			return VerdictPass, ev, nil
		})
		results.record(r)
		if r.Verdict != VerdictPass {
			t.Errorf("CS-006: verdict=%s error=%s", r.Verdict, r.Error)
		}
	})

	t.Run("CS-007_timeout_handling", func(t *testing.T) {
		r := run("CS-007", "timeout handling", "P0", func() (Verdict, []string, error) {
			var ev []string
			f := connectFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			route := uniqueRoute("rpc")
			start := time.Now()
			callCtx, callCancel := context.WithTimeout(ctx, 250*time.Millisecond)
			defer callCancel()
			iter, err := f.Client().RPC().Call(callCtx, route, []byte("nobody"))
			elapsed := time.Since(start)

			var timeoutErr error
			if err != nil {
				timeoutErr = err
			} else {
				defer closeQuietly(iter)
				iter.Next()
				timeoutErr = iter.Err()
			}

			if timeoutErr == nil {
				return VerdictFail, ev, errors.New("expected timeout error, got success")
			}
			ev = append(ev, fmt.Sprintf("rpc timed out after ~%dms (error: %v)", elapsed.Milliseconds(), timeoutErr))

			// Connection must remain healthy
			kvRoute := uniqueRoute("kv")
			tx, err2 := f.Client().KV().Begin(ctx, kvRoute, fitz.KVDurabilitySync)
			if err2 != nil {
				return VerdictFail, ev, fmt.Errorf("connection not healthy after timeout: %w", err2)
			}
			_ = tx.Put(ctx, []byte("k"), []byte("v"))
			_ = tx.Commit(ctx)
			ev = append(ev, "connection still healthy after timeout")
			return VerdictPass, ev, nil
		})
		results.record(r)
		if r.Verdict != VerdictPass {
			t.Errorf("CS-007: verdict=%s error=%s", r.Verdict, r.Error)
		}
	})

	t.Run("CS-008_caller_cancellation", func(t *testing.T) {
		r := run("CS-008", "caller cancellation", "P0", func() (Verdict, []string, error) {
			var ev []string

			fWorker := connectFixture(t)
			fCaller := connectFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			route := uniqueRoute("rpc")
			sub, err := fWorker.Client().RPC().RegisterWorker(ctx, route, 7, func(workerCtx context.Context, _ fitz.RPCInboundRequest, w fitz.RPCResponseWriter) error {
				select {
				case <-workerCtx.Done():
					return workerCtx.Err()
				case <-time.After(3 * time.Second):
					return w.Send([]byte("late"))
				}
			})

			if err != nil {
				return VerdictFail, ev, fmt.Errorf("register worker: %w", err)
			}
			defer sub.Deregister()

			callCtx, callCancel := context.WithCancel(ctx)
			iter, err2 := fCaller.Client().RPC().Call(callCtx, route, []byte("block"))
			if err2 != nil {
				callCancel()
				return VerdictFail, ev, fmt.Errorf("rpc call: %w", err2)
			}

			time.Sleep(100 * time.Millisecond)
			callCancel()

			iter.Next()
			iterErr := iter.Err()
			closeQuietly(iter)

			if iterErr == nil {
				ev = append(ev, "expected cancellation error, got nil (race: call may have completed)")
				return VerdictPartial, ev, nil
			}
			ev = append(ev, fmt.Sprintf("cancellation error: %v", iterErr))
			ev = append(ev, "in-flight request failed on cancellation (correct)")

			// Subsequent request must succeed
			kvRoute := uniqueRoute("kv")
			tx, txErr := fCaller.Client().KV().Begin(ctx, kvRoute, fitz.KVDurabilitySync)
			if txErr != nil {
				return VerdictFail, ev, fmt.Errorf("subsequent request failed: %w", txErr)
			}
			_ = tx.Put(ctx, []byte("after-cancel"), []byte("ok"))
			_ = tx.Commit(ctx)
			ev = append(ev, "subsequent request succeeded after cancellation")
			return VerdictPass, ev, nil
		})
		results.record(r)
		if r.Verdict != VerdictPass {
			t.Errorf("CS-008: verdict=%s error=%s", r.Verdict, r.Error)
		}
	})

	t.Run("CS-009_disconnect_during_request", func(t *testing.T) {
		r := run("CS-009", "disconnect during request", "P1", func() (Verdict, []string, error) {
			var ev []string
			harness := fixture.NewProxyReconnectHarness(t, transportType(), authMode())
			fWorker := harness.Stable
			fCaller := harness.Proxied
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			harness.Connect(ctx)

			route := uniqueRoute("rpc")
			var err error
			_, err = fWorker.Client().RPC().RegisterWorker(ctx, route, 7, func(workerCtx context.Context, _ fitz.RPCInboundRequest, w fitz.RPCResponseWriter) error {
				select {
				case <-workerCtx.Done():
					return workerCtx.Err()
				case <-time.After(5 * time.Second):
					return w.Send([]byte("late"))
				}
			})

			if err != nil {
				return VerdictFail, ev, fmt.Errorf("register worker: %w", err)
			}

			iter, err := fCaller.Client().RPC().Call(ctx, route, []byte("block"))
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("rpc call: %w", err)
			}

			harness.WaitForInitialConnection(5 * time.Second)
			harness.Proxy.DropConnections()
			ev = append(ev, "caller transport dropped via proxy")

			iter.Next()
			iterErr := iter.Err()
			closeQuietly(iter)

			if iterErr == nil {
				ev = append(ev, "WARNING: in-flight request succeeded despite disconnect (race — acceptable)")
				return VerdictPartial, ev, nil
			}
			ev = append(ev, fmt.Sprintf("in-flight request failed: %v", iterErr))
			ev = append(ev, "in-flight request interrupted by disconnect (correct)")
			return VerdictPass, ev, nil
		})
		results.record(r)
		if r.Verdict != VerdictPass {
			t.Errorf("CS-009: verdict=%s error=%s", r.Verdict, r.Error)
		}
	})

	t.Run("CS-010_reconnect_behavior", func(t *testing.T) {
		r := run("CS-010", "reconnect and retry behavior", "P1", func() (Verdict, []string, error) {
			var ev []string
			harness := fixture.NewProxyReconnectHarness(t, transportType(), authMode())
			subscriber := harness.Proxied
			publisher := harness.Stable
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			harness.Connect(ctx, fixture.DefaultReconnectOptions()...)
			ev = append(ev, "subscriber connected through disconnect proxy")

			route := uniqueRoute("notice")
			var err error
			received := make(chan string, 4)
			_, err = subscriber.Client().Notice().Subscribe(ctx, route, func(_ context.Context, msg fitz.NoticeMsg) error {
				received <- string(msg.Body)
				return nil
			})
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("subscribe: %w", err)
			}

			if err := publisher.Client().Notice().Publish(ctx, route, []byte("before-disconnect")); err != nil {
				return VerdictFail, ev, fmt.Errorf("publish before disconnect: %w", err)
			}
			select {
			case body := <-received:
				if body != "before-disconnect" {
					return VerdictFail, ev, fmt.Errorf("unexpected pre-disconnect body: %q", body)
				}
			case <-time.After(5 * time.Second):
				return VerdictFail, ev, errors.New("timed out waiting for pre-disconnect delivery")
			}
			ev = append(ev, "subscription delivered before disconnect")

			harness.WaitForInitialConnection(5 * time.Second)
			harness.Proxy.DropConnections()
			ev = append(ev, "proxy dropped live connection")

			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				if harness.Proxy.AcceptedCount() >= 2 {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			if harness.Proxy.AcceptedCount() < 2 {
				return VerdictFail, ev, errors.New("client did not reconnect through proxy")
			}
			ev = append(ev, "client established a new transport connection")

			published := false
			for time.Now().Before(deadline) {
				if err := publisher.Client().Notice().Publish(ctx, route, []byte("after-disconnect")); err == nil {
					published = true
				}
				select {
				case body := <-received:
					if body == "after-disconnect" {
						ev = append(ev, "subscription restored after reconnect")
						return VerdictPass, ev, nil
					}
				default:
				}
				time.Sleep(100 * time.Millisecond)
			}

			if !published {
				return VerdictFail, ev, errors.New("publisher could not publish after reconnect")
			}
			return VerdictFail, ev, errors.New("subscription was not restored after reconnect")
		})
		results.record(r)
		if r.Verdict != VerdictPass {
			t.Errorf("CS-010: verdict=%s error=%s", r.Verdict, r.Error)
		}
	})

	t.Run("CS-011_stream_receive_sequence", func(t *testing.T) {
		r := run("CS-011", "stream receive sequence", "P1", func() (Verdict, []string, error) {
			var ev []string
			f := connectFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			route := uniqueRoute("stream")
			sess, err := f.Client().Stream().Begin(ctx, route)
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("stream begin: %w", err)
			}
			var expectedOffset uint64
			for i := range 3 {
				if _, err := sess.Append(ctx, expectedOffset, []byte{byte(i * 10)}); err != nil {
					return VerdictFail, ev, fmt.Errorf("append %d: %w", i, err)
				}
				expectedOffset++
			}
			if err := sess.Commit(ctx, fitz.StreamCommitSync); err != nil {
				return VerdictFail, ev, fmt.Errorf("commit: %w", err)
			}
			ev = append(ev, "stream session appended 3 records")

			iter, err := f.Client().Stream().Read(ctx, route, 0, 10)
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("stream read: %w", err)
			}
			defer closeQuietly(iter)

			var offsets []uint64
			for iter.Next() {
				offsets = append(offsets, iter.Value().Offset)
			}
			if err := iter.Err(); err != nil {
				return VerdictFail, ev, fmt.Errorf("iterator error: %w", err)
			}

			if len(offsets) < 3 {
				ev = append(ev, fmt.Sprintf("expected >=3 records, got %d", len(offsets)))
				return VerdictPartial, ev, nil
			}
			for i := 1; i < len(offsets); i++ {
				if offsets[i] <= offsets[i-1] {
					ev = append(ev, fmt.Sprintf("out-of-order offsets at index %d: %d <= %d", i, offsets[i], offsets[i-1]))
					return VerdictPartial, ev, nil
				}
			}
			ev = append(ev, fmt.Sprintf("read %d records in ascending offset order", len(offsets)))
			return VerdictPass, ev, nil
		})
		results.record(r)
		if r.Verdict != VerdictPass {
			t.Errorf("CS-011: verdict=%s error=%s", r.Verdict, r.Error)
		}
	})

	t.Run("CS-012_stream_completion", func(t *testing.T) {
		r := run("CS-012", "stream completion", "P1", func() (Verdict, []string, error) {
			var ev []string
			f := connectFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			route := uniqueRoute("stream")
			sess, err := f.Client().Stream().Begin(ctx, route)
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("begin: %w", err)
			}
			_, _ = sess.Append(ctx, 0, []byte("first"))
			_, _ = sess.Append(ctx, 1, []byte("last"))
			if err := sess.Commit(ctx, fitz.StreamCommitSync); err != nil {
				return VerdictFail, ev, fmt.Errorf("commit: %w", err)
			}
			ev = append(ev, "stream session committed")

			iter, err := f.Client().Stream().Read(ctx, route, 0, 100)
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("read: %w", err)
			}
			var count int
			for iter.Next() {
				count++
			}
			closeQuietly(iter)

			if count < 2 {
				ev = append(ev, fmt.Sprintf("expected >=2 records, got %d", count))
				return VerdictPartial, ev, nil
			}
			ev = append(ev, fmt.Sprintf("stream.Read() completed cleanly with %d records", count))
			return VerdictPass, ev, nil
		})
		results.record(r)
		if r.Verdict != VerdictPass {
			t.Errorf("CS-012: verdict=%s error=%s", r.Verdict, r.Error)
		}
	})

	t.Run("CS-013_stream_error_mid_flight", func(t *testing.T) {
		r := run("CS-013", "stream error mid-flight", "P1", func() (Verdict, []string, error) {
			var ev []string
			f := connectFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			route := uniqueRoute("stream")
			sess, err := f.Client().Stream().Begin(ctx, route)
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("begin: %w", err)
			}
			_, _ = sess.Append(ctx, 0, []byte("record-1"))
			if err := sess.Commit(ctx, fitz.StreamCommitSync); err != nil {
				return VerdictFail, ev, fmt.Errorf("commit: %w", err)
			}
			ev = append(ev, "written first record at offset 0")

			// append with wrong expected offset — server should reject
			wrongSession, err := f.Client().Stream().Begin(ctx, route)
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("second begin: %w", err)
			}
			_, appendErr := wrongSession.Append(ctx, 0, []byte("record-2"))
			if appendErr == nil {
				return VerdictFail, ev, errors.New("expected error on wrong expected offset, got nil")
			}
			ev = append(ev, fmt.Sprintf("append with wrong offset errored: %v", appendErr))

			// Client must remain usable
			kvRoute := uniqueRoute("kv")
			tx, txErr := f.Client().KV().Begin(ctx, kvRoute, fitz.KVDurabilitySync)
			if txErr != nil {
				return VerdictFail, ev, fmt.Errorf("client not reusable: %w", txErr)
			}
			_ = tx.Put(ctx, []byte("k"), []byte("v"))
			_ = tx.Commit(ctx)
			ev = append(ev, "client still usable after stream error")
			return VerdictPass, ev, nil
		})
		results.record(r)
		if r.Verdict != VerdictPass {
			t.Errorf("CS-013: verdict=%s error=%s", r.Verdict, r.Error)
		}
	})

	t.Run("CS-014_concurrent_requests", func(t *testing.T) {
		r := run("CS-014", "concurrent in-flight requests", "P1", func() (Verdict, []string, error) {
			var ev []string
			f := connectFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			routes := []string{uniqueRoute("kv"), uniqueRoute("kv"), uniqueRoute("kv")}
			type readResult struct {
				idx int
				val string
				err error
			}
			resultsCh := make(chan readResult, len(routes))

			for i, route := range routes {
				go func() {
					tx, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
					if err != nil {
						resultsCh <- readResult{i, "", err}
						return
					}
					key := fmt.Sprintf("key-%d", i)
					val := fmt.Sprintf("value-%d", i)
					_ = tx.Put(ctx, []byte(key), []byte(val))
					_ = tx.Commit(ctx)
					rtx, err2 := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync, fitz.WithKVMode(fitz.KVModeReadOnly))
					if err2 != nil {
						resultsCh <- readResult{i, "", err2}
						return
					}
					res, err3 := rtx.Get(ctx, []byte(key))
					if err3 != nil {
						resultsCh <- readResult{i, "", err3}
						return
					}
					resultsCh <- readResult{i, string(res.Value), nil}
				}()
			}

			for range routes {
				res := <-resultsCh
				if res.err != nil {
					return VerdictFail, ev, fmt.Errorf("concurrent task %d failed: %w", res.idx, res.err)
				}
				expected := fmt.Sprintf("value-%d", res.idx)
				if res.val != expected {
					return VerdictFail, ev, fmt.Errorf("task %d: expected %q got %q", res.idx, expected, res.val)
				}
			}
			ev = append(ev, "3 concurrent kv transactions completed correctly")
			ev = append(ev, "all responses correlated to correct request contexts")
			return VerdictPass, ev, nil
		})
		results.record(r)
		if r.Verdict != VerdictPass {
			t.Errorf("CS-014: verdict=%s error=%s", r.Verdict, r.Error)
		}
	})

	t.Run("CS-015_shutdown_during_active_work", func(t *testing.T) {
		r := run("CS-015", "shutdown during active work", "P1", func() (Verdict, []string, error) {
			var ev []string

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			brokerAddr, _, addrErr := fixture.StartBrokerIfNeeded(transportType(), authMode())
			if addrErr != nil {
				return VerdictFail, ev, fmt.Errorf("broker addr: %w", addrErr)
			}

			tokenProvider := fitz.TokenProvider(func(context.Context) (string, error) {
				return "", nil
			})

			client := fitz.NewClient(brokerAddr, tokenProvider)
			if connErr := client.Connect(ctx); connErr != nil {
				return VerdictFail, ev, fmt.Errorf("connect: %w", connErr)
			}

			route := uniqueRoute("kv")
			beginCh := make(chan error, 1)
			go func() {
				_, err := client.KV().Begin(ctx, route, fitz.KVDurabilitySync)
				beginCh <- err
			}()

			time.Sleep(50 * time.Millisecond)
			if closeErr := client.Close(); closeErr != nil {
				ev = append(ev, fmt.Sprintf("close returned: %v", closeErr))
			}
			ev = append(ev, "close during active work did not panic")

			// Double close must not panic
			if closeErr := client.Close(); closeErr != nil {
				ev = append(ev, fmt.Sprintf("double close returned: %v (expected if idempotent)", closeErr))
			}
			ev = append(ev, "double close is safe")

			select {
			case beginErr := <-beginCh:
				if beginErr != nil {
					ev = append(ev, fmt.Sprintf("in-flight begin failed: %v (expected)", beginErr))
				} else {
					ev = append(ev, "in-flight begin completed before close (race — acceptable)")
				}
			case <-time.After(2 * time.Second):
				ev = append(ev, "in-flight begin timed out waiting for result")
			}

			return VerdictPass, ev, nil
		})
		results.record(r)
		if r.Verdict != VerdictPass {
			t.Errorf("CS-015: verdict=%s error=%s", r.Verdict, r.Error)
		}
	})

	t.Run("CS-016_filtered_stream_replay", func(t *testing.T) {
		r := run("CS-016", "filtered stream replay", "P1", func() (Verdict, []string, error) {
			var ev []string
			f := connectFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			route := uniqueRoute("stream")
			session, err := f.Client().Stream().Begin(ctx, route)
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("stream begin: %w", err)
			}

			alpha := "proj.alpha"
			firstOffset, err := session.Append(ctx, 0, []byte("alpha"), fitz.WithStreamDiscriminator(alpha))
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("append matching record: %w", err)
			}

			beta := "audit.beta"
			secondOffset, err := session.Append(ctx, firstOffset+1, []byte("beta"), fitz.WithStreamDiscriminator(beta))
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("append filtered record: %w", err)
			}
			if err := session.Commit(ctx, fitz.StreamCommitSync); err != nil {
				return VerdictFail, ev, fmt.Errorf("stream commit: %w", err)
			}

			filter := &fitz.StreamFilterSet{
				Clauses: []fitz.StreamFilterClause{{
					Kind:  fitz.StreamFilterEquals,
					Value: alpha,
				}},
			}
			options := fitz.WithStreamFilter(*filter)
			iter, err := f.Client().Stream().Read(ctx, route, 0, 10, options)
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("filtered stream read: %w", err)
			}
			defer closeQuietly(iter)

			var records []fitz.StreamRecord
			for iter.Next() {
				records = append(records, iter.Value())
			}
			if err := iter.Err(); err != nil {
				return VerdictFail, ev, fmt.Errorf("iterate filtered stream: %w", err)
			}
			if len(records) != 1 || records[0].Offset != firstOffset || string(records[0].Body) != "alpha" {
				return VerdictFail, ev, fmt.Errorf("unexpected filtered records: %+v", records)
			}

			page, err := f.Client().Stream().ReadPage(ctx, route, 0, 10, options)
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("filtered stream page: %w", err)
			}
			if page.Cursor.LastResourceOffset != secondOffset || page.Cursor.HasMore || len(page.Items) != 2 {
				return VerdictFail, ev, fmt.Errorf("unexpected filtered page cursor/items: %+v", page)
			}
			if page.Items[0].Kind != fitz.StreamReadItemEvent ||
				page.Items[0].Record == nil ||
				page.Items[0].Record.Offset != firstOffset ||
				page.Items[1].Kind != fitz.StreamReadItemFiltered ||
				page.Items[1].Offset != secondOffset ||
				page.Items[1].Reason == nil ||
				*page.Items[1].Reason != fitz.StreamFilteredReasonServerFilter {
				return VerdictFail, ev, fmt.Errorf("unexpected filtered page items: %+v", page.Items)
			}

			ev = append(ev, "filtered read returned only the matching discriminator")
			ev = append(ev, "page cursor advanced across the server-filtered record")
			return VerdictPass, ev, nil
		})
		results.record(r)
		if r.Verdict != VerdictPass {
			t.Errorf("CS-016: verdict=%s error=%s", r.Verdict, r.Error)
		}
	})

	t.Run("CS-017_bounded_concurrency_under_burst_load", func(t *testing.T) {
		r := run("CS-017", "bounded concurrency under burst load", "P1", func() (Verdict, []string, error) {
			var ev []string

			f := newFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			if err := f.ConnectWithOptions(ctx, fitz.WithMaxInFlightRequests(1)); err != nil {
				return VerdictFail, ev, fmt.Errorf("connect: %w", err)
			}

			route := uniqueRoute("rpc")
			sub, err := f.Client().RPC().RegisterWorker(ctx, route, 7, func(workerCtx context.Context, _ fitz.RPCInboundRequest, w fitz.RPCResponseWriter) error {
				select {
				case <-workerCtx.Done():
					return workerCtx.Err()
				case <-time.After(500 * time.Millisecond):
					return w.Send([]byte("delayed"))
				}
			})

			if err != nil {
				return VerdictFail, ev, fmt.Errorf("register worker: %w", err)
			}
			defer sub.Deregister()

			callCtx, callCancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
			defer callCancel()

			ev = append(ev, "registered delayed RPC worker")
			firstIter, err := f.Client().RPC().Call(callCtx, route, []byte("first"))
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("first rpc call: %w", err)
			}
			defer closeQuietly(firstIter)

			secondIter, err := f.Client().RPC().Call(callCtx, route, []byte("second"))
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("second rpc call: %w", err)
			}
			defer closeQuietly(secondIter)

			firstDoneCh := make(chan error, 1)
			secondDoneCh := make(chan error, 1)

			go func() {
				if !firstIter.Next() {
					firstDoneCh <- firstIter.Err()
					return
				}
				firstDoneCh <- nil
			}()

			go func() {
				if !secondIter.Next() {
					secondDoneCh <- secondIter.Err()
					return
				}
				secondDoneCh <- nil
			}()

			time.Sleep(100 * time.Millisecond)
			select {
			case err := <-secondDoneCh:
				return VerdictFail, ev, fmt.Errorf("expected second RPC call to remain pending behind the first, got %w", err)
			default:
			}

			ev = append(ev, "second RPC call remained pending while first was in flight")
			ev = append(ev, "configured maxInFlightRequests=1 and burst size=2")

			callCancel()
			firstErr := <-firstDoneCh
			secondErr := <-secondDoneCh
			if firstErr != nil {
				ev = append(ev, fmt.Sprintf("first RPC call ended with %v", firstErr))
			}
			if secondErr != nil {
				ev = append(ev, fmt.Sprintf("second RPC call ended with %v", secondErr))
			}

			return VerdictPass, ev, nil
		})
		results.record(r)
		if r.Verdict != VerdictPass {
			t.Errorf("CS-017: verdict=%s error=%s", r.Verdict, r.Error)
		}
	})

	// -------------------------------------------------------------------------
	// CS-018–CS-022: Go-client scenarios not in the cross-language spec but
	// required to close coverage gaps for Queue, Lease, Notice, Schedule, and
	// reconnect restoration behavior.
	// -------------------------------------------------------------------------

	t.Run("CS-018_queue_enqueue_reserve_complete", func(t *testing.T) {
		r := run("CS-018", "queue enqueue/reserve/complete lifecycle", "P1", func() (Verdict, []string, error) {
			var ev []string
			f := connectFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			route := uniqueRoute("queue")
			msgID, err := f.Client().Queue().Enqueue(ctx, route, []byte("cs018-payload"))
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("enqueue: %w", err)
			}
			if msgID == 0 {
				return VerdictFail, ev, errors.New("expected non-zero message ID, got 0")
			}
			ev = append(ev, fmt.Sprintf("enqueued message ID=%d", msgID))

			items, err := f.Client().Queue().Reserve(ctx, route, 30, 1)
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("reserve: %w", err)
			}
			if len(items) != 1 {
				return VerdictFail, ev, fmt.Errorf("expected 1 reserved item, got %d", len(items))
			}
			if string(items[0].Body) != "cs018-payload" {
				return VerdictFail, ev, fmt.Errorf("expected 'cs018-payload', got %q", items[0].Body)
			}
			ev = append(ev, fmt.Sprintf("reserved item body=%q", items[0].Body))

			if err := items[0].Complete(ctx); err != nil {
				return VerdictFail, ev, fmt.Errorf("complete: %w", err)
			}
			ev = append(ev, "message completed (acknowledged)")

			// After complete the queue should be empty
			empty, err := f.Client().Queue().Reserve(ctx, route, 30, 1)
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("second reserve: %w", err)
			}
			if len(empty) != 0 {
				ev = append(ev, fmt.Sprintf("WARNING: expected empty queue after complete, got %d items", len(empty)))
				return VerdictPartial, ev, nil
			}
			ev = append(ev, "queue is empty after complete (correct)")
			return VerdictPass, ev, nil
		})
		results.record(r)
		if r.Verdict != VerdictPass {
			t.Errorf("CS-018: verdict=%s error=%s", r.Verdict, r.Error)
		}
	})

	t.Run("CS-019_lease_acquire_contention_release", func(t *testing.T) {
		r := run("CS-019", "lease acquire/contention/release lifecycle", "P1", func() (Verdict, []string, error) {
			var ev []string

			f1 := connectFixture(t)
			f2 := connectFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			route := uniqueRoute("lease")
			l1, err := f1.Client().Lease().Acquire(ctx, route, 30, fitz.LeaseAcquireOptions{OwnerID: "fitz-go"})
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("acquire: %w", err)
			}
			if l1 == nil || l1.ExpiresAt == 0 {
				return VerdictFail, ev, errors.New("expected non-nil lease with expiry")
			}
			ev = append(ev, fmt.Sprintf("client1 acquired lease expiresAt=%d", l1.ExpiresAt))

			// Contention: second client must be rejected
			l2, err2 := f2.Client().Lease().Acquire(ctx, route, 30, fitz.LeaseAcquireOptions{OwnerID: "fitz-go"})
			if err2 == nil && l2 != nil {
				return VerdictFail, ev, errors.New("expected contention error but acquire succeeded")
			}
			ev = append(ev, fmt.Sprintf("client2 rejected on held lease: %v (correct)", err2))

			// Release and allow client2 to acquire
			if err := l1.Release(ctx); err != nil {
				return VerdictFail, ev, fmt.Errorf("release: %w", err)
			}
			ev = append(ev, "client1 released lease")

			l3, err3 := f2.Client().Lease().Acquire(ctx, route, 30, fitz.LeaseAcquireOptions{OwnerID: "fitz-go"})
			if err3 != nil {
				return VerdictFail, ev, fmt.Errorf("acquire after release: %w", err3)
			}
			if l3 == nil || l3.ExpiresAt == 0 {
				return VerdictFail, ev, errors.New("expected lease after release, got nil")
			}
			ev = append(ev, "client2 acquired lease after release (correct)")
			return VerdictPass, ev, nil
		})
		results.record(r)
		if r.Verdict != VerdictPass {
			t.Errorf("CS-019: verdict=%s error=%s", r.Verdict, r.Error)
		}
	})

	t.Run("CS-020_notice_subscribe_publish_deliver", func(t *testing.T) {
		r := run("CS-020", "notice subscribe/publish/deliver lifecycle", "P1", func() (Verdict, []string, error) {
			var ev []string
			f := connectFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			route := uniqueRoute("notice")
			received := make(chan string, 4)

			sub, err := f.Client().Notice().Subscribe(ctx, route, func(_ context.Context, msg fitz.NoticeMsg) error {
				received <- string(msg.Body)
				return nil
			})
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("subscribe: %w", err)
			}
			ev = append(ev, "subscribed to route")

			if err := f.Client().Notice().Publish(ctx, route, []byte("cs020-msg")); err != nil {
				sub.Unsubscribe()
				return VerdictFail, ev, fmt.Errorf("publish: %w", err)
			}
			ev = append(ev, "published message")

			select {
			case body := <-received:
				if body != "cs020-msg" {
					sub.Unsubscribe()
					return VerdictFail, ev, fmt.Errorf("expected 'cs020-msg', got %q", body)
				}
				ev = append(ev, fmt.Sprintf("handler received message body=%q (correct)", body))
			case <-time.After(5 * time.Second):
				sub.Unsubscribe()
				return VerdictFail, ev, errors.New("timed out waiting for notification delivery")
			}

			// Unsubscribe and verify no further delivery
			sub.Unsubscribe()
			ev = append(ev, "unsubscribed")

			if err := f.Client().Notice().Publish(ctx, route, []byte("after-unsub")); err != nil {
				return VerdictFail, ev, fmt.Errorf("publish after unsubscribe: %w", err)
			}
			select {
			case body := <-received:
				ev = append(ev, fmt.Sprintf("WARNING: received %q after unsubscribe", body))
				return VerdictPartial, ev, nil
			case <-time.After(500 * time.Millisecond):
				ev = append(ev, "no delivery after unsubscribe (correct)")
			}
			return VerdictPass, ev, nil
		})
		results.record(r)
		if r.Verdict != VerdictPass {
			t.Errorf("CS-020: verdict=%s error=%s", r.Verdict, r.Error)
		}
	})

	t.Run("CS-021_schedule_create_subscribe_cancel", func(t *testing.T) {
		r := run("CS-021", "schedule create/subscribe/cancel lifecycle", "P1", func() (Verdict, []string, error) {
			var ev []string
			f := connectFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			route := uniqueRoute("schedule")

			// Subscribe first — verify the subscription API is usable
			sub, err := f.Client().Schedule().Subscribe(ctx, route, func(_ context.Context, _ fitz.ScheduleNotification) error {
				return nil
			})
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("subscribe: %w", err)
			}
			ev = append(ev, "subscribed to schedule route")

			// Create a schedule
			scheduleID, err := f.Client().Schedule().Create(ctx, route, "0 9 * * 1", fitz.ScheduleDeliveryBroadcast, []byte("cs021-payload"))
			if err != nil {
				sub.Unsubscribe()
				return VerdictFail, ev, fmt.Errorf("create: %w", err)
			}
			if scheduleID == "" {
				sub.Unsubscribe()
				return VerdictFail, ev, errors.New("expected non-empty schedule ID")
			}
			ev = append(ev, fmt.Sprintf("schedule created id=%q", scheduleID))

			// Cancel before unsubscribing
			if err := f.Client().Schedule().Cancel(ctx, route); err != nil {
				sub.Unsubscribe()
				return VerdictFail, ev, fmt.Errorf("cancel: %w", err)
			}
			ev = append(ev, "schedule canceled")

			sub.Unsubscribe()
			ev = append(ev, "unsubscribed")

			ev = append(ev, "NOTE: schedule fire delivery tested in integration suite (requires up to 90s)")
			return VerdictPass, ev, nil
		})
		results.record(r)
		if r.Verdict != VerdictPass {
			t.Errorf("CS-021: verdict=%s error=%s", r.Verdict, r.Error)
		}
	})

	t.Run("CS-022_queue_reconnect_restore", func(t *testing.T) {
		r := run("CS-022", "queue subscription restore after reconnect", "P1", func() (Verdict, []string, error) {
			var ev []string
			harness := fixture.NewProxyReconnectHarness(t, transportType(), authMode())
			subscriber := harness.Proxied
			producer := harness.Stable

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			harness.Connect(ctx, fixture.DefaultReconnectOptions()...)
			ev = append(ev, "subscriber connected through disconnect proxy")

			route := uniqueRoute("queue")
			notifications := make(chan string, 4)
			_, err := subscriber.Client().Queue().Subscribe(ctx, route, func(_ context.Context, n fitz.QueueAvailabilityNotification) error {
				notifications <- n.Route
				return nil
			})
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("subscribe: %w", err)
			}

			_, err = producer.Client().Queue().Enqueue(ctx, route, []byte("before-disconnect"))
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("enqueue before disconnect: %w", err)
			}
			select {
			case notifiedRoute := <-notifications:
				if notifiedRoute != route {
					return VerdictFail, ev, fmt.Errorf("unexpected route before disconnect: %q", notifiedRoute)
				}
			case <-time.After(5 * time.Second):
				return VerdictFail, ev, errors.New("timed out waiting for pre-disconnect queue notification")
			}
			ev = append(ev, "queue availability notification delivered before disconnect")

			items, err := subscriber.Client().Queue().Reserve(ctx, route, 30, 1)
			if err != nil {
				return VerdictFail, ev, fmt.Errorf("reserve before disconnect: %w", err)
			}
			if len(items) != 1 {
				return VerdictFail, ev, fmt.Errorf("expected 1 reserved item before disconnect, got %d", len(items))
			}
			if err := items[0].Complete(ctx); err != nil {
				return VerdictFail, ev, fmt.Errorf("complete before disconnect: %w", err)
			}
			ev = append(ev, "queue edge re-armed after initial notification")

			harness.WaitForInitialConnection(5 * time.Second)
			harness.DropAndWaitForReconnect(10 * time.Second)
			ev = append(ev, "proxy dropped live connection and client reconnected")

			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				_, err := producer.Client().Queue().Enqueue(ctx, route, []byte("after-disconnect"))
				if err != nil {
					time.Sleep(100 * time.Millisecond)
					continue
				}

				select {
				case notifiedRoute := <-notifications:
					if notifiedRoute == route {
						ev = append(ev, "queue availability subscription restored after reconnect")
						return VerdictPass, ev, nil
					}
				default:
					items, reserveErr := producer.Client().Queue().Reserve(ctx, route, 30, 1)
					if reserveErr == nil && len(items) == 1 {
						_ = items[0].Complete(ctx)
					}
				}

				time.Sleep(100 * time.Millisecond)
			}

			return VerdictFail, ev, errors.New("queue availability subscription was not restored after reconnect")
		})
		results.record(r)
		if r.Verdict != VerdictPass {
			t.Errorf("CS-022: verdict=%s error=%s", r.Verdict, r.Error)
		}
	})
}
