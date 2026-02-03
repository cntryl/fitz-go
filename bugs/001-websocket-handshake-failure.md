# Bug #001: WebSocket Connection Handshake Failure

**Status:** Open (Fix Available)  
**Priority:** High  
**Component:** Transport/WebSocket  
**Reported:** 2026-02-03  
**Affects:** AC-CONN-001 (WebSocket transport)

## Summary
WebSocket connections to Fitz broker fail with "bad handshake" error. TCP connections work correctly on port 4091, but WebSocket upgrade fails on port 4090.

## Environment
- **Broker:** Fitz (Docker container from compose.yml)
- **Broker ports:** 4090 (HTTP/WebSocket), 4091 (TCP)
- **Broker config:** `FITZ_AUTH_REQUIRED=FALSE`, `FITZ_STORAGE_MODE=memory`
- **Client:** cntryl-go SDK
- **WebSocket library:** gorilla/websocket v1.5.3
- **Date:** 2026-02-03

## Expected Behavior
Per compose.yml documentation:
```yaml
# - WebSocket:    ws://localhost:4090/ws
```

WebSocket connection to `ws://localhost:4090/ws` should:
1. Complete WebSocket handshake with 101 Switching Protocols
2. Accept binary frames for TLV protocol
3. Successfully process CONNECT frame (MessageType=1, Channel=0, Body=JWT)

## Actual Behavior
- WebSocket handshake fails immediately
- Error: `websocket: bad handshake`
- Broker logs show: `INFO fitz::boot::handlers: HTTP connection from 172.18.0.1:XXXXX`
- No WebSocket upgrade occurs (no `WebSocket connection` log entry)

## Steps to Reproduce
1. Start broker: `docker compose up -d`
2. Verify HTTP health: `curl http://localhost:4090/healthz` (returns `{"status":"ok"}`)
3. Attempt WebSocket connection to `ws://localhost:4090/ws`
4. WebSocket handshake fails with "bad handshake"

## Tested Endpoints
- ❌ `ws://localhost:4090/ws` - bad handshake
- ❌ `ws://localhost:4090/` - bad handshake
- ✅ `http://localhost:4090/healthz` - works (200 OK)
- ✅ `tcp://localhost:4091` - works (CONNECT succeeds)

## Client Implementation Details
Go client using gorilla/websocket:
```go
dialer := websocket.DefaultDialer
conn, _, err := dialer.DialContext(ctx, "ws://localhost:4090/ws", nil)
// Returns: websocket: bad handshake
```

Handshake request headers (standard):
- Connection: Upgrade
- Upgrade: websocket
- Sec-WebSocket-Version: 13
- Sec-WebSocket-Key: [random base64]
- Origin: http://localhost:4090

## Broker Logs (Relevant)
```
fitz-node  | 2026-02-03T17:14:03.047939Z  INFO fitz::boot::handlers: HTTP connection from 172.18.0.1:39088
fitz-node  | 2026-02-03T17:14:03.151522Z  INFO fitz::boot::handlers: HTTP connection from 172.18.0.1:39104
```
(No WebSocket-specific logs, only HTTP connection attempts)

## Questions for Broker Team
1. Is WebSocket support fully implemented in the current broker build?
2. What is the correct WebSocket endpoint path (`/ws`, `/`, other)?
3. Does the broker require a specific `Sec-WebSocket-Protocol` header?
4. Are there additional WebSocket configuration requirements for anonymous mode?
5. Should CLIENT_SPEC.md be updated if WebSocket is not yet supported?

## Workaround
TCP transport on port 4091 works correctly. Tests are proceeding with TCP-only until WebSocket is resolved.

## Impact
- Cannot verify protocol equivalence between TCP and WebSocket transports (CLIENT_SPEC.md requirement)
- AC-CONN-001 test incomplete (only TCP verified)
- Integration tests using `RunWithBothTransports()` will only test TCP

## Related Files
- `test/transport_test.go` - TestShouldConnectViaWebSocketGivenValidAddressWhenWebSocketTransportUsed (skipped)
- `internal/client/dialer.go` - dialWebSocket() implementation
- `test/fixture/fixture.go` - Default WebSocket address configuration

## Fix Available (Not Yet Deployed)

**Implementation:** Fitz broker `src/boot/handlers.rs`  
**Status:** Fix documented but not yet applied to broker source

### Root Cause
The `handle_websocket` function was stubbed out and returning HTTP 501 Not Implemented:
```rust
// TODO: Implement WebSocket upgrade using tungstenite
Ok(hyper::Response::builder()
    .status(501)
    .body(hyper::Body::from("WebSocket upgrade not yet implemented"))
    .unwrap())
```

### Fix Summary
Implemented full WebSocket upgrade and session handling in `src/boot/handlers.rs`:

1. **WebSocket Handshake** - Validates required headers (Upgrade, Connection, Sec-WebSocket-Version, Sec-WebSocket-Key) and performs HTTP 101 upgrade using `hyper-tungstenite::upgrade()`

2. **Session Handler** (`run_websocket_session`) - Bidirectional frame processing, TLV decoding via `Session::on_frame()`, proper error handling and cleanup, follows same pattern as TCP transport

3. **Protocol Details**:
   - Binary WebSocket frames → TLV-encoded messages
   - Any path works for WebSocket upgrade (not restricted to `/ws`)
   - Standard RFC 6455 WebSocket handshake
   - Session lifecycle management with proper cleanup

### Verification Steps
1. Rebuild broker: `docker compose build`
2. Restart broker: `docker compose up -d`
3. Remove skip from `TestShouldConnectViaWebSocketGivenValidAddressWhenWebSocketTransportUsed`
4. Run WebSocket tests: `go test ./test -run WebSocket -v`

### Client SDK Actions Required (Once Broker Fixed)
- [ ] Rebuild broker with WebSocket implementation
- [ ] Remove `t.Skip()` from WebSocket connection test
- [ ] Verify WebSocket connection succeeds
- [ ] Enable `RunWithBothTransports()` for all domain tests
- [ ] Verify protocol equivalence between TCP and WebSocket

### Remaining Broker Work
- **BLOCKING:** Apply WebSocket implementation to broker `src/boot/handlers.rs`
- Outbound frame routing (session → WebSocket sender)
- Backpressure handling for slow clients
- Full integration tests with domain roundtrip
- Load testing for concurrent WebSocket connections

## Current Test Status (2026-02-03 17:33 UTC)
- ✅ TCP connection test passing
- ❌ WebSocket test still failing - broker logs show "HTTP connection" not "WebSocket connection established"
- 🔄 Broker rebuild completed but WebSocket fix not present in source
- **ACTION REQUIRED:** Fix must be applied to fitz broker repository `src/boot/handlers.rs`
