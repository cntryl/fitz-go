# Fitz Client Specification

## Purpose ✅

This document defines the client contract, wire protocol expectations, and acceptance criteria for any Fitz client implementation. It is language-agnostic and focuses solely on behaviour the broker requires over supported transports (WebSocket and TCP).

---

## Terminology (Strict) ⚠️

Use these exact terms in implementations and docs: **realm**, **area**, **resource**, **operation**, **route**. Never use forbidden terms (e.g., tenant, namespace, endpoint).

---

## Supported Transports 🔌

- WebSocket (binary frames) — recommended for browsers and bidirectional usage.
- Raw TCP (TLV stream) — identical wire semantics; clients must implement streaming TLV codec (length-prefixed frames).

Clients MUST behave identically across both transports—framing differs only by transport encapsulation (WebSocket binary vs. TCP stream with the same TLV frame bytes).

---

## Wire Protocol Summary (TLV Frame) 🔧

Fitz uses binary TLV frames. Each frame contains a header then a TLV payload.

Frame structure (conceptual):

- Length (u32) — total frame bytes following length
- Type (u8) — frame type (CONN_OPEN, ACK, REG, DAT, PUB, REQ, ERR, etc.)
- Flags (u8) — per-frame flags (reserved/extension)
- Channel (u32) — logical channel id (for routing replies)
- TLV payload — repeating [Tag u8][Len u16][Value...]

Common TLV tags used by the broker:

- TAG_ROUTE (0x20): route string (e.g., `stream://realm/area/resource/append`)
- TAG_ID (0x21): correlation id / message id
- TAG_BODY (0x22): payload bytes (domain-specific encoding: JSON/CBOR/opaque)
- TAG_TOKEN (0x10): authentication token (JWT)
- TAG_ERR (0x3F): error code + message

(Refer to the repository specs for complete type/tag lists.)

---

## Connection Lifecycle & Handshake 🔄

1. Open transport (WebSocket/TCP) to broker.
2. Send CONN_OPEN frame (Optionally include `TAG_TOKEN` for authentication and an optional `TAG_ROUTE` if binding a default realm/area).
3. Broker will reply with ACK (or ERR). If authentication fails, broker responds with `NOT_AUTHENTICATED` or `AUTHORIZATION_FAILED` but keeps the session alive (client may retry with a new token).
4. Client MUST register the channel mapping: on first frame seen from a channel id the server maps `channel_id → conn_id` for replies (clients should include a channel id in requests that expect a direct reply).
5. Heartbeats: client SHOULD send periodic heartbeat frames or messages as a keepalive (see Heartbeat below).
6. Close: send CONN_CLOSE or perform transport close.

---

## Authentication & Security 🔒

- JWT tokens are the primary mechanism: include `TAG_TOKEN` inside `CONN_OPEN` or in an auth-specific frame.
- Always use TLS: `wss://` for WebSocket, TLS for TCP where available. Clients MUST validate server certificates.
- Authorization is route-based (claims contain allowed route patterns). The broker enforces this.

---

## Heartbeats & Idle Timeouts ❤️

- The broker expects periodic activity from the client. Clients SHOULD send a heartbeat frame at a configurable interval (30s recommended) and react to server heartbeat responses.
- If a session has no activity for the server's idle timeout, the server may drop the session. Clients must be prepared to reconnect and re-establish subscriptions/state.

---

## Routing & Message Semantics 🧭

- Routes are URIs with the form: `<domain>://<realm>/<area>/<resource>/<operation>`. Example: `stream://acme/orders/append`.
- When making an RPC-like request, include a `TAG_ID` (correlation id) and specify a `channel` (or `TAG_REPLY_ROUTE`) so replies are routed correctly.
- Domain semantics (ordering, at-least-once/at-most-once) are per-domain (streams, queues, notices). Clients MUST follow domain-specific contracts (see domain specs). Client library SHOULD expose idiomatic primitives that map to domain operations.

---

## Error Handling & Retries ⚠️

- Server returns `ERR` frames with `TAG_ERR` containing a code and message. Clients MUST interpret common errors:
  - `MESSAGE_TOO_LARGE` — drop/resize the payload and retry if possible
  - `NOT_AUTHENTICATED` / `AUTHORIZATION_FAILED` — re-authenticate and retry subject to policy
  - `SESSION_EXPIRED` — recreate session and replay non-idempotent operations only if safe
- Retry policy: clients SHOULD use exponential backoff. For non-idempotent operations, do not retry unless the client can guarantee idempotency.

---

## Flow Control & Backpressure 🧰

- The broker may apply per-session or per-channel quotas. Clients MUST handle `ERR` frames or ACKs that indicate backpressure and throttle sending.
- Avoid unbounded in-memory buffering of outbound messages. Provide a configurable write queue limit; on backpressure return errors to callers rather than silently dropping.

---

## Extensions & Versioning ✨

- The TLV codec allows extension tags; clients SHOULD ignore unknown tags and preserve them when acting as proxies.
- If a broker introduces an incompatible wire change, a new documented version will be published. Clients SHOULD expose configuration to pin protocol behaviors where relevant.

---

## Acceptance Criteria / Test Suite (must pass) ✅

Each client implementation MUST include an automated test suite that validates the following minimal cases against a reference broker instance.

1. Basic connect (WebSocket) — send CONN_OPEN with valid JWT, expect ACK. ✅
2. Basic connect (TCP) — same as above over raw TCP TLV stream. ✅
3. Heartbeat — client sends heartbeat and receives heartbeat response. ✅
4. Subscribe / Publish (Notice domain) — subscribe to `notice://realm/area/res/*`, publish a notice, and receive a DAT frame. ✅
5. RPC request/response — send a REQ with TAG_ID and expect a correlated reply with same TAG_ID on same channel. ✅
6. Error handling — provoke an `AUTHORIZATION_FAILED` and verify client surfaces the error and can re-authenticate. ✅
7. Large message — send a message exceeding `max_frame_size` and assert server returns `MESSAGE_TOO_LARGE`. ✅
8. Reconnect & resume — drop transport, reconnect, and validate client can re-subscribe and continue receiving messages (best-effort; domain semantics apply). ✅

Each test MUST be runnable over both WebSocket and TCP transports.

---

## Implementation Tips (Language-agnostic) 💡

- Provide both synchronous and asynchronous APIs depending on language conventions.
- Expose primitives: connect(), authenticate(token), subscribe(route), publish(route, body), request(route, body) → future/response, on_message(callback).
- Keep TLV encode/decode utilities well-tested and provide sample vectors in different languages.

---

## Next Steps / Proposal ✍️

- Add a machine-readable TLV type/tag registry to the specs directory.
- Publish canonical acceptance tests (playbook) and a lightweight test harness the community can run against any broker.

---

## Domains & Client Methods (API surface) 📚

Below are the canonical client-facing methods for each Fitz domain. These are language-agnostic method signatures and the wire/TLV tags they rely on. Implementations should expose idiomatic wrappers around these primitives (async/sync variants depending on language).

### Notice Domain (Fire-and-forget) 🔔

Purpose: fast, session-scoped fanout (notifications).

Client methods:

- subscribe(route: string) -> ack
  - TLV: TAG_ROUTE (pattern), TAG_SUBSCRIBE
- unsubscribe(route: string) -> ack
  - TLV: TAG_ROUTE, TAG_UNSUBSCRIBE
- publish(route: string, body: bytes) -> ok (optional ack)
  - TLV: TAG_ROUTE, TAG_BODY

Semantics/acceptance tests:

- subscribe → receive published DAT frames matching pattern
- publish → best-effort delivery; under backpressure, server may drop and emit metrics

### Stream Domain (Durable append-only logs) 📜

Purpose: durable, strictly ordered append/read with watermarks.

Client methods:

- append(route: string, body: bytes, expected_offset?: u64) -> AppendResult{resource_offset, area_offset, realm_offset}
  - TLV: TAG_ROUTE, TAG_BODY, TAG_EXPECTED_OFFSET
- read_resource(route: string, from: u64, limit: u32) -> [StreamRecord]
  - TLV: TAG_ROUTE, TAG_FROM, TAG_LIMIT
- read_area(route_pattern: string, from_area_offset: u64, limit: u32) -> [StreamRecord]
  - TLV: TAG_ROUTE (wildcard), TAG_FROM, TAG_LIMIT

Semantics/acceptance tests:

- append with expected_offset enforces optimistic concurrency (ERR_CONCURRENCY_CONFLICT on mismatch)
- reads obey watermarks (no records beyond watermark)
- durable replay after restart

### Queue Domain (Durable at-least-once) 📦

Purpose: durable FIFO-ish leases with visibility timeouts.

Client methods:

- enqueue(route: string, body: bytes) -> message_id
  - TLV: TAG_ROUTE, TAG_BODY
- reserve(route: string, lease_secs: u32, batch_size?: u32) -> List<Lease{ id, body, token, lease_secs }>
  - TLV: TAG_ROUTE, TAG_LEASE, TAG_BATCH_SIZE
- extend(route: string, id: string, token: u64, lease_secs: u32) -> ok
  - TLV: TAG_ID, TAG_DELIVERY_TOKEN, TAG_LEASE
- complete(route: string, id: string, token: u64) -> ok
  - TLV: TAG_ID, TAG_DELIVERY_TOKEN
- peek(route: string, limit?: u32) -> [Message] (optional)

Semantics/acceptance tests:

- enqueue → reserve → complete cycle
- lease expiry puts messages back onto ready queue
- token mismatch yields `QUEUE_INVALID_TOKEN`

### RPC Domain (Request/Response & Streaming) 🧩

Purpose: low-latency request/response with reply inbox semantics and streaming replies.

Client methods:

- request(route: string, body: bytes, timeout?: Duration) -> Response (sync)/Future<Response>
  - TLV: TAG_ROUTE, TAG_ID, TAG_BODY, TAG_ROUTE_REPLY
- request_stream(route: string, body: bytes) -> Stream<ResponseChunk>
  - Supports sequenced chunks with TAG_SEQ, TAG_STREAM_END
- register_worker(route: string, handler) -> subscription ack (server-side worker API)

Semantics/acceptance tests:

- single-request → single-reply correlation via TAG_ID
- streaming responses reassembled and ordered by TAG_SEQ
- backpressure & RPC_BACKPRESSURE behavior

### KV Domain (Durable key-value) 🗂️

Purpose: simple durable CRUD and range operations.

Client methods:

- put(route: string, key: string, value: bytes) -> ok
  - TLV: TAG_ROUTE, TAG_BODY (value), KEY in route
- get(route: string, key: string) -> Option<bytes>
- delete(route: string, key: string) -> ok
- scan(route_prefix: string, start_key?: string, end_key?: string, limit?: u32) -> List<(key,value)>
- delete_range(route_prefix: string, start_key: string, end_key: string) -> count

Semantics/acceptance tests:

- put/get/delete correctness and persistence across restarts
- scan returns lexicographically ordered pairs

### Lease Domain (Ephemeral coordination) 🔐

Purpose: in-memory exclusive leases (acquire/renew/release).

Client methods:

- acquire(route: string, ttl_secs: u32) -> { token: bytes, expires_at: timestamp } | LEASE_HELD
  - TLV: TAG_ROUTE, TAG_TTL
- renew(route: string, token: bytes, ttl_secs: u32) -> { expires_at } | INVALID_TOKEN
- release(route: string, token: bytes) -> ok | INVALID_TOKEN

Semantics/acceptance tests:

- acquire grants token when free; held returns LEASE_HELD
- renew extends expiry only with valid token
- release removes ownership

---

## Domain-level Acceptance Tests (additions) ✅

Client implementations MUST include automated tests for each domain covering the bullets above. Tests must be runnable over WebSocket and TCP transports and included in the canonical test harness.

---

## Constants & TLV Registry (canonical) 🧾

This section collects the canonical numeric constants clients must implement. Where the repo already defines values they are referenced; where not, we propose ranges and concrete values that should be added to a machine-readable registry (`docs/specs/tlv_registry.toml`) and to the source code as constants.

### 1) Channel IDs (u8) 🔢

| ChannelId | Value | Purpose                               |
| --------- | ----: | ------------------------------------- |
| Control   |     0 | Control/handshake/connection messages |
| Pub       |     1 | Publishing/notice traffic             |
| Sub       |     2 | Subscriptions / delivery traffic      |
| Rpc       |     3 | RPC request/response traffic          |
| Lease     |     4 | Lease domain messages                 |
| Internal  |     5 | Internal/engine-only messages         |

> Source: `src/protocol/frame.rs` — use `ChannelId::from_u8()` for decoding.

### 2) Top-level Message Types (MessageType / u16) ✉️

- `MessageType` is a u16. The TLV codec encodes small values as single-byte types; values > 0xFE are encoded (escape + u16 BE).
- Canonical control message:
  - CONNECT = 1 (use `MessageType::CONNECT` in code)

Per-domain mapping: each domain defines its own `MessageType` values for message fields (the broker uses numeric message types as the per-field TLV key). Examples:

- RPC request (common mapping):
  - 1 = correlation_id
  - 2 = route
  - 3 = reply_inbox
  - 4 = body
- RPC response chunk mapping:
  - 10 = correlation_id
  - 11 = seq
  - 12 = stream_end flag
  - 13 = body
- KV domain example (from code): 103 used for `kv get` operation in `frame_context` (clients should map domain ops to agreed message types).

> Recommendation: publish a definitive per-domain `message_type` table in `docs/specs/tlv_registry.toml` so clients and codecs can be auto-generated.

### 3) Common TLV Tags (in-frame keys; hex values) 🏷️

| Tag name   |  Hex | Meaning                                              |
| ---------- | ---: | ---------------------------------------------------- |
| TAG_TOKEN  | 0x10 | Auth token / small metadata (JWT or JSON)            |
| TAG_ROUTE  | 0x20 | Route string (e.g., `stream://realm/area/res/op`)    |
| TAG_ID     | 0x21 | Correlation / message id                             |
| TAG_BODY   | 0x22 | Body / payload bytes                                 |
| TAG_STATUS | 0x30 | Status string ("ok" / "error")                       |
| TAG_ERROR  | 0x31 | Error symbolic name (e.g., `ERR_SUBSCRIPTION_LIMIT`) |
| TAG_ERR    | 0x3F | Generic error compound (code + message)              |

Domain-specific tags (Notice):
| TAG_SUBSCRIBE | 0x90 | Subscribe marker (presence indicates subscribe) |
| TAG_UNSUBSCRIBE | 0x91 | Unsubscribe marker |
| TAG_NOTICE_MARK | 0x92 | Optional notice marker tag |

> Note: Some domains also use small varint/JSON/CBOR encoded fields under the 0x10 slot; ensure your parser supports common encodings.

### 4) Error Code Ranges & Examples 🚨

Domains use numeric ranges to avoid collisions. Known canonical values:

- Stream domain (2xxx):
  - 2001 = ERR_CONCURRENCY_CONFLICT
  - 2002 = ERR_OFFSET_TOO_FAR_AHEAD
  - 2003 = ERR_INVALID_READ_BOUND
  - 2004 = ERR_READ_BEYOND_WATERMARK
- Notice domain (3xxx):
  - 3001 = ERR_INVALID_NOTICE_ROUTE
  - 3002 = ERR_INVALID_NOTICE_PATTERN
  - 3003 = ERR_SUBSCRIPTION_LIMIT
  - 3004 = ERR_TRANSPORT_CLOSED
- Queue domain suggestion (4xxx): reserve 4000–4099
  - e.g., 4001 = QUEUE_INVALID_TOKEN
- Control domain (5xxx):
  - 5001 = ERR_INVALID_HEARTBEAT
  - 5002 = ERR_METRICS_TOO_LARGE
  - 5003 = ERR_INVALID_CONFIG
  - 5004 = ERR_SHUTDOWN_IN_PROGRESS
  - 5005 = ERR_CONTROL_PLANE_UNAVAILABLE
- Lease domain suggestion (6xxx): reserve 6000–6099
  - e.g., 6001 = LEASE_HELD, 6002 = INVALID_TOKEN, 6003 = LEASE_EXPIRED

> Action: We'll add a definitive mapping file (`docs/specs/tlv_registry.toml`) and a generated header for client languages so these codes are authoritative and single-sourced.

### 5) Formatting & Conventions ✅

- All constants are documented in the TLV registry and in code as named constants/enums.
- Clients MUST parse unknown numeric values robustly: unknown MessageType → treat as opaque field; unknown TAG → ignore but preserve if proxying.
- Error frames include both `TAG_ERR` (numeric code) and `TAG_ERROR` (symbolic string) for human readability.

---

> Next action options: (1) add concrete TLV frame byte-level examples per method, (2) draft the acceptance test harness (examples + runner), or (3) add concise pseudo-code quickstarts for each domain. Which should I do next? 🚀
