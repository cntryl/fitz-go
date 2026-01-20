# Complete Flow: Client Creation → Connection → KV Operations

## Step-by-Step Walkthrough

### 1. Create the Client

```go
package main

import (
    "context"
    "fmt"
    "log"
    
    fitz "github.com/cntryl/cntryl-go"
    "github.com/cntryl/cntryl-go/internal/client"
)

func main() {
    // Define a token provider function
    // This is called when connecting/reconnecting to get fresh JWT
    tokenProvider := func(ctx context.Context) (string, error) {
        // In production: fetch from auth service, read from file, etc.
        // For unauthenticated connections, return ""
        return "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...", nil
    }
    
    // Create the client
    // Address format determines transport:
    //   - "localhost:9090" or "tcp://localhost:9090" → TCP
    //   - "ws://localhost:9090/ws" → WebSocket
    //   - "wss://broker.example.com/ws" → Secure WebSocket (recommended)
    c := client.NewClient(
        "wss://broker.example.com/ws",  // broker address
        tokenProvider,                   // auth token function
        client.WithRetryBackoff(200*time.Millisecond),
        client.WithMaxRetries(3),
    )
```

**What happens here:**
- `NewClient` creates a client instance with:
  - Address: determines TCP vs WebSocket transport
  - TokenProvider: function for JWT tokens (supports renewal on reconnect)
  - Options: retry policy, custom dialer for testing, etc.
- **No network activity yet** - just configuration

---

### 2. Establish Connection

```go
    ctx := context.Background()
    
    // Connect to the broker
    if err := c.Connect(ctx); err != nil {
        log.Fatalf("failed to connect: %v", err)
    }
    defer c.Close()
    
    fmt.Println("✅ Connected to broker")
```

**What happens during `Connect()`:**

1. **Transport Layer** (`internal/client/dialer.go`):
   - Parses address to determine protocol (tcp, ws, wss)
   - For TCP: uses `net.Dialer.DialContext()`
   - For WebSocket: uses `websocket.Dial()` from `golang.org/x/net/websocket`
   - Returns `net.Conn` (bidirectional stream)

2. **Retry Logic** (`internal/client/client.go`):
   - Attempts connection with exponential backoff
   - Respects context cancellation
   - Configurable via `WithRetryBackoff()` and `WithMaxRetries()`

3. **Mux Initialization** (`internal/transport/mux.go`):
   ```go
   c.mux = transport.NewMux(conn)  // wraps net.Conn
   c.mux.Start()                   // launches read/write loops
   ```
   - Creates TLV frame multiplexer
   - Starts two goroutines:
     - `readLoop()`: reads frames from connection → `inCh`
     - `writeLoop()`: writes frames from `outCh` → connection
   - Buffered channels (128 frames) for flow control

4. **Handshake** (TODO in current code):
   - Should call `tokenProvider(ctx)` to get JWT
   - Send `CONN_OPEN` frame with `TAG_TOKEN`
   - Wait for `ACK` or `ERR` from broker
   - Register channel mapping for request/response routing

5. **Domain Clients Initialization**:
   - `initializeDomainClients()` creates KV, Stream, Queue, etc.
   - Each domain client gets reference to the mux for sending/receiving frames

---

### 3. Get the KV Client

```go
    // Get the KV domain client
    kvClient := c.KV()
```

**What happens:**
- Thread-safe accessor returns the initialized KV client
- KV client has internal reference to `mux` for frame I/O
- No separate connection - reuses the multiplexed transport

---

### 4. Work with KV - Read-Only Transaction

```go
    // Begin a read-only transaction
    // Create a strongly-typed route using the domain's Route type
    route := kv.NewRoute("prod", "users", "profiles")
    
    readTx, err := kvClient.BeginRead(ctx, route)
    if err != nil {
        log.Fatalf("failed to begin read tx: %v", err)
    }
    defer readTx.Rollback(ctx)
```

**What happens during `BeginRead()`:**

1. **Frame Construction**:
   ```go
   frame := Frame{
       Type: FrameTypeRequest,      // domain-specific
       Channel: generateChannelID(), // for routing response
       Body: encodeTLV(
           TAG_ROUTE, []byte(route),
           TAG_OPERATION, []byte("begin_read"),
       ),
   }
   ```

2. **Send via Mux**:
   ```go
   mux.Send(frame)  // → writeLoop → connection
   ```

3. **Wait for Response**:
   ```go
   respFrame := <-mux.In()  // readLoop → inCh → caller
   ```

4. **Create Transaction Object**:
   - Parse response TLV (transaction ID, etc.)
   - Return `ReadTx` implementation with:
     - Transaction ID
     - Reference to mux for subsequent operations
     - Route context

---

### 5. Perform Read Operations

```go
    // Get a single key
    key := []byte("user:12345")
    value, found, err := readTx.Get(ctx, key)
    if err != nil {
        log.Fatalf("get failed: %v", err)
    }
    if found {
        fmt.Printf("Value: %s\n", value)
    }
```

**What happens during `Get()`:**

1. **Send Request**:
   ```go
   frame := Frame{
       Type: FrameTypeRequest,
       Channel: txChannelID,
       Body: encodeTLV(
           TAG_TX_ID, txID,
           TAG_OPERATION, []byte("get"),
           TAG_KEY, key,
       ),
   }
   mux.Send(frame)
   ```

2. **Receive Response**:
   ```go
   respFrame := <-mux.In()
   // Parse TLV: TAG_VALUE, TAG_FOUND, or TAG_ERR
   ```

---

### 6. Scan with Iterator

```go
    // Scan for multiple keys
    query := []byte("user:*")  // query syntax depends on implementation
    iter, err := readTx.Scan(ctx, query, 100)
    if err != nil {
        log.Fatalf("scan failed: %v", err)
    }
    defer iter.Close()
    
    // Iterate over results
    for iter.Next() {
        pair := iter.Value()
        fmt.Printf("Key: %s, Value: %s\n", pair.Key, pair.Value)
    }
    if err := iter.Err(); err != nil {
        log.Fatalf("iteration error: %v", err)
    }
```

**What happens during `Scan()`:**

1. **Streaming Response Pattern**:
   - Initial request frame with query
   - Broker streams back multiple response frames
   - Iterator manages async frame reception
   - `iter.Next()` blocks waiting for next frame

2. **Iterator Implementation** (`internal/iter/iter.go`):
   - Receives frames from `mux.In()` filtered by channel ID
   - Buffers results
   - Implements `Next()`, `Value()`, `Err()` pattern

---

### 7. Commit or Rollback

```go
    // For read-only transactions, commit is typically a no-op
    // but maintains API consistency
    if err := readTx.Commit(ctx); err != nil {
        log.Fatalf("commit failed: %v", err)
    }
    
    fmt.Println("✅ Read transaction completed")
```

---

### 8. Write Transaction

```go
    // Begin a read/write transaction
    writeTx, err := kvClient.Begin(ctx, route)
    if err != nil {
        log.Fatalf("failed to begin write tx: %v", err)
    }
    defer writeTx.Rollback(ctx)
    
    // Put operations
    if err := writeTx.Put(ctx, []byte("user:67890"), []byte(`{"name":"Alice"}`)); err != nil {
        log.Fatalf("put failed: %v", err)
    }
    
    // Insert (fails if key exists)
    if err := writeTx.Insert(ctx, []byte("user:11111"), []byte(`{"name":"Bob"}`)); err != nil {
        log.Printf("insert failed (key exists?): %v", err)
    }
    
    // Delete operations
    if err := writeTx.Delete(ctx, []byte("user:00000")); err != nil {
        log.Fatalf("delete failed: %v", err)
    }
    
    // Commit the transaction
    if err := writeTx.Commit(ctx); err != nil {
        log.Fatalf("commit failed: %v", err)
    }
    
    fmt.Println("✅ Write transaction committed")
```

**What happens:**
- Same frame-based protocol as reads
- Each mutation sends a frame with operation + data
- All operations are buffered until `Commit()`
- Broker applies atomically on commit

---

### 9. Cleanup

```go
    // Close the client when done
    if err := c.Close(); err != nil {
        log.Printf("close error: %v", err)
    }
}
```

**What happens during `Close()`:**

1. **Send CONN_CLOSE frame** (when implemented)
2. **Mux shutdown**:
   - Cancel context → stops read/write loops
   - Close channels (`inCh`, `outCh`)
   - Close underlying `net.Conn`
3. **Domain clients cleanup** (if needed)

---

## Key Architectural Points

### Bidirectional Communication
- **Single connection** for all operations (TCP or WebSocket)
- **Multiplexing** via channel IDs in frames
- **Independent read/write loops** in mux
- **Concurrent operations** supported (multiple domain clients, transactions)

### Connection Flow
```
Application
    ↓
fitz.Client (interface)
    ↓
internal/client.Client (concrete impl)
    ↓
Dialer (TCP or WebSocket)
    ↓
net.Conn (bidirectional stream)
    ↓
transport.Mux (frame multiplexer)
    ↓ ↑
readLoop ←→ writeLoop
    ↓ ↑
Domain Clients (KV, Stream, Queue, etc.)
```

### KV Operation Flow
```
kvClient.BeginRead(route)
    ↓
Construct frame with TAG_ROUTE, TAG_OPERATION
    ↓
mux.Send(frame) → writeLoop → net.Conn → broker
    ↓
readLoop ← net.Conn ← broker response
    ↓
mux.In() ← response frame
    ↓
Parse TLV → return ReadTx
```

### Current State
✅ **Implemented:**
- Client creation with token provider
- TCP and WebSocket dialer
- Connection with retry/backoff
- Mux with bidirectional frame I/O
- Domain client interfaces (KV, Stream, etc.)

🚧 **TODO:**
- CONN_OPEN handshake with JWT
- Domain client concrete implementations
- TLV encoding/decoding helpers
- Error code handling per CLIENT_SPEC.md
- Heartbeat integration
