package bench

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/v2/internal/core/connection"
	"github.com/cntryl/fitz-go/v2/internal/core/subscriptions"
	"github.com/cntryl/fitz-go/v2/internal/protocol"
)

type echoTransport struct {
	frames chan []byte
	closed chan struct{}
	once   sync.Once
}

func newEchoTransport(buffer int) *echoTransport {
	return &echoTransport{
		frames: make(chan []byte, buffer),
		closed: make(chan struct{}),
	}
}

func (t *echoTransport) Write(ctx context.Context, frame []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.closed:
		return io.EOF
	default:
	}

	msgType, _, err := protocol.DecodeFrame(frame)
	if err != nil {
		return err
	}
	if msgType == protocol.MessageTypeConnect {
		return nil
	}

	resp := protocol.EncodeFrame(msgType, []byte{0})
	select {
	case t.frames <- resp:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-t.closed:
		return io.EOF
	}
}

func (t *echoTransport) Read(ctx context.Context) ([]byte, error) {
	select {
	case frame := <-t.frames:
		return frame, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.closed:
		return nil, io.EOF
	}
}

func (t *echoTransport) Close() error {
	t.once.Do(func() {
		close(t.closed)
	})
	return nil
}

func (t *echoTransport) RemoteAddr() string {
	return "bench://echo"
}

func benchmarkConnectionConfig() connection.Config {
	return connection.Config{
		AuthSettleDelay: 1 * time.Millisecond,
		ReadTimeout:     -1,
		WriteTimeout:    -1,
	}
}

func benchmarkSendRequestHotPath(b *testing.B, msgType uint16, payload []byte) {
	b.Helper()

	ctx := context.Background()
	trans := newEchoTransport(1024)
	conn := connection.New(trans, benchmarkConnectionConfig())
	if err := conn.Start(ctx); err != nil {
		b.Fatalf("start connection: %v", err)
	}
	defer closeQuietly(conn)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := conn.SendRequest(ctx, msgType, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFrameEncode(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 128)
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		frame := protocol.EncodeFrameOwned(protocol.MessageTypeKvPut, payload)
		frame.Release()
	}
}

func BenchmarkKVRoundTrip(b *testing.B) {
	msgType := protocol.MessageTypeKvGet
	payload := bytes.Repeat([]byte("k"), 32)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		frame := protocol.EncodeFrame(msgType, payload)
		decodedType, decodedPayload, err := protocol.DecodeFrame(frame)
		if err != nil {
			b.Fatal(err)
		}
		if decodedType != msgType || len(decodedPayload) != len(payload) {
			b.Fatal("unexpected decode output")
		}
	}
}

func BenchmarkRPCCorrelation(b *testing.B) {
	mux := connection.NewMultiplexer()
	msgType := protocol.MessageTypeRpcRequest
	resp := []byte("ok")

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		ch := make(chan []byte, 1)
		mux.RegisterRequest(msgType, ch, nil)
		mux.Dispatch(msgType, resp)
		<-ch
	}
}

func BenchmarkNoticeDispatch(b *testing.B) {
	mux := connection.NewMultiplexer()
	var delivered atomic.Int64

	mux.SetNotifyHandler(protocol.MessageTypeNoticeNotify, func(subID uint64, route string, payload []byte) {
		delivered.Add(1)
	})

	route := "notice://bench/area/resource"
	body := []byte("payload")
	payload := make([]byte, 8+4+len(route)+4+len(body))
	off := 0
	binary.BigEndian.PutUint64(payload[off:off+8], 1)
	off += 8
	binary.BigEndian.PutUint32(payload[off:off+4], uint32(len(route)))
	off += 4
	copy(payload[off:off+len(route)], []byte(route))
	off += len(route)
	binary.BigEndian.PutUint32(payload[off:off+4], uint32(len(body)))
	off += 4
	copy(payload[off:off+len(body)], body)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		mux.Dispatch(protocol.MessageTypeNoticeNotify, payload)
	}
	b.StopTimer()
	if delivered.Load() != int64(b.N) {
		b.Fatalf("unexpected delivery count: got %d want %d", delivered.Load(), b.N)
	}
}

func BenchmarkConnectionSendRequest(b *testing.B) {
	benchmarkSendRequestHotPath(b, protocol.MessageTypeQueueReserve, []byte{1, 2, 3, 4})
}

func BenchmarkQueueReserveHotPath(b *testing.B) {
	benchmarkSendRequestHotPath(b, protocol.MessageTypeQueueReserve, []byte("queue-reserve"))
}

func BenchmarkQueueCompleteHotPath(b *testing.B) {
	benchmarkSendRequestHotPath(b, protocol.MessageTypeQueueComplete, []byte("queue-complete"))
}

func BenchmarkKVGetHotPath(b *testing.B) {
	ctx := context.Background()
	trans := newEchoTransport(1024)
	conn := connection.New(trans, benchmarkConnectionConfig())
	if err := conn.Start(ctx); err != nil {
		b.Fatalf("start connection: %v", err)
	}
	defer closeQuietly(conn)

	payload := []byte{0x01}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := conn.SendRequest(ctx, protocol.MessageTypeKvGet, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKVPutHotPath(b *testing.B) {
	ctx := context.Background()
	trans := newEchoTransport(1024)
	conn := connection.New(trans, benchmarkConnectionConfig())
	if err := conn.Start(ctx); err != nil {
		b.Fatalf("start connection: %v", err)
	}
	defer closeQuietly(conn)

	payload := []byte{0x01, 0x02}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := conn.SendRequest(ctx, protocol.MessageTypeKvPut, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLeaseAcquireHotPath(b *testing.B) {
	ctx := context.Background()
	trans := newEchoTransport(1024)
	conn := connection.New(trans, benchmarkConnectionConfig())
	if err := conn.Start(ctx); err != nil {
		b.Fatalf("start connection: %v", err)
	}
	defer closeQuietly(conn)

	payload := []byte{0x01, 0x02, 0x03}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := conn.SendRequest(ctx, protocol.MessageTypeLeaseAcquire, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStreamBeginHotPath(b *testing.B) {
	benchmarkSendRequestHotPath(b, protocol.MessageTypeStreamBegin, []byte("stream://bench/realm/area/resource"))
}

func BenchmarkStreamAppendHotPath(b *testing.B) {
	benchmarkSendRequestHotPath(b, protocol.MessageTypeStreamAppend, bytes.Repeat([]byte("x"), 64))
}

func BenchmarkScheduleCreateHotPath(b *testing.B) {
	benchmarkSendRequestHotPath(b, protocol.MessageTypeScheduleCreate, []byte("schedule://bench/realm/area/resource"))
}

func BenchmarkScheduleCancelHotPath(b *testing.B) {
	benchmarkSendRequestHotPath(b, protocol.MessageTypeScheduleCancel, []byte("schedule://bench/realm/area/resource"))
}

func BenchmarkSubscriptionRegistryRestore(b *testing.B) {
	registry := subscriptions.NewRegistry[string]()
	for idx := range 16 {
		pattern := fmt.Sprintf("notice://bench/%02d/resource", idx)
		_, _, err := registry.Subscribe(pattern, "handler", func(string) (uint64, error) {
			return uint64(idx + 1), nil
		})
		if err != nil {
			b.Fatalf("seed subscription: %v", err)
		}
	}

	var nextID atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := registry.Restore(
			func(string) (uint64, error) {
				return nextID.Add(1), nil
			},
			func(string, uint64) error { return nil },
		); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNoticePublishHotPath(b *testing.B) {
	ctx := context.Background()
	trans := newEchoTransport(1024)
	conn := connection.New(trans, benchmarkConnectionConfig())
	if err := conn.Start(ctx); err != nil {
		b.Fatalf("start connection: %v", err)
	}
	defer closeQuietly(conn)

	payload := []byte("bench-publish")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := conn.SendFireAndForget(ctx, protocol.MessageTypeNoticePublish, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRPCCallHotPath(b *testing.B) {
	ctx := context.Background()
	trans := newEchoTransport(1024)
	conn := connection.New(trans, benchmarkConnectionConfig())
	if err := conn.Start(ctx); err != nil {
		b.Fatalf("start connection: %v", err)
	}
	defer closeQuietly(conn)

	payload := []byte("rpc-call")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := conn.SendRequest(ctx, protocol.MessageTypeRpcRequest, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRPCCorrelation1KInFlight(b *testing.B) {
	mux := connection.NewMultiplexer()
	msgType := protocol.MessageTypeRpcRequest

	const inflight = 1024
	chs := make([]chan []byte, inflight)
	for i := range inflight {
		ch := make(chan []byte, 1)
		chs[i] = ch
		mux.RegisterRequest(msgType, ch, nil)
	}

	resp := []byte("ok")
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		idx := i % inflight
		mux.Dispatch(msgType, resp)
		<-chs[idx]
		mux.RegisterRequest(msgType, chs[idx], nil)
	}
}

func BenchmarkKVTransactionLoopback(b *testing.B) {
	ctx := context.Background()
	trans := newEchoTransport(2048)
	conn := connection.New(trans, benchmarkConnectionConfig())
	if err := conn.Start(ctx); err != nil {
		b.Fatalf("start connection: %v", err)
	}
	defer closeQuietly(conn)

	beginPayload := []byte{0x01}
	putPayload := []byte{0x02}
	commitPayload := []byte{0x03}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := conn.SendRequest(ctx, protocol.MessageTypeKvBegin, beginPayload); err != nil {
			b.Fatal(err)
		}
		if _, err := conn.SendRequest(ctx, protocol.MessageTypeKvPut, putPayload); err != nil {
			b.Fatal(err)
		}
		if _, err := conn.SendRequest(ctx, protocol.MessageTypeKvCommit, commitPayload); err != nil {
			b.Fatal(err)
		}
	}
}
