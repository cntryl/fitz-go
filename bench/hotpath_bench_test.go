package bench

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/protocol"
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

func BenchmarkFrameEncode(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 128)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		frame := protocol.EncodeFrameOwned(protocol.MessageTypeKvPut, payload)
		frame.Release()
	}
}

func BenchmarkKVRoundTrip(b *testing.B) {
	msgType := protocol.MessageTypeKvGet
	payload := bytes.Repeat([]byte("k"), 32)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
	for i := 0; i < b.N; i++ {
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
	for i := 0; i < b.N; i++ {
		mux.Dispatch(protocol.MessageTypeNoticeNotify, payload)
	}
	b.StopTimer()
	if delivered.Load() != int64(b.N) {
		b.Fatalf("unexpected delivery count: got %d want %d", delivered.Load(), b.N)
	}
}

func BenchmarkConnectionSendRequest(b *testing.B) {
	ctx := context.Background()
	trans := newEchoTransport(1024)
	conn := connection.New(trans, connection.Config{
		AuthSettleDelay: 1 * time.Millisecond,
		ReadTimeout:     0,
		WriteTimeout:    0,
	})

	if err := conn.Start(ctx); err != nil {
		b.Fatalf("start connection: %v", err)
	}
	defer conn.Close()

	payload := []byte{1, 2, 3, 4}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.SendRequest(ctx, protocol.MessageTypeQueueReserve, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKVGetHotPath(b *testing.B) {
	ctx := context.Background()
	trans := newEchoTransport(1024)
	conn := connection.New(trans, connection.Config{AuthSettleDelay: 1 * time.Millisecond})
	if err := conn.Start(ctx); err != nil {
		b.Fatalf("start connection: %v", err)
	}
	defer conn.Close()

	payload := []byte{0x01}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.SendRequest(ctx, protocol.MessageTypeKvGet, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKVPutHotPath(b *testing.B) {
	ctx := context.Background()
	trans := newEchoTransport(1024)
	conn := connection.New(trans, connection.Config{AuthSettleDelay: 1 * time.Millisecond})
	if err := conn.Start(ctx); err != nil {
		b.Fatalf("start connection: %v", err)
	}
	defer conn.Close()

	payload := []byte{0x01, 0x02}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.SendRequest(ctx, protocol.MessageTypeKvPut, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLeaseAcquireHotPath(b *testing.B) {
	ctx := context.Background()
	trans := newEchoTransport(1024)
	conn := connection.New(trans, connection.Config{AuthSettleDelay: 1 * time.Millisecond})
	if err := conn.Start(ctx); err != nil {
		b.Fatalf("start connection: %v", err)
	}
	defer conn.Close()

	payload := []byte{0x01, 0x02, 0x03}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.SendRequest(ctx, protocol.MessageTypeLeaseAcquire, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNoticePublishHotPath(b *testing.B) {
	ctx := context.Background()
	trans := newEchoTransport(1024)
	conn := connection.New(trans, connection.Config{AuthSettleDelay: 1 * time.Millisecond})
	if err := conn.Start(ctx); err != nil {
		b.Fatalf("start connection: %v", err)
	}
	defer conn.Close()

	payload := []byte("bench-publish")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := conn.SendFireAndForget(ctx, protocol.MessageTypeNoticePublish, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRPCCallHotPath(b *testing.B) {
	ctx := context.Background()
	trans := newEchoTransport(1024)
	conn := connection.New(trans, connection.Config{AuthSettleDelay: 1 * time.Millisecond})
	if err := conn.Start(ctx); err != nil {
		b.Fatalf("start connection: %v", err)
	}
	defer conn.Close()

	payload := []byte("rpc-call")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
	for i := 0; i < inflight; i++ {
		ch := make(chan []byte, 1)
		chs[i] = ch
		mux.RegisterRequest(msgType, ch, nil)
	}

	resp := []byte("ok")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % inflight
		mux.Dispatch(msgType, resp)
		<-chs[idx]
		mux.RegisterRequest(msgType, chs[idx], nil)
	}
}

func BenchmarkKVTransactionLoopback(b *testing.B) {
	ctx := context.Background()
	trans := newEchoTransport(2048)
	conn := connection.New(trans, connection.Config{AuthSettleDelay: 1 * time.Millisecond})
	if err := conn.Start(ctx); err != nil {
		b.Fatalf("start connection: %v", err)
	}
	defer conn.Close()

	beginPayload := []byte{0x01}
	putPayload := []byte{0x02}
	commitPayload := []byte{0x03}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
