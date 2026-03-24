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
