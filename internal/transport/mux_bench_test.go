package transport

import (
	"io"
	"net"
	"testing"
)

// nopWriteCloser wraps an io.Writer and implements io.ReadWriteCloser with a
// no-op Close and Read returning EOF.
type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Read([]byte) (int, error) { return 0, io.EOF }
func (nopWriteCloser) Close() error             { return nil }

func BenchmarkEncodeFrame_Serial(b *testing.B) {
	m := NewMux(nopWriteCloser{Writer: io.Discard})
	frame := Frame{Type: FrameTypeConnOpen, Flags: 0, Channel: 1, Body: make([]byte, 256)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := m.encodeFrame(frame); err != nil {
			b.Fatalf("encode failed: %v", err)
		}
	}
}

func BenchmarkMux_SendParallel(b *testing.B) {
	c1, c2 := net.Pipe()
	m1 := NewMux(c1)
	m2 := NewMux(c2)
	m1.Start()
	m2.Start()
	defer func() { _ = m1.Close(); _ = m2.Close(); c1.Close(); c2.Close() }()

	// consume inbound frames to avoid blocking at receiver side
	go func() {
		for range m2.In() {
			// discard
		}
	}()

	frame := Frame{Type: FrameTypeConnOpen, Flags: 0, Channel: 1, Body: []byte("x")}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := m1.Send(frame); err != nil {
				b.Fatalf("send failed: %v", err)
			}
		}
	})
}
