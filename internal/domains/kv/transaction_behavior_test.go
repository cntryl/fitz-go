package kv

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/v2/internal/core/connection"
	"github.com/cntryl/fitz-go/v2/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type transactionScriptTransport struct {
	mu     sync.Mutex
	writes int
	readCh chan []byte
	closed chan struct{}
	close  sync.Once
}

func newTransactionScriptTransport() *transactionScriptTransport {
	return &transactionScriptTransport{readCh: make(chan []byte, 1), closed: make(chan struct{})}
}

func (s *transactionScriptTransport) Write(ctx context.Context, _ []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closed:
		return connection.ErrConnectionClosed
	default:
	}
	s.mu.Lock()
	s.writes++
	s.mu.Unlock()
	return nil
}

func (s *transactionScriptTransport) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.closed:
		return nil, connection.ErrConnectionClosed
	case frame := <-s.readCh:
		return frame, nil
	}
}

func (s *transactionScriptTransport) Close() error {
	s.close.Do(func() { close(s.closed) })
	return nil
}

func (s *transactionScriptTransport) RemoteAddr() string { return "scripted://kv-transaction" }

func (s *transactionScriptTransport) writeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writes
}

func TestShouldCloseTransactionGivenRejectedCommitWhenCommitCalled(t *testing.T) {
	transport := newTransactionScriptTransport()
	conn := connection.New(transport, connection.Config{ReadTimeout: time.Second})
	require.NoError(t, conn.Start(context.Background()))
	t.Cleanup(func() { _ = conn.Close() })
	tx := &transaction{route: "kv://realm/area/resource", conn: conn, txID: 7}
	baseWrites := transport.writeCount()
	go func() {
		require.Eventually(t, func() bool { return transport.writeCount() >= baseWrites+1 }, time.Second, 10*time.Millisecond)
		transport.readCh <- protocol.EncodeFrame(protocol.MessageTypeKvCommit, kvDomainErrorPayload(2001, "commit rejected"))
	}()

	err := tx.Commit(context.Background())

	require.Error(t, err)
	assert.True(t, tx.committed.Load())
	assert.NoError(t, tx.Rollback(context.Background()))
	assert.Equal(t, baseWrites+1, transport.writeCount())
}

func kvDomainErrorPayload(code uint32, message string) []byte {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	buf.WriteByte(1)
	connection.WriteU32BE(buf, code)
	connection.WriteString(buf, message)
	return append([]byte(nil), buf.Bytes()...)
}
