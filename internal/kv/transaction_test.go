package kv

import (
	"context"
	"testing"

	"github.com/cntryl/cntryl-go/internal/transport"
	"github.com/stretchr/testify/assert"
)

// MockMux implements a simple mock for transport.Mux for testing.
type MockMux struct {
	lastFrameSent *transport.Frame
	sendErr       error
}

func (m *MockMux) Send(f transport.Frame) error {
	m.lastFrameSent = &f
	return m.sendErr
}

func (m *MockMux) In() <-chan transport.Frame {
	return make(<-chan transport.Frame)
}

func (m *MockMux) Close() error {
	return nil
}

func (m *MockMux) Ctx() context.Context {
	return context.Background()
}

// TestShouldSendPutRequestGivenReadWriteTxWhenPutCalled tests Put sends immediately.
func TestShouldSendPutRequestGivenReadWriteTxWhenPutCalled(t *testing.T) {
	// Arrange
	mockMux := &MockMux{}
	route := NewRoute("testRealm", "testArea", "testResource")
	tx := &transaction{
		route:    route,
		mux:      mockMux,
		readOnly: false,
		txID:     1,
	}

	ctx := context.Background()
	key := []byte("key1")
	value := []byte("value1")

	// Act
	err := tx.Put(ctx, key, value)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, mockMux.lastFrameSent)
	assert.Equal(t, FrameTypeReq, mockMux.lastFrameSent.Type)
	assert.Equal(t, ChannelKV, mockMux.lastFrameSent.Channel)
}

// TestShouldReturnErrorGivenReadOnlyTxWhenPutCalled tests Put on read-only transaction.
func TestShouldReturnErrorGivenReadOnlyTxWhenPutCalled(t *testing.T) {
	// Arrange
	route := NewRoute("testRealm", "testArea", "testResource")
	tx := &transaction{
		route:    route,
		mux:      nil,
		readOnly: true,
		txID:     1,
	}

	ctx := context.Background()

	// Act
	err := tx.Put(ctx, []byte("key"), []byte("value"))

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot mutate in read-only transaction")
}

// TestShouldSendDeleteRequestGivenReadWriteTxWhenDeleteCalled tests Delete sends immediately.
func TestShouldSendDeleteRequestGivenReadWriteTxWhenDeleteCalled(t *testing.T) {
	// Arrange
	mockMux := &MockMux{}
	route := NewRoute("testRealm", "testArea", "testResource")
	tx := &transaction{
		route:    route,
		mux:      mockMux,
		readOnly: false,
		txID:     1,
	}

	ctx := context.Background()
	key := []byte("keyToDelete")

	// Act
	err := tx.Delete(ctx, key)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, mockMux.lastFrameSent)
	assert.Equal(t, FrameTypeReq, mockMux.lastFrameSent.Type)
}

// TestShouldSendInsertRequestGivenReadWriteTxWhenInsertCalled tests Insert sends immediately.
func TestShouldSendInsertRequestGivenReadWriteTxWhenInsertCalled(t *testing.T) {
	// Arrange
	mockMux := &MockMux{}
	route := NewRoute("testRealm", "testArea", "testResource")
	tx := &transaction{
		route:    route,
		mux:      mockMux,
		readOnly: false,
		txID:     1,
	}

	ctx := context.Background()
	key := []byte("newKey")
	value := []byte("newValue")

	// Act
	err := tx.Insert(ctx, key, value)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, mockMux.lastFrameSent)
}

// TestShouldSendDeleteRangeRequestGivenReadWriteTxWhenDeleteRangeCalled tests DeleteRange sends immediately.
func TestShouldSendDeleteRangeRequestGivenReadWriteTxWhenDeleteRangeCalled(t *testing.T) {
	// Arrange
	mockMux := &MockMux{}
	route := NewRoute("testRealm", "testArea", "testResource")
	tx := &transaction{
		route:    route,
		mux:      mockMux,
		readOnly: false,
		txID:     1,
	}

	ctx := context.Background()
	startKey := []byte("start")
	endKey := []byte("end")

	// Act
	count, err := tx.DeleteRange(ctx, startKey, endKey)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
	assert.NotNil(t, mockMux.lastFrameSent)
}

// TestShouldReturnNoopGivenReadOnlyTxWhenCommitCalled tests Commit on read-only transaction.
func TestShouldReturnNoopGivenReadOnlyTxWhenCommitCalled(t *testing.T) {
	// Arrange
	route := NewRoute("testRealm", "testArea", "testResource")
	tx := &transaction{
		route:    route,
		mux:      nil,
		readOnly: true,
		txID:     1,
	}

	ctx := context.Background()

	// Act
	err := tx.Commit(ctx)

	// Assert
	assert.NoError(t, err)
	assert.True(t, tx.committed.Load())
}

// TestShouldSendCommitRequestGivenReadWriteTxWhenCommitCalled tests Commit sends signal.
func TestShouldSendCommitRequestGivenReadWriteTxWhenCommitCalled(t *testing.T) {
	// Arrange
	mockMux := &MockMux{}
	route := NewRoute("testRealm", "testArea", "testResource")
	tx := &transaction{
		route:    route,
		mux:      mockMux,
		readOnly: false,
		txID:     1,
	}

	ctx := context.Background()

	// Act
	err := tx.Commit(ctx)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, mockMux.lastFrameSent)
	assert.True(t, tx.committed.Load())
}

// TestShouldReturnErrorGivenEmptyKeyWhenPutCalled tests Put with empty key.
func TestShouldReturnErrorGivenEmptyKeyWhenPutCalled(t *testing.T) {
	// Arrange
	mockMux := &MockMux{}
	route := NewRoute("testRealm", "testArea", "testResource")
	tx := &transaction{
		route:    route,
		mux:      mockMux,
		readOnly: false,
		txID:     1,
	}

	ctx := context.Background()

	// Act
	err := tx.Put(ctx, []byte{}, []byte("value"))

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key cannot be empty")
}

// TestShouldReturnErrorGivenRolledBackTxWhenCommitCalled tests Commit after Rollback.
func TestShouldReturnErrorGivenRolledBackTxWhenCommitCalled(t *testing.T) {
	// Arrange
	mockMux := &MockMux{}
	route := NewRoute("testRealm", "testArea", "testResource")
	tx := &transaction{
		route:    route,
		mux:      mockMux,
		readOnly: false,
		txID:     1,
	}

	ctx := context.Background()

	// Act: Rollback first.
	errRollback := tx.Rollback(ctx)
	assert.NoError(t, errRollback)

	// Then try to Commit.
	errCommit := tx.Commit(ctx)

	// Assert
	assert.Error(t, errCommit)
	assert.Contains(t, errCommit.Error(), "already rolled back")
}

// TestShouldReturnErrorGivenCommittedTxWhenRollbackCalled tests Rollback after Commit.
func TestShouldReturnErrorGivenCommittedTxWhenRollbackCalled(t *testing.T) {
	// Arrange
	mockMux := &MockMux{}
	route := NewRoute("testRealm", "testArea", "testResource")
	tx := &transaction{
		route:    route,
		mux:      mockMux,
		readOnly: true, // Read-only so Commit succeeds without broker interaction.
		txID:     1,
	}

	ctx := context.Background()

	// Act: Commit first.
	errCommit := tx.Commit(ctx)
	assert.NoError(t, errCommit)

	// Then try to Rollback.
	errRollback := tx.Rollback(ctx)

	// Assert
	assert.Error(t, errRollback)
	assert.Contains(t, errRollback.Error(), "already committed")
}

// TestShouldReturnErrorGivenLimitZeroWhenScanCalled tests Scan with zero limit.
func TestShouldReturnErrorGivenLimitZeroWhenScanCalled(t *testing.T) {
	// Arrange
	route := NewRoute("testRealm", "testArea", "testResource")
	tx := &transaction{
		route:    route,
		mux:      nil,
		readOnly: true,
		txID:     1,
	}

	ctx := context.Background()

	// Act
	_, err := tx.Scan(ctx, []byte("start"), []byte("end"), 0)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "limit must be > 0")
}

// TestShouldReturnErrorGivenReadOnlyTxWhenDeleteCalled tests Delete on read-only transaction.
func TestShouldReturnErrorGivenReadOnlyTxWhenDeleteCalled(t *testing.T) {
	// Arrange
	route := NewRoute("testRealm", "testArea", "testResource")
	tx := &transaction{
		route:    route,
		mux:      nil,
		readOnly: true,
		txID:     1,
	}

	ctx := context.Background()

	// Act
	err := tx.Delete(ctx, []byte("key"))

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot mutate in read-only transaction")
}

// TestShouldReturnErrorGivenReadOnlyTxWhenInsertCalled tests Insert on read-only transaction.
func TestShouldReturnErrorGivenReadOnlyTxWhenInsertCalled(t *testing.T) {
	// Arrange
	route := NewRoute("testRealm", "testArea", "testResource")
	tx := &transaction{
		route:    route,
		mux:      nil,
		readOnly: true,
		txID:     1,
	}

	ctx := context.Background()

	// Act
	err := tx.Insert(ctx, []byte("key"), []byte("value"))

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot mutate in read-only transaction")
}

// TestShouldReturnErrorGivenReadOnlyTxWhenDeleteRangeCalled tests DeleteRange on read-only transaction.
func TestShouldReturnErrorGivenReadOnlyTxWhenDeleteRangeCalled(t *testing.T) {
	// Arrange
	route := NewRoute("testRealm", "testArea", "testResource")
	tx := &transaction{
		route:    route,
		mux:      nil,
		readOnly: true,
		txID:     1,
	}

	ctx := context.Background()

	// Act
	_, err := tx.DeleteRange(ctx, []byte("start"), []byte("end"))

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot mutate in read-only transaction")
}

// TestShouldNotAllowCommitTwiceWhenCommitCalledTwice tests double Commit prevention.
func TestShouldNotAllowCommitTwiceWhenCommitCalledTwice(t *testing.T) {
	// Arrange
	mockMux := &MockMux{}
	route := NewRoute("testRealm", "testArea", "testResource")
	tx := &transaction{
		route:    route,
		mux:      mockMux,
		readOnly: true,
		txID:     1,
	}

	ctx := context.Background()

	// Act: First commit succeeds.
	err1 := tx.Commit(ctx)
	assert.NoError(t, err1)

	// Second commit should fail.
	err2 := tx.Commit(ctx)

	// Assert
	assert.Error(t, err2)
	assert.Contains(t, err2.Error(), "already committed")
}

// TestShouldReturnErrorGivenCommittedTxWhenPutCalled tests Put after Commit.
func TestShouldReturnErrorGivenCommittedTxWhenPutCalled(t *testing.T) {
	// Arrange
	mockMux := &MockMux{}
	route := NewRoute("testRealm", "testArea", "testResource")
	tx := &transaction{
		route:    route,
		mux:      mockMux,
		readOnly: false,
		txID:     1,
	}

	ctx := context.Background()

	// Act: Commit first.
	_ = tx.Commit(ctx)

	// Then try Put.
	err := tx.Put(ctx, []byte("key"), []byte("value"))

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already committed")
}

// TestShouldReturnErrorGivenRolledBackTxWhenPutCalled tests Put after Rollback.
func TestShouldReturnErrorGivenRolledBackTxWhenPutCalled(t *testing.T) {
	// Arrange
	mockMux := &MockMux{}
	route := NewRoute("testRealm", "testArea", "testResource")
	tx := &transaction{
		route:    route,
		mux:      mockMux,
		readOnly: false,
		txID:     1,
	}

	ctx := context.Background()

	// Act: Rollback first.
	_ = tx.Rollback(ctx)

	// Then try Put.
	err := tx.Put(ctx, []byte("key"), []byte("value"))

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already rolled back")
}

// TestShouldSendRollbackRequestGivenReadWriteTxWhenRollbackCalled tests Rollback sends signal.
func TestShouldSendRollbackRequestGivenReadWriteTxWhenRollbackCalled(t *testing.T) {
	// Arrange
	mockMux := &MockMux{}
	route := NewRoute("testRealm", "testArea", "testResource")
	tx := &transaction{
		route:    route,
		mux:      mockMux,
		readOnly: false,
		txID:     1,
	}

	ctx := context.Background()

	// Act
	err := tx.Rollback(ctx)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, mockMux.lastFrameSent)
	assert.True(t, tx.rolledback.Load())
}

// TestShouldReturnErrorGivenEmptyStartKeyWhenDeleteRangeCalled tests DeleteRange with empty startKey.
func TestShouldReturnErrorGivenEmptyStartKeyWhenDeleteRangeCalled(t *testing.T) {
	// Arrange
	mockMux := &MockMux{}
	route := NewRoute("testRealm", "testArea", "testResource")
	tx := &transaction{
		route:    route,
		mux:      mockMux,
		readOnly: false,
		txID:     1,
	}

	ctx := context.Background()

	// Act
	_, err := tx.DeleteRange(ctx, []byte{}, []byte("end"))

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be empty")
}
