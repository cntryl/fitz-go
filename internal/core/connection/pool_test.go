package connection

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldReturnDefaultCapacityGivenNewPoolWhenGetCalled(t *testing.T) {
	pool := NewByteSlicePool()

	b := pool.Get()

	require.NotNil(t, b)
	assert.Len(t, *b, 0)
	assert.Equal(t, 1024, cap(*b))
}

func TestShouldResetLengthGivenSliceWhenPutCalled(t *testing.T) {
	pool := NewByteSlicePool()
	b := make([]byte, 3, 8)
	copy(b, []byte("abc"))

	pool.Put(&b)

	assert.Len(t, b, 0)
	assert.Equal(t, 8, cap(b))
}

func TestShouldLeaveOversizedSliceUntouchedGivenPutCalled(t *testing.T) {
	pool := NewByteSlicePool()
	b := make([]byte, 5, 64*1024+1)
	copy(b, []byte("hello"))

	pool.Put(&b)

	assert.Len(t, b, 5)
	assert.Equal(t, 64*1024+1, cap(b))
}

func TestShouldAllocateRequestedCapacityGivenTooSmallPoolSliceWhenGetWithCapacityCalled(t *testing.T) {
	pool := NewByteSlicePool()

	b := pool.GetWithCapacity(2048)

	require.NotNil(t, b)
	assert.Len(t, *b, 0)
	assert.Equal(t, 2048, cap(*b))
}
