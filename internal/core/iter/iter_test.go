//nolint:gosec,errcheck
package iter

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShouldIterateAllItemsGivenSliceIteratorWhenForEachCalled verifies
// ForEach iterates over all items in a SliceIterator.
func TestShouldIterateAllItemsGivenSliceIteratorWhenForEachCalled(t *testing.T) {
	// Arrange
	items := []int{1, 2, 3, 4, 5}
	it := NewSliceIterator(items)
	var collected []int

	// Act
	err := ForEach(it, func(v int) error {
		collected = append(collected, v)
		return nil
	})

	// Assert
	require.NoError(t, err)
	assert.Equal(t, items, collected)
}

// TestShouldStopOnErrorGivenCallbackReturnsErrorWhenForEachCalled verifies
// ForEach stops iteration when the callback returns an error.
func TestShouldStopOnErrorGivenCallbackReturnsErrorWhenForEachCalled(t *testing.T) {
	// Arrange
	items := []int{1, 2, 3, 4, 5}
	it := NewSliceIterator(items)
	var collected []int
	expectedErr := errors.New("test error")

	// Act
	err := ForEach(it, func(v int) error {
		collected = append(collected, v)
		if v == 3 {
			return expectedErr
		}
		return nil
	})

	// Assert
	require.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Equal(t, []int{1, 2, 3}, collected, "should stop after error")
}

// TestShouldCloseIteratorGivenForEachCompletesWhenCalled verifies
// ForEach closes the iterator after successful iteration.
func TestShouldCloseIteratorGivenForEachCompletesWhenCalled(t *testing.T) {
	// Arrange
	it := &trackingIterator{items: []int{1, 2, 3}}

	// Act
	err := ForEach(it, func(v int) error {
		return nil
	})

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 1, it.closeCount)
}

// TestShouldReturnIteratorErrorGivenIteratorFailsWhenForEachCalled verifies
// ForEach returns iterator errors after successful callbacks.
func TestShouldReturnIteratorErrorGivenIteratorFailsWhenForEachCalled(t *testing.T) {
	// Arrange - create a mock failing iterator
	it := &failingIterator{
		items: []int{1, 2, 3},
		err:   errors.New("iterator error"),
	}

	// Act
	err := ForEach(it, func(v int) error {
		return nil
	})

	// Assert
	require.Error(t, err)
	assert.Equal(t, "iterator error", err.Error())
}

// --- Mock failing iterator for testing ---

type failingIterator struct {
	items []int
	index int
	err   error
}

type trackingIterator struct {
	items      []int
	index      int
	closeCount int
}

func (it *failingIterator) Next() bool {
	it.index++
	return it.index < len(it.items)
}

func (it *failingIterator) Value() int {
	return it.items[it.index]
}

func (it *failingIterator) Err() error {
	return it.err
}

func (it *failingIterator) Close() error {
	return nil
}

func (it *trackingIterator) Next() bool {
	it.index++
	return it.index < len(it.items)
}

func (it *trackingIterator) Value() int {
	return it.items[it.index]
}

func (it *trackingIterator) Err() error {
	return nil
}

func (it *trackingIterator) Close() error {
	it.closeCount++
	return nil
}
