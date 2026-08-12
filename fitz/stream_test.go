package fitz

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShouldUseGlobalCursorGivenFilteredGlobalSelector(t *testing.T) {
	globalOffset := uint64(17)
	cursor := StreamReadCursor{LastResourceOffset: 3, LastGlobalOffset: &globalOffset}

	for _, selector := range []string{
		"stream://*/area/resource",
		"stream://*/area/*",
		"stream://*/*/resource",
	} {
		t.Run(selector, func(t *testing.T) {
			assert.Equal(t, globalOffset+1, streamNextOffset(selector, 3, cursor))
		})
	}
}

func TestShouldKeepOffsetGivenFilteredGlobalPageWithoutProgress(t *testing.T) {
	cursor := StreamReadCursor{LastResourceOffset: 17}

	assert.Equal(t, uint64(3), streamNextOffset("stream://*/area/*", 3, cursor))
}
