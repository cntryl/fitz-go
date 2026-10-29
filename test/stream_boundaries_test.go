//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/fitz"
	"github.com/cntryl/fitz-go/test/fixture"
	"github.com/stretchr/testify/require"
)

func TestShouldPreserveStreamEventMetadataGivenAppendWhenReplayed(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		f.ConnectOrFail(ctx)

		route := f.UniqueRoute("stream")
		session, err := f.Client().Stream().Begin(ctx, route)
		require.NoError(t, err)
		metadata := []byte("event metadata")
		_, err = session.Append(ctx, 0, []byte("event"), fitz.WithStreamMetadata(metadata))
		require.NoError(t, err)
		_, err = session.Append(ctx, 1, []byte("empty"), fitz.WithStreamMetadata([]byte{}))
		require.NoError(t, err)
		_, err = session.Append(ctx, 2, []byte("absent"))
		require.NoError(t, err)
		require.NoError(t, session.Commit(ctx, fitz.StreamCommitSync))

		iter, err := f.Client().Stream().Read(ctx, route, 0, 10)
		require.NoError(t, err)
		defer closeQuietly(iter)
		require.True(t, iter.Next())
		require.Equal(t, metadata, iter.Value().Metadata)
		require.True(t, iter.Next())
		require.NotNil(t, iter.Value().Metadata)
		require.Empty(t, iter.Value().Metadata)
		require.True(t, iter.Next())
		require.Nil(t, iter.Value().Metadata)
		require.False(t, iter.Next())
		require.NoError(t, iter.Err())
	})
}

func TestShouldEnforceStreamEventBoundaryGivenLongRouteWhenReplayedInPages(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		f.ConnectOrFail(ctx)

		prefix := f.UniqueRoute("stream")
		route := prefix + strings.Repeat("r", 4096-len(prefix))
		metadata := []byte("0123456789abcdef")
		body := make([]byte, 400*1024-len(metadata))
		for i := range body {
			body[i] = 42
		}
		for offset := range 3 {
			session, err := f.Client().Stream().Begin(ctx, route)
			require.NoError(t, err)
			if offset == 0 {
				oversized := make([]byte, len(body)+1)
				_, err = session.Append(ctx, 0, oversized, fitz.WithStreamMetadata(metadata))
				var brokerError *fitz.DomainError
				require.ErrorAs(t, err, &brokerError)
				require.EqualValues(t, 2008, brokerError.Code)
				require.Zero(t, brokerError.Effect)
			}
			body[0] = byte(offset)
			assigned, err := session.Append(ctx, uint64(offset), body, fitz.WithStreamMetadata(metadata))
			require.NoError(t, err)
			require.Equal(t, uint64(offset), assigned)
			require.NoError(t, session.Commit(ctx, fitz.StreamCommitSync))
		}

		var records []fitz.StreamRecord
		var next uint64
		pages := 0
		for {
			page, err := f.Client().Stream().ReadPage(ctx, route, next, 10)
			require.NoError(t, err)
			pages++
			for _, item := range page.Items {
				if item.Kind == fitz.StreamReadItemEvent {
					require.NotNil(t, item.Record)
					records = append(records, *item.Record)
				}
			}
			if !page.Cursor.HasMore {
				break
			}
			require.GreaterOrEqual(t, page.Cursor.LastResourceOffset, next)
			next = page.Cursor.LastResourceOffset + 1
		}
		require.Greater(t, pages, 1)
		require.Len(t, records, 3)
		for offset, record := range records {
			require.Equal(t, route, record.Route)
			require.Equal(t, uint64(offset), record.Offset)
			require.Len(t, record.Body, len(body))
			require.Equal(t, byte(offset), record.Body[0])
			require.Equal(t, body[1:], record.Body[1:])
			require.Equal(t, metadata, record.Metadata)
		}
	})
}

func TestShouldRetainGlobalOffsetsGivenFilteredReplayWhenReadingAcrossRealms(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		f.ConnectOrFail(ctx)

		routes := []string{f.UniqueRoute("stream"), f.UniqueRoute("stream")}
		discriminator := fmt.Sprintf("global-%d", time.Now().UnixNano())
		for index, route := range routes {
			session, err := f.Client().Stream().Begin(ctx, route)
			require.NoError(t, err)
			_, err = session.Append(ctx, 0, []byte{byte(index)}, fitz.WithStreamDiscriminator(discriminator))
			require.NoError(t, err)
			require.NoError(t, session.Commit(ctx, fitz.StreamCommitSync))
		}

		filter := fitz.StreamFilterSet{Clauses: []fitz.StreamFilterClause{{Kind: fitz.StreamFilterEquals, Value: discriminator}}}
		var records []fitz.StreamRecord
		var next uint64
		var cursorFingerprint, capturedWatermark *uint64
		for {
			options := []fitz.StreamReadOption{fitz.WithStreamFilter(filter)}
			if cursorFingerprint != nil && capturedWatermark != nil {
				options = append(options, fitz.WithStreamCursor(*cursorFingerprint, *capturedWatermark))
			}
			page, err := f.Client().Stream().ReadPage(ctx, "stream://**", next, 100, options...)
			require.NoError(t, err)
			for _, item := range page.Items {
				if item.Kind == fitz.StreamReadItemEvent {
					require.NotNil(t, item.Record)
					require.NotNil(t, item.Record.GlobalOffset)
					records = append(records, *item.Record)
				}
			}
			if !page.Cursor.HasMore {
				break
			}
			require.NotNil(t, page.Cursor.LastGlobalOffset)
			require.GreaterOrEqual(t, *page.Cursor.LastGlobalOffset, next)
			next = *page.Cursor.LastGlobalOffset + 1
			cursorFingerprint = page.Cursor.CursorFingerprint
			capturedWatermark = page.Cursor.CapturedWatermark
		}
		require.Len(t, records, 2)
		require.Equal(t, routes[0], records[0].Route)
		require.Equal(t, routes[1], records[1].Route)
		require.Less(t, *records[0].GlobalOffset, *records[1].GlobalOffset)

		iter, err := f.Client().Stream().Read(ctx, "stream://**", *records[0].GlobalOffset, 10, fitz.WithStreamFilter(filter))
		require.NoError(t, err)
		defer closeQuietly(iter)
		require.True(t, iter.Next())
		require.NotNil(t, iter.Value().GlobalOffset)
		require.Equal(t, *records[0].GlobalOffset, *iter.Value().GlobalOffset)
	})
}
