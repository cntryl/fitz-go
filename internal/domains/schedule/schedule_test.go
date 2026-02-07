package schedule

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/cntryl/cntryl-go/internal/core/transport"
	"github.com/stretchr/testify/require"
)

func TestShouldReturnParsedScheduleEntriesGivenTLVWhenDecoding(t *testing.T) {
	// Arrange
	enc := transport.NewTLVEncoder()
	enc.AddUint64(transport.TagID, 123)
	enc.AddString(transport.TagRoute, "a")
	enc.AddString(transport.TagBody, "cron")
	enc.AddUint64(transport.TagID, 456)
	enc.AddString(transport.TagRoute, "b")
	enc.AddString(transport.TagBody, "cron2")
	b := enc.Encode()

	// Act
	dec, err := transport.NewTLVDecoder(b)

	// Assert
	require.NoError(t, err)
	ids := dec.GetAll(transport.TagID)
	routes := dec.GetAll(transport.TagRoute)
	crons := dec.GetAll(transport.TagBody)

	entries := make([]ScheduleEntry, 0, len(ids))
	for i := range ids {
		idstr := ""
		if len(ids[i]) == 8 {
			id := binary.BigEndian.Uint64(ids[i])
			idstr = fmt.Sprintf("%d", id)
		} else {
			idstr = string(ids[i])
		}
		e := ScheduleEntry{ID: idstr}
		if i < len(routes) {
			e.Route = string(routes[i])
		}
		if i < len(crons) {
			e.CronExpr = string(crons[i])
		}
		entries = append(entries, e)
	}

	require.Len(t, entries, 2)
	require.Equal(t, "123", entries[0].ID)
	require.Equal(t, "456", entries[1].ID)
	require.Equal(t, "a", entries[0].Route)
	require.Equal(t, "cron2", entries[1].CronExpr)
}
