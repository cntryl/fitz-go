//go:build integration

package conformance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/fitz"
)

func runSameTypeReserves(t *testing.T) (Verdict, []string, error) {
	t.Helper()
	f := connectFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	queue := f.Client().Queue()
	parkedRoute, readyRoute := uniqueRoute("queue"), uniqueRoute("queue")
	if _, err := queue.Enqueue(ctx, readyRoute, []byte("second")); err != nil {
		return VerdictFail, nil, err
	}
	type result struct {
		items []*fitz.QueueItem
		err   error
	}
	parked := make(chan result, 1)
	go func() {
		items, err := queue.ReserveWithOptions(ctx, parkedRoute, 30, fitz.WithQueueReserveBatchSize(1), fitz.WithQueueReserveWaitSeconds(5))
		parked <- result{items: items, err: err}
	}()
	time.Sleep(100 * time.Millisecond)
	select {
	case result := <-parked:
		return VerdictFail, nil, fmt.Errorf("first reserve did not park: %v", result.err)
	default:
	}
	secondCtx, secondCancel := context.WithTimeout(ctx, 2*time.Second)
	defer secondCancel()
	second, err := queue.ReserveWithOptions(secondCtx, readyRoute, 30, fitz.WithQueueReserveBatchSize(1), fitz.WithQueueReserveWaitSeconds(0))
	if err != nil || len(second) != 1 || string(second[0].Body) != "second" {
		return VerdictFail, nil, fmt.Errorf("second reserve: items=%d error=%v", len(second), err)
	}
	if _, err := queue.Enqueue(ctx, parkedRoute, []byte("first")); err != nil {
		return VerdictFail, nil, err
	}
	first := <-parked
	if first.err != nil || len(first.items) != 1 || string(first.items[0].Body) != "first" {
		return VerdictFail, nil, fmt.Errorf("first reserve: items=%d error=%v", len(first.items), first.err)
	}
	return VerdictPass, []string{"second reserve resolved first with its own payload", "parked first reserve resolved with its own payload"}, nil
}

func runStreamBoundary(t *testing.T) (Verdict, []string, error) {
	t.Helper()
	f := connectFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream := f.Client().Stream()
	prefix := uniqueRoute("stream")
	route := prefix + strings.Repeat("r", 4096-len(prefix))
	metadata := []byte("0123456789abcdef")
	body := bytes.Repeat([]byte{42}, 400*1024-len(metadata))
	for offset := range 3 {
		session, err := stream.Begin(ctx, route)
		if err != nil {
			return VerdictFail, nil, err
		}
		if offset == 0 {
			_, err = session.Append(ctx, 0, make([]byte, len(body)+1), fitz.WithStreamMetadata(metadata))
			var domain *fitz.DomainError
			if !errors.As(err, &domain) || uint32(domain.Code) != 2008 || domain.Effect != 0 {
				return VerdictFail, nil, fmt.Errorf("oversized event did not return BAD_REQUEST/NOT_APPLIED: %v", err)
			}
		}
		assigned, err := session.Append(ctx, uint64(offset), body, fitz.WithStreamMetadata(metadata))
		if err != nil || assigned != uint64(offset) {
			return VerdictFail, nil, fmt.Errorf("boundary append %d: offset=%d error=%v", offset, assigned, err)
		}
		if err := session.Commit(ctx, fitz.StreamCommitSync); err != nil {
			return VerdictFail, nil, err
		}
	}
	var next uint64
	pages, records := 0, 0
	for {
		page, err := stream.ReadPage(ctx, route, next, 10)
		if err != nil {
			return VerdictFail, nil, err
		}
		pages++
		for _, item := range page.Items {
			if item.Kind != fitz.StreamReadItemEvent || item.Record == nil || item.Record.Route != route ||
				item.Record.Offset != uint64(records) || !bytes.Equal(item.Record.Body, body) || !bytes.Equal(item.Record.Metadata, metadata) {
				return VerdictFail, nil, fmt.Errorf("replayed boundary event %d did not match", records)
			}
			records++
		}
		if !page.Cursor.HasMore {
			break
		}
		if page.Cursor.LastResourceOffset < next {
			return VerdictFail, nil, errors.New("resource cursor did not advance")
		}
		next = page.Cursor.LastResourceOffset + 1
	}
	if pages < 2 || records != 3 {
		return VerdictFail, nil, fmt.Errorf("replayed %d events in %d pages", records, pages)
	}
	return VerdictPass, []string{"409600 combined bytes accepted; 409601 rejected with BAD_REQUEST/NOT_APPLIED", "three complete events replayed in order on a 4096-byte route across multiple pages"}, nil
}
