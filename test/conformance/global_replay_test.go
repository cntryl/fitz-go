//go:build integration

package conformance

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/fitz"
)

func runGlobalFilteredReplay(t *testing.T) (Verdict, []string, error) {
	t.Helper()
	f := connectFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	stream := f.Client().Stream()
	initial, err := stream.ReadPage(ctx, "stream://**", 0, 1)
	if err != nil {
		return VerdictFail, nil, fmt.Errorf("read initial global watermark: %w", err)
	}
	if initial.Cursor.CapturedWatermark == nil {
		return VerdictFail, nil, fmt.Errorf("initial global watermark is missing")
	}
	next := *initial.Cursor.CapturedWatermark
	firstRoute := uniqueRoute("stream")
	secondRoute := strings.Replace(firstRoute, "stream://conformance/", "stream://other-realm/", 1)
	discriminator := "match-" + firstRoute
	for _, route := range []string{firstRoute, secondRoute} {
		session, beginErr := stream.Begin(ctx, route)
		if beginErr != nil {
			return VerdictFail, nil, beginErr
		}
		if _, appendErr := session.Append(ctx, 0, []byte("skip"), fitz.WithStreamDiscriminator("skip")); appendErr != nil {
			return VerdictFail, nil, appendErr
		}
		if _, appendErr := session.Append(ctx, 1, []byte("keep"), fitz.WithStreamDiscriminator(discriminator)); appendErr != nil {
			return VerdictFail, nil, appendErr
		}
		if commitErr := session.Commit(ctx, fitz.StreamCommitSync); commitErr != nil {
			return VerdictFail, nil, commitErr
		}
	}

	filter := fitz.StreamFilterSet{Clauses: []fitz.StreamFilterClause{{Kind: fitz.StreamFilterEquals, Value: discriminator}}}
	var records []fitz.StreamRecord
	var fingerprint, watermark *uint64
	filteredOnly := false
	pages := 0
	for {
		options := []fitz.StreamReadOption{fitz.WithStreamFilter(filter)}
		if fingerprint != nil && watermark != nil {
			options = append(options, fitz.WithStreamCursor(*fingerprint, *watermark))
		}
		page, readErr := stream.ReadPage(ctx, "stream://**", next, 1, options...)
		if readErr != nil {
			return VerdictFail, nil, readErr
		}
		pages++
		pageEvents := 0
		for _, item := range page.Items {
			if item.Kind == fitz.StreamReadItemEvent && item.Record != nil {
				pageEvents++
				records = append(records, *item.Record)
			}
		}
		filteredOnly = filteredOnly || (len(page.Items) > 0 && pageEvents == 0)
		if !page.Cursor.HasMore {
			break
		}
		if page.Cursor.LastGlobalOffset == nil || *page.Cursor.LastGlobalOffset < next {
			return VerdictFail, nil, fmt.Errorf("global cursor failed to advance from %d", next)
		}
		next = *page.Cursor.LastGlobalOffset + 1
		fingerprint, watermark = page.Cursor.CursorFingerprint, page.Cursor.CapturedWatermark
	}
	if pages < 2 || !filteredOnly || len(records) != 2 || records[0].Route != firstRoute || records[1].Route != secondRoute {
		return VerdictFail, nil, fmt.Errorf("unexpected replay: pages=%d filtered_only=%t records=%d", pages, filteredOnly, len(records))
	}
	if records[0].GlobalOffset == nil || records[1].GlobalOffset == nil || *records[0].GlobalOffset >= *records[1].GlobalOffset {
		return VerdictFail, nil, fmt.Errorf("global event offsets missing or out of order")
	}
	return VerdictPass, []string{
		fmt.Sprintf("global filtered replay yielded %d pages across two realms", pages),
		"filtered-only pages advanced the global cursor through the captured tail",
	}, nil
}
