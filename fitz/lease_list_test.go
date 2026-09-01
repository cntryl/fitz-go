package fitz

import (
	"context"
	"testing"

	internaliter "github.com/cntryl/fitz-go/v2/internal/core/iter"
	internallease "github.com/cntryl/fitz-go/v2/internal/domains/lease"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeLeaseClientForList is a minimal fake of internallease.Client, following
// this repo's established public-API-adapter test convention (see
// fitz/rpc_test.go's fakeRPCClient): it lets us prove, without a live broker,
// that the public fitz.LeaseClient.List method is reachable, compiles against
// the exported LeaseEntry/ListOption/Iterator types, and correctly forwards
// results from the internal domain client.
type fakeLeaseClientForList struct {
	internallease.Client

	listIter internaliter.Iterator[internallease.LeaseEntry]
	listErr  error
	gotOpts  internallease.ListOptions
}

func (f *fakeLeaseClientForList) List(ctx context.Context, pattern string, opts ...internallease.ListOption) (internaliter.Iterator[internallease.LeaseEntry], error) {
	for _, opt := range opts {
		opt(&f.gotOpts)
	}
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.listIter, nil
}

// TestShouldExposeLeaseListThroughPublicClientGivenExternalConsumerUsage proves
// that an external consumer of the canonical fitz package (using only the
// public LeaseClient interface, LeaseEntry, ListOption/WithListLimit, and
// Iterator types) can compile and drive client.Lease().List(...) end to end.
func TestShouldExposeLeaseListThroughPublicClientGivenExternalConsumerUsage(t *testing.T) {
	fakeEntry := internallease.LeaseEntry{
		Route:             "lease://acme/renderers/doc-1",
		OwnerID:           "worker-7",
		HolderIncarnation: 3,
		AcquiredAt:        "2026-08-29T00:00:00Z",
		ExpiresInSecs:     30,
		Renewals:          2,
	}
	fake := &fakeLeaseClientForList{
		listIter: internaliter.NewSliceIterator([]internallease.LeaseEntry{fakeEntry}),
	}

	var client LeaseClient = &leaseClient{inner: fake}

	it, err := client.List(context.Background(), "lease://acme/renderers/*", WithListLimit(50))
	require.NoError(t, err)
	defer func() { _ = it.Close() }()

	require.True(t, it.Next())
	entry := it.Value()
	assert.Equal(t, LeaseEntry{
		Route:             fakeEntry.Route,
		OwnerID:           fakeEntry.OwnerID,
		HolderIncarnation: fakeEntry.HolderIncarnation,
		AcquiredAt:        fakeEntry.AcquiredAt,
		ExpiresInSecs:     fakeEntry.ExpiresInSecs,
		Renewals:          fakeEntry.Renewals,
	}, entry)
	assert.False(t, it.Next())
	require.NoError(t, it.Err())
	assert.Equal(t, uint32(50), fake.gotOpts.Limit)
}

func TestShouldSurfaceListErrorGivenInnerFailureWhenListCalled(t *testing.T) {
	fake := &fakeLeaseClientForList{listErr: internallease.ErrInvalidListPattern}
	var client LeaseClient = &leaseClient{inner: fake}

	_, err := client.List(context.Background(), "lease://acme/x/y")
	require.ErrorIs(t, err, internallease.ErrInvalidListPattern)
	require.ErrorIs(t, err, ErrLeaseInvalidListPattern)
}
