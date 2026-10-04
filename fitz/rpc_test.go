package fitz

import (
	"context"
	"testing"

	"github.com/cntryl/fitz-go/internal/core/iter"
	internalrpc "github.com/cntryl/fitz-go/internal/domains/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeRPCClient struct {
	callIter internalrpc.CallIterator
	callErr  error
}

type fakeRPCCallIterator struct {
	iter.Iterator[internalrpc.ResponseFrame]
}

func (fakeRPCCallIterator) CancellationResult() <-chan internalrpc.CancellationOutcome {
	result := make(chan internalrpc.CancellationOutcome, 1)
	result <- internalrpc.CancellationNotRequested
	close(result)
	return result
}

func (f *fakeRPCClient) RegisterWorker(context.Context, string, uint32, internalrpc.RPCHandler) (*internalrpc.Subscription, error) {
	return nil, nil
}

func (f *fakeRPCClient) Call(context.Context, string, []byte) (internalrpc.CallIterator, error) {
	if f.callErr != nil {
		return nil, f.callErr
	}
	return f.callIter, nil
}

func TestShouldForwardRPCResponseFramesGivenIteratorWhenCallCalled(t *testing.T) {
	fakeFrame := internalrpc.ResponseFrame{Body: []byte("hello"), Sequence: 7}
	client := &rpcClient{inner: &fakeRPCClient{callIter: fakeRPCCallIterator{iter.NewSliceIterator([]internalrpc.ResponseFrame{fakeFrame})}}}

	it, err := client.Call(context.Background(), "rpc://acme/echo", []byte("ignored"))
	require.NoError(t, err)
	defer func() { _ = it.Close() }()

	require.True(t, it.Next())
	frame := it.Value()
	assert.Equal(t, fakeFrame.Sequence, frame.Sequence)
	assert.Equal(t, fakeFrame.Body, frame.Body)
	assert.False(t, it.Next())
}
