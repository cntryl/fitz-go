package fitz

import (
	"context"
	"testing"

	"github.com/cntryl/fitz-go/v2/internal/core/iter"
	internalrpc "github.com/cntryl/fitz-go/v2/internal/domains/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeRPCClient struct {
	callIter iter.Iterator[internalrpc.ResponseFrame]
	callErr  error
}

func (f *fakeRPCClient) RegisterWorker(context.Context, string, uint32, internalrpc.RPCHandler) (*internalrpc.Subscription, error) {
	return nil, nil
}

func (f *fakeRPCClient) Call(context.Context, string, []byte) (iter.Iterator[internalrpc.ResponseFrame], error) {
	if f.callErr != nil {
		return nil, f.callErr
	}
	return f.callIter, nil
}

func TestShouldForwardRPCResponseFramesGivenIteratorWhenCallCalled(t *testing.T) {
	fakeFrame := internalrpc.ResponseFrame{Body: []byte("hello"), Sequence: 7}
	client := &rpcClient{inner: &fakeRPCClient{callIter: iter.NewSliceIterator([]internalrpc.ResponseFrame{fakeFrame})}}

	it, err := client.Call(context.Background(), "rpc://acme/echo", []byte("ignored"))
	require.NoError(t, err)
	defer func() { _ = it.Close() }()

	require.True(t, it.Next())
	frame := it.Value()
	assert.Equal(t, fakeFrame.Sequence, frame.Sequence)
	assert.Equal(t, fakeFrame.Body, frame.Body)
	assert.False(t, it.Next())
}
