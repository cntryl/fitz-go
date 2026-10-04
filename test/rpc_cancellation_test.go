//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/fitz"
	"github.com/cntryl/fitz-go/test/fixture"
	"github.com/stretchr/testify/require"
)

func TestShouldPropagateCancellationAndDeadlinesThroughSDKChain(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		for _, reason := range []string{"cancel", "deadline"} {
			t.Run(reason, func(t *testing.T) {
				// Arrange
				a, b, c := fixture.NewTestFixture(t, transport), fixture.NewTestFixture(t, transport), fixture.NewTestFixture(t, transport)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				a.ConnectOrFail(ctx)
				b.ConnectOrFail(ctx)
				c.ConnectOrFail(ctx)
				middle, leaf := a.UniqueRoute("rpc"), c.UniqueRoute("rpc")
				started, middleCleaned, leafCleaned := make(chan struct{}), make(chan struct{}), make(chan struct{})
				unrelatedStarted, unrelatedRelease := make(chan context.Context, 1), make(chan struct{})
				budgets := make(chan time.Duration, 2)
				leafSub, err := c.Client().RPC().RegisterWorker(ctx, leaf, 2, func(handlerCtx context.Context, request fitz.RPCInboundRequest, writer fitz.RPCResponseWriter) error {
					switch string(request.Body) {
					case "unrelated":
						unrelatedStarted <- handlerCtx
						select {
						case <-unrelatedRelease:
							return writer.Send(request.Body)
						case <-handlerCtx.Done():
							return handlerCtx.Err()
						}
					case "probe":
						return writer.Send(request.Body)
					default:
						deadline, ok := handlerCtx.Deadline()
						if !ok {
							return context.DeadlineExceeded
						}
						budgets <- time.Until(deadline)
						close(started)
						<-handlerCtx.Done()
						close(leafCleaned)
						return handlerCtx.Err()
					}
				})
				require.NoError(t, err)
				defer leafSub.Deregister()
				middleSub, err := b.Client().RPC().RegisterWorker(ctx, middle, 1, func(handlerCtx context.Context, request fitz.RPCInboundRequest, writer fitz.RPCResponseWriter) error {
					if string(request.Body) == "target" {
						deadline, ok := handlerCtx.Deadline()
						if !ok {
							return context.DeadlineExceeded
						}
						budgets <- time.Until(deadline)
						defer close(middleCleaned)
					}
					child, callErr := b.Client().RPC().Call(handlerCtx, leaf, request.Body)
					if callErr != nil {
						return callErr
					}
					defer child.Close()
					for child.Next() {
						if sendErr := writer.Send(child.Value().Body); sendErr != nil {
							return sendErr
						}
					}
					return child.Err()
				})
				require.NoError(t, err)
				defer middleSub.Deregister()
				unrelated, err := a.Client().RPC().Call(ctx, leaf, []byte("unrelated"))
				require.NoError(t, err)
				defer unrelated.Close()
				var unrelatedContext context.Context
				select {
				case unrelatedContext = <-unrelatedStarted:
				case <-ctx.Done():
					t.Fatal("unrelated handler did not start")
				}
				budget := 5 * time.Second
				if reason == "deadline" {
					budget = 750 * time.Millisecond
				}
				callCtx, stopCall := context.WithTimeout(ctx, budget)
				defer stopCall()
				call, err := a.Client().RPC().Call(callCtx, middle, []byte("target"))
				require.NoError(t, err)
				defer call.Close()
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal("chain did not start")
				}
				parentBudget, leafBudget := <-budgets, <-budgets

				// Act
				if reason == "cancel" {
					stopCall()
				}
				require.False(t, call.Next())
				select {
				case <-middleCleaned:
				case <-ctx.Done():
					t.Fatal("middle cleanup did not finish")
				}
				select {
				case <-leafCleaned:
				case <-ctx.Done():
					t.Fatal("leaf cleanup did not finish")
				}

				// Assert
				require.Positive(t, leafBudget)
				require.LessOrEqual(t, leafBudget, parentBudget)
				require.NoError(t, unrelatedContext.Err())
				close(unrelatedRelease)
				require.True(t, unrelated.Next())
				require.Equal(t, []byte("unrelated"), unrelated.Value().Body)
				probe, err := a.Client().RPC().Call(ctx, middle, []byte("probe"))
				require.NoError(t, err)
				defer probe.Close()
				require.True(t, probe.Next())
				require.Equal(t, []byte("probe"), probe.Value().Body)
			})
		}
	})
}
