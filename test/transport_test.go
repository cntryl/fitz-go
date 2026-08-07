//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/v2/fitz"
	coretransport "github.com/cntryl/fitz-go/v2/internal/core/transport"
	"github.com/cntryl/fitz-go/v2/internal/protocol"
	"github.com/cntryl/fitz-go/v2/test/fixture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldAcceptAnonymousAccessGivenAuthDisabledWhenTCPConnectCalled(t *testing.T) {
	f := fixture.NewTestFixture(t, fixture.TransportTCP)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f.ConnectOrFail(ctx)
	require.NotNil(t, f.Client())
}

func TestShouldAcceptAnonymousAccessGivenAuthDisabledWhenWebSocketConnectCalled(t *testing.T) {
	f := fixture.NewTestFixture(t, fixture.TransportWebSocket)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f.ConnectOrFail(ctx)
	require.NotNil(t, f.Client())
}

func TestShouldConnectGivenValidJWTWhenTCPConnectSent(t *testing.T) {
	f := fixture.NewTestFixture(t, fixture.TransportTCP)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	f.ConnectWithAuthOrFail(ctx, fixture.AuthModeValidJWT)

	require.NotNil(t, f.Client())
}

func TestShouldConnectGivenValidJWTWhenWebSocketConnectSent(t *testing.T) {
	f := fixture.NewTestFixture(t, fixture.TransportWebSocket)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	f.ConnectWithAuthOrFail(ctx, fixture.AuthModeValidJWT)

	require.NotNil(t, f.Client())
}

func TestShouldConnectGivenJWTWithoutSchedulePermissionWhenConnectCalled(t *testing.T) {
	fixture.RunWithTransportsOnly(t, func(t *testing.T, transportType fixture.TransportType) {
		addr, stop, err := fixture.StartBrokerIfNeeded(transportType, fixture.AuthModeValidJWT)
		require.NoError(t, err)
		t.Cleanup(stop)

		secret := os.Getenv(fixture.EnvBrokerJWTHMACSecret)
		if secret == "" {
			secret = "dev-test-secret"
		}
		audience := os.Getenv(fixture.EnvBrokerJWTAudience)
		if audience == "" {
			audience = "fitz"
		}

		token, err := fixture.GenerateScopedTestJWT(secret, audience, []string{"kv://**#*"})
		require.NoError(t, err)

		client := fitz.NewClient(addr, func(context.Context) (string, error) {
			return token, nil
		})
		t.Cleanup(func() {
			_ = client.Close()
		})

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, client.Connect(ctx))

		route := fmt.Sprintf("kv://test-%d/area/resource", time.Now().UnixNano())
		tx, err := client.KV().Begin(ctx, route, fitz.KVDurabilitySync)
		require.NoError(t, err)
		require.NoError(t, tx.Rollback(ctx))

		_, err = client.Schedule().List(ctx, nil, nil)
		require.Error(t, err)
	})
}

func TestShouldRejectExpiredJWTGivenAuthEnabledBrokerConfiguredWhenConnectCalled(t *testing.T) {
	fixture.RunWithTransportsOnly(t, func(t *testing.T, transportType fixture.TransportType) {
		f := fixture.NewTestFixture(t, transportType)
		f.SetAuthMode(fixture.AuthModeExpiredJWT)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err := f.Connect(ctx)
		require.Error(t, err)
	})
}

func TestShouldRejectInvalidSignatureJWTGivenAuthEnabledBrokerConfiguredWhenConnectCalled(t *testing.T) {
	fixture.RunWithTransportsOnly(t, func(t *testing.T, transportType fixture.TransportType) {
		f := fixture.NewTestFixture(t, transportType)
		f.SetAuthMode(fixture.AuthModeInvalidSignature)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err := f.Connect(ctx)
		require.Error(t, err)
	})
}

func TestShouldNotRetryGivenAuthenticationRejectionWhenReconnectEnabled(t *testing.T) {
	fixture.RunWithTransportsOnly(t, func(t *testing.T, transportType fixture.TransportType) {
		f := fixture.NewTestFixture(t, transportType)
		f.SetAuthMode(fixture.AuthModeInvalidSignature)
		var reconnectAttempts atomic.Int32
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err := f.ConnectWithOptions(
			ctx,
			fitz.WithReconnect(true, 10*time.Millisecond, 3),
			fitz.WithLifecycleHandler(func(event fitz.LifecycleEvent) {
				if event.Event == "reconnect_start" {
					reconnectAttempts.Add(1)
				}
			}),
		)

		require.Error(t, err)
		time.Sleep(50 * time.Millisecond)
		assert.Equal(t, int32(0), reconnectAttempts.Load())
	})
}

func TestShouldProduceIdenticalBehaviorGivenSameOperationWhenDifferentTransports(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		client := f.Client()
		require.NotNil(t, client)
		require.NotNil(t, client.Notice())
		require.NotNil(t, client.Stream())
		require.NotNil(t, client.Queue())
		require.NotNil(t, client.RPC())
		require.NotNil(t, client.KV())
		require.NotNil(t, client.Lease())
		require.NotNil(t, client.Schedule())
	})
}

func TestShouldReconnectGivenConnectionLostWhenReconnectAttempted(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		require.NoError(t, f.Client().Close())

		f2 := fixture.NewTestFixture(t, transport)
		f2.ConnectOrFail(ctx)
		require.NotNil(t, f2.Client())
	})
}

func TestShouldDropSessionStateGivenDisconnectWhenReconnect(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("notice")
		received := make(chan struct{}, 1)
		sub, err := f.Client().Notice().Subscribe(ctx, route, func(_ context.Context, _ fitz.NoticeMsg) error {
			received <- struct{}{}
			return nil
		})
		require.NoError(t, err)

		require.NoError(t, f.Client().Close())
		sub.Unsubscribe()

		f2 := fixture.NewTestFixture(t, transport)
		f2.ConnectOrFail(ctx)
		require.NotNil(t, f2.Client())
	})
}

func TestShouldNotRecoverNoticeSubscriptionGivenDisconnectWhenReconnectedWithoutResubscribe(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		subscriber := fixture.NewTestFixture(t, transport)
		reconnected := fixture.NewTestFixture(t, transport)
		publisher := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		subscriber.ConnectOrFail(ctx)
		publisher.ConnectOrFail(ctx)
		route := subscriber.UniqueRoute("notice")
		received := make(chan struct{}, 2)

		sub, err := subscriber.Client().Notice().Subscribe(ctx, route, func(_ context.Context, _ fitz.NoticeMsg) error {
			received <- struct{}{}
			return nil
		})
		require.NoError(t, err)

		require.NoError(t, publisher.Client().Notice().Publish(ctx, route, []byte("before-disconnect")))
		select {
		case <-received:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for initial notice delivery")
		}

		require.NoError(t, subscriber.Client().Close())
		sub.Unsubscribe()
		reconnected.ConnectOrFail(ctx)
		require.NoError(t, publisher.Client().Notice().Publish(ctx, route, []byte("after-disconnect")))

		select {
		case <-received:
			t.Fatal("unexpected notice delivery after reconnect without resubscribe")
		case <-time.After(750 * time.Millisecond):
		}
		require.NotNil(t, reconnected.Client())
	})
}

func TestShouldRestoreNoticeSubscriptionGivenLiveDisconnectWhenReconnectEnabled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		harness := fixture.NewProxyReconnectHarness(t, transport, fixture.AuthModeForTestName(t.Name()))
		subscriber := harness.Proxied
		publisher := harness.Stable

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		harness.Connect(ctx, fixture.DefaultReconnectOptions()...)

		route := subscriber.UniqueRoute("notice")
		var err error
		received := make(chan string, 4)
		_, err = subscriber.Client().Notice().Subscribe(ctx, route, func(_ context.Context, msg fitz.NoticeMsg) error {
			received <- string(msg.Body)
			return nil
		})
		require.NoError(t, err)

		require.NoError(t, publisher.Client().Notice().Publish(ctx, route, []byte("before-disconnect")))
		select {
		case body := <-received:
			require.Equal(t, "before-disconnect", body)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for initial notice delivery")
		}

		harness.WaitForInitialConnection(5 * time.Second)
		harness.DropAndWaitForReconnect(10 * time.Second)

		require.Eventually(t, func() bool {
			if err := publisher.Client().Notice().Publish(ctx, route, []byte("after-disconnect")); err != nil {
				return false
			}

			select {
			case body := <-received:
				return body == "after-disconnect"
			default:
				return false
			}
		}, 10*time.Second, 100*time.Millisecond)
	})
}

func TestShouldRejectNonConnectFrameGivenNewTransportWhenFrameSentBeforeAuthentication(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transportType fixture.TransportType) {
		authMode := fixture.AuthModeForTestName(t.Name())
		addr, stop, err := fixture.StartBrokerIfNeeded(transportType, authMode)
		require.NoError(t, err)
		t.Cleanup(stop)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var trans coretransport.Transport
		switch transportType {
		case fixture.TransportTCP:
			trans, err = coretransport.DialTCP(ctx, addr)
		case fixture.TransportWebSocket:
			trans, err = coretransport.DialWebSocket(ctx, addr)
		default:
			t.Fatalf("unsupported transport: %s", transportType)
		}
		require.NoError(t, err)
		t.Cleanup(func() { _ = trans.Close() })

		frame := protocol.EncodeFrameOwned(protocol.MessageTypeKvBegin, nil)
		defer frame.Release()
		require.NoError(t, trans.Write(ctx, frame.Bytes()))
		_, err = trans.Read(ctx)
		require.Error(t, err)
	})
}

func TestShouldFailConnectGivenInvalidAddressWhenConnectCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		if transport == fixture.TransportTCP {
			f.SetBrokerAddr("localhost:39999")
		} else {
			f.SetBrokerAddr("ws://localhost:39998/ws")
		}

		require.Error(t, f.Connect(ctx))
	})
}

func TestShouldFailConnectGivenCanceledContextWhenConnectCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := f.Connect(ctx)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

func TestShouldFailConnectGivenShortTimeoutWhenConnectToUnreachable(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		if transport == fixture.TransportTCP {
			f.SetBrokerAddr("localhost:39999")
		} else {
			f.SetBrokerAddr("ws://localhost:39998/ws")
		}

		require.Error(t, f.Connect(ctx))
	})
}

func TestShouldReturnErrorGivenOperationAfterCloseWhenDomainMethodCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrFail(ctx)
		route := f.UniqueRoute("kv")
		require.NoError(t, f.Client().Close())

		_, err := f.Client().KV().Begin(ctx, route, fitz.KVDurabilitySync)
		require.Error(t, err)
	})
}

func TestShouldReturnErrorGivenContextCanceledWhenLongRequestInFlight(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		fWorker := fixture.NewTestFixture(t, transport)
		fCaller := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		fWorker.ConnectOrFail(ctx)
		fCaller.ConnectOrFail(ctx)
		route := fWorker.UniqueRoute("rpc")

		sub, err := fWorker.Client().RPC().RegisterWorker(ctx, route, 7, func(handlerCtx context.Context, _ fitz.RPCInboundRequest, w fitz.RPCResponseWriter) error {
			select {
			case <-time.After(200 * time.Millisecond):
				return w.Send([]byte("late"))
			case <-handlerCtx.Done():
				return handlerCtx.Err()
			}
		})

		require.NoError(t, err)
		defer sub.Deregister()

		callCtx, callCancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			iter, err := fCaller.Client().RPC().Call(callCtx, route, []byte("block"))
			if err != nil {
				done <- err
				return
			}
			defer func() { _ = iter.Close() }()

			_ = iter.Next()
			done <- iter.Err()
		}()

		time.Sleep(50 * time.Millisecond)
		callCancel()

		select {
		case err := <-done:
			require.Error(t, err)
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("RPC iterator did not stop after context cancel")
		}
	})
}
