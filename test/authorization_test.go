//nolint:errcheck
package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/fitz"
	coreerrors "github.com/cntryl/fitz-go/internal/core/errors"
	"github.com/cntryl/fitz-go/test/fixture"
	"github.com/stretchr/testify/require"
)

const (
	unauthorizedKV       coreerrors.ErrorCode = 1011
	unauthorizedStream   coreerrors.ErrorCode = 2009
	unauthorizedNotice   coreerrors.ErrorCode = 3009
	unauthorizedQueue    coreerrors.ErrorCode = 4009
	unauthorizedLease    coreerrors.ErrorCode = 5009
	unauthorizedRPC      coreerrors.ErrorCode = 6009
	unauthorizedSchedule coreerrors.ErrorCode = 7009
)

type unauthorizedCase struct {
	name        string
	permissions []string
	routeScheme string
	expected    coreerrors.ErrorCode
	invoke      func(context.Context, *fitz.Client, string) error
}

func TestShouldRejectUnauthorizedOperationsGivenLimitedJWTWhenCallingEachDomain(t *testing.T) {
	fixture.RunWithTransportsOnly(t, func(t *testing.T, transportType fixture.TransportType) {
		cases := []unauthorizedCase{
			{
				name:        "kv_begin",
				permissions: []string{"kv://**#read"},
				routeScheme: "kv",
				expected:    unauthorizedKV,
				invoke: func(ctx context.Context, client *fitz.Client, route string) error {
					tx, err := client.KV().Begin(ctx, route, fitz.KVDurabilitySync)
					if err != nil {
						return err
					}
					defer tx.Rollback(ctx)
					return tx.Put(ctx, []byte("key"), []byte("value"))
				},
			},
			{
				name:        "queue_enqueue",
				permissions: []string{"queue://**#read"},
				routeScheme: "queue",
				expected:    unauthorizedQueue,
				invoke: func(ctx context.Context, client *fitz.Client, route string) error {
					_, err := client.Queue().Enqueue(ctx, route, []byte("payload"))
					return err
				},
			},
			{
				name:        "notice_subscribe",
				permissions: []string{},
				routeScheme: "notice",
				expected:    unauthorizedNotice,
				invoke: func(ctx context.Context, client *fitz.Client, route string) error {
					_, err := client.Notice().Subscribe(ctx, route, func(context.Context, fitz.NoticeMsg) error {
						return nil
					})
					return err
				},
			},
			{
				name:        "rpc_call",
				permissions: []string{"rpc://**#read"},
				routeScheme: "rpc",
				expected:    unauthorizedRPC,
				invoke: func(ctx context.Context, client *fitz.Client, route string) error {
					_, err := client.RPC().RegisterWorker(ctx, route, func(context.Context, fitz.RPCInboundRequest, fitz.RPCResponseWriter) error {
						return nil
					})
					return err
				},
			},
			{
				name:        "lease_acquire",
				permissions: []string{"lease://**#read"},
				routeScheme: "lease",
				expected:    unauthorizedLease,
				invoke: func(ctx context.Context, client *fitz.Client, route string) error {
					_, err := client.Lease().Acquire(ctx, route, 30)
					return err
				},
			},
			{
				name:        "stream_begin",
				permissions: []string{"stream://**#read"},
				routeScheme: "stream",
				expected:    unauthorizedStream,
				invoke: func(ctx context.Context, client *fitz.Client, route string) error {
					session, err := client.Stream().Begin(ctx, route)
					if err != nil {
						return err
					}
					defer session.Rollback(ctx)
					_, err = session.Append(ctx, 0, []byte("payload"))
					return err
				},
			},
			{
				name:        "schedule_create",
				permissions: []string{"schedule://**#read"},
				routeScheme: "schedule",
				expected:    unauthorizedSchedule,
				invoke: func(ctx context.Context, client *fitz.Client, route string) error {
					_, err := client.Schedule().Create(ctx, route, "*/5 * * * *", []byte("payload"))
					return err
				},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				client := newUnauthorizedClient(t, transportType, tc.permissions)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				err := tc.invoke(ctx, client, uniqueRoute(tc.routeScheme))
				require.Error(t, err)
			})
		}
	})
}

func newUnauthorizedClient(t *testing.T, transport fixture.TransportType, permissions []string) *fitz.Client {
	t.Helper()

	addr, stop, err := fixture.StartBrokerIfNeeded(transport, fixture.AuthModeValidJWT)
	require.NoError(t, err)
	t.Cleanup(stop)

	secret := os.Getenv(fixture.EnvBrokerJWTHMACSecret)
	if secret == "" {
		secret = "test-secret-key"
	}
	audience := os.Getenv(fixture.EnvBrokerJWTAudience)
	if audience == "" {
		audience = "fitz"
	}

	token, err := fixture.GenerateScopedTestJWT(secret, audience, permissions)
	require.NoError(t, err)

	client := fitz.NewClient(addr, func(context.Context) (string, error) {
		return token, nil
	}, fitz.WithAuthSettleDelay(20*time.Millisecond))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	require.NoError(t, client.Connect(ctx))
	t.Cleanup(func() {
		_ = client.Close()
	})

	return client
}

func uniqueRoute(scheme string) string {
	nonce := time.Now().UnixNano()
	if scheme == "schedule" {
		return fmt.Sprintf("%s://realm-%d/area-%d/resource-%d/run", scheme, nonce, nonce+1, nonce+2)
	}
	return fmt.Sprintf("%s://realm-%d/area-%d/resource-%d", scheme, nonce, nonce+1, nonce+2)
}
