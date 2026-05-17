//nolint:gosec
package fixture

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/fitz"
)

// Environment variables for broker configuration.
// Set these in CI/CD environments or when testing against remote brokers.
const (
	// EnvBrokerTCPAddr specifies the TCP broker address (default: localhost:4091)
	EnvBrokerTCPAddr = "FITZ_BROKER_TCP_ADDR"
	// EnvBrokerWSAddr specifies the WebSocket broker address (default: ws://localhost:4090/ws)
	EnvBrokerWSAddr = "FITZ_BROKER_WS_ADDR"
	// EnvBrokerAuthTCPAddr overrides the auth-required TCP broker address.
	EnvBrokerAuthTCPAddr = "FITZ_BROKER_AUTH_TCP_ADDR"
	// EnvBrokerAuthWSAddr overrides the auth-required WebSocket broker address.
	EnvBrokerAuthWSAddr = "FITZ_BROKER_AUTH_WS_ADDR"
	// EnvBrokerAnonTCPAddr overrides the anonymous TCP broker address.
	EnvBrokerAnonTCPAddr = "FITZ_BROKER_ANON_TCP_ADDR"
	// EnvBrokerAnonWSAddr overrides the anonymous WebSocket broker address.
	EnvBrokerAnonWSAddr = "FITZ_BROKER_ANON_WS_ADDR"
	// EnvBrokerAuthRequired enables auth acceptance tests when set to "true".
	EnvBrokerAuthRequired = "FITZ_BROKER_AUTH_REQUIRED"
	// EnvBrokerJWTHMACSecret configures the HMAC secret used to mint test JWTs.
	EnvBrokerJWTHMACSecret = "FITZ_BROKER_JWT_HMAC_SECRET"
	// EnvBrokerJWTAudience configures the JWT audience expected by the broker.
	EnvBrokerJWTAudience = "FITZ_BROKER_JWT_AUDIENCE"
	// EnvDebugClientLogs enables verbose client logs during integration tests.
	EnvDebugClientLogs = "FITZ_GO_DEBUG_LOGS"
)

// Note: Integration tests require a running Fitz broker.
// Set FITZ_BROKER_TCP_ADDR and FITZ_BROKER_WS_ADDR environment variables to override defaults.

// TestFixture manages broker connections and test lifecycle for integration tests.
// It supports both TCP and WebSocket transports to verify protocol equivalence.
type TestFixture struct {
	t            *testing.T
	transport    TransportType
	brokerAddr   string
	authMode     AuthMode
	client       *fitz.Client
	cleanupFuncs []func()
}

type AuthMode string

const (
	AuthModeAnonymous        AuthMode = "anonymous"
	AuthModeValidJWT         AuthMode = "valid_jwt"
	AuthModeExpiredJWT       AuthMode = "expired_jwt"
	AuthModeInvalidSignature AuthMode = "invalid_signature"
)

// NewTestFixture creates a test fixture with the specified transport.
// Broker addresses can be configured via environment variables or use localhost defaults.
// Use FITZ_BROKER_TCP_ADDR (default: localhost:4091) and FITZ_BROKER_WS_ADDR (default: ws://localhost:4090/ws).
// Auth is disabled; an empty token is always sent.
func NewTestFixture(t *testing.T, transport TransportType) *TestFixture {
	t.Helper()

	authMode := authModeFromTestName(t.Name())
	brokerAddr, err := brokerAddrFor(transport, authMode)
	if err != nil {
		t.Fatalf("unsupported transport type: %s", transport)
	}

	f := &TestFixture{
		t:            t,
		transport:    transport,
		brokerAddr:   brokerAddr,
		authMode:     authMode,
		cleanupFuncs: []func(){},
	}

	// Automatically register cleanup with test
	t.Cleanup(f.cleanup)

	return f
}

// Connect establishes a connection to the broker with no authentication.
func (f *TestFixture) Connect(ctx context.Context) error {
	f.t.Helper()

	return f.connect(ctx)
}

// ConnectWithOptions establishes a connection to the broker using additional Fitz client options.
func (f *TestFixture) ConnectWithOptions(ctx context.Context, opts ...fitz.Option) error {
	f.t.Helper()

	return f.connect(ctx, opts...)
}

func (f *TestFixture) connect(ctx context.Context, opts ...fitz.Option) error {
	f.t.Helper()

	tokenProvider, err := f.tokenProviderForMode()
	if err != nil {
		return err
	}

	clientOpts := append([]fitz.Option{}, opts...)
	if strings.EqualFold(os.Getenv(EnvDebugClientLogs), "true") {
		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
		clientOpts = append(clientOpts, fitz.WithLogger(logger))
	}
	f.client = fitz.NewClient(f.brokerAddr, tokenProvider, clientOpts...)
	return f.client.Connect(ctx)
}

func (f *TestFixture) SetAuthMode(mode AuthMode) {
	f.authMode = mode
	if addr, err := brokerAddrFor(f.transport, mode); err == nil {
		f.brokerAddr = addr
	}
}

// SetBrokerAddr overrides the fixture broker address.
func (f *TestFixture) SetBrokerAddr(addr string) {
	f.brokerAddr = addr
}

// StartBrokerIfNeeded returns the broker address for the requested transport.
// Broker-backed acceptance tests only run when the corresponding environment
// variable is explicitly configured.
func StartBrokerIfNeeded(transport TransportType, authMode AuthMode) (addr string, stop func(), err error) {
	addr, err = brokerAddrFor(transport, authMode)
	if err != nil {
		return "", nil, err
	}
	return addr, func() {}, nil
}

// Client returns the connected Fitz client.
func (f *TestFixture) Client() *fitz.Client {
	if f.client == nil {
		f.t.Fatal("client not connected; call Connect() first")
	}
	return f.client
}

// cleanup executes all registered cleanup functions and closes the client connection.
// This is automatically registered with t.Cleanup() during fixture creation.
func (f *TestFixture) cleanup() {
	// Run all cleanup functions in reverse order
	for i := len(f.cleanupFuncs) - 1; i >= 0; i-- {
		f.cleanupFuncs[i]()
	}

	// Close client connection
	if f.client != nil {
		if err := f.client.Close(); err != nil {
			f.t.Logf("error closing client: %v", err)
		}
	}
}

// AddCleanup registers a function to be called during cleanup.
func (f *TestFixture) AddCleanup(fn func()) {
	f.cleanupFuncs = append(f.cleanupFuncs, fn)
}

// ConnectOrFail connects to the broker using the configured broker address and
// fails the test immediately when the broker or auth path is not usable.
func (f *TestFixture) ConnectOrFail(ctx context.Context) {
	f.t.Helper()

	addr, stop, err := StartBrokerIfNeeded(f.transport, f.authMode)
	if err != nil {
		f.t.Fatalf("broker not available: %v", err)
	}
	f.SetBrokerAddr(addr)
	f.AddCleanup(func() { stop() })

	if err := f.Connect(ctx); err != nil {
		f.t.Fatalf("broker not available: %v", err)
	}
	if f.authMode != AuthModeAnonymous {
		if err := f.probeAuthenticatedConnection(ctx); err != nil {
			f.t.Fatalf("auth broker not available: %v", err)
		}
	}
}

func (f *TestFixture) ConnectWithAuthOrFail(ctx context.Context, mode AuthMode) {
	f.t.Helper()
	f.SetAuthMode(mode)
	f.ConnectOrFail(ctx)
}

// ConnectOrSkip is retained as a compatibility wrapper for older tests but no
// longer skips; broker failures are fatal.
func (f *TestFixture) ConnectOrSkip(ctx context.Context) {
	f.ConnectOrFail(ctx)
}

// UniqueRealm generates a unique realm name for test isolation.
func (f *TestFixture) UniqueRealm() string {
	return fmt.Sprintf("test-%d", time.Now().UnixNano())
}

// UniqueArea generates a unique area name for test isolation.
func (f *TestFixture) UniqueArea() string {
	return fmt.Sprintf("area-%d", time.Now().UnixNano())
}

// UniqueResource generates a unique resource name for test isolation.
func (f *TestFixture) UniqueResource() string {
	return fmt.Sprintf("resource-%d", time.Now().UnixNano())
}

// UniqueRoute generates a unique route string for the given domain scheme.
func (f *TestFixture) UniqueRoute(scheme string) string {
	realm := f.UniqueRealm()
	area := f.UniqueArea()
	resource := f.UniqueResource()
	if scheme == "schedule" {
		return fmt.Sprintf("%s://%s/%s/%s/run", scheme, realm, area, resource)
	}
	return fmt.Sprintf("%s://%s/%s/%s", scheme, realm, area, resource)
}

func (f *TestFixture) tokenProviderForMode() (fitz.TokenProvider, error) {
	secret := os.Getenv(EnvBrokerJWTHMACSecret)
	if secret == "" {
		secret = "test-secret-key"
	}
	audience := os.Getenv(EnvBrokerJWTAudience)
	if audience == "" {
		audience = "fitz"
	}

	switch f.authMode {
	case AuthModeAnonymous:
		return func(context.Context) (string, error) { return "", nil }, nil
	case AuthModeValidJWT:
		token, err := GenerateValidTestJWT(secret, audience)
		if err != nil {
			return nil, err
		}
		return func(context.Context) (string, error) { return token, nil }, nil
	case AuthModeExpiredJWT:
		token, err := GenerateExpiredTestJWT(secret, audience)
		if err != nil {
			return nil, err
		}
		return func(context.Context) (string, error) { return token, nil }, nil
	case AuthModeInvalidSignature:
		token, err := GenerateInvalidSignatureTestJWT(secret, audience)
		if err != nil {
			return nil, err
		}
		return func(context.Context) (string, error) { return token, nil }, nil
	default:
		return nil, fmt.Errorf("unsupported auth mode: %s", f.authMode)
	}
}

func (f *TestFixture) probeAuthenticatedConnection(ctx context.Context) error {
	probeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	tx, err := f.client.KV().Begin(probeCtx, f.UniqueRoute("kv"), fitz.KVDurabilitySync)
	if err != nil {
		return err
	}
	return tx.Rollback(probeCtx)
}

func authModeFromTestName(name string) AuthMode {
	switch {
	case containsPathSegment(name, string(AuthModeValidJWT)):
		return AuthModeValidJWT
	case containsPathSegment(name, string(AuthModeExpiredJWT)):
		return AuthModeExpiredJWT
	case containsPathSegment(name, string(AuthModeInvalidSignature)):
		return AuthModeInvalidSignature
	default:
		return AuthModeAnonymous
	}
}

func AuthModeForTestName(name string) AuthMode {
	return authModeFromTestName(name)
}

func containsPathSegment(name string, segment string) bool {
	for _, part := range splitTestName(name) {
		if part == segment {
			return true
		}
	}
	return false
}

func splitTestName(name string) []string {
	return strings.Split(name, "/")
}

func brokerAddrFor(transport TransportType, authMode AuthMode) (string, error) {
	switch authMode {
	case AuthModeAnonymous:
		return brokerAddrFromEnv(transport, EnvBrokerAnonTCPAddr, EnvBrokerAnonWSAddr, "localhost:4191", "ws://localhost:4190/ws", true)
	case AuthModeValidJWT, AuthModeExpiredJWT, AuthModeInvalidSignature:
		return brokerAddrFromEnv(transport, EnvBrokerAuthTCPAddr, EnvBrokerAuthWSAddr, "localhost:4091", "ws://localhost:4090/ws", false)
	default:
		return "", fmt.Errorf("unsupported auth mode: %s", authMode)
	}
}

func brokerAddrFromEnv(transport TransportType, tcpEnv string, wsEnv string, tcpDefault string, wsDefault string, allowGenericFallback bool) (string, error) {
	switch transport {
	case TransportTCP:
		if addr := os.Getenv(tcpEnv); addr != "" {
			return addr, nil
		}
		if allowGenericFallback {
			if addr := os.Getenv(EnvBrokerTCPAddr); addr != "" {
				return addr, nil
			}
		}
		return tcpDefault, nil
	case TransportWebSocket:
		if addr := os.Getenv(wsEnv); addr != "" {
			return addr, nil
		}
		if allowGenericFallback {
			if addr := os.Getenv(EnvBrokerWSAddr); addr != "" {
				return addr, nil
			}
		}
		return wsDefault, nil
	default:
		return "", fmt.Errorf("unsupported transport: %s", transport)
	}
}
