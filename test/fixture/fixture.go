package fixture

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	fitz "github.com/cntryl/fitz-go/fitz"
	"github.com/cntryl/fitz-go/internal/core/client"
	"github.com/cntryl/fitz-go/internal/core/types"
)

// Environment variables for broker configuration.
// Set these in CI/CD environments or when testing against remote brokers.
const (
	// EnvBrokerTCPAddr specifies the TCP broker address (default: localhost:4091)
	EnvBrokerTCPAddr = "FITZ_BROKER_TCP_ADDR"
	// EnvBrokerWSAddr specifies the WebSocket broker address (default: ws://localhost:4090/ws)
	EnvBrokerWSAddr = "FITZ_BROKER_WS_ADDR"
	// EnvBrokerAuthRequired enables auth acceptance tests when set to "true".
	EnvBrokerAuthRequired = "FITZ_BROKER_AUTH_REQUIRED"
	// EnvBrokerJWTHMACSecret configures the HMAC secret used to mint test JWTs.
	EnvBrokerJWTHMACSecret = "FITZ_BROKER_JWT_HMAC_SECRET"
	// EnvBrokerJWTAudience configures the JWT audience expected by the broker.
	EnvBrokerJWTAudience = "FITZ_BROKER_JWT_AUDIENCE"
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
	client       *client.Client
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

	// Get broker addresses from environment or use localhost defaults
	var brokerAddr string
	switch transport {
	case TransportTCP:
		brokerAddr = os.Getenv(EnvBrokerTCPAddr)
		if brokerAddr == "" {
			brokerAddr = "localhost:4091"
		}
	case TransportWebSocket:
		brokerAddr = os.Getenv(EnvBrokerWSAddr)
		if brokerAddr == "" {
			brokerAddr = "ws://localhost:4090/ws"
		}
	default:
		t.Fatalf("unsupported transport type: %s", transport)
	}

	f := &TestFixture{
		t:            t,
		transport:    transport,
		brokerAddr:   brokerAddr,
		authMode:     AuthModeAnonymous,
		cleanupFuncs: []func(){},
	}

	// Automatically register cleanup with test
	t.Cleanup(f.cleanup)

	return f
}

// Connect establishes a connection to the broker with no authentication.
func (f *TestFixture) Connect(ctx context.Context) error {
	f.t.Helper()

	tokenProvider, err := f.tokenProviderForMode()
	if err != nil {
		return err
	}

	f.client = client.NewClient(f.brokerAddr, tokenProvider)
	return f.client.Connect(ctx)
}

func (f *TestFixture) SetAuthMode(mode AuthMode) {
	f.authMode = mode
}

// SetBrokerAddr overrides the fixture broker address.
func (f *TestFixture) SetBrokerAddr(addr string) {
	f.brokerAddr = addr
}

// StartBrokerIfNeeded returns the broker address for the requested transport.
// Broker-backed acceptance tests only run when the corresponding environment
// variable is explicitly configured.
func StartBrokerIfNeeded(transport TransportType) (addr string, stop func(), err error) {
	switch transport {
	case TransportTCP:
		addr = os.Getenv(EnvBrokerTCPAddr)
		if addr == "" {
			return "", nil, fmt.Errorf("%s not configured", EnvBrokerTCPAddr)
		}
		return addr, func() {}, nil
	case TransportWebSocket:
		addr = os.Getenv(EnvBrokerWSAddr)
		if addr == "" {
			return "", nil, fmt.Errorf("%s not configured", EnvBrokerWSAddr)
		}
		return addr, func() {}, nil
	default:
		return "", nil, fmt.Errorf("unsupported transport: %s", transport)
	}
}

// Client returns the connected Fitz client.
func (f *TestFixture) Client() fitz.Client {
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

// ConnectOrSkip connects to the broker using StartBrokerIfNeeded and skips
// the test when no broker is available.
func (f *TestFixture) ConnectOrSkip(ctx context.Context) {
	f.t.Helper()

	addr, stop, err := StartBrokerIfNeeded(f.transport)
	if err != nil {
		f.t.Skipf("broker not available: %v", err)
	}
	f.SetBrokerAddr(addr)
	f.AddCleanup(func() { stop() })

	if err := f.Connect(ctx); err != nil {
		f.t.Skipf("broker not available: %v", err)
	}
}

func (f *TestFixture) ConnectWithAuthOrSkip(ctx context.Context, mode AuthMode) {
	f.t.Helper()
	if os.Getenv(EnvBrokerAuthRequired) != "true" {
		f.t.Skip("auth-enabled broker not configured")
	}
	f.SetAuthMode(mode)
	f.ConnectOrSkip(ctx)
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
		return fmt.Sprintf("%s://%s/%s/%s/%s", scheme, realm, area, resource, "run")
	}
	return fmt.Sprintf("%s://%s/%s/%s", scheme, realm, area, resource)
}

func (f *TestFixture) tokenProviderForMode() (types.TokenProvider, error) {
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
