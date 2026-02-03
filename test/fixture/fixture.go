package fixture

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	fitz "github.com/cntryl/cntryl-go"
	"github.com/cntryl/cntryl-go/internal/client"
)

// TestFixture manages broker connections and test lifecycle for integration tests.
// It supports both TCP and WebSocket transports to verify protocol equivalence.
type TestFixture struct {
	t            *testing.T
	transport    TransportType
	brokerAddr   string
	authRequired bool
	clientID     string
	clientSecret string
	client       *client.Client
	cleanupFuncs []func()
}

// NewTestFixture creates a test fixture with the specified transport.
// Reads broker configuration from environment variables:
//   - FITZ_BROKER_ADDR (default: localhost:4091 for TCP, ws://localhost:4090/ws for WS)
//   - FITZ_AUTH_REQUIRED (default: false)
//   - FITZ_CLIENT_ID (required if auth enabled)
//   - FITZ_CLIENT_SECRET (required if auth enabled)
func NewTestFixture(t *testing.T, transport TransportType) *TestFixture {
	t.Helper()

	// Read configuration from environment
	authRequired := os.Getenv("FITZ_AUTH_REQUIRED") == "true" || os.Getenv("FITZ_AUTH_REQUIRED") == "TRUE"
	clientID := os.Getenv("FITZ_CLIENT_ID")
	clientSecret := os.Getenv("FITZ_CLIENT_SECRET")

	// Determine broker address based on transport
	brokerAddr := os.Getenv("FITZ_BROKER_ADDR")
	if brokerAddr == "" {
		switch transport {
		case TransportTCP:
			brokerAddr = "localhost:4091"
		case TransportWebSocket:
			brokerAddr = "ws://localhost:4090/ws"
		default:
			t.Fatalf("unsupported transport type: %s", transport)
		}
	}

	// Validate auth configuration
	if authRequired && (clientID == "" || clientSecret == "") {
		t.Fatal("FITZ_AUTH_REQUIRED=true but FITZ_CLIENT_ID or FITZ_CLIENT_SECRET not set")
	}

	f := &TestFixture{
		t:            t,
		transport:    transport,
		brokerAddr:   brokerAddr,
		authRequired: authRequired,
		clientID:     clientID,
		clientSecret: clientSecret,
		cleanupFuncs: []func(){},
	}

	// Automatically register cleanup with test
	t.Cleanup(f.cleanup)

	return f
}

// Connect establishes a connection to the broker with appropriate authentication.
func (f *TestFixture) Connect(ctx context.Context) error {
	f.t.Helper()

	var tokenProvider fitz.TokenProvider
	if f.authRequired {
		tokenProvider = func(ctx context.Context) (string, error) {
			// For auth-enabled mode, generate JWT using client credentials
			return GenerateTestJWT(f.clientID, f.clientSecret)
		}
	} else {
		// For auth-disabled mode, send empty token
		tokenProvider = func(ctx context.Context) (string, error) {
			return "", nil
		}
	}

	f.client = client.NewClient(f.brokerAddr, tokenProvider)
	return f.client.Connect(ctx)
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
