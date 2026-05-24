package fixture

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/fitz"
)

const reconnectPollInterval = 20 * time.Millisecond

// ProxyReconnectHarness wires one reconnecting client through a disconnect proxy
// plus one stable peer that talks directly to the broker.
type ProxyReconnectHarness struct {
	t       *testing.T
	Proxy   *DisconnectProxy
	Proxied *TestFixture
	Stable  *TestFixture
}

// DefaultReconnectOptions returns the retry settings used by the proxy-backed
// reconnect integration tests.
func DefaultReconnectOptions() []fitz.Option {
	return []fitz.Option{
		fitz.WithReconnect(true, 25*time.Millisecond, 20),
		fitz.WithReconnectMaxDelay(50 * time.Millisecond),
	}
}

func NewProxyReconnectHarness(t *testing.T, transport TransportType, authMode AuthMode) *ProxyReconnectHarness {
	t.Helper()

	backendAddr, stop, err := StartBrokerIfNeeded(transport, authMode)
	if err != nil {
		t.Fatalf("broker not available: %v", err)
	}
	t.Cleanup(stop)

	proxy := NewDisconnectProxy(t, transport, backendAddr)

	proxied := NewTestFixture(t, transport)
	proxied.SetAuthMode(authMode)
	proxied.SetBrokerAddr(proxy.Addr())

	stable := NewTestFixture(t, transport)
	stable.SetAuthMode(authMode)

	return &ProxyReconnectHarness{
		t:       t,
		Proxy:   proxy,
		Proxied: proxied,
		Stable:  stable,
	}
}

func (h *ProxyReconnectHarness) Connect(ctx context.Context, proxiedOpts ...fitz.Option) {
	h.t.Helper()

	if err := h.Proxied.ConnectWithOptions(ctx, proxiedOpts...); err != nil {
		h.t.Fatalf("connect proxied client: %v", err)
	}
	if err := h.Stable.Connect(ctx); err != nil {
		h.t.Fatalf("connect stable client: %v", err)
	}
}

func (h *ProxyReconnectHarness) WaitForInitialConnection(timeout time.Duration) {
	h.t.Helper()
	h.WaitForAcceptedCount(1, timeout)
}

func (h *ProxyReconnectHarness) WaitForAcceptedCount(target int64, timeout time.Duration) {
	h.t.Helper()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	ticker := time.NewTicker(reconnectPollInterval)
	defer ticker.Stop()

	for {
		if got := h.Proxy.AcceptedCount(); got >= target {
			return
		}

		select {
		case <-deadline.C:
			h.t.Fatalf("timed out waiting for proxy accepted count >= %d (got %d)", target, h.Proxy.AcceptedCount())
		case <-ticker.C:
		}
	}
}

func (h *ProxyReconnectHarness) DropAndWaitForReconnect(timeout time.Duration) {
	h.t.Helper()

	current := h.Proxy.AcceptedCount()
	if current < 1 {
		h.WaitForInitialConnection(timeout)
		current = h.Proxy.AcceptedCount()
	}

	h.Proxy.DropConnections()
	h.WaitForAcceptedCount(current+1, timeout)
}
