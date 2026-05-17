package fixture

import "testing"

// TransportType identifies the transport protocol used for testing.
type TransportType string

const (
	TransportTCP       TransportType = "tcp"
	TransportWebSocket TransportType = "ws"
)

// RunWithBothTransports runs a test function with both TCP and WebSocket transports,
// and the standard anonymous/authenticated acceptance matrix.
func RunWithBothTransports(t *testing.T, testFn func(t *testing.T, transport TransportType)) {
	t.Helper()

	authModes := []AuthMode{AuthModeAnonymous, AuthModeValidJWT}
	transports := []TransportType{TransportTCP, TransportWebSocket}
	for _, authMode := range authModes {
		t.Run(string(authMode), func(t *testing.T) {
			for _, transport := range transports {
				t.Run(string(transport), func(t *testing.T) {
					testFn(t, transport)
				})
			}
		})
	}
}

// RunWithTransportsOnly runs a test function across TCP and WebSocket without
// introducing an auth-mode matrix.
func RunWithTransportsOnly(t *testing.T, testFn func(t *testing.T, transport TransportType)) {
	t.Helper()

	transports := []TransportType{TransportTCP, TransportWebSocket}
	for _, transport := range transports {
		t.Run(string(transport), func(t *testing.T) {
			testFn(t, transport)
		})
	}
}
