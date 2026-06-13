package fixture

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// DisconnectProxy forwards raw TCP bytes to the broker and can forcibly close
// all active connections to simulate a live network drop.
type DisconnectProxy struct {
	listener     net.Listener
	transport    TransportType
	backendHost  string
	backendAddr  string
	acceptedConn atomic.Int64
	closed       chan struct{}
	closeOnce    sync.Once
	mu           sync.Mutex
	pairs        map[int64]proxyConnPair
	nextPairID   int64
}

type proxyConnPair struct {
	client  net.Conn
	backend net.Conn
}

func NewDisconnectProxy(t *testing.T, transport TransportType, backendAddr string) *DisconnectProxy {
	t.Helper()

	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen disconnect proxy: %v", err)
	}

	proxy := &DisconnectProxy{
		listener:    listener,
		transport:   transport,
		backendHost: proxyBackendHost(t, transport, backendAddr),
		backendAddr: backendAddr,
		closed:      make(chan struct{}),
		pairs:       make(map[int64]proxyConnPair),
	}
	t.Cleanup(func() { _ = proxy.Close() })
	go proxy.acceptLoop()

	return proxy
}

func (p *DisconnectProxy) Addr() string {
	if p.transport == TransportWebSocket {
		backendURL, err := url.Parse(p.backendAddr)
		if err != nil {
			return p.listener.Addr().String()
		}
		backendURL.Host = p.listener.Addr().String()
		return backendURL.String()
	}

	return p.listener.Addr().String()
}

func (p *DisconnectProxy) AcceptedCount() int64 {
	return p.acceptedConn.Load()
}

func (p *DisconnectProxy) DropConnections() {
	p.mu.Lock()
	pairs := make([]proxyConnPair, 0, len(p.pairs))
	for _, pair := range p.pairs {
		pairs = append(pairs, pair)
	}
	p.mu.Unlock()

	for _, pair := range pairs {
		_ = pair.client.Close()
		_ = pair.backend.Close()
	}
}

func (p *DisconnectProxy) Close() error {
	p.closeOnce.Do(func() {
		close(p.closed)
		_ = p.listener.Close()
		p.DropConnections()
	})
	return nil
}

func (p *DisconnectProxy) acceptLoop() {
	for {
		clientConn, err := p.listener.Accept()
		if err != nil {
			select {
			case <-p.closed:
				return
			default:
				return
			}
		}

		var dialer net.Dialer
		backendConn, err := dialer.DialContext(context.Background(), "tcp", p.backendHost)
		if err != nil {
			_ = clientConn.Close()
			continue
		}

		pairID := atomic.AddInt64(&p.nextPairID, 1)
		p.acceptedConn.Add(1)

		p.mu.Lock()
		p.pairs[pairID] = proxyConnPair{client: clientConn, backend: backendConn}
		p.mu.Unlock()

		go p.pipePair(pairID, clientConn, backendConn)
	}
}

func (p *DisconnectProxy) pipePair(pairID int64, clientConn net.Conn, backendConn net.Conn) {
	var closeOnce sync.Once
	cleanup := func() {
		closeOnce.Do(func() {
			_ = clientConn.Close()
			_ = backendConn.Close()
			p.mu.Lock()
			delete(p.pairs, pairID)
			p.mu.Unlock()
		})
	}

	if p.transport == TransportWebSocket {
		p.pipeWebSocketPair(clientConn, backendConn, cleanup)
		return
	}

	go func() {
		defer cleanup()
		_, _ = io.Copy(clientConn, backendConn)
	}()

	defer cleanup()
	_, _ = io.Copy(backendConn, clientConn)
}

func (p *DisconnectProxy) pipeWebSocketPair(clientConn net.Conn, backendConn net.Conn, cleanup func()) {
	reader := bufio.NewReader(clientConn)
	header, err := readHTTPHeader(reader)
	if err != nil {
		cleanup()
		return
	}

	header = rewriteWebSocketOrigin(header, proxyOriginForBackend(p.backendAddr))
	if _, err := backendConn.Write(header); err != nil {
		cleanup()
		return
	}

	go func() {
		defer cleanup()
		_, _ = io.Copy(clientConn, backendConn)
	}()

	defer cleanup()
	_, _ = io.Copy(backendConn, reader)
}

func readHTTPHeader(reader *bufio.Reader) ([]byte, error) {
	var header []byte
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		header = append(header, line...)
		if len(header) > 32*1024 {
			return nil, io.ErrShortBuffer
		}
		if strings.HasSuffix(string(header), "\r\n\r\n") {
			return header, nil
		}
	}
}

func rewriteWebSocketOrigin(header []byte, origin string) []byte {
	if origin == "" {
		return header
	}

	text := string(header)
	lines := strings.Split(text, "\r\n")
	replaced := false
	insertAt := len(lines) - 2
	for i, line := range lines {
		if strings.HasPrefix(strings.ToLower(line), "origin:") {
			lines[i] = "Origin: " + origin
			replaced = true
			break
		}
		if strings.HasPrefix(strings.ToLower(line), "host:") {
			insertAt = i + 1
		}
	}
	if !replaced {
		next := append([]string{}, lines[:insertAt]...)
		next = append(next, "Origin: "+origin)
		next = append(next, lines[insertAt:]...)
		lines = next
	}
	return []byte(strings.Join(lines, "\r\n"))
}

func proxyOriginForBackend(backendAddr string) string {
	backendURL, err := url.Parse(backendAddr)
	if err != nil {
		return ""
	}
	scheme := "http"
	if backendURL.Scheme == "wss" {
		scheme = "https"
	}
	return scheme + "://" + backendURL.Host
}

func proxyBackendHost(t *testing.T, transport TransportType, backendAddr string) string {
	t.Helper()

	if transport == TransportWebSocket {
		backendURL, err := url.Parse(backendAddr)
		if err != nil {
			t.Fatalf("parse websocket broker address: %v", err)
		}
		return backendURL.Host
	}

	return backendAddr
}
