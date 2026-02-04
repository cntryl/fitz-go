package client

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteReadFrameMasking(t *testing.T) {
	var buf bytes.Buffer
	payload := []byte("hello-world")
	// write masked frame (client style)
	require.NoError(t, writeFrame(&buf, 2, payload, true))
	// read it back (readFrame will unmask)
	op, got, err := readFrame(&buf)
	require.NoError(t, err)
	require.Equal(t, byte(2), op)
	require.Equal(t, payload, got)
}

func TestHandshakeAndFrameExchange(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	host := "example.local:80"
	path := "/ws"

	// server goroutine: perform handshake response and then echo frames
	srvErrCh := make(chan error, 1)
	go func() {
		defer close(srvErrCh)
		// read request until blank line
		reqBuf := make([]byte, 4096)
		n, err := c2.Read(reqBuf)
		if err != nil {
			srvErrCh <- fmt.Errorf("server read req: %w", err)
			return
		}
		req := string(reqBuf[:n])
		// extract Sec-WebSocket-Key
		for _, line := range strings.Split(req, "\r\n") {
			if strings.HasPrefix(line, "Sec-WebSocket-Key:") {
				key := strings.TrimSpace(strings.TrimPrefix(line, "Sec-WebSocket-Key:"))
				h := sha1.New()
				h.Write([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
				acc := base64.StdEncoding.EncodeToString(h.Sum(nil))
				resp := fmt.Sprintf("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", acc)
				if _, err := c2.Write([]byte(resp)); err != nil {
					srvErrCh <- fmt.Errorf("server write resp: %w", err)
					return
				}
				break
			}
		}

		// now read a frame from client
		op, payload, err := readFrame(c2)
		if err != nil {
			srvErrCh <- fmt.Errorf("server read frame: %w", err)
			return
		}
		if op != 2 {
			srvErrCh <- fmt.Errorf("expected opcode 2, got %d", op)
			return
		}
		// send a ping and expect a pong
		if err := writeFrame(c2, 9, []byte("ping-payload"), false); err != nil {
			srvErrCh <- fmt.Errorf("server write ping: %w", err)
			return
		}
		// read pong
		op2, pong, err := readFrame(c2)
		if err != nil {
			srvErrCh <- fmt.Errorf("server read pong: %w", err)
			return
		}
		if op2 != 10 {
			srvErrCh <- fmt.Errorf("expected opcode 10 (pong), got %d", op2)
			return
		}
		if string(pong) != "ping-payload" {
			srvErrCh <- fmt.Errorf("pong payload mismatch: %s", string(pong))
			return
		}

		// echo back (server frames should not be masked)
		if err := writeFrame(c2, 2, payload, false); err != nil {
			srvErrCh <- fmt.Errorf("server write frame: %w", err)
			return
		}

		srvErrCh <- nil
	}()
	// client side handshake against c1
	require.NoError(t, doClientHandshake(c1, host, path))

	ws := &wsConnAdapter{conn: c1}
	// write a message to server
	msg := []byte("ping")
	n, err := ws.Write(msg)
	require.NoError(t, err)
	require.Equal(t, len(msg), n)

	// read echoed message
	got, err := ws.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, msg, got)

	// check server goroutine succeeded
	err = <-srvErrCh
	require.NoError(t, err)
}
