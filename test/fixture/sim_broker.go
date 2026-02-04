package fixture

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cntryl/cntryl-go/internal/notice"
	"github.com/cntryl/cntryl-go/internal/transport"
)

// SimBroker is a minimal in-process broker used for deterministic integration tests.
// It supports both TCP and WebSocket (basic handshake and binary frames) transports
// and a small subset of the Notice domain required by tests (SUBSCRIBE, UNSUBSCRIBE, PUBLISH).
type SimBroker struct {
	ln       net.Listener
	mu       sync.Mutex
	conns    map[net.Conn]*connState
	shutdown chan struct{}
}

type connState struct {
	conn   net.Conn
	isWS   bool
	subset []string // subscribed route patterns
	mu     sync.Mutex
}

// StartSimBroker starts a broker listening on an ephemeral port. transportType selects TCP or WebSocket behavior.
// Returns the address to connect to (host:port or ws://host:port/path) and a shutdown function.
func StartSimBroker(transportType string) (addr string, stop func(), err error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	sb := &SimBroker{ln: ln, conns: make(map[net.Conn]*connState), shutdown: make(chan struct{})}
	go sb.acceptLoop(transportType)
	hostPort := ln.Addr().String()
	fmt.Printf("[simbroker] listening on %s for transport=%s\n", hostPort, transportType)
	if transportType == string(TransportWebSocket) {
		addr = fmt.Sprintf("ws://%s/ws", hostPort)
	} else {
		addr = hostPort
	}
	stop = func() {
		close(sb.shutdown)
		_ = ln.Close()
		sb.mu.Lock()
		for c := range sb.conns {
			_ = c.Close()
		}
		sb.mu.Unlock()
	}
	return addr, stop, nil
}

func (s *SimBroker) acceptLoop(transportType string) {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.shutdown:
				return
			default:
				return
			}
		}
		cs := &connState{conn: conn, subset: []string{}}
		fmt.Printf("[simbroker] accept conn from %s\n", conn.RemoteAddr())
		s.mu.Lock()
		s.conns[conn] = cs
		s.mu.Unlock()
		if transportType == string(TransportWebSocket) {
			// Perform server-side WebSocket handshake
			if err := doServerHandshake(conn); err != nil {
				_ = conn.Close()
				continue
			}
			cs.isWS = true
		}
		go s.handleConn(cs)
	}
}

func (s *SimBroker) handleConn(cs *connState) {
	defer func() {
		_ = cs.conn.Close()
		s.mu.Lock()
		delete(s.conns, cs.conn)
		s.mu.Unlock()
	}()
	for {
		var f transport.Frame
		var err error
		if cs.isWS {
			f, err = readWSMessage(cs.conn)
		} else {
			f, err = readMessage(cs.conn)
		}
		if err != nil {
			return
		}
		s.processFrame(cs, f)
	}
}

func (s *SimBroker) processFrame(cs *connState, f transport.Frame) {
	// Only handle Notice domain frames here
	if f.Channel != notice.ChannelPub && f.Channel != notice.ChannelSub {
		return
	}
	dec, err := transport.NewTLVDecoder(f.Body)
	if err != nil {
		return
	}
	// Subscribe
	if f.Type == notice.NoticeSubscribe && f.Channel == notice.ChannelSub {
		r := dec.GetString(transport.TagRoute)
		fmt.Printf("[simbroker] SUBSCRIBE from %s route=%s\n", cs.conn.RemoteAddr(), r)
		cs.mu.Lock()
		cs.subset = append(cs.subset, r)
		cs.mu.Unlock()
		return
	}
	// Unsubscribe
	if f.Type == notice.NoticeUnsubscribe && f.Channel == notice.ChannelSub {
		r := dec.GetString(transport.TagRoute)
		fmt.Printf("[simbroker] UNSUBSCRIBE from %s route=%s\n", cs.conn.RemoteAddr(), r)
		cs.mu.Lock()
		newsubs := make([]string, 0, len(cs.subset))
		for _, p := range cs.subset {
			if p != r {
				newsubs = append(newsubs, p)
			}
		}
		cs.subset = newsubs
		cs.mu.Unlock()
		// Send unsubscribe ack
		encAck := transport.NewTLVEncoder()
		encAck.AddUint8(transport.TagOp, 1)
		encAck.AddString(transport.TagRoute, r)
		ackFrame := transport.Frame{Type: transport.FrameTypeResp, Flags: 0, Channel: notice.ChannelSub, Body: encAck.Encode()}
		if cs.isWS {
			_ = writeWSMessage(cs.conn, ackFrame)
		} else {
			_ = writeMessage(cs.conn, ackFrame)
		}
		fmt.Printf("[simbroker] sent UNSUBSCRIBE_ACK to %s route=%s\n", cs.conn.RemoteAddr(), r)
		return
	}
	// Unsubscribe all
	if f.Type == notice.NoticeUnsubscribeAll && f.Channel == notice.ChannelSub {
		cs.mu.Lock()
		cs.subset = []string{}
		cs.mu.Unlock()
		// Send unsubscribe-all ack
		encAck := transport.NewTLVEncoder()
		encAck.AddUint8(transport.TagOp, 2)
		ackFrame := transport.Frame{Type: transport.FrameTypeResp, Flags: 0, Channel: notice.ChannelSub, Body: encAck.Encode()}
		if cs.isWS {
			_ = writeWSMessage(cs.conn, ackFrame)
		} else {
			_ = writeMessage(cs.conn, ackFrame)
		}
		fmt.Printf("[simbroker] sent UNSUBSCRIBE_ALL_ACK to %s\n", cs.conn.RemoteAddr())
		return
	}
	// Publish
	if f.Type == notice.NoticePublish && f.Channel == notice.ChannelPub {
		r := dec.GetString(transport.TagRoute)
		b := dec.GetBytes(transport.TagBody)
		fmt.Printf("[simbroker] PUBLISH route=%s bodylen=%d\n", r, len(b))
		// Fanout to all subscribers whose pattern matches r
		s.mu.Lock()
		for _, con := range s.conns {
			con.mu.Lock()
			for _, pat := range con.subset {
				if matchRoute(pat, r) {
					fmt.Printf("[simbroker] - notifying %s for pattern=%s\n", con.conn.RemoteAddr(), pat)
					enc := transport.NewTLVEncoder()
					enc.AddString(transport.TagRoute, r)
					enc.AddBytes(transport.TagBody, b)
					frameOut := transport.Frame{Type: transport.FrameTypeResp, Flags: 0, Channel: notice.ChannelSub, Body: enc.Encode()}
					if con.isWS {
						_ = writeWSMessage(con.conn, frameOut)
					} else {
						_ = writeMessage(con.conn, frameOut)
					}
					break
				}
			}
			con.mu.Unlock()
		}
		s.mu.Unlock()
		return
	}
}

// matchRoute supports '*' single segment and '**' multi-segment wildcards.
func matchRoute(pattern, route string) bool {
	if pattern == route {
		return true
	}
	pSegs := strings.Split(pattern, "/")
	rSegs := strings.Split(route, "/")
	pi, ri := 0, 0
	for pi < len(pSegs) && ri < len(rSegs) {
		if pSegs[pi] == "**" {
			// Match rest
			if pi == len(pSegs)-1 {
				return true
			}
			// Try to find next segment match
			next := pSegs[pi+1]
			for ri < len(rSegs) {
				if rSegs[ri] == next {
					break
				}
				ri++
			}
			pi++
			continue
		}
		if pSegs[pi] == "*" {
			pi++
			ri++
			continue
		}
		if pSegs[pi] != rSegs[ri] {
			return false
		}
		pi++
		ri++
	}
	// trailing patterns of '**' match
	for pi < len(pSegs) && pSegs[pi] == "**" {
		pi++
	}
	return pi == len(pSegs) && ri == len(rSegs)
}

// Simple TCP length-prefixed frame reader/writer and WebSocket minimal framing
func readMessage(r io.Reader) (transport.Frame, error) {
	// Peek first 2 bytes to heuristically decide WebSocket vs TCP
	br := bufio.NewReader(r)
	b, err := br.Peek(2)
	if err != nil {
		return transport.Frame{}, err
	}
	// WebSocket handshake starts with 'GET'
	if string(b) == "GE" {
		// This should not happen here; handshake is already performed for ws
		return transport.Frame{}, errors.New("unexpected GET in readMessage")
	}
	// For TCP or after WebSocket handshake, try TCP length prefix
	// Read 4-byte length
	var lenBuf [4]byte
	if _, err := io.ReadFull(br, lenBuf[:]); err != nil {
		return transport.Frame{}, err
	}
	l := binary.BigEndian.Uint32(lenBuf[:])
	payload := make([]byte, l)
	if _, err := io.ReadFull(br, payload); err != nil {
		return transport.Frame{}, err
	}
	f := transport.Frame{
		Type:    payload[0],
		Flags:   payload[1],
		Channel: binary.BigEndian.Uint32(payload[2:6]),
		Body:    append([]byte(nil), payload[6:]...),
	}
	return f, nil
}

func writeMessage(w io.Writer, f transport.Frame) error {
	// TCP length prefix
	bufLen := 1 + 1 + 4 + len(f.Body)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(bufLen))
	parts := make([]byte, 0, 4+bufLen)
	parts = append(parts, lenBuf[:]...)
	parts = append(parts, f.Type)
	parts = append(parts, f.Flags)
	var chBuf [4]byte
	binary.BigEndian.PutUint32(chBuf[:], f.Channel)
	parts = append(parts, chBuf[:]...)
	parts = append(parts, f.Body...)
	_, err := w.Write(parts)
	return err
}

// readWSMessage reads a single (unfragmented) WebSocket binary frame from r and returns the payload parsed
// as a transport.Frame (expects the payload starts with type|flags|channel(4)|body).
func readWSMessage(r io.Reader) (transport.Frame, error) {
	br := bufio.NewReader(r)
	// Read first two bytes
	b1, err := br.ReadByte()
	if err != nil {
		return transport.Frame{}, err
	}
	b2, err := br.ReadByte()
	if err != nil {
		return transport.Frame{}, err
	}
	fin := (b1 & 0x80) != 0
	opcode := b1 & 0x0f
	if !fin || opcode != 2 {
		return transport.Frame{}, errors.New("unsupported or fragmented WS frame")
	}
	masked := (b2 & 0x80) != 0
	payloadLen := int(b2 & 0x7f)
	if payloadLen == 126 {
		var extLen [2]byte
		if _, err := io.ReadFull(br, extLen[:]); err != nil {
			return transport.Frame{}, err
		}
		payloadLen = int(binary.BigEndian.Uint16(extLen[:]))
	} else if payloadLen == 127 {
		var extLen [8]byte
		if _, err := io.ReadFull(br, extLen[:]); err != nil {
			return transport.Frame{}, err
		}
		payloadLen = int(binary.BigEndian.Uint64(extLen[:]))
	}
	var maskKey [4]byte
	if masked {
		if _, err := io.ReadFull(br, maskKey[:]); err != nil {
			return transport.Frame{}, err
		}
	}
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(br, payload); err != nil {
		return transport.Frame{}, err
	}
	if masked {
		for i := 0; i < payloadLen; i++ {
			payload[i] ^= maskKey[i%4]
		}
	}
	if len(payload) < 6 {
		return transport.Frame{}, errors.New("invalid payload")
	}
	f := transport.Frame{
		Type:    payload[0],
		Flags:   payload[1],
		Channel: binary.BigEndian.Uint32(payload[2:6]),
		Body:    append([]byte(nil), payload[6:]...),
	}
	return f, nil
}

// writeWSMessage writes a single unmasked binary WebSocket frame with the given payload
func writeWSMessage(w io.Writer, f transport.Frame) error {
	payload := make([]byte, 0, 1+1+4+len(f.Body))
	payload = append(payload, f.Type)
	payload = append(payload, f.Flags)
	var chBuf [4]byte
	binary.BigEndian.PutUint32(chBuf[:], f.Channel)
	payload = append(payload, chBuf[:]...)
	payload = append(payload, f.Body...)
	// Write header
	var header []byte
	header = append(header, 0x82) // FIN+binary
	plen := len(payload)
	if plen < 126 {
		header = append(header, byte(plen))
	} else if plen < 65536 {
		header = append(header, 126)
		var ext [2]byte
		binary.BigEndian.PutUint16(ext[:], uint16(plen))
		header = append(header, ext[:]...)
	} else {
		header = append(header, 127)
		var ext [8]byte
		binary.BigEndian.PutUint64(ext[:], uint64(plen))
		header = append(header, ext[:]...)
	}
	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// doServerHandshake handles a minimal RFC6455 server handshake over conn.
func doServerHandshake(conn net.Conn) error {
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	r := bufio.NewReader(conn)
	req, err := http.ReadRequest(r)
	if err != nil {
		return err
	}
	key := req.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		return errors.New("missing Sec-WebSocket-Key")
	}
	accept := computeAcceptKey(key)
	resp := "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n"
	_, err = conn.Write([]byte(resp))
	if err != nil {
		return err
	}
	conn.SetReadDeadline(time.Time{})
	return nil
}

func computeAcceptKey(key string) string {
	h := sha1.New()
	h.Write([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}
