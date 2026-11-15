package jazz

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
	"unicode/utf8"
)

// The server side of RFC 6455, as far as Jazz's sync socket needs it:
// binary messages (fragmented or not), pings and pongs, and the closing
// handshake. It enforces what a server must of a client (masked frames,
// no reserved bits, control-frame rules, well-formed fragmentation and
// closes), caps message sizes, and drops a connection that goes quiet.
// Text messages are refused: Jazz's protocol is binary.

// Limits of the endpoint's sockets.
const (
	// wsMaxMessage caps a message (all its fragments): a larger one closes
	// the socket with 1009 before it is read, as Jazz's own cap on a sync
	// message (2 MiB) would after. Large values travel in chunks.
	wsMaxMessage = 2 << 20
	// wsIdle is how long a socket may stay silent (no frame of any kind)
	// before it is dropped; wsPing is how often the server pings it, so a
	// live client always answers within wsIdle.
	wsIdle = 75 * time.Second
	wsPing = 30 * time.Second
	// wsWriteTimeout bounds each write.
	wsWriteTimeout = 30 * time.Second
	// wsCloseTimeout is how long the server waits for the client's close
	// after sending its own.
	wsCloseTimeout = 5 * time.Second
)

// Close codes (RFC 6455 §7.4.1).
const (
	wsNormal          = 1000
	wsProtocolError   = 1002
	wsUnsupportedData = 1003
	wsInvalidPayload  = 1007
	wsTooBig          = 1009
	wsInternalError   = 1011
)

// wsError is why a socket must close, and with which code.
type wsError struct {
	code   uint16
	reason string
}

func (e *wsError) Error() string { return fmt.Sprintf("websocket %d: %s", e.code, e.reason) }

func protocolError(reason string) error { return &wsError{wsProtocolError, reason} }

type wsConn struct {
	conn net.Conn
	r    *bufio.Reader
	mu   sync.Mutex // guards writes and closed
	// closed: a close frame was sent; nothing more is.
	closed bool
	done   chan struct{}
	once   sync.Once
}

// checkUpgrade is why r isn't a WebSocket opening handshake this server
// accepts (RFC 6455 §4.2.1), and the HTTP status to refuse it with.
func checkUpgrade(r *http.Request) (int, string) {
	if r.Method != http.MethodGet {
		return http.StatusMethodNotAllowed, "a websocket upgrade is a GET"
	}
	if !headerHas(r.Header, "Connection", "upgrade") || !headerHas(r.Header, "Upgrade", "websocket") {
		return http.StatusBadRequest, "not a websocket upgrade"
	}
	if r.Header.Get("Sec-WebSocket-Version") != "13" {
		return http.StatusUpgradeRequired, "websocket version 13 only"
	}
	key, err := base64.StdEncoding.DecodeString(r.Header.Get("Sec-WebSocket-Key"))
	if err != nil || len(key) != 16 {
		return http.StatusBadRequest, "bad Sec-WebSocket-Key"
	}
	return 0, ""
}

// headerHas reports whether a comma-separated header lists token.
func headerHas(h http.Header, name, token string) bool {
	for _, value := range h.Values(name) {
		for _, part := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

func isUpgrade(r *http.Request) bool {
	return headerHas(r.Header, "Upgrade", "websocket")
}

// upgrade completes the opening handshake of a request checkUpgrade
// accepted, taking over its connection.
func upgrade(w http.ResponseWriter, r *http.Request) (*wsConn, error) {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("the server can't hand over its connection")
	}
	conn, rw, err := hijacker.Hijack()
	if err != nil {
		return nil, err
	}
	sum := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	header := "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " +
		base64.StdEncoding.EncodeToString(sum[:]) + "\r\n\r\n"
	conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
	if _, err := rw.WriteString(header); err != nil {
		conn.Close()
		return nil, err
	}
	if err := rw.Flush(); err != nil {
		conn.Close()
		return nil, err
	}
	c := &wsConn{conn: conn, r: rw.Reader, done: make(chan struct{})}
	go c.keepalive()
	return c, nil
}

func newWSConn(conn net.Conn) *wsConn {
	return &wsConn{conn: conn, r: bufio.NewReader(conn), done: make(chan struct{})}
}

// keepalive pings the client until the socket ends.
func (c *wsConn) keepalive() {
	ticker := time.NewTicker(wsPing)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if c.write(opPing, nil) != nil {
				return
			}
		case <-c.done:
			return
		}
	}
}

const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xa
)

// readMessage is the client's next binary message, answering pings on
// the way. It returns io.EOF after the client's close (which it echoes),
// a *wsError for what breaks the protocol, and other errors for the
// connection's.
func (c *wsConn) readMessage() ([]byte, error) {
	var message []byte
	fragmented := false
	for {
		c.conn.SetReadDeadline(time.Now().Add(wsIdle))
		fin, opcode, payload, err := c.readFrame()
		if err != nil {
			return nil, err
		}
		switch opcode {
		case opPing:
			if err := c.write(opPong, payload); err != nil {
				return nil, err
			}
		case opPong:
		case opClose:
			code, reason, err := parseClose(payload)
			if err != nil {
				return nil, err
			}
			if code == 0 {
				code = wsNormal
			}
			c.closeWith(code, reason)
			return nil, io.EOF
		case opText:
			return nil, &wsError{wsUnsupportedData, "text messages are not supported"}
		case opBinary:
			if fragmented {
				return nil, protocolError("a new message inside a fragmented one")
			}
			message, fragmented = payload, !fin
		case opContinuation:
			if !fragmented {
				return nil, protocolError("a continuation without a message")
			}
			if len(message)+len(payload) > wsMaxMessage {
				return nil, &wsError{wsTooBig, "message too large"}
			}
			message = append(message, payload...)
			fragmented = !fin
		}
		if (opcode == opBinary || opcode == opContinuation) && !fragmented {
			return message, nil
		}
	}
}

// readFrame reads a frame, enforcing the rules of §5 for a client's.
func (c *wsConn) readFrame() (fin bool, opcode byte, payload []byte, err error) {
	var head [2]byte
	if _, err := io.ReadFull(c.r, head[:]); err != nil {
		return false, 0, nil, err
	}
	fin, opcode = head[0]&0x80 != 0, head[0]&0x0f
	if head[0]&0x70 != 0 {
		return false, 0, nil, protocolError("reserved bits set")
	}
	switch opcode {
	case opContinuation, opText, opBinary:
	case opClose, opPing, opPong:
		if !fin {
			return false, 0, nil, protocolError("a fragmented control frame")
		}
	default:
		return false, 0, nil, protocolError(fmt.Sprintf("unknown opcode %#x", opcode))
	}
	if head[1]&0x80 == 0 {
		return false, 0, nil, protocolError("an unmasked frame from the client")
	}
	n := uint64(head[1] & 0x7f)
	switch n {
	case 126:
		var b [2]byte
		if _, err := io.ReadFull(c.r, b[:]); err != nil {
			return false, 0, nil, err
		}
		n = uint64(binary.BigEndian.Uint16(b[:]))
	case 127:
		var b [8]byte
		if _, err := io.ReadFull(c.r, b[:]); err != nil {
			return false, 0, nil, err
		}
		n = binary.BigEndian.Uint64(b[:])
		if n>>63 != 0 {
			return false, 0, nil, protocolError("a length with its high bit set")
		}
	}
	if opcode >= opClose && n > 125 {
		return false, 0, nil, protocolError("a control frame longer than 125 bytes")
	}
	if n > wsMaxMessage {
		return false, 0, nil, &wsError{wsTooBig, "frame too large"}
	}
	var mask [4]byte
	if _, err := io.ReadFull(c.r, mask[:]); err != nil {
		return false, 0, nil, err
	}
	payload = make([]byte, n)
	if _, err := io.ReadFull(c.r, payload); err != nil {
		return false, 0, nil, err
	}
	for i := range payload {
		payload[i] ^= mask[i%4]
	}
	return fin, opcode, payload, nil
}

// parseClose reads a close frame's body (§5.5.1, §7.4).
func parseClose(payload []byte) (uint16, string, error) {
	switch {
	case len(payload) == 0:
		return 0, "", nil
	case len(payload) == 1:
		return 0, "", protocolError("a one-byte close")
	}
	code := binary.BigEndian.Uint16(payload)
	if !validCloseCode(code) {
		return 0, "", protocolError(fmt.Sprintf("close code %d", code))
	}
	reason := payload[2:]
	if !utf8.Valid(reason) {
		return 0, "", &wsError{wsInvalidPayload, "a close reason that isn't UTF-8"}
	}
	return code, string(reason), nil
}

func validCloseCode(code uint16) bool {
	switch {
	case code >= 1000 && code <= 1003, code >= 1007 && code <= 1011:
		return true
	case code >= 3000 && code <= 4999:
		return true
	}
	return false
}

// write sends one unmasked frame, as a server does; nothing after the
// close.
func (c *wsConn) write(opcode byte, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	return c.writeLocked(opcode, payload)
}

func (c *wsConn) writeLocked(opcode byte, payload []byte) error {
	header := []byte{0x80 | opcode}
	switch n := len(payload); {
	case n < 126:
		header = append(header, byte(n))
	case n < 1<<16:
		header = append(header, 126, byte(n>>8), byte(n))
	default:
		header = binary.BigEndian.AppendUint64(append(header, 127), uint64(n))
	}
	c.conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
	if _, err := c.conn.Write(header); err != nil {
		return err
	}
	_, err := c.conn.Write(payload)
	return err
}

// closeWith sends the server's close (once) and ends the socket: the
// connection closes when the client answers, or after wsCloseTimeout.
func (c *wsConn) closeWith(code uint16, reason string) {
	c.once.Do(func() {
		close(c.done)
		c.mu.Lock()
		if len(reason) > 123 {
			reason = reason[:123]
			for !utf8.ValidString(reason) {
				reason = reason[:len(reason)-1]
			}
		}
		c.writeLocked(opClose, append(binary.BigEndian.AppendUint16(nil, code), reason...))
		c.closed = true
		c.mu.Unlock()
		// The TCP connection closes after the client's own close, or once
		// it fails to answer in time.
		c.conn.SetReadDeadline(time.Now().Add(wsCloseTimeout))
		go func() {
			time.Sleep(wsCloseTimeout)
			c.conn.Close()
		}()
	})
}

// fail closes the socket for err: its code if it broke the protocol,
// else at once.
func (c *wsConn) fail(err error) {
	var ws *wsError
	switch {
	case errors.As(err, &ws):
		c.closeWith(ws.code, ws.reason)
	case errors.Is(err, io.EOF):
		c.closeWith(wsNormal, "")
	default:
		c.once.Do(func() { close(c.done) })
		c.conn.Close()
	}
}

// close is Jazz closing the socket with code and reason.
func (c *wsConn) close(code uint16, reason string) {
	if !validCloseCode(code) {
		code = wsInternalError
	}
	c.closeWith(code, reason)
}
