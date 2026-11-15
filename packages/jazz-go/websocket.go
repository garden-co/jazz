package jazz

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
)

// A minimal server side of RFC 6455, enough for Jazz's sync socket:
// binary messages (fragmented or not), pings, and closing.

type wsConn struct {
	conn net.Conn
	r    *bufio.Reader
	mu   sync.Mutex // guards writes
}

func isUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket") &&
		strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

func upgrade(w http.ResponseWriter, r *http.Request) (*wsConn, error) {
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		return nil, errors.New("not a websocket upgrade")
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("the server can't hand over its connection")
	}
	conn, rw, err := hijacker.Hijack()
	if err != nil {
		return nil, err
	}
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	header := "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " +
		base64.StdEncoding.EncodeToString(sum[:]) + "\r\n"
	if protocol := r.Header.Get("Sec-WebSocket-Protocol"); protocol != "" {
		header += "Sec-WebSocket-Protocol: " + strings.TrimSpace(strings.Split(protocol, ",")[0]) + "\r\n"
	}
	if _, err := rw.WriteString(header + "\r\n"); err != nil {
		conn.Close()
		return nil, err
	}
	if err := rw.Flush(); err != nil {
		conn.Close()
		return nil, err
	}
	return &wsConn{conn: conn, r: rw.Reader}, nil
}

const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xa
)

// readMessage is the next data message, answering pings on the way; it
// returns io.EOF once the client closed.
func (c *wsConn) readMessage() ([]byte, error) {
	var message []byte
	for {
		fin, opcode, payload, err := c.readFrame()
		if err != nil {
			return nil, err
		}
		switch opcode {
		case opPing:
			c.write(opPong, payload)
		case opPong:
		case opClose:
			return nil, io.EOF
		case opText, opBinary, opContinuation:
			message = append(message, payload...)
			if fin {
				return message, nil
			}
		}
	}
}

func (c *wsConn) readFrame() (bool, byte, []byte, error) {
	var head [2]byte
	if _, err := io.ReadFull(c.r, head[:]); err != nil {
		return false, 0, nil, err
	}
	fin, opcode, masked, n := head[0]&0x80 != 0, head[0]&0x0f, head[1]&0x80 != 0, uint64(head[1]&0x7f)
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
	}
	if n > 256<<20 {
		return false, 0, nil, errors.New("websocket frame too large")
	}
	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(c.r, mask[:]); err != nil {
			return false, 0, nil, err
		}
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(c.r, payload); err != nil {
		return false, 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return fin, opcode, payload, nil
}

// write sends one unmasked frame, as a server does.
func (c *wsConn) write(opcode byte, payload []byte) error {
	header := []byte{0x80 | opcode}
	switch n := len(payload); {
	case n < 126:
		header = append(header, byte(n))
	case n < 1<<16:
		header = append(header, 126, byte(n>>8), byte(n))
	default:
		header = binary.BigEndian.AppendUint64(append(header, 127), uint64(n))
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.conn.Write(header); err != nil {
		return err
	}
	_, err := c.conn.Write(payload)
	return err
}

func (c *wsConn) close(code uint16, reason string) {
	payload := binary.BigEndian.AppendUint16(nil, code)
	if len(reason) > 123 {
		reason = reason[:123]
	}
	c.write(opClose, append(payload, reason...))
	c.conn.Close()
}
