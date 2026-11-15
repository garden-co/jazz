package jazz

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// frame is a client frame: masked unless unmasked, with raw first byte.
func frame(first byte, payload []byte, unmasked bool) []byte {
	var b bytes.Buffer
	b.WriteByte(first)
	maskBit := byte(0x80)
	if unmasked {
		maskBit = 0
	}
	switch n := len(payload); {
	case n < 126:
		b.WriteByte(maskBit | byte(n))
	case n < 1<<16:
		b.WriteByte(maskBit | 126)
		binary.Write(&b, binary.BigEndian, uint16(n))
	default:
		b.WriteByte(maskBit | 127)
		binary.Write(&b, binary.BigEndian, uint64(n))
	}
	if unmasked {
		b.Write(payload)
		return b.Bytes()
	}
	mask := [4]byte{1, 2, 3, 4}
	b.Write(mask[:])
	for i, c := range payload {
		b.WriteByte(c ^ mask[i%4])
	}
	return b.Bytes()
}

// serve feeds raw bytes to a server-side socket and returns what
// readMessage made of them, and the close code the server sent (0 if
// none).
func serve(t *testing.T, raw ...[]byte) ([]byte, error, uint16) {
	t.Helper()
	client, server := net.Pipe()
	c := newWSConn(server)
	go func() {
		for _, r := range raw {
			client.Write(r)
		}
	}()
	type result struct {
		message []byte
		err     error
	}
	done := make(chan result, 1)
	go func() {
		m, err := c.readMessage()
		if err != nil {
			c.fail(err)
		}
		done <- result{m, err}
	}()
	// Read what the server answers (pongs, then its close frame, if any).
	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	var got []byte
	var code uint16
	buf := make([]byte, 256)
	for code == 0 {
		n, err := client.Read(buf)
		got = append(got, buf[:n]...)
		// Skip whole pong frames; stop at the close.
		for len(got) >= 2 && got[0] == 0x80|opPong && len(got) >= 2+int(got[1]) {
			got = got[2+int(got[1]):]
		}
		if len(got) >= 4 && got[0] == 0x80|opClose {
			code = binary.BigEndian.Uint16(got[2:4])
		}
		if err != nil {
			break
		}
	}
	client.Close()
	r := <-done
	return r.message, r.err, code
}

func closeCode(err error) uint16 {
	var ws *wsError
	if errors.As(err, &ws) {
		return ws.code
	}
	return 0
}

func TestWebSocketRefusesMalformedFrames(t *testing.T) {
	for _, c := range []struct {
		name string
		raw  [][]byte
		code uint16
	}{
		{"unmasked", [][]byte{frame(0x80|opBinary, []byte("hi"), true)}, wsProtocolError},
		{"reserved bits", [][]byte{frame(0xc0|opBinary, []byte("hi"), false)}, wsProtocolError},
		{"unknown opcode", [][]byte{frame(0x80|0x3, nil, false)}, wsProtocolError},
		{"fragmented ping", [][]byte{frame(opPing, nil, false)}, wsProtocolError},
		{"long ping", [][]byte{frame(0x80|opPing, make([]byte, 126), false)}, wsProtocolError},
		{"continuation first", [][]byte{frame(0x80|opContinuation, []byte("x"), false)}, wsProtocolError},
		{"message inside a message", [][]byte{frame(opBinary, []byte("a"), false), frame(0x80|opBinary, []byte("b"), false)}, wsProtocolError},
		{"text", [][]byte{frame(0x80|opText, []byte("hi"), false)}, wsUnsupportedData},
		{"one-byte close", [][]byte{frame(0x80|opClose, []byte{3}, false)}, wsProtocolError},
		{"bad close code", [][]byte{frame(0x80|opClose, []byte{0x03, 0xec}, false)}, wsProtocolError},
		{"close reason not utf-8", [][]byte{frame(0x80|opClose, []byte{0x03, 0xe8, 0xff}, false)}, wsInvalidPayload},
		{"oversized frame", [][]byte{{0x80 | opBinary, 0x80 | 127, 0, 0, 0, 0, 0x7f, 0, 0, 0}}, wsTooBig},
		{"length high bit", [][]byte{{0x80 | opBinary, 0x80 | 127, 0x80, 0, 0, 0, 0, 0, 0, 0}}, wsProtocolError},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err, sent := serve(t, c.raw...)
			if closeCode(err) != c.code || sent != c.code {
				t.Fatalf("err %v (code %d), sent close %d; want %d", err, closeCode(err), sent, c.code)
			}
		})
	}
}

func TestWebSocketReadsMessagesAndAnswersPings(t *testing.T) {
	// A ping in the middle of a fragmented message is answered, and the
	// fragments join.
	message, err, _ := serve(t,
		frame(opBinary, []byte("hel"), false),
		frame(0x80|opPing, []byte("p"), false),
		frame(0x80|opContinuation, []byte("lo"), false),
	)
	if err != nil || string(message) != "hello" {
		t.Fatalf("%q %v", message, err)
	}
	// The client's close is echoed with its code.
	_, err, sent := serve(t, frame(0x80|opClose, []byte{0x0f, 0xa0}, false)) // 4000
	if !errors.Is(err, io.EOF) || sent != 4000 {
		t.Fatalf("close: %v, echoed %d", err, sent)
	}
}

func TestWebSocketHandshakeChecks(t *testing.T) {
	ok := http.Header{
		"Connection": {"keep-alive, Upgrade"}, "Upgrade": {"websocket"},
		"Sec-Websocket-Version": {"13"}, "Sec-Websocket-Key": {"dGhlIHNhbXBsZSBub25jZQ=="},
	}
	if status, why := checkUpgrade(&http.Request{Method: "GET", Header: ok}); status != 0 {
		t.Fatalf("a good upgrade: %d %s", status, why)
	}
	for name, change := range map[string]func(http.Header){
		"old version": func(h http.Header) { h.Set("Sec-Websocket-Version", "8") },
		"short key":   func(h http.Header) { h.Set("Sec-Websocket-Key", "c2hvcnQ=") },
		"no upgrade":  func(h http.Header) { h.Del("Upgrade") },
	} {
		h := ok.Clone()
		change(h)
		if status, _ := checkUpgrade(&http.Request{Method: "GET", Header: h}); status == 0 {
			t.Errorf("%s: accepted", name)
		}
	}
	if status, _ := checkUpgrade(&http.Request{Method: "POST", Header: ok}); status == 0 {
		t.Error("a POST upgrade: accepted")
	}
}
