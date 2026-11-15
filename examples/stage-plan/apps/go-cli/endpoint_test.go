package main

// A stand-in for an app backend's Jazz endpoint (/jazz), as far as an end
// user's client meets it: backend-sign-in, and the sync socket, whose
// prelude it admits and relays to the core. It ports jazz-app's
// tests/relay (itself the logic of TS createJazz's admitPrelude) to the
// standard library: its authenticate reads "Authorization: Bearer
// token-<user>".

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

const backendIssuer = "jazz:backend"

type endpoint struct {
	srv    server
	bundle string
	url    string
}

func startEndpoint(t *testing.T, srv server, bundle string) *endpoint {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e := &endpoint{srv: srv, bundle: bundle, url: "http://" + listener.Addr().String() + "/jazz"}
	httpServer := &http.Server{Handler: http.HandlerFunc(e.serve)}
	go httpServer.Serve(listener)
	t.Cleanup(func() { httpServer.Close() })
	return e
}

func authenticate(authorization string) (string, bool) {
	user, ok := strings.CutPrefix(authorization, "Bearer token-")
	return user, ok && user != ""
}

func (e *endpoint) serve(w http.ResponseWriter, r *http.Request) {
	if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		e.relay(w, r)
		return
	}
	if !strings.HasSuffix(r.URL.Path, "/accounts/backend-sign-in") {
		http.NotFound(w, r)
		return
	}
	user, ok := authenticate(r.Header.Get("Authorization"))
	if !ok {
		http.Error(w, "signed_out", http.StatusUnauthorized)
		return
	}
	account, err := e.accountOf(user)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"account":   account,
		"identity":  map[string]any{"issuer": backendIssuer, "subject": user},
		"claims":    map[string]any{},
		"expiresAt": 0,
	})
}

// accountOf: the core resolves (or creates) the user's account.
func (e *endpoint) accountOf(user string) (string, error) {
	body, _ := json.Marshal(map[string]any{"identity": map[string]any{"issuer": backendIssuer, "subject": user}})
	req, _ := http.NewRequest("POST", fmt.Sprintf("%s/apps/%s/admin/accounts/login-or-register", e.srv.url, e.srv.appID), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Jazz-Backend-Secret", e.srv.backendSecret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		Account string `json:"account"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || out.Account == "" {
		return "", fmt.Errorf("login-or-register: %d %v", resp.StatusCode, err)
	}
	return out.Account, nil
}

// admit is admitPrelude: a device's own proof, or a session its bearer
// vouches for, rewritten with the backend's secret and the bundle.
func (e *endpoint) admit(prelude []byte, authorization string) ([]byte, error) {
	var p map[string]any
	if err := json.Unmarshal(prelude, &p); err != nil {
		return nil, err
	}
	auth, ok := p["auth"].(map[string]any)
	if !ok {
		return nil, errors.New("no auth")
	}
	for _, credential := range []string{"backend_secret", "admin_secret"} {
		if auth[credential] != nil {
			return nil, errors.New("a client states " + credential)
		}
	}
	if _, device := auth["jwt_token"].(string); device {
		if auth["backend_session"] != nil {
			return nil, errors.New("a device states a session")
		}
	} else {
		session, _ := auth["backend_session"].(map[string]any)
		stated, _ := session["user_id"].(string)
		user, ok := authenticate(authorization)
		if !ok || user != stated {
			return nil, errors.New("the bearer isn't the stated user")
		}
		account, err := e.accountOf(user)
		if err != nil {
			return nil, err
		}
		auth["backend_session"] = map[string]any{
			"account_id": account, "issuer": backendIssuer, "user_id": user, "claims": map[string]any{},
		}
	}
	auth["backend_secret"] = e.srv.backendSecret
	p["bundle"] = e.bundle
	return json.Marshal(p)
}

func (e *endpoint) relay(w http.ResponseWriter, r *http.Request) {
	conn, rw, err := w.(http.Hijacker).Hijack()
	if err != nil {
		return
	}
	defer conn.Close()
	sum := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n",
		base64.StdEncoding.EncodeToString(sum[:]))
	if rw.Flush() != nil {
		return
	}
	opcode, prelude, err := readFrame(rw.Reader)
	if err != nil || opcode != 2 {
		return
	}
	admitted, err := e.admit(prelude, r.Header.Get("Authorization"))
	if err != nil {
		return
	}
	core, coreReader, err := dialCore(e.srv)
	if err != nil {
		return
	}
	defer core.Close()
	if _, err := core.Write(maskedFrame(2, admitted)); err != nil {
		return
	}
	// From here on, frames pass as they are: the client's are masked, as
	// the core expects of a client; the core's aren't, as the client
	// expects of a server.
	done := make(chan struct{}, 2)
	go func() { io.Copy(core, rw.Reader); done <- struct{}{} }()
	go func() { io.Copy(conn, coreReader); done <- struct{}{} }()
	<-done
}

func dialCore(srv server) (net.Conn, *bufio.Reader, error) {
	u, err := url.Parse(srv.url)
	if err != nil {
		return nil, nil, err
	}
	conn, err := net.Dial("tcp", u.Host)
	if err != nil {
		return nil, nil, err
	}
	key := make([]byte, 16)
	rand.Read(key)
	fmt.Fprintf(conn, "GET /apps/%s/ws HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: %s\r\n\r\n",
		srv.appID, u.Host, base64.StdEncoding.EncodeToString(key))
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, nil)
	if err != nil || resp.StatusCode != http.StatusSwitchingProtocols {
		conn.Close()
		return nil, nil, fmt.Errorf("core upgrade: %v %v", resp, err)
	}
	return conn, reader, nil
}

// readFrame reads one (unfragmented) frame, unmasking a client's.
func readFrame(r *bufio.Reader) (byte, []byte, error) {
	var head [2]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return 0, nil, err
	}
	opcode, masked, n := head[0]&0x0f, head[1]&0x80 != 0, uint64(head[1]&0x7f)
	switch n {
	case 126:
		var b [2]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, nil, err
		}
		n = uint64(binary.BigEndian.Uint16(b[:]))
	case 127:
		var b [8]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, nil, err
		}
		n = binary.BigEndian.Uint64(b[:])
	}
	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(r, mask[:]); err != nil {
			return 0, nil, err
		}
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return opcode, payload, nil
}

// maskedFrame is a final frame as a client sends it.
func maskedFrame(opcode byte, payload []byte) []byte {
	var out bytes.Buffer
	out.WriteByte(0x80 | opcode)
	switch n := len(payload); {
	case n < 126:
		out.WriteByte(0x80 | byte(n))
	case n < 1<<16:
		out.WriteByte(0x80 | 126)
		binary.Write(&out, binary.BigEndian, uint16(n))
	default:
		out.WriteByte(0x80 | 127)
		binary.Write(&out, binary.BigEndian, uint64(n))
	}
	var mask [4]byte
	rand.Read(mask[:])
	out.Write(mask[:])
	for i, b := range payload {
		out.WriteByte(b ^ mask[i%4])
	}
	return out.Bytes()
}
