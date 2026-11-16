package jazz

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// EndpointUser is who a request is for, as your Authenticate says.
type EndpointUser struct {
	// UserID is your auth provider's id of the user.
	UserID string
	Claims map[string]any
	// ExpiresAt is when the user's credential expires.
	ExpiresAt time.Time
}

// Authenticate is your check of a request to the endpoint: the user its
// credentials (e.g. a bearer token) name, or nil for no one. Return
// AnyDeviceAccount to let a local-first device account sign in on its own
// proof.
type Authenticate func(r *http.Request) (*EndpointUser, error)

// AnyDeviceAccount is what Authenticate returns for a request that may sign
// in as a local-first device account (an EndpointUser without a UserID).
var AnyDeviceAccount = &EndpointUser{}

// EndpointOptions configures an Endpoint.
type EndpointOptions struct {
	// BasePath is where it is mounted (default "/jazz").
	BasePath string
	// SessionTTL is how long a session it vouches for lasts.
	SessionTTL time.Duration
	// MaxSockets caps its open sync sockets (more close with 1013); 0
	// keeps Jazz's default.
	MaxSockets int
}

// Endpoint is your backend's Jazz endpoint, TS createJazz's handler: end
// users' clients (TS, Swift, Kotlin, Go) sign in and sync through it.
// Mount it on your server at BasePath; it is an http.Handler.
type Endpoint struct {
	ptr  uintptr
	id   uint64
	auth Authenticate
	mu   sync.RWMutex
}

var (
	endpoints    sync.Map // id → *Endpoint
	nextEndpoint atomic.Uint64
	sockets      sync.Map // id → *wsConn
	nextSocket   atomic.Uint64
)

// NewEndpoint is db's endpoint (db: a backend, opened with Permissions).
func NewEndpoint(db *DB, auth Authenticate, opts EndpointOptions) (*Endpoint, error) {
	options := map[string]any{}
	if opts.BasePath != "" {
		options["basePath"] = opts.BasePath
	}
	if opts.SessionTTL > 0 {
		options["sessionTtlSecs"] = int64(opts.SessionTTL / time.Second)
	}
	if opts.MaxSockets > 0 {
		options["maxSockets"] = opts.MaxSockets
	}
	text, _ := json.Marshal(options)
	e := &Endpoint{id: nextEndpoint.Add(1), auth: auth}
	endpoints.Store(e.id, e)
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.handle == 0 {
		endpoints.Delete(e.id)
		return nil, &Error{Kind: KindClosed, Message: "closed"}
	}
	var cerr uintptr
	e.ptr = ffi.endpointNew(db.handle, ffi.authCallback, uintptr(e.id), cstr(string(text)), &cerr)
	if err := takeError(cerr); err != nil || e.ptr == 0 {
		endpoints.Delete(e.id)
		if err == nil {
			err = &Error{Kind: KindRefused, Message: "endpoint failed"}
		}
		return nil, err
	}
	return e, nil
}

// Close frees the endpoint.
func (e *Endpoint) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ptr != 0 {
		ffi.endpointFree(e.ptr)
		e.ptr = 0
		endpoints.Delete(e.id)
	}
}

type requestJSON struct {
	Method  string      `json:"method"`
	Path    string      `json:"path"`
	Headers [][2]string `json:"headers"`
	Body    string      `json:"body,omitempty"`
}

func toRequestJSON(r *http.Request, body []byte) string {
	req := requestJSON{Method: r.Method, Path: r.URL.RequestURI()}
	for name, values := range r.Header {
		for _, v := range values {
			req.Headers = append(req.Headers, [2]string{strings.ToLower(name), v})
		}
	}
	if len(body) > 0 {
		req.Body = base64.StdEncoding.EncodeToString(body)
	}
	b, _ := json.Marshal(req)
	return string(b)
}

func fromRequestJSON(text string) *http.Request {
	var req requestJSON
	_ = json.Unmarshal([]byte(text), &req)
	u, _ := url.ParseRequestURI(req.Path)
	if u == nil {
		u = &url.URL{Path: req.Path}
	}
	r := &http.Request{Method: req.Method, URL: u, Header: http.Header{}, RequestURI: req.Path}
	for _, h := range req.Headers {
		r.Header.Add(h[0], h[1])
	}
	if body, err := base64.StdEncoding.DecodeString(req.Body); err == nil && len(body) > 0 {
		r.Body = io.NopCloser(strings.NewReader(string(body)))
		r.ContentLength = int64(len(body))
	}
	return r.WithContext(context.Background())
}

// onAuth is jazz-ffi's authenticate callback: a malloc'ed answer, or 0
// for no one.
func onAuth(ctx, request uintptr) (result uintptr) {
	// A panicking Authenticate names no one (fails closed).
	defer func() {
		if recover() != nil {
			result = 0
		}
	}()
	value, ok := endpoints.Load(uint64(ctx))
	if !ok {
		return 0
	}
	user, err := value.(*Endpoint).auth(fromRequestJSON(goString(request)))
	if err != nil || user == nil {
		return 0
	}
	answer := map[string]any{"kind": "deviceAccount"}
	if user.UserID != "" {
		answer = map[string]any{"kind": "user", "userId": user.UserID, "expiresAt": user.ExpiresAt.Unix()}
		if user.Claims != nil {
			answer["claims"] = user.Claims
		}
	}
	b, _ := json.Marshal(answer)
	return mallocString(string(b)) // jazz-ffi frees it
}

// ServeHTTP answers a request to the endpoint, and serves its sync socket.
func (e *Endpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	e.mu.RLock()
	if e.ptr == 0 {
		e.mu.RUnlock()
		http.Error(w, "the endpoint is closed", http.StatusServiceUnavailable)
		return
	}
	socket := ffi.endpointIsSocket(e.ptr, cstr(r.URL.Path)) == 1
	e.mu.RUnlock()
	if socket && isUpgrade(r) {
		if status, why := checkUpgrade(r); status != 0 {
			if status == http.StatusUpgradeRequired {
				w.Header().Set("Sec-WebSocket-Version", "13")
			}
			http.Error(w, why, status)
			return
		}
		e.serveSocket(w, r)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var cerr uintptr
	e.mu.RLock()
	cresp := ffi.endpointHandle(e.ptr, cstr(toRequestJSON(r, body)), &cerr)
	e.mu.RUnlock()
	if err := takeError(cerr); err != nil || cresp == 0 {
		http.Error(w, "jazz endpoint failed", http.StatusInternalServerError)
		return
	}
	var resp struct {
		Status  int         `json:"status"`
		Headers [][2]string `json:"headers"`
		Body    string      `json:"body"`
	}
	text, _ := takeString(cresp)
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		http.Error(w, "jazz endpoint: bad response", http.StatusInternalServerError)
		return
	}
	for _, h := range resp.Headers {
		w.Header().Add(h[0], h[1])
	}
	w.WriteHeader(resp.Status)
	if b, err := base64.StdEncoding.DecodeString(resp.Body); err == nil {
		w.Write(b)
	}
}

func (e *Endpoint) serveSocket(w http.ResponseWriter, r *http.Request) {
	upgradeJSON := toRequestJSON(r, nil)
	conn, err := upgrade(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	id := nextSocket.Add(1)
	sockets.Store(id, conn)
	var cerr uintptr
	e.mu.RLock()
	s := ffi.endpointSocket(e.ptr, cstr(upgradeJSON), ffi.socketMessage, ffi.socketClosed, uintptr(id), &cerr)
	e.mu.RUnlock()
	if err := takeError(cerr); err != nil || s == 0 {
		sockets.Delete(id)
		conn.close(wsInternalError, "jazz endpoint refused the socket")
		return
	}
	defer func() {
		ffi.socketFree(s) // waits for on_close
		sockets.Delete(id)
	}()
	for {
		message, err := conn.readMessage()
		if err != nil {
			conn.fail(err)
			return
		}
		if len(message) == 0 {
			continue
		}
		var cerr uintptr
		pushed := ffi.socketPush(s, unsafe.Pointer(&message[0]), uintptr(len(message)), &cerr)
		runtime.KeepAlive(message)
		if takeError(cerr) != nil || pushed != 0 {
			return
		}
	}
}

// onSocketMessage is jazz-ffi's on_message: a message for the client.
func onSocketMessage(ctx, data, n uintptr) {
	defer func() { _ = recover() }()
	if value, ok := sockets.Load(uint64(ctx)); ok {
		value.(*wsConn).write(opBinary, goBytes(data, n))
	}
}

// onSocketClosed is jazz-ffi's on_close: the code and reason to close with.
func onSocketClosed(ctx, code, reason uintptr) {
	defer func() { _ = recover() }()
	if value, ok := sockets.Load(uint64(ctx)); ok {
		value.(*wsConn).close(uint16(code), goString(reason))
	}
}

// ForUser is a handle acting as the request's user (TS forReqUser), for a
// backend route to read and write as them. Close it when done.
func (e *Endpoint) ForUser(r *http.Request) (*DB, error) {
	var cerr uintptr
	e.mu.RLock()
	h := ffi.endpointForUser(e.ptr, cstr(toRequestJSON(r, nil)), &cerr)
	e.mu.RUnlock()
	if err := takeError(cerr); err != nil || h == 0 {
		if err == nil {
			err = &Error{Kind: KindRefused, Message: "no user"}
		}
		return nil, err
	}
	return &DB{handle: h}, nil
}
