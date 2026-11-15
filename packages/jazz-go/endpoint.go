package jazz

/*
#include <stdint.h>
#include <stdlib.h>
#include "jazz_ffi.h"
extern JazzEndpoint* jazz_go_endpoint_new(JazzHandle* h, uintptr_t ctx, const char* options, char** err);
extern JazzSocket* jazz_go_endpoint_socket(JazzEndpoint* e, const char* upgrade, uintptr_t ctx, char** err);
*/
import "C"

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
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
	ptr  *C.JazzEndpoint
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
	coptions := C.CString(string(text))
	defer C.free(unsafe.Pointer(coptions))
	e := &Endpoint{id: nextEndpoint.Add(1), auth: auth}
	endpoints.Store(e.id, e)
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.handle == nil {
		endpoints.Delete(e.id)
		return nil, &Error{Kind: KindClosed, Message: "closed"}
	}
	var cerr *C.char
	e.ptr = C.jazz_go_endpoint_new(db.handle, C.uintptr_t(e.id), coptions, &cerr)
	if err := takeError(cerr); err != nil || e.ptr == nil {
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
	if e.ptr != nil {
		C.jazz_endpoint_free(e.ptr)
		e.ptr = nil
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

//export jazzGoAuthCallback
func jazzGoAuthCallback(ctx C.uintptr_t, request *C.char) *C.char {
	value, ok := endpoints.Load(uint64(ctx))
	if !ok {
		return nil
	}
	user, err := value.(*Endpoint).auth(fromRequestJSON(C.GoString(request)))
	if err != nil || user == nil {
		return nil
	}
	answer := map[string]any{"kind": "deviceAccount"}
	if user.UserID != "" {
		answer = map[string]any{"kind": "user", "userId": user.UserID, "expiresAt": user.ExpiresAt.Unix()}
		if user.Claims != nil {
			answer["claims"] = user.Claims
		}
	}
	b, _ := json.Marshal(answer)
	return C.CString(string(b)) // jazz-ffi frees it
}

// ServeHTTP answers a request to the endpoint, and serves its sync socket.
func (e *Endpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	e.mu.RLock()
	if e.ptr == nil {
		e.mu.RUnlock()
		http.Error(w, "the endpoint is closed", http.StatusServiceUnavailable)
		return
	}
	path := C.CString(r.URL.Path)
	socket := C.jazz_endpoint_is_socket(e.ptr, path) == 1
	C.free(unsafe.Pointer(path))
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
	creq := C.CString(toRequestJSON(r, body))
	defer C.free(unsafe.Pointer(creq))
	var cerr *C.char
	e.mu.RLock()
	cresp := C.jazz_endpoint_handle(e.ptr, creq, &cerr)
	e.mu.RUnlock()
	if err := takeError(cerr); err != nil || cresp == nil {
		http.Error(w, "jazz endpoint failed", http.StatusInternalServerError)
		return
	}
	var resp struct {
		Status  int         `json:"status"`
		Headers [][2]string `json:"headers"`
		Body    string      `json:"body"`
	}
	text := C.GoString(cresp)
	C.jazz_string_free(cresp)
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
	cup := C.CString(upgradeJSON)
	var cerr *C.char
	e.mu.RLock()
	s := C.jazz_go_endpoint_socket(e.ptr, cup, C.uintptr_t(id), &cerr)
	e.mu.RUnlock()
	C.free(unsafe.Pointer(cup))
	if err := takeError(cerr); err != nil || s == nil {
		sockets.Delete(id)
		conn.close(wsInternalError, "jazz endpoint refused the socket")
		return
	}
	defer func() {
		C.jazz_socket_free(s) // waits for on_close
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
		var cerr *C.char
		pushed := C.jazz_socket_push(s, (*C.uchar)(unsafe.Pointer(&message[0])), C.size_t(len(message)), &cerr)
		if takeError(cerr) != nil || pushed != 0 {
			return
		}
	}
}

//export jazzGoSocketMessage
func jazzGoSocketMessage(ctx C.uintptr_t, data *C.uchar, n C.size_t) {
	if value, ok := sockets.Load(uint64(ctx)); ok {
		value.(*wsConn).write(opBinary, C.GoBytes(unsafe.Pointer(data), C.int(n)))
	}
}

//export jazzGoSocketClosed
func jazzGoSocketClosed(ctx C.uintptr_t, code C.ushort, reason *C.char) {
	if value, ok := sockets.Load(uint64(ctx)); ok {
		value.(*wsConn).close(uint16(code), C.GoString(reason))
	}
}

// ForUser is a handle acting as the request's user (TS forReqUser), for a
// backend route to read and write as them. Close it when done.
func (e *Endpoint) ForUser(r *http.Request) (*DB, error) {
	creq := C.CString(toRequestJSON(r, nil))
	defer C.free(unsafe.Pointer(creq))
	var cerr *C.char
	e.mu.RLock()
	h := C.jazz_endpoint_for_user(e.ptr, creq, &cerr)
	e.mu.RUnlock()
	if err := takeError(cerr); err != nil || h == nil {
		if err == nil {
			err = &Error{Kind: KindRefused, Message: "no user"}
		}
		return nil, err
	}
	return &DB{handle: h}, nil
}
