package jazz

/*
#include <stdint.h>
#include <stdlib.h>
#include "jazz_ffi.h"
extern JazzHandle* jazz_go_open_user(const char* config, uintptr_t ctx, char** err);
extern JazzWatch* jazz_go_watch(JazzHandle* h, const char* what, uintptr_t ctx, char** err);
*/
import "C"

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// Auth is how an end user signs in: Token (a JWT from your auth provider)
// or DeviceAccount / DeviceKey (a local-first account kept on this device).
// A backend sets Permissions, BackendSecret, AdminSecret and Session
// instead.
type Auth interface {
	authJSON() (map[string]any, error)
}

// TokenReason is why a TokenProvider is asked.
type TokenReason string

const (
	TokenConnect TokenReason = "connect"
	TokenRenew   TokenReason = "renew"
	// TokenExpired: the last token was refused; get a fresh one.
	TokenExpired TokenReason = "expired"
	// TokenRefresh: the app asked (db.Refresh); get a fresh one.
	TokenRefresh TokenReason = "refresh"
)

// TokenProvider gets the signed-in user's token: "" when no one is
// signed in (the app then signs out). An error counts as "no token" too.
type TokenProvider func(ctx context.Context, reason TokenReason) (string, error)

type tokenAuth struct{ provider TokenProvider }

// Token signs in with the bearer tokens provider gets, asking again on
// renewal and when one is refused.
func Token(provider TokenProvider) Auth { return tokenAuth{provider} }

func (tokenAuth) authJSON() (map[string]any, error) { return map[string]any{"bearer": true}, nil }

type deviceAuth struct {
	key   string
	store SecretStore
}

// DeviceKey signs in as the local-first account whose key is key
// ("jazz-auth-v1:…", from GenerateDeviceKey).
func DeviceKey(key string) Auth { return deviceAuth{key: key} }

// DeviceAccount signs in as this device's local-first account, whose key
// store keeps: it is created (signing up) the first time.
func DeviceAccount(store SecretStore) Auth { return deviceAuth{store: store} }

func (a deviceAuth) authJSON() (map[string]any, error) {
	key := a.key
	if key == "" {
		var err error
		if key, err = a.store.Load(); err != nil {
			return nil, err
		}
		if key == "" {
			if key, err = GenerateDeviceKey(); err != nil {
				return nil, err
			}
			if err := a.store.Save(key); err != nil {
				return nil, err
			}
		}
	}
	return map[string]any{"deviceKey": key}, nil
}

// SecretStore keeps a device account's key: the OS keychain, ideally.
type SecretStore interface {
	// Load is the key, or "" when there is none yet.
	Load() (string, error)
	Save(key string) error
}

// FileStore keeps the key in a file readable only by its owner (0600): for
// hosts without a keychain.
func FileStore(path string) SecretStore { return fileStore(path) }

type fileStore string

func (f fileStore) Load() (string, error) {
	b, err := os.ReadFile(string(f))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	return strings.TrimSpace(string(b)), err
}

func (f fileStore) Save(key string) error {
	if err := os.MkdirAll(filepath.Dir(string(f)), 0o700); err != nil {
		return err
	}
	return os.WriteFile(string(f), []byte(key+"\n"), 0o600)
}

// GenerateDeviceKey is a new device account's key: keep it like a
// password.
func GenerateDeviceKey() (string, error) {
	var cerr *C.char
	key := C.jazz_generate_device_key(&cerr)
	if err := takeError(cerr); err != nil {
		return "", err
	}
	defer C.jazz_string_free(key)
	return C.GoString(key), nil
}

// token providers, by the id jazz-ffi's token callback gets.
var (
	providers    sync.Map
	nextProvider atomic.Uint64
)

//export jazzGoTokenCallback
func jazzGoTokenCallback(ctx C.uintptr_t, reason *C.char) *C.char {
	value, ok := providers.Load(uint64(ctx))
	if !ok {
		return nil
	}
	c, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	token, err := value.(TokenProvider)(c, TokenReason(C.GoString(reason)))
	if err != nil || token == "" {
		return nil
	}
	return C.CString(token) // jazz-ffi frees it
}

func openUser(config string, provider TokenProvider) (*C.JazzHandle, uint64, error) {
	id := nextProvider.Add(1)
	providers.Store(id, provider)
	text := C.CString(config)
	defer C.free(unsafe.Pointer(text))
	var cerr *C.char
	h := C.jazz_go_open_user(text, C.uintptr_t(id), &cerr)
	if err := takeError(cerr); err != nil || h == nil {
		providers.Delete(id)
		if err == nil {
			err = &Error{Kind: KindRefused, Message: "open failed"}
		}
		return nil, 0, err
	}
	return h, id, nil
}

// SessionStatus is whether someone is signed in.
type SessionStatus string

const (
	SignedIn  SessionStatus = "signedIn"
	Expired   SessionStatus = "expired"
	SignedOut SessionStatus = "signedOut"
)

// User is who is signed in.
type User struct {
	Account string          `json:"account"`
	Issuer  string          `json:"issuer"`
	Subject string          `json:"subject"`
	Claims  json.RawMessage `json:"claims"`
	// Kind is how they signed in (a device account, or a bearer).
	Kind string `json:"kind"`
}

// SessionState is an end user's session.
type SessionState struct {
	Status SessionStatus `json:"status"`
	User   *User         `json:"user"`
	// Reason says why an Expired session expired.
	Reason string `json:"reason,omitempty"`
}

// SyncStatus is the state of the app's link to its server, as Jazz
// reports it (e.g. {"status": "connected"}).
type SyncStatus struct {
	Link json.RawMessage `json:"link"`
}

// Session is the end user's session, or nil for a local app or a
// backend.
func (db *DB) Session(ctx context.Context) (*SessionState, error) {
	var s *SessionState
	err := db.call(ctx, "session", map[string]any{}, &s)
	return s, err
}

// SyncStatus is the app's link to its server.
func (db *DB) SyncStatus(ctx context.Context) (SyncStatus, error) {
	var s SyncStatus
	err := db.call(ctx, "syncStatus", map[string]any{}, &s)
	return s, err
}

// Refresh asks the TokenProvider for a fresh token now.
func (db *DB) Refresh(ctx context.Context) error {
	return db.call(ctx, "refresh", map[string]any{}, nil)
}

// SignOut signs the end user out and closes the app.
func (db *DB) SignOut(ctx context.Context) error {
	if err := db.call(ctx, "signOut", map[string]any{}, nil); err != nil {
		return err
	}
	return db.Close()
}

// OnSession is the session's states, the current one first, until ctx is
// done.
func (db *DB) OnSession(ctx context.Context) (<-chan SessionState, error) {
	return watch(ctx, db, "session", func(e event) (SessionState, bool) {
		var s SessionState
		if e.kind == "error" || json.Unmarshal([]byte(e.json), &s) != nil {
			return s, true
		}
		return s, false
	})
}

// OnSyncStatus is the link's states, the current one first, until ctx is
// done.
func (db *DB) OnSyncStatus(ctx context.Context) (<-chan SyncStatus, error) {
	return watch(ctx, db, "sync", func(e event) (SyncStatus, bool) {
		var s SyncStatus
		if e.kind == "error" || json.Unmarshal([]byte(e.json), &s) != nil {
			return s, true
		}
		return s, false
	})
}

func watch[T any](ctx context.Context, db *DB, what string, decode func(event) (T, bool)) (<-chan T, error) {
	return stream(ctx, db, decode, func(id uint64) (func(), error) {
		cwhat := C.CString(what)
		defer C.free(unsafe.Pointer(cwhat))
		var cerr *C.char
		w := C.jazz_go_watch(db.handle, cwhat, C.uintptr_t(id), &cerr)
		if err := takeError(cerr); err != nil || w == nil {
			if err == nil {
				err = &Error{Kind: KindRefused, Message: "watch failed"}
			}
			return nil, err
		}
		return func() { C.jazz_unwatch(w) }, nil
	})
}
