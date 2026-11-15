// Package jazz is the Go runtime for Jazz: it opens an app through the
// jazz-ffi C ABI (cgo) and reads and writes its rows as JSON, typed by the
// bindings `jazz-tools generate` writes into a `jazzapp` package beside an
// app's jazz.app.json and go.mod.
//
// Building needs the jazz-ffi static library: `cargo build -p jazz-ffi`
// in the Jazz repository puts it where this package looks first
// (target/debug, then target/release); elsewhere, point CGO_LDFLAGS at its
// directory (`-L<dir>`), which cgo searches before these.
package jazz

/*
#cgo CFLAGS: -I${SRCDIR}/../../crates/jazz-ffi/include
#cgo LDFLAGS: -L${SRCDIR}/../../target/debug -L${SRCDIR}/../../target/release
#cgo linux LDFLAGS: -l:libjazz_ffi.a -lm -ldl -lpthread
#cgo darwin LDFLAGS: -ljazz_ffi -framework CoreFoundation -framework Security -framework SystemConfiguration
#include <stdlib.h>
#include "jazz_ffi.h"
*/
import "C"

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"unsafe"
)

// Config is how an app is opened. Generated bindings' Open sets App.
type Config struct {
	// App is the text of jazz.app.json.
	App string `json:"app"`
	// Permissions is the text of jazz.permissions.json: set, the app is
	// opened as a backend (it publishes the bundle and acts as Session).
	Permissions string `json:"permissions,omitempty"`
	AppID       string `json:"appId,omitempty"`
	// ServerURL is the sync server; empty keeps the app local.
	ServerURL string `json:"serverUrl,omitempty"`
	// DataDir is where persistent storage lives.
	DataDir string `json:"dataDir,omitempty"`
	// Storage is StorageMemory or StoragePersistent (the default).
	Storage       Storage `json:"storage,omitempty"`
	BackendSecret string  `json:"backendSecret,omitempty"`
	// AdminSecret lets a backend publish the app's schema and permissions.
	AdminSecret string `json:"adminSecret,omitempty"`
	// Session is the user a backend acts as. Its Issuer is "jazz:backend"
	// (left empty, it is set so).
	Session *Session `json:"session,omitempty"`
}

// Storage is where an app keeps its rows.
type Storage string

const (
	StorageMemory     Storage = "memory"
	StoragePersistent Storage = "persistent"
)

// BackendIssuer is the issuer of a backend's Session.
const BackendIssuer = "jazz:backend"

// Session is the user an app acts as.
type Session struct {
	Issuer  string `json:"issuer"`
	Subject string `json:"subject"`
}

// ErrClosed is returned by calls on a closed DB.
var ErrClosed = errors.New("jazz: closed")

// Error is what Jazz refused or failed at.
type Error struct {
	// Kind is "refused" (validation, permissions, not supported yet),
	// "io" or "closed".
	Kind    string
	Message string
}

func (e *Error) Error() string { return "jazz: " + e.Message }

// Is makes errors.Is(err, ErrClosed) hold for a closed handle.
func (e *Error) Is(target error) bool { return target == ErrClosed && e.Kind == "closed" }

func takeError(err *C.char) error {
	if err == nil {
		return nil
	}
	text := C.GoString(err)
	C.jazz_string_free(err)
	return parseError(text)
}

func parseError(text string) error {
	// jazz-ffi errors are `{"kind","message"}` JSON, or plain text.
	var e struct {
		Kind    string `json:"kind"`
		Message string `json:"message"`
	}
	if json.Unmarshal([]byte(text), &e) == nil && e.Message != "" {
		return &Error{Kind: strings.ToLower(e.Kind), Message: e.Message}
	}
	if strings.EqualFold(text, "closed") {
		return &Error{Kind: "closed", Message: "closed"}
	}
	return &Error{Kind: "refused", Message: text}
}

// DB is an open app. It is safe for concurrent use.
type DB struct {
	mu     sync.RWMutex
	handle *C.JazzHandle
}

// Open opens the app cfg describes. Prefer the generated jazzapp.Open,
// which embeds the app's jazz.app.json.
func Open(ctx context.Context, cfg Config) (*DB, error) {
	if cfg.Session != nil && cfg.Session.Issuer == "" {
		session := *cfg.Session
		session.Issuer = BackendIssuer
		cfg.Session = &session
	}
	config, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	type opened struct {
		h   *C.JazzHandle
		err error
	}
	done := make(chan opened, 1)
	go func() {
		text := C.CString(string(config))
		defer C.free(unsafe.Pointer(text))
		var cerr *C.char
		h := C.jazz_open(text, &cerr)
		done <- opened{h, takeError(cerr)}
	}()
	select {
	case o := <-done:
		if o.err != nil {
			return nil, o.err
		}
		return &DB{handle: o.h}, nil
	case <-ctx.Done():
		go func() {
			if o := <-done; o.h != nil {
				C.jazz_close(o.h)
			}
		}()
		return nil, ctx.Err()
	}
}

// Close closes the app; later calls return ErrClosed.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.handle != nil {
		C.jazz_close(db.handle)
		db.handle = nil
	}
	return nil
}

// call runs jazz_call(method, args) and decodes its result into out.
// Calls block in Jazz; a cancelled ctx returns at once and the call's
// result is dropped, but the call itself runs to its end: a write may
// still apply.
func (db *DB) call(ctx context.Context, method string, args any, out any) error {
	argsJSON, err := json.Marshal(args)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	type result struct {
		text string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		db.mu.RLock()
		defer db.mu.RUnlock()
		if db.handle == nil {
			done <- result{err: &Error{Kind: "closed", Message: "closed"}}
			return
		}
		cmethod := C.CString(method)
		cargs := C.CString(string(argsJSON))
		defer C.free(unsafe.Pointer(cmethod))
		defer C.free(unsafe.Pointer(cargs))
		var cerr *C.char
		res := C.jazz_call(db.handle, cmethod, cargs, &cerr)
		var r result
		if res != nil {
			r.text = C.GoString(res)
			C.jazz_string_free(res)
		}
		r.err = takeError(cerr)
		done <- r
	}()
	select {
	case r := <-done:
		if r.err != nil {
			return r.err
		}
		if out == nil || r.text == "" {
			return nil
		}
		return json.Unmarshal([]byte(r.text), out)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Update changes the fields patch (a generated patch struct, or any value
// that marshals to a JSON object of columns) sets on the row of table
// with id.
func (db *DB) Update(ctx context.Context, table, id string, patch any) error {
	return db.call(ctx, "update", map[string]any{"table": table, "id": id, "patch": patch}, nil)
}

// Delete deletes the row of table with id.
func (db *DB) Delete(ctx context.Context, table, id string) error {
	return db.call(ctx, "delete", map[string]any{"table": table, "id": id}, nil)
}

// Account is the id of the account the app acts as (its session's user),
// or "" when it has none.
func (db *DB) Account(ctx context.Context) (string, error) {
	var account *string
	if err := db.call(ctx, "account", map[string]any{}, &account); err != nil {
		return "", err
	}
	if account == nil {
		return "", nil
	}
	return *account, nil
}
