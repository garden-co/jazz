// Package jazz is the Go runtime for Jazz: it opens an app through the
// jazz-ffi C ABI (cgo) and reads and writes its rows as JSON, typed by the
// bindings `jazz-tools generate` writes into a `jazzapp` package beside an
// app's jazz.app.json and go.mod.
//
// It needs no cgo and no C compiler: it loads jazz-ffi's shared library
// at run time (see ffi.go for where it finds it).
package jazz

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
)

// Config is how an app is opened. Generated bindings' Open sets App.
type Config struct {
	// App is the text of jazz.app.json.
	App   string `json:"app"`
	AppID string `json:"appId,omitempty"`
	// Permissions is the text of the app's jazz.permissions.json: a
	// backend client (Auth: Credential) needs it, as TS createJazzClient
	// does. It is not published.
	Permissions string `json:"permissions,omitempty"`
	// ServerURL is the sync server; empty keeps the app local.
	ServerURL string `json:"serverUrl,omitempty"`
	// DataDir is where persistent storage lives.
	DataDir string `json:"dataDir,omitempty"`
	// Storage is StorageMemory or StoragePersistent (the default).
	Storage Storage `json:"storage,omitempty"`
	// Computed is the app's implementation of its generated Computed
	// interface (jazzapp.Open sets it).
	Computed any `json:"-"`
	// Auth signs an end user in (Token, DeviceAccount, DeviceKey): with
	// ServerURL set to the app's Jazz endpoint (its backend's /jazz).
	Auth Auth `json:"-"`
	// AllowPlaintext lets ServerURL be http:// or ws:// to a host other than
	// this machine (passed to the core as allowInsecureHttp). Credentials
	// then cross the network unencrypted: only for a private network you
	// trust.
	AllowPlaintext bool `json:"allowInsecureHttp,omitempty"`
}

// String is c without its secrets, so logging a Config never leaks them.
// (json.Marshal(c), which Open sends to the core, does include them.)
func (c Config) String() string {
	auth := "none"
	if c.Auth != nil {
		auth = fmt.Sprintf("%T", c.Auth)
	}
	return fmt.Sprintf("jazz.Config{AppID: %q, ServerURL: %q, Storage: %q, DataDir: %q, Auth: %s}",
		c.AppID, c.ServerURL, c.Storage, c.DataDir, auth)
}

// GoString is String, for %#v.
func (c Config) GoString() string { return c.String() }

// checkTransport refuses plaintext (http, ws) to a host other than this
// machine, unless AllowPlaintext.
func (c Config) checkTransport() error {
	if c.ServerURL == "" || c.AllowPlaintext {
		return nil
	}
	u, err := url.Parse(c.ServerURL)
	if err != nil {
		return &Error{Kind: KindRefused, Message: "config.ServerURL is not a URL"}
	}
	switch strings.ToLower(u.Scheme) {
	case "https", "wss":
		return nil
	case "http", "ws":
		host := u.Hostname()
		if host == "localhost" || strings.HasSuffix(host, ".localhost") {
			return nil
		}
		if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
			return nil
		}
		return &Error{Kind: KindRefused, Message: "config.ServerURL is plaintext (" + u.Scheme + "://) to " + host +
			": use https:// or wss://, or set AllowPlaintext for a trusted private network"}
	}
	return &Error{Kind: KindRefused, Message: "config.ServerURL must be http(s):// or ws(s)://"}
}

// Storage is where an app keeps its rows.
type Storage string

const (
	StorageMemory     Storage = "memory"
	StoragePersistent Storage = "persistent"
)

// DB is an open app. It is safe for concurrent use.
type DB struct {
	mu       sync.RWMutex
	handle   uintptr
	app      any
	provider uint64 // the TokenProvider's id, if any

	// streams are the DB's open subscriptions and watches, by id: each
	// stops its jazz-ffi side at once; its channel closes once what was
	// queued is sent.
	streamsMu sync.Mutex
	streams   map[uint64]func()
}

// endStreams ends the DB's subscriptions and watches (closing their
// channels) before the handle closes.
func (db *DB) endStreams() {
	db.streamsMu.Lock()
	ends := make([]func(), 0, len(db.streams))
	for _, end := range db.streams {
		ends = append(ends, end)
	}
	db.streams = nil
	db.streamsMu.Unlock()
	for _, end := range ends {
		end()
	}
}

func (db *DB) computed() any { return db.app }

// Open opens the app cfg describes. Prefer the generated jazzapp.Open,
// which embeds the app's jazz.app.json.
func Open(ctx context.Context, cfg Config) (*DB, error) {
	if err := cfg.checkTransport(); err != nil {
		return nil, err
	}
	if err := loadFFI(); err != nil {
		return nil, err
	}
	var auth map[string]any
	if cfg.Auth != nil {
		var err error
		if auth, err = cfg.Auth.authJSON(); err != nil {
			return nil, err
		}
	}
	config, err := json.Marshal(struct {
		Config
		Contract int            `json:"contract"`
		AuthJSON map[string]any `json:"auth,omitempty"`
	}{cfg, 2, auth})
	if err != nil {
		return nil, err
	}
	type opened struct {
		h        uintptr
		err      error
		provider uint64
	}
	done := make(chan opened, 1)
	go func() {
		if t, ok := cfg.Auth.(tokenAuth); ok {
			h, provider, err := openUser(string(config), t.provider)
			done <- opened{h, err, provider}
			return
		}
		var cerr uintptr
		h := ffi.open(cstr(string(config)), &cerr)
		done <- opened{h: h, err: takeError(cerr)}
	}()
	select {
	case o := <-done:
		if o.err != nil {
			return nil, o.err
		}
		return &DB{handle: o.h, app: cfg.Computed, provider: o.provider}, nil
	case <-ctx.Done():
		go func() {
			if o := <-done; o.h != 0 {
				ffi.close(o.h)
			}
		}()
		return nil, ctx.Err()
	}
}

// Close closes the app; later calls return ErrClosed.
func (db *DB) Close() error {
	db.endStreams()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.handle != 0 {
		ffi.close(db.handle)
		db.handle = 0
		// The token callback is valid until jazz_close returns.
		providers.Delete(db.provider)
	}
	return nil
}

// call runs jazz_call(method, args) and decodes its result into out.
// Calls block in Jazz; a cancelled ctx returns at once and the call's
// result is dropped, but the call itself runs to its end: a write may
// still apply.
func (db *DB) call(ctx context.Context, method string, args map[string]any, out any) error {
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
		if db.handle == 0 {
			done <- result{err: &Error{Kind: KindClosed, Message: "closed"}}
			return
		}
		var cerr uintptr
		res := ffi.call(db.handle, cstr(method), cstr(string(argsJSON)), &cerr)
		var r result
		r.text, _ = takeString(res)
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
func (db *DB) Update(ctx context.Context, table, id string, patch any, opts ...WriteOption) error {
	return update(ctx, db, table, id, patch, opts)
}

// Delete deletes the row of table with id.
func (db *DB) Delete(ctx context.Context, table, id string, opts ...WriteOption) error {
	return remove(ctx, db, table, id, opts)
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
