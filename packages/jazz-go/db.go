// Package jazz is the Go runtime for Jazz: it opens an app through the
// jazz-ffi C ABI and reads and writes its rows as JSON, typed by the
// bindings `npx jazz-tools generate` writes from the app's schema into a
// `jazzapp` package beside its go.mod.
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
	"time"
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
	// DataDir is where persistent storage lives. An end user's store (or a
	// scoped backend client's) is in DataDir/<account>/, so each user of one
	// directory has their own; an app without a user refuses a store in
	// DataDir itself that a signed-in user wrote.
	DataDir string `json:"dataDir,omitempty"`
	// Storage is StorageMemory or StoragePersistent (the default).
	Storage Storage `json:"storage,omitempty"`
	// Computed is the app's implementation of its generated Computed
	// interface (jazzapp.Open sets it).
	Computed any `json:"-"`
	// Auth signs an end user in (Token, DeviceAccount, DeviceKey): with
	// ServerURL set to the app's Jazz endpoint (its backend's /jazz).
	Auth Auth `json:"-"`
	// Encryption is where this device keeps its encryption keys, for an app
	// that declares end-to-end encryption (an app that does is refused
	// without it). Not for a backend: it holds no keys.
	Encryption *EncryptionConfig `json:"-"`
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
	store    uint64 // the private store's id, if any

	// streams are the DB's open subscriptions and watches, by id: each
	// stops its jazz-ffi side at once; its channel closes once what was
	// queued is sent.
	streamsMu sync.Mutex
	streams   map[uint64]*openStream
}

// openStream is one of a DB's subscriptions or watches.
type openStream struct {
	sub *subscriber
	// end stops its jazz-ffi side; its channel closes once what was
	// queued is sent.
	end func()
}

// awaitStreamsEnd waits, up to d, until jazz-ffi has sent each of the
// DB's streams its last callback: after signOut or deleteClientStorage
// close the app, so Close doesn't cut off the states before it (the
// session's final signedOut).
func (db *DB) awaitStreamsEnd(d time.Duration) {
	for deadline := time.Now().Add(d); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		db.streamsMu.Lock()
		open := 0
		for _, st := range db.streams {
			st.sub.mu.Lock()
			if !st.sub.ended {
				open++
			}
			st.sub.mu.Unlock()
		}
		db.streamsMu.Unlock()
		if open == 0 {
			return
		}
	}
}

// settleStreams ends the DB's streams once jazz-ffi has closed the app
// (signOut, deleteClientStorage): first letting each take its last
// states, then ending them, before Close frees the handle.
func (db *DB) settleStreams() {
	db.awaitStreamsEnd(2 * time.Second)
	db.endStreams()
}

// endStreams ends the DB's subscriptions and watches (closing their
// channels) before the handle closes.
func (db *DB) endStreams() {
	db.streamsMu.Lock()
	ends := make([]func(), 0, len(db.streams))
	for _, st := range db.streams {
		ends = append(ends, st.end)
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
	return open(ctx, cfg)
}

// open opens the app once the FFI is loaded and transport is checked.
func open(ctx context.Context, cfg Config) (*DB, error) {
	var auth map[string]any
	if cfg.Auth != nil {
		var err error
		if auth, err = cfg.Auth.authJSON(); err != nil {
			return nil, err
		}
	}
	var environment string
	var addCreatorAsMember *bool
	if cfg.Encryption != nil {
		if cfg.Encryption.Store == nil {
			return nil, &Error{Kind: KindRefused, Message: "Config.Encryption needs a Store"}
		}
		environment = cfg.Encryption.Environment
		if option := cfg.Encryption.AddCreatorAsMember; option != nil && !*option {
			addCreatorAsMember = option
		}
	}
	config, err := json.Marshal(struct {
		Config
		Contract           int            `json:"contract"`
		AuthJSON           map[string]any `json:"auth,omitempty"`
		Environment        string         `json:"environment,omitempty"`
		AddCreatorAsMember *bool          `json:"addCreatorAsMember,omitempty"`
	}{cfg, 2, auth, environment, addCreatorAsMember})
	if err != nil {
		return nil, err
	}
	type opened struct {
		h        uintptr
		err      error
		provider uint64
		store    uint64
	}
	done := make(chan opened, 1)
	go func() {
		var store PrivateStore
		if cfg.Encryption != nil {
			store = cfg.Encryption.Store
		}
		auth, _ := cfg.Auth.(tokenAuth)
		h, provider, storeID, err := openNative(string(config), auth.provider, store)
		done <- opened{h: h, err: err, provider: provider, store: storeID}
	}()
	select {
	case o := <-done:
		if o.err != nil {
			return nil, o.err
		}
		return &DB{handle: o.h, app: cfg.Computed, provider: o.provider, store: o.store}, nil
	case <-ctx.Done():
		go func() {
			if o := <-done; o.h != 0 {
				ffi.close(o.h)
				releaseCallbacks(o.provider, o.store)
			}
		}()
		return nil, ctx.Err()
	}
}

// openNative registers callbacks before opening and releases them if open fails.
func openNative(config string, tokens TokenProvider, store PrivateStore) (uintptr, uint64, uint64, error) {
	if store != nil && ffi.openEncrypted == nil {
		return 0, 0, 0, &Error{Kind: KindRefused, Message: "this libjazz_ffi has no end-to-end encryption (an older build)"}
	}
	var provider, storeID uint64
	var tokenCB uintptr
	if tokens != nil {
		provider = nextProvider.Add(1)
		providers.Store(provider, tokens)
		tokenCB = ffi.tokenCallback
	}
	var h, cerr uintptr
	input := cstr(config)
	switch {
	case store != nil:
		storeID = nextStore.Add(1)
		stores.Store(storeID, store)
		h = ffi.openEncrypted(input, tokenCB, uintptr(provider), ffi.storeCallback, uintptr(storeID), &cerr)
	case tokens != nil:
		h = ffi.openUser(input, tokenCB, uintptr(provider), &cerr)
	default:
		h = ffi.open(input, &cerr)
	}
	if err := takeError(cerr); err != nil || h == 0 {
		releaseCallbacks(provider, storeID)
		if err == nil {
			err = &Error{Kind: KindRefused, Message: "open failed"}
		}
		return 0, 0, 0, err
	}
	return h, provider, storeID, nil
}

// Close closes the app; later calls return ErrClosed.
func (db *DB) Close() error {
	db.endStreams()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.handle != 0 {
		ffi.close(db.handle)
		db.handle = 0
		releaseCallbacks(db.provider, db.store)
	}
	return nil
}

// releaseCallbacks runs after native close so both callbacks remain valid until it returns.
func releaseCallbacks(provider, store uint64) {
	providers.Delete(provider)
	stores.Delete(store)
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
