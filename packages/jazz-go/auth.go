package jazz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// Auth is how an end user signs in: Token (a JWT from your auth provider)
// or DeviceAccount / DeviceKey (a device account, kept on this device).
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
// signed in (the app then signs out). An error (a network blip at your
// auth provider) keeps the session: Jazz asks again later. ctx is
// cancelled when the DB or Session closes, which waits for the provider to
// return.
type TokenProvider func(ctx context.Context, reason TokenReason) (string, error)

type tokenAuth struct {
	provider   TokenProvider
	credential bool
}

// Token signs in with the bearer tokens provider gets, asking again on
// renewal and when one is refused.
func Token(provider TokenProvider) Auth { return tokenAuth{provider: provider} }

func (a tokenAuth) authJSON() (map[string]any, error) {
	if a.credential {
		return map[string]any{"credential": true}, nil
	}
	return map[string]any{"bearer": true}, nil
}

type deviceAuth struct {
	key   string
	store SecretStore
}

// DeviceKey signs in as the device account whose key is key
// ("jazz-auth-v1:…", from GenerateDeviceKey).
func DeviceKey(key string) Auth { return deviceAuth{key: key} }

// DeviceAccount signs in as this device's device account, whose key
// store keeps: it is created (signing up) the first time.
func DeviceAccount(store SecretStore) Auth { return deviceAuth{store: store} }

// String keeps the key out of logs.
func (a deviceAuth) String() string { return "jazz.DeviceAccount([redacted])" }

// GoString is String, for %#v.
func (a deviceAuth) GoString() string { return a.String() }

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
			if f, ok := a.store.(fileStore); ok {
				key, err = f.create(key)
			} else {
				err = a.store.Save(key)
			}
			if err != nil {
				return nil, err
			}
		}
	}
	return map[string]any{"deviceKey": key}, nil
}

// SecretStore keeps a device account's key: the OS keychain, ideally.
type SecretStore interface {
	// Load is the key, or "" only when there is none yet: DeviceAccount
	// then saves a new account's over whatever is there. A key that's
	// there but can't be read is an error.
	Load() (string, error)
	Save(key string) error
}

// FileStore keeps the key in a file readable only by its owner (0600): for
// hosts without a keychain. Only a missing file means there is no key yet:
// an empty or unreadable one is an error, and is left as it is. Saving
// replaces the file whole, so its directory must be writable.
func FileStore(path string) SecretStore { return fileStore(path) }

type fileStore string

func (f fileStore) Load() (string, error) {
	path := string(f)
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		if _, lerr := os.Lstat(path); errors.Is(lerr, os.ErrNotExist) {
			return "", nil
		}
		// Something is there: a key another opener created just now, or a
		// link to a file that isn't (an unmounted volume, say), which
		// isn't a missing key.
		b, err = os.ReadFile(path)
	}
	if err != nil {
		return "", err
	}
	key := strings.TrimSpace(string(b))
	if key == "" {
		return "", fmt.Errorf("jazz: the device key file %s is empty: restore the key, or delete the file to start as a new account", path)
	}
	return key, nil
}

func (f fileStore) Save(key string) error {
	path := string(f)
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("jazz: not saving an empty device key to %s: delete the file to start as a new account", path)
	}
	if target, err := filepath.EvalSymlinks(path); err == nil {
		path = target
	}
	temp, err := writeBeside(path, key)
	if err != nil {
		return err
	}
	defer os.Remove(temp) // already gone once renamed
	if err := os.Rename(temp, path); err != nil {
		return err
	}
	syncDir(filepath.Dir(path))
	return nil
}

// create keeps key as a new account's and returns it, or the key another
// opener kept first.
func (f fileStore) create(key string) (string, error) {
	path := string(f)
	temp, err := writeBeside(path, key)
	if err != nil {
		return "", err
	}
	defer os.Remove(temp)
	err = os.Link(temp, path)
	if errors.Is(err, os.ErrExist) {
		kept, err := f.Load()
		if err == nil && kept == "" {
			err = fmt.Errorf("jazz: the device key file %s was removed while opening", path)
		}
		return kept, err
	}
	if err != nil {
		// A file system without hard links: still whole, but the last
		// opener wins.
		if err := f.Save(key); err != nil {
			return "", err
		}
		return key, nil
	}
	syncDir(filepath.Dir(path))
	return key, nil
}

// writeBeside writes key durably to a new 0600 file in path's directory.
func writeBeside(path, key string) (string, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	file, err := os.CreateTemp(dir, ".device-key-*.tmp")
	if err != nil {
		return "", err
	}
	_, err = file.WriteString(key + "\n")
	if err == nil {
		err = file.Sync()
	}
	if cerr := file.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(file.Name())
		return "", err
	}
	return file.Name(), nil
}

// syncDir makes a rename or link in dir survive a power cut, where the file
// system can.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
}

// GenerateDeviceKey is a new device account's key: keep it like a
// password.
func GenerateDeviceKey() (string, error) {
	if err := loadFFI(); err != nil {
		return "", err
	}
	var cerr uintptr
	key := ffi.generateKey(&cerr)
	if err := takeError(cerr); err != nil {
		return "", err
	}
	text, _ := takeString(key)
	return text, nil
}

// token providers, by the id jazz-ffi's token callback gets.
var (
	providers    sync.Map
	nextProvider atomic.Uint64
)

// onToken is jazz-ffi's token callback: 0 with *token set (NULL: signed
// out), or non-zero for a failed fetch, *token its message.
func onToken(ctx, reason, token uintptr) (status int32) {
	out := (*uintptr)(unsafe.Add(unsafe.Pointer(nil), token))
	*out = 0
	// A panicking provider fails closed as a failed fetch: no token, the
	// session stays, and the panic doesn't cross into jazz-ffi.
	defer func() {
		if r := recover(); r != nil {
			*out = mallocString(fmt.Sprintf("token provider panicked: %v", r))
			status = 1
		}
	}()
	value, ok := providers.Load(uint64(ctx))
	if !ok {
		return 0 // the app closed: signed out
	}
	c, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	fetching := fetchesOf(uint64(ctx))
	fetch, ok := fetching.begin(cancel)
	if !ok {
		*out = mallocString("the app is closing")
		return 1
	}
	defer fetching.done(fetch)
	t, err := value.(TokenProvider)(c, TokenReason(goString(reason)))
	if err == nil && t == "" && fetching.isClosing() {
		// Cancelled by closing: a failed fetch, not a sign-out.
		err = context.Canceled
	}
	if err != nil {
		// A failed fetch: the session stays, and Jazz asks again.
		*out = mallocString(err.Error()) // jazz-ffi frees it
		return 1
	}
	if t != "" {
		*out = mallocString(t) // jazz-ffi frees it
	}
	return 0 // "" signs the user out
}

func openUser(config string, id uint64, provider TokenProvider) (uintptr, uint64, error) {
	providers.Store(id, provider)
	var cerr uintptr
	h := ffi.openUser(cstr(config), ffi.tokenCallback, uintptr(id), &cerr)
	if err := takeError(cerr); err != nil || h == 0 {
		releaseProvider(id)
		if err == nil {
			err = &Error{Kind: KindRefused, Message: "open failed"}
		}
		return 0, 0, err
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

// SyncStatus is how the app's sync is going, as the TS client's
// SyncStatus.
type SyncStatus struct {
	// Link is the connection's state, e.g. {"status": "connected"} or
	// {"status": "offline", "reason": "network"|"auth"|…, "auth": …}.
	Link json.RawMessage `json:"link"`
	// PendingUploads is how many writes the server hasn't accepted yet.
	PendingUploads int `json:"pendingUploads"`
	// OldestPendingAt is when the oldest of them was made.
	OldestPendingAt *Timestamp `json:"oldestPendingAt"`
	// Rejected and LastError describe writes the server refused.
	Rejected  json.RawMessage `json:"rejected"`
	LastError json.RawMessage `json:"lastError"`
	// Synced: nothing is pending.
	Synced bool `json:"synced"`
}

// Session is the end user's session, or nil for a local app. On a DB
// ForUser returned it is ErrNotSupported: the session is the backend
// client's, shared by every user.
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

// Link links the signed-in device account to the user bearer (a token
// from your auth provider) signs in as, and returns that user. On a DB
// from a Session it is ErrNotSupported: use the Session's
// LinkDeviceAccount. On a DB ForUser returned it is ErrNotSupported.
func (db *DB) Link(ctx context.Context, bearer string) (*User, error) {
	var u *User
	err := db.call(ctx, "link", map[string]any{"bearer": bearer}, &u)
	return u, err
}

// Refresh asks the TokenProvider for a fresh token now. On a DB ForUser
// returned it is ErrNotSupported: refresh the backend client's DB.
func (db *DB) Refresh(ctx context.Context) error {
	return db.call(ctx, "refresh", map[string]any{}, nil)
}

// SignOut signs the end user out and closes the app. When ctx ends first
// it returns ctx.Err(), and the app is freed once signed out. On a DB from
// a Session it is ErrNotSupported, and the DB stays open: use the
// Session's SignOut.
// A refused SignOut leaves the app open, as on a DB ForUser returned,
// where it is ErrNotSupported: the session is the backend client's,
// shared by every user. Close that DB instead.
func (db *DB) SignOut(ctx context.Context) error {
	if err := db.callOr(ctx, "signOut", map[string]any{}, nil, db.closeAfter); err != nil {
		return err
	}
	db.settleStreams()
	return db.Close()
}

// OnSession is the session's states, the current one first, until ctx is
// done. On a DB ForUser returned it is ErrNotSupported, as Session is.
func (db *DB) OnSession(ctx context.Context) (<-chan SessionState, error) {
	return watch(ctx, db, "session", decodeSessionState)
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

// decodeSessionState reads a session watch's state.
func decodeSessionState(e event) (SessionState, bool) {
	var s SessionState
	if e.kind == "error" || json.Unmarshal([]byte(e.json), &s) != nil {
		return s, true
	}
	return s, false
}

// watchStates is a watch's stream decoding: decode's bool says the event
// isn't a state, which ends the stream without it.
func watchStates[T any](decode func(event) (T, bool)) func(event) (T, bool, bool) {
	return func(e event) (T, bool, bool) {
		v, end := decode(e)
		if end || e.kind == "closed" {
			return v, true, true
		}
		return v, false, false
	}
}

// watch runs a jazz-ffi watch of what. decode's bool says the event isn't
// a state (an error, or the watch closing): the channel then closes
// without it.
func watch[T any](ctx context.Context, db *DB, what string, decode func(event) (T, bool)) (<-chan T, error) {
	return stream(ctx, db, watchStates(decode), func(id uint64) (func(), error) {
		var cerr uintptr
		w := ffi.watch(db.handle, cstr(what), ffi.subscriptionCallback, uintptr(id), &cerr)
		if err := takeError(cerr); err != nil || w == 0 {
			if err == nil {
				err = &Error{Kind: KindRefused, Message: "watch failed"}
			}
			return nil, err
		}
		return func() { ffi.unwatch(w) }, nil
	})
}
