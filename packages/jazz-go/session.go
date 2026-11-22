package jazz

import (
	"context"
	"encoding/json"
)

// A Session follows who is signed in to the app, as TS's JazzSession: the
// app's own sign-in (a bearer from Token), a device account, or no one. It
// gives a DB for the user signed in now (DB), replaced when the user
// changes, and reports each change (Watch). For apps with sign-in flows:
// sign in, sign out, create, restore or link a device account.

// SessionPhase is where a Session is.
type SessionPhase string

const (
	SessionStarting      SessionPhase = "starting"
	SessionReady         SessionPhase = "ready"
	SessionSignedOut     SessionPhase = "signed-out"
	SessionTransitioning SessionPhase = "transitioning"
	SessionFailed        SessionPhase = "error"
	SessionClosed        SessionPhase = "closed"
)

// SessionSnapshot is a Session's state.
type SessionSnapshot struct {
	Status SessionPhase `json:"status"`
	// User is who is signed in, while someone is.
	User *User `json:"user"`
	// ClientEpoch changes whenever the user (and so Session.DB) changes.
	ClientEpoch uint64 `json:"clientEpoch"`
	// Error is why a SessionFailed session failed.
	Error *struct {
		Kind    string `json:"kind"`
		Message string `json:"message"`
	} `json:"error"`
	// Recovery says what gets a failed session going: "action" (call the
	// method again) or "session" (Retry).
	Recovery string `json:"recovery,omitempty"`
}

// SessionStart is what a Session follows when it opens.
type SessionStart string

const (
	// StartWithBackend follows the app's bearer (Config.Auth = Token): the
	// default, which needs one.
	StartWithBackend SessionStart = "backend"
	// StartWithDevice signs in as this device's account.
	StartWithDevice SessionStart = "device"
	// StartSignedOut starts with no one signed in.
	StartSignedOut SessionStart = "signed-out"
)

// SessionOptions configures OpenSession.
type SessionOptions struct {
	// Start is what the session follows at first (StartWithBackend if
	// empty).
	Start SessionStart
	// LinkGuest: a user with no account yet takes over the device guest's
	// (otherwise only LinkDeviceAccount does).
	LinkGuest bool
}

// Session is an open user-following session. It is safe for concurrent
// use.
type Session struct {
	app any
	// owner holds the session's handle, calls in flight and watches.
	owner *DB
}

// OpenSession opens a session for the app cfg describes: cfg.App and
// cfg.Computed as jazzapp sets them (cfg.App = jazzapp.App), ServerURL the
// app's /jazz, and DataDir where each account's store and the device key
// live. cfg.Auth is Token (the app's own sign-in) or nil.
func OpenSession(ctx context.Context, cfg Config, opts SessionOptions) (*Session, error) {
	if err := cfg.checkTransport(); err != nil {
		return nil, err
	}
	if err := loadFFI(); err != nil {
		return nil, err
	}
	if ffi.sessionOpen == nil || ffi.sessionCall == nil || ffi.sessionJazz == nil ||
		ffi.sessionWatch == nil || ffi.sessionClose == nil {
		return nil, &Error{Kind: KindNotSupported, Message: "sessions are not supported yet by this libjazz_ffi"}
	}
	var provider TokenProvider
	var auth map[string]any
	switch a := cfg.Auth.(type) {
	case nil:
	case tokenAuth:
		if a.credential {
			return nil, &Error{Kind: KindRefused, Message: "a Session follows end users: use Open for a backend client"}
		}
		provider, auth = a.provider, map[string]any{"bearer": true}
	default:
		return nil, &Error{Kind: KindRefused, Message: "a Session's Auth is Token or nil (it manages device accounts itself)"}
	}
	config, err := json.Marshal(struct {
		Config
		Contract  int            `json:"contract"`
		AuthJSON  map[string]any `json:"auth,omitempty"`
		Initial   SessionStart   `json:"initial,omitempty"`
		LinkGuest bool           `json:"linkGuest,omitempty"`
	}{cfg, 2, auth, opts.Start, opts.LinkGuest})
	if err != nil {
		return nil, err
	}
	type opened struct {
		h        uintptr
		provider uint64
		err      error
	}
	done := make(chan opened, 1)
	go func() {
		var id uint64
		cb, cbCtx := uintptr(0), uintptr(0)
		if provider != nil {
			id = nextProvider.Add(1)
			providers.Store(id, provider)
			cb, cbCtx = ffi.tokenCallback, uintptr(id)
		}
		var cerr uintptr
		h := ffi.sessionOpen(cstr(string(config)), cb, cbCtx, &cerr)
		err := takeError(cerr)
		if err == nil && h == 0 {
			err = &Error{Kind: KindRefused, Message: "session open failed"}
		}
		if err != nil {
			releaseProvider(id)
		}
		done <- opened{h, id, err}
	}()
	select {
	case o := <-done:
		if o.err != nil {
			return nil, o.err
		}
		return &Session{app: cfg.Computed,
			owner: &DB{handle: o.h, app: cfg.Computed, provider: o.provider}}, nil
	case <-ctx.Done():
		go func() {
			if o := <-done; o.h != 0 {
				ffi.sessionClose(o.h)
				releaseProvider(o.provider)
			}
		}()
		return nil, ctx.Err()
	}
}

// call runs a session method, decoding its result into out.
func (s *Session) call(ctx context.Context, method string, args map[string]any, out any) error {
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
		h, release, err := s.owner.acquire(nil)
		if err != nil {
			done <- result{err: err}
			return
		}
		defer release()
		var cerr uintptr
		res := ffi.sessionCall(h, cstr(method), cstr(string(argsJSON)), &cerr)
		var r result
		r.text, _ = takeString(res)
		r.err = takeError(cerr)
		done <- r
	}()
	select {
	case r := <-done:
		if r.err != nil || out == nil || r.text == "" {
			return r.err
		}
		return json.Unmarshal([]byte(r.text), out)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Snapshot is the session's state now.
func (s *Session) Snapshot(ctx context.Context) (SessionSnapshot, error) {
	var snap SessionSnapshot
	err := s.call(ctx, "snapshot", map[string]any{}, &snap)
	return snap, err
}

// DB is a DB acting for the user signed in now, or ErrNoUser while no one
// is (Error.SessionStatus says where the session is). Once the user
// changes its calls fail with ErrReplaced: get the new one when
// ClientEpoch changes. Close it when done; the session stays open.
func (s *Session) DB(ctx context.Context) (*DB, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	session, release, err := s.owner.acquire(nil)
	if err != nil {
		return nil, err
	}
	defer release()
	var cerr uintptr
	h := ffi.sessionJazz(session, &cerr)
	if err := takeError(cerr); err != nil {
		return nil, err
	}
	if h == 0 {
		return nil, &Error{Kind: KindRefused, Message: "no handle"}
	}
	return &DB{handle: h, app: s.app}, nil
}

// Refresh says the app's own sign-in changed: the session asks the Token
// provider again and follows the user it names.
func (s *Session) Refresh(ctx context.Context) error {
	return s.call(ctx, "refresh", map[string]any{}, nil)
}

// CreateDeviceAccount signs in as a new device account.
func (s *Session) CreateDeviceAccount(ctx context.Context) error {
	return s.call(ctx, "createDeviceAccount", map[string]any{}, nil)
}

// ReopenDeviceAccount signs in again as this device's account.
func (s *Session) ReopenDeviceAccount(ctx context.Context) error {
	return s.call(ctx, "reopenDeviceAccount", map[string]any{}, nil)
}

// RestoreDeviceAccount signs in as the device account whose key is key
// (from ExportDeviceKey), on this device from now on.
func (s *Session) RestoreDeviceAccount(ctx context.Context, key string) error {
	return s.call(ctx, "restoreDeviceAccount", map[string]any{"deviceKey": key}, nil)
}

// LinkDeviceAccount links the device account to the user the app's own
// sign-in names.
func (s *Session) LinkDeviceAccount(ctx context.Context) error {
	return s.call(ctx, "linkDeviceAccount", map[string]any{}, nil)
}

// ExportDeviceKey is the device account's key ("jazz-auth-v1:…"): keep it
// like a password.
func (s *Session) ExportDeviceKey(ctx context.Context) (string, error) {
	var key string
	err := s.call(ctx, "exportDeviceKey", map[string]any{}, &key)
	return key, err
}

// SignOut signs the user out; the session stays open, signed out.
func (s *Session) SignOut(ctx context.Context) error {
	return s.call(ctx, "signOut", map[string]any{}, nil)
}

// Retry gets a failed session (Recovery "session") going again.
func (s *Session) Retry(ctx context.Context) error {
	return s.call(ctx, "retry", map[string]any{}, nil)
}

// Watch is the session's snapshots, the current one first, until ctx is
// done or the session closes.
func (s *Session) Watch(ctx context.Context) (<-chan SessionSnapshot, error) {
	decode := func(e event) (SessionSnapshot, bool, bool) {
		var snap SessionSnapshot
		if e.kind != "session" || json.Unmarshal([]byte(e.json), &snap) != nil {
			return snap, true, true
		}
		return snap, false, false
	}
	return stream(ctx, s.owner, decode, func(id uint64) (func(), error) {
		var cerr uintptr
		w := ffi.sessionWatch(s.owner.handle, ffi.subscriptionCallback, uintptr(id), &cerr)
		if err := takeError(cerr); err != nil || w == 0 {
			if err == nil {
				err = &Error{Kind: KindRefused, Message: "watch failed"}
			}
			return nil, err
		}
		return func() { ffi.unwatch(w) }, nil
	})
}

// Close closes the session without waiting for an operation in flight:
// it and its DBs' calls end with ErrClosed (close the DBs too), and its
// watches end. As with DB.Close, it cancels the Token provider's ctx and
// waits for the provider to return, and the provider may call it.
func (s *Session) Close() error {
	return s.owner.closeAndFree(0,
		func(h uintptr) error {
			if err := callClose(ffi.sessionCall, h, "{}"); unknownClose(err) {
				return err
			}
			return nil
		},
		func(h uintptr) error {
			ffi.sessionClose(h)
			return nil
		})
}
