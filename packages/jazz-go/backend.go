package jazz

import (
	"context"
	"encoding/json"
	"time"
)

// A backend client is a Go service syncing through the app's main backend
// (the TS server's /jazz) with a credential: an OIDC token the main
// backend trusts. Trusted clients act as the system and can act for any
// user (ForUser); scoped clients act as their own backend user.

// Credential connects as a backend client with the OIDC tokens provider
// gets, asked for the same reasons as Token's. Set
// ServerURL to the main backend's /jazz. Open fails with ErrRefused when
// the main backend trusts the token neither way. Config.Permissions must
// be the app's jazz.permissions.json.
func Credential(provider TokenProvider) Auth {
	return tokenAuth{provider: provider, credential: true}
}

// BackendKind is how the main backend admitted a backend client.
type BackendKind string

const (
	// BackendTrusted: the client acts as the system and may ForUser.
	BackendTrusted BackendKind = "trusted"
	// BackendScoped: the client acts as its own backend user.
	BackendScoped BackendKind = "scoped"
)

// BackendClient is how the main backend admitted db, or "" when db isn't
// a backend client.
func (db *DB) BackendClient(ctx context.Context) (BackendKind, error) {
	var kind *BackendKind
	if err := db.call(ctx, "backendClient", map[string]any{}, &kind); err != nil {
		return "", err
	}
	if kind == nil {
		return "", nil
	}
	return *kind, nil
}

// ForUser is db acting as the user userID, with claims as that user's
// claims (nil for none), for a trusted backend client. It never creates
// an account: a user without one is ErrNoAccount; a user the main
// backend won't let it act for (its 403) is ErrPermission; a scoped
// client, or a DB ForUser returned, gets ErrRefused. Close the result
// when done (db stays open); after db closes, the result's calls return
// ErrClosed. db's Shutdown drains the result's writes too.
func (db *DB) ForUser(ctx context.Context, userID string, claims map[string]any) (*DB, error) {
	if ffi.forUser == nil {
		return nil, &Error{Kind: KindNotSupported, Message: "ForUser is not supported yet by this libjazz_ffi"}
	}
	if claims == nil {
		claims = map[string]any{}
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type opened struct {
		h   uintptr
		err error
	}
	done := make(chan opened, 1)
	go func() {
		db.mu.RLock()
		defer db.mu.RUnlock()
		if db.handle == 0 {
			done <- opened{err: &Error{Kind: KindClosed, Message: "closed"}}
			return
		}
		var cerr uintptr
		h := ffi.forUser(db.handle, cstr(userID), cstr(string(claimsJSON)), &cerr)
		err := takeError(cerr)
		if err == nil && h == 0 {
			err = &Error{Kind: KindRefused, Message: "for user failed"}
		}
		done <- opened{h, err}
	}()
	select {
	case o := <-done:
		if o.err != nil {
			return nil, o.err
		}
		return &DB{handle: o.h, app: db.app}, nil
	case <-ctx.Done():
		go func() {
			if o := <-done; o.h != 0 {
				ffi.close(o.h)
			}
		}()
		return nil, ctx.Err()
	}
}

// DefaultShutdownTimeout is how long Shutdown drains writes when ctx has
// no deadline, as the TS client's close().
const DefaultShutdownTimeout = 10 * time.Second

// Shutdown closes db after its queued writes reach the server, waiting
// until ctx's deadline (DefaultShutdownTimeout without one). It closes in
// every case; with memory storage, writes still pending then are lost and
// it returns ErrUnsynced (Error.Pending counts them). Close closes at
// once.
func (db *DB) Shutdown(ctx context.Context) error {
	timeout := DefaultShutdownTimeout
	if deadline, ok := ctx.Deadline(); ok {
		timeout = max(time.Until(deadline), 0)
	}
	if ctx.Err() != nil {
		timeout = 0
	}
	if ffi.closeWithin == nil {
		// An older libjazz_ffi: drain with waitForSync, then close.
		c, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		err := db.call(c, "waitForSync", map[string]any{}, nil)
		db.Close()
		if c.Err() != nil {
			// Timed out: some writes may not have reached the server (this
			// libjazz_ffi can't count them).
			return &Error{Kind: KindUnsynced, Message: "closed before every write reached the server"}
		}
		return err
	}
	db.endStreams()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.handle == 0 {
		return nil
	}
	var cerr uintptr
	ffi.closeWithin(db.handle, uint64(timeout.Milliseconds()), &cerr)
	db.handle = 0
	providers.Delete(db.provider)
	return takeError(cerr)
}
