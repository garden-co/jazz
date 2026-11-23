package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	jazz "github.com/garden-co/jazz/packages/jazz-go/v2"

	"github.com/garden-co/jazz/examples/stage-plan/apps/go-cli/jazzapp"
)

func openSession(t *testing.T, ctx context.Context, srv server, auth jazz.Auth, opts jazz.SessionOptions) *jazz.Session {
	t.Helper()
	s, err := jazz.OpenSession(ctx, jazz.Config{
		App: jazzapp.App, AppID: srv.appID, ServerURL: srv.url,
		Storage: jazz.StoragePersistent, DataDir: t.TempDir(), Auth: auth,
	}, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// waitStatus waits for a snapshot with status from w.
func waitStatus(t *testing.T, w <-chan jazz.SessionSnapshot, status jazz.SessionPhase) jazz.SessionSnapshot {
	t.Helper()
	timeout := time.After(15 * time.Second)
	for {
		select {
		case snap, ok := <-w:
			if !ok {
				t.Fatalf("the watch ended before %q", status)
			}
			if snap.Status == status {
				return snap
			}
		case <-timeout:
			t.Fatalf("no %q snapshot", status)
		}
	}
}

// A Session follows the app's own sign-in: a DB for the user, replaced
// when they sign out.
func TestSessionFollowsTheBearer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	srv := testServer(t)
	token := "token-alice"
	s := openSession(t, ctx, srv, jazz.Token(func(context.Context, jazz.TokenReason) (string, error) {
		return token, nil
	}), jazz.SessionOptions{})
	w, err := s.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ready := waitStatus(t, w, jazz.SessionReady)
	if ready.User == nil || ready.User.Account == "" {
		t.Fatalf("ready without a user: %+v", ready)
	}
	db, err := s.DB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := ensureShow(ctx, db, "Session Night"); err != nil {
		t.Fatal(err)
	}

	if err := s.SignOut(ctx); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, w, jazz.SessionSignedOut)
	if _, err := s.DB(ctx); !errors.Is(err, jazz.ErrNoUser) {
		t.Fatalf("DB while signed out: %v", err)
	}
	if _, err := db.Account(ctx); !errors.Is(err, jazz.ErrReplaced) {
		t.Fatalf("the signed-out user's DB: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for range w {
	}
	if _, err := s.Snapshot(ctx); !errors.Is(err, jazz.ErrClosed) {
		t.Fatalf("a closed session's snapshot: %v", err)
	}
}

// Device accounts: create one, export its key, restore it in another
// session as the same account.
func TestSessionDeviceAccounts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	srv := testServer(t)
	s := openSession(t, ctx, srv, nil, jazz.SessionOptions{Start: jazz.StartSignedOut})
	w, err := s.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, w, jazz.SessionSignedOut)
	if err := s.CreateDeviceAccount(ctx); err != nil {
		t.Fatal(err)
	}
	created := waitStatus(t, w, jazz.SessionReady)
	key, err := s.ExportDeviceKey(ctx)
	if err != nil || !strings.HasPrefix(key, "jazz-auth-v1:") {
		t.Fatalf("exported key %q, %v", key, err)
	}

	other := openSession(t, ctx, srv, nil, jazz.SessionOptions{Start: jazz.StartSignedOut})
	ow, err := other.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, ow, jazz.SessionSignedOut)
	if err := other.RestoreDeviceAccount(ctx, key); err != nil {
		t.Fatal(err)
	}
	restored := waitStatus(t, ow, jazz.SessionReady)
	if restored.User == nil || created.User == nil || restored.User.Account != created.User.Account {
		t.Fatalf("restored %+v, created %+v", restored.User, created.User)
	}
}
