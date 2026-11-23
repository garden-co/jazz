package jazz

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"
)

// A DB from a Session can't sign out or link behind the session's back:
// being refused leaves it, its watches and the session as they were.
func TestASessionsDBRefusesSignOutAndLink(t *testing.T) {
	if err := loadFFI(); err != nil {
		t.Skipf("no libjazz_ffi: %v", err)
	}
	app, err := os.ReadFile("../../examples/docs/todo-client-go/jazz.app.json")
	if err != nil {
		t.Skipf("outside the repository: %v", err)
	}
	// Nothing answers: a device account needs no server to be ready.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	session, err := OpenSession(ctx, Config{
		App:       string(app),
		AppID:     "8f3c2a7e-1b4d-4c9e-9a2f-6d5e4c3b2a10",
		ServerURL: "http://" + address + "/jazz",
		Storage:   StoragePersistent,
		DataDir:   t.TempDir(),
	}, SessionOptions{Start: StartWithDevice})
	if err != nil {
		t.Fatalf("opening a session: %v", err)
	}
	defer session.Close()
	var db *DB
	for db == nil {
		if db, err = session.DB(ctx); err != nil {
			if !errors.Is(err, ErrNoUser) {
				t.Fatalf("the session's DB: %v", err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	defer db.Close()
	before, err := session.Snapshot(ctx)
	if err != nil || before.Status != SessionReady || before.User == nil {
		t.Fatalf("the session before: %+v, %v", before, err)
	}
	watching, stopWatching := context.WithCancel(ctx)
	defer stopWatching()
	syncing, err := db.OnSyncStatus(watching)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := <-syncing; !ok {
		t.Fatal("the sync watch ended before the test")
	}

	for _, refused := range []struct {
		name string
		call func() error
	}{
		{"Link", func() error { _, err := db.Link(ctx, "a user's token"); return err }},
		{"SignOut", func() error { return db.SignOut(ctx) }},
	} {
		if err := refused.call(); !errors.Is(err, ErrNotSupported) {
			t.Errorf("%s on a session's DB: %v, want ErrNotSupported", refused.name, err)
		}
		now, err := session.Snapshot(ctx)
		if err != nil || now.Status != SessionReady || now.ClientEpoch != before.ClientEpoch ||
			now.User == nil || now.User.Account != before.User.Account {
			t.Fatalf("the session after its DB's %s: %+v, %v", refused.name, now, err)
		}
		row := map[string]any{"title": "after " + refused.name, "done": false}
		if _, err := Insert[map[string]any, map[string]any](ctx, db, "todos", row); err != nil {
			t.Fatalf("the DB after a refused %s: %v", refused.name, err)
		}
	}
	db.streamsMu.Lock()
	streams := len(db.streams)
	db.streamsMu.Unlock()
	if streams != 1 {
		t.Fatalf("the DB has %d streams after being refused, want its sync watch", streams)
	}

	// The session's own SignOut is the way.
	if err := session.SignOut(ctx); err != nil {
		t.Fatalf("the session's SignOut: %v", err)
	}
	if now, err := session.Snapshot(ctx); err != nil || now.Status != SessionSignedOut {
		t.Fatalf("the session after its SignOut: %+v, %v", now, err)
	}
}
