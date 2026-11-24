package jazz

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"
)

// closeBudget is how long a close may take: it ends the calls waiting for
// the server at once (Shutdown when its drain ends), then gives stream
// sessions up to 2s to end.
const closeBudget = 5 * time.Second

// offlineConfig is an app with a server nothing answers on: what a client
// on a lost network has.
func offlineConfig(t *testing.T) Config {
	t.Helper()
	if err := loadFFI(); err != nil {
		t.Skipf("no libjazz_ffi: %v", err)
	}
	app, err := os.ReadFile("../../examples/docs/todo-client-go/jazz.app.json")
	if err != nil {
		t.Skipf("outside the repository: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	// Not t.TempDir: a store that a hung close still holds can't be removed.
	dir, err := os.MkdirTemp("", "jazz-go-close-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return Config{
		App:       string(app),
		AppID:     "8f3c2a7e-1b4d-4c9e-9a2f-6d5e4c3b2a10",
		ServerURL: "http://" + address + "/jazz",
		Storage:   StoragePersistent,
		DataDir:   dir,
	}
}

func openOffline(t *testing.T) (*DB, Config) {
	t.Helper()
	cfg := offlineConfig(t)
	key, err := GenerateDeviceKey()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Auth = DeviceKey(key)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("opening offline: %v", err)
	}
	return db, cfg
}

// pendingSync writes a row the server can't get and leaves a WaitForSync
// waiting for it, returning that wait's result.
func pendingSync(t *testing.T, ctx context.Context, db *DB) <-chan error {
	t.Helper()
	write, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	row := map[string]any{"title": "offline", "done": false}
	if _, err := Insert[map[string]any, map[string]any](write, db, "todos", row); err != nil {
		t.Fatalf("inserting offline: %v", err)
	}
	waited := make(chan error, 1)
	go func() { waited <- db.WaitForSync(ctx, WriteGlobal) }()
	select {
	case err := <-waited:
		t.Fatalf("WaitForSync returned without a server: %v", err)
	case <-time.After(time.Second):
	}
	return waited
}

func returnsWithin(t *testing.T, what string, budget time.Duration, f func() error) error {
	t.Helper()
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- f() }()
	select {
	case err := <-done:
		t.Logf("%s returned after %.1fs: %v", what, time.Since(start).Seconds(), err)
		return err
	case <-time.After(budget):
		t.Fatalf("%s still running after %v", what, budget)
		return nil
	}
}

// endsClosed is the call a close interrupted failing with ErrClosed (its
// write may still apply).
func endsClosed(t *testing.T, waited <-chan error) {
	t.Helper()
	err := returnsWithin(t, "the interrupted WaitForSync", closeBudget, func() error { return <-waited })
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("the interrupted WaitForSync: %v, want ErrClosed", err)
	}
}

func TestCloseEndsAPendingCall(t *testing.T) {
	t.Parallel()
	db, cfg := openOffline(t)
	waited := pendingSync(t, context.Background(), db)
	if err := returnsWithin(t, "Close", closeBudget, db.Close); err != nil {
		t.Fatal(err)
	}
	endsClosed(t, waited)
	// Closed means the store is free again.
	reopened, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("reopening the store after Close: %v", err)
	}
	_ = returnsWithin(t, "closing the reopened app", closeBudget, reopened.Close)
}

func TestCallsFailAtOnceWhileClosing(t *testing.T) {
	t.Parallel()
	db, _ := openOffline(t)
	pendingSync(t, context.Background(), db)
	go db.Close()
	time.Sleep(500 * time.Millisecond)
	err := returnsWithin(t, "a call made during Close", 5*time.Second, func() error {
		_, err := db.Account(context.Background())
		return err
	})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("a call made during Close: %v, want ErrClosed", err)
	}
}

func TestCloseAfterACancelledCall(t *testing.T) {
	t.Parallel()
	db, _ := openOffline(t)
	ctx, cancel := context.WithCancel(context.Background())
	waited := pendingSync(t, ctx, db)
	cancel()
	if err := <-waited; !errors.Is(err, context.Canceled) {
		t.Fatalf("the cancelled call: %v", err)
	}
	if err := returnsWithin(t, "Close", closeBudget, db.Close); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownWithAPendingCall(t *testing.T) {
	t.Parallel()
	db, _ := openOffline(t)
	waited := pendingSync(t, context.Background(), db)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// Persistent storage keeps the unsent write: no ErrUnsynced.
	if err := returnsWithin(t, "Shutdown", closeBudget, func() error { return db.Shutdown(ctx) }); err != nil {
		t.Fatal(err)
	}
	endsClosed(t, waited)
}

func openOfflineSession(t *testing.T) (*Session, *DB) {
	t.Helper()
	cfg := offlineConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	session, err := OpenSession(ctx, cfg, SessionOptions{Start: StartWithDevice})
	if err != nil {
		t.Fatalf("opening a session offline: %v", err)
	}
	for {
		db, err := session.DB(ctx)
		if err == nil {
			return session, db
		}
		if !errors.Is(err, ErrNoUser) {
			t.Fatalf("the session's DB: %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("the session got no user offline: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func TestSessionCloseEndsItsDBsPendingCall(t *testing.T) {
	t.Parallel()
	session, db := openOfflineSession(t)
	waited := pendingSync(t, context.Background(), db)
	if err := returnsWithin(t, "Session.Close", closeBudget, session.Close); err != nil {
		t.Fatal(err)
	}
	// Closed, not replaced: there is no session to fetch a new DB from.
	endsClosed(t, waited)
	_ = returnsWithin(t, "the session DB's Close", closeBudget, db.Close)
}

func TestSessionDBCloseWithAPendingCall(t *testing.T) {
	t.Parallel()
	session, db := openOfflineSession(t)
	waited := pendingSync(t, context.Background(), db)
	if err := returnsWithin(t, "the session DB's Close", closeBudget, db.Close); err != nil {
		t.Fatal(err)
	}
	endsClosed(t, waited)
	_ = returnsWithin(t, "Session.Close", closeBudget, session.Close)
}
