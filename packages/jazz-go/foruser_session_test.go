package jazz

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A DB ForUser returned shares its backend client's session: it can't
// read, renew, link or end it, and being refused leaves both DBs open.
func TestAForUserDBCantReachItsBackendClientsSession(t *testing.T) {
	if err := loadFFI(); err != nil {
		t.Skipf("no libjazz_ffi: %v", err)
	}
	app, appErr := os.ReadFile("../../examples/docs/todo-client-go/jazz.app.json")
	permissions, permissionsErr := os.ReadFile("../../examples/docs/todo-client-go/jazz.permissions.json")
	if appErr != nil || permissionsErr != nil {
		t.Skipf("outside the repository: %v, %v", appErr, permissionsErr)
	}
	// A main backend that trusts the client and knows the user, but can't sync.
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/backend/admit"):
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/backend/user-session"):
			w.Header().Set("content-type", "application/json")
			_, _ = w.Write([]byte(`{"account":"6f9619ff-8b86-4d01-b42d-00c04fc964ff","issuer":"jazz:backend"}`))
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	t.Cleanup(backend.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var refreshed atomic.Int32
	parent, err := Open(ctx, Config{
		App:         string(app),
		AppID:       "8f3c2a7e-1b4d-4c9e-9a2f-6d5e4c3b2a10",
		ServerURL:   backend.URL + "/jazz",
		Storage:     StorageMemory,
		Permissions: string(permissions),
		Auth: Credential(func(_ context.Context, reason TokenReason) (string, error) {
			if reason == TokenRefresh {
				refreshed.Add(1)
			}
			return "machine", nil
		}),
	})
	if err != nil {
		t.Fatalf("opening a backend client: %v", err)
	}
	defer parent.Close()
	child, err := parent.ForUser(ctx, "alice", nil)
	if err != nil {
		t.Fatalf("ForUser: %v", err)
	}
	defer child.Close()
	watching, stopWatching := context.WithCancel(ctx)
	defer stopWatching()
	syncing, err := child.OnSyncStatus(watching)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := <-syncing; !ok {
		t.Fatal("the user's sync watch ended before the test")
	}

	for _, refused := range []struct {
		name string
		call func() error
	}{
		{"Session", func() error { _, err := child.Session(ctx); return err }},
		{"OnSession", func() error { _, err := child.OnSession(watching); return err }},
		{"Refresh", func() error { return child.Refresh(ctx) }},
		{"Link", func() error { _, err := child.Link(ctx, "alice's token"); return err }},
		{"SignOut", func() error { return child.SignOut(ctx) }},
	} {
		if err := refused.call(); !errors.Is(err, ErrNotSupported) {
			t.Errorf("%s on a ForUser DB: %v, want ErrNotSupported", refused.name, err)
		}
		session, err := parent.Session(ctx)
		if err != nil || session == nil || session.Status != SignedIn {
			t.Fatalf("the backend client's session after the user's %s: %+v, %v", refused.name, session, err)
		}
	}
	if n := refreshed.Load(); n != 0 {
		t.Errorf("a user's DB renewed the backend client's token %d times", n)
	}

	row := map[string]any{"title": "still open", "done": false}
	if _, err := Insert[map[string]any, map[string]any](ctx, child, "todos", row); err != nil {
		t.Fatalf("the user's DB after a refused SignOut: %v", err)
	}
	child.streamsMu.Lock()
	streams := len(child.streams)
	child.streamsMu.Unlock()
	if streams != 1 {
		t.Fatalf("the user's DB has %d streams after a refused SignOut, want its sync watch", streams)
	}
	if err := parent.Refresh(ctx); err != nil || refreshed.Load() == 0 {
		t.Fatalf("the backend client can't renew afterwards: %v, %d refreshes", err, refreshed.Load())
	}
}
