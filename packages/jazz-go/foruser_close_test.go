package jazz

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// A main backend that admits a trusted backend client, then can't sync it.
func openBackendOffline(t *testing.T, provider TokenProvider) *DB {
	t.Helper()
	cfg := offlineConfig(t)
	permissions, err := os.ReadFile("../../examples/docs/todo-client-go/jazz.permissions.json")
	if err != nil {
		t.Fatal(err)
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/backend/admit"):
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/backend/user-session"):
			w.Header().Set("content-type", "application/json")
			_, _ = w.Write([]byte(`{"account":"6f9619ff-8b86-4d01-b42d-00c04fc964ff","issuer":"jazz:backend"}`))
		default:
			w.WriteHeader(http.StatusServiceUnavailable) // no sync
		}
	}))
	t.Cleanup(func() { go backend.Close() })
	cfg.ServerURL = backend.URL + "/jazz"
	cfg.Storage, cfg.DataDir = StorageMemory, ""
	cfg.Permissions = string(permissions)
	cfg.Auth = Credential(provider)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("opening a backend client: %v", err)
	}
	return db
}

func openForUserOffline(t *testing.T) (parent, child *DB) {
	t.Helper()
	parent = openBackendOffline(t, func(context.Context, TokenReason) (string, error) { return "machine", nil })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var err error
	if child, err = parent.ForUser(ctx, "alice", nil); err != nil {
		t.Fatalf("ForUser: %v", err)
	}
	return parent, child
}

func TestForUserDBCloseWithAPendingCall(t *testing.T) {
	t.Parallel()
	_, child := openForUserOffline(t)
	waited := pendingSync(t, context.Background(), child)
	if err := returnsWithin(t, "the ForUser DB's Close", closeBudget, child.Close); err != nil {
		t.Fatal(err)
	}
	endsClosed(t, waited)
}
