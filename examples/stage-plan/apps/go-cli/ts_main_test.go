package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	jazz "github.com/garden-co/jazz/packages/jazz-go/v2"

	"github.com/garden-co/jazz/examples/stage-plan/apps/go-cli/jazzapp"
)

// workloadTokens are the OIDC tokens a TS main backend's test issuer
// minted for its backend clients, by the backend's URL.
type workloadTokens struct{ Trusted, Scoped string }

var workloads sync.Map // url → workloadTokens

// tsMainBackend is StagePlan's main backend in TypeScript (createJazz,
// ts-main/main-backend.ts), run with tsx from JAZZ_TS_MAIN_DIR (a
// directory with jazz-tools installed and StagePlan's generated files:
// ../react-localfirst by default). Go's end users and backend clients
// sync through its /jazz, as they do in production.
func tsMainBackend(t *testing.T) server {
	t.Helper()
	dir := os.Getenv("JAZZ_TS_MAIN_DIR")
	if dir == "" {
		dir = "../react-localfirst"
	}
	tsx := os.Getenv("JAZZ_TSX")
	if tsx == "" {
		tsx = "../../../../node_modules/.bin/tsx"
	}
	if abs, err := filepath.Abs(tsx); err == nil {
		tsx = abs
	}
	script, err := os.ReadFile("ts-main/main-backend.ts")
	if err != nil {
		t.Fatal(err)
	}
	// Beside the app's jazz.generated.ts, so its imports resolve there.
	copied := filepath.Join(dir, ".go-main-backend.ts")
	if err := os.WriteFile(copied, script, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(copied) })
	cmd := exec.Command(tsx, ".go-main-backend.ts")
	cmd.Dir = dir
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the TS main backend with %s: %v", tsx, err)
	}
	t.Cleanup(func() { cmd.Process.Signal(os.Interrupt); cmd.Wait() })
	lines := bufio.NewReader(out)
	url, err := lines.ReadString('\n')
	if err != nil {
		t.Fatalf("the TS main backend printed no URL: %v", err)
	}
	url = strings.TrimSpace(url)
	tokensLine, err := lines.ReadString('\n')
	if err != nil {
		t.Fatalf("the TS main backend printed no workload tokens: %v", err)
	}
	var tokens workloadTokens
	if err := json.Unmarshal([]byte(tokensLine), &tokens); err != nil {
		t.Fatal(err)
	}
	workloads.Store(url, tokens)
	appID, err := jazz.EndpointAppID(url)
	if err != nil {
		t.Fatal(err)
	}
	return server{appID: appID, url: url}
}

// openBackendClient connects to srv as a backend client with token, or
// skips when this libjazz_ffi has no credential mode yet.
func openBackendClient(t *testing.T, srv server, token string) *jazz.DB {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	permissions, err := os.ReadFile("jazz.permissions.json")
	if err != nil {
		t.Fatal(err)
	}
	db, err := jazzapp.Open(ctx, jazz.Config{
		AppID:       srv.appID,
		ServerURL:   srv.url,
		Storage:     jazz.StorageMemory,
		Permissions: string(permissions),
		Auth: jazz.Credential(func(context.Context, jazz.TokenReason) (string, error) {
			return token, nil
		}),
	})
	if err != nil && strings.Contains(err.Error(), `{ "bearer": true }`) {
		t.Skipf("this libjazz_ffi has no backend-client mode yet: %v", err)
	}
	if err != nil {
		t.Fatalf("opening a backend client: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	kind, err := db.BackendClient(ctx)
	if errors.Is(err, jazz.ErrNotSupported) || errors.Is(err, jazz.ErrRefused) && kind == "" {
		t.Skipf("this libjazz_ffi has no backend-client mode yet: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// TestBackendClient: a trusted Go backend client of the TS main backend
// acts for an end user, whose Go app sees its write; a scoped one acts as
// its own user.
func TestBackendClient(t *testing.T) {
	if os.Getenv("JAZZ_MAIN_BACKEND") != "ts" {
		t.Skip("needs the TS main backend (JAZZ_MAIN_BACKEND=ts)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	srv := testServer(t)
	v, _ := workloads.Load(srv.url)
	tokens := v.(workloadTokens)

	trusted := openBackendClient(t, srv, tokens.Trusted)
	if kind, err := trusted.BackendClient(ctx); err != nil || kind != jazz.BackendTrusted {
		t.Fatalf("trusted client's kind = %q, %v", kind, err)
	}

	// Alice has no account until she first signs in.
	if _, err := trusted.ForUser(ctx, "alice", nil); !errors.Is(err, jazz.ErrNoAccount) {
		t.Fatalf("ForUser before alice signed in = %v, want ErrNoAccount", err)
	}
	alice := openTest(t, srv, "alice")
	if _, err := alice.Account(ctx); err != nil {
		t.Fatal(err)
	}
	asAlice, err := trusted.ForUser(ctx, "alice", nil)
	if err != nil {
		t.Fatal(err)
	}
	show, err := ensureShow(ctx, asAlice, "Backend Night")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := addTask(ctx, asAlice, show.ID, "Doors"); err != nil {
		t.Fatal(err)
	}
	// Shutdown drains the writes made for alice too.
	if err := trusted.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if _, err := jazzapp.Shows.Query().All(ctx, asAlice); !errors.Is(err, jazz.ErrClosed) {
		t.Errorf("a ForUser DB's query after its parent's Shutdown = %v, want ErrClosed", err)
	}
	asAlice.Close()
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		shows, err := jazzapp.Shows.Query().Where(jazzapp.Shows.ID.Eq(show.ID)).Tier(jazz.ReadRemote).All(ctx, alice)
		if err == nil && len(shows) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("alice's app never saw the show the backend client wrote for her: %v, %v", shows, err)
		}
	}

	scoped := openBackendClient(t, srv, tokens.Scoped)
	if kind, err := scoped.BackendClient(ctx); err != nil || kind != jazz.BackendScoped {
		t.Fatalf("scoped client's kind = %q, %v", kind, err)
	}
	if _, err := scoped.ForUser(ctx, "alice", nil); !errors.Is(err, jazz.ErrRefused) {
		t.Fatalf("ForUser on a scoped client = %v, want ErrRefused", err)
	}
}
