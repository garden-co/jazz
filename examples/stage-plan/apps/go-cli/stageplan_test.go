package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	jazz "github.com/garden-co/jazz/packages/jazz-go/v2"

	"github.com/garden-co/jazz/examples/stage-plan/apps/go-cli/jazzapp"
)

// The app's files are StagePlan's, byte for byte.
func TestAppFilesAreStagePlans(t *testing.T) {
	for _, name := range []string{"jazz.app.json", "jazz.permissions.json"} {
		ours, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		canonical, err := os.ReadFile("../react-localfirst/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(ours, canonical) {
			t.Errorf("%s differs from ../react-localfirst/%s: copy it again", name, name)
		}
	}
}

// testServer is a Jazz core (`jazz-tools server`, JAZZ_TOOLS) on a memory
// store, and in front of it jazz-app-support's test relay (JAZZ_TEST_RELAY), the
// stand-in for a TS main backend's /jazz: it publishes StagePlan's files and
// signs "Bearer token-<user>" in as <user>. With JAZZ_MAIN_BACKEND=ts it
// is a real TS main backend instead (tsMainBackend).
func testServer(t *testing.T) server {
	t.Helper()
	if os.Getenv("JAZZ_MAIN_BACKEND") == "ts" {
		return tsMainBackend(t)
	}
	tools := os.Getenv("JAZZ_TOOLS")
	if tools == "" {
		tools = "../../../../target/debug/jazz-tools"
	}
	relay := os.Getenv("JAZZ_TEST_RELAY")
	if relay == "" {
		relay = "../../../../target/debug/jazz-app-test-relay"
	}
	appID := randomUUID(t)
	const backendSecret, adminSecret = "go-cli-test-backend-secret", "go-cli-test-admin-secret"
	core := startCore(t, tools, "server", appID, "--in-memory",
		"--backend-secret", backendSecret, "--admin-secret", adminSecret)
	r := exec.Command(relay, "--core", core, "--app-id", appID, "--app", "jazz.app.json", "--permissions", "jazz.permissions.json")
	r.Env = append(os.Environ(), "RUST_LOG=warn", "JAZZ_BACKEND_SECRET="+backendSecret, "JAZZ_ADMIN_SECRET="+adminSecret)
	r.Stderr = os.Stderr
	out, err := r.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(); err != nil {
		t.Fatalf("starting %s (cargo build -p jazz-app-support --features test-relay --bin jazz-app-test-relay): %v", relay, err)
	}
	t.Cleanup(func() { r.Process.Signal(os.Interrupt); r.Wait() })
	line, err := bufio.NewReader(out).ReadString('\n')
	if err != nil {
		t.Fatalf("the relay printed no endpoint: %v", err)
	}
	return server{appID: appID, url: strings.TrimSpace(line)}
}

// startCore starts `jazz-tools <args> --port 0` and returns its URL once it
// listens. The core picks its own port and writes it to JAZZ_BOUND_PORT_FILE
// after binding: a port picked here (listen on :0, close, hand it over) could
// be taken by another socket before the core binds it, and the core would
// exit with "Address already in use".
func startCore(t *testing.T, tools string, args ...string) string {
	t.Helper()
	portFile := filepath.Join(t.TempDir(), "bound-port")
	cmd := exec.Command(tools, append(args, "--port", "0")...)
	cmd.Env = append(os.Environ(), "RUST_LOG=warn", "JAZZ_BOUND_PORT_FILE="+portFile)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting %s (build it with cargo build -p jazz-tools --bin jazz-tools): %v", tools, err)
	}
	exited := make(chan struct{})
	var waitErr error
	go func() { waitErr = cmd.Wait(); close(exited) }()
	t.Cleanup(func() { cmd.Process.Kill(); <-exited })
	deadline := time.After(30 * time.Second)
	for {
		// The core ends the file with a newline once the write is complete.
		if b, err := os.ReadFile(portFile); err == nil && strings.HasSuffix(string(b), "\n") {
			port, err := strconv.Atoi(strings.TrimSpace(string(b)))
			if err != nil {
				t.Fatalf("the core's bound port %q: %v", b, err)
			}
			return fmt.Sprintf("http://127.0.0.1:%d", port)
		}
		select {
		case <-exited:
			t.Fatalf("the core exited before it listened: %v", waitErr)
		case <-deadline:
			t.Fatal("the core didn't listen within 30s")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func randomUUID(t *testing.T) string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func openTest(t *testing.T, srv server, user string) *jazz.DB {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := openUser(ctx, srv, "token-"+user, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestBoard(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srv := testServer(t)
	db := openTest(t, srv, "alice")

	show, err := ensureShow(ctx, db, "Go Night")
	if err != nil {
		t.Fatal(err)
	}
	again, err := ensureShow(ctx, db, "Another")
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != show.ID {
		t.Fatalf("a second show was created: %v, %v", show, again)
	}

	for _, title := range []string{"Load-in", "Line check", "Soundcheck"} {
		if _, err := addTask(ctx, db, show.ID, title); err != nil {
			t.Fatal(err)
		}
	}
	tasks, err := listTasks(ctx, db, show.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 3 || tasks[0].Title != "Load-in" || tasks[2].Title != "Soundcheck" {
		t.Fatalf("tasks by rank: %+v", tasks)
	}
	if tasks[1].Status != jazzapp.TaskStatusTodo || tasks[1].Notes != nil {
		t.Fatalf("a new task: %+v", tasks[1])
	}

	if err := moveTask(ctx, db, tasks[1].ID, jazzapp.TaskStatusDoing); err != nil {
		t.Fatal(err)
	}
	moved, err := jazzapp.Tasks.Query().Where(jazzapp.Tasks.ID.Eq(tasks[1].ID)).One(ctx, db)
	if err != nil || moved == nil || moved.Status != jazzapp.TaskStatusDoing {
		t.Fatalf("moved: %+v, %v", moved, err)
	}
}

func TestLiveBoardFollowsRank(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srv := testServer(t)
	db := openTest(t, srv, "alice")
	show, err := ensureShow(ctx, db, "Live Night")
	if err != nil {
		t.Fatal(err)
	}
	a, _ := addTask(ctx, db, show.ID, "Load-in")
	b, _ := addTask(ctx, db, show.ID, "Line check")
	live, err := jazzapp.Tasks.Query().
		Where(jazzapp.Tasks.ShowID.Eq(show.ID)).
		OrderBy(jazzapp.Tasks.Rank.Asc()).
		Live(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	<-live.Ready()
	order := func() []string {
		var out []string
		for _, task := range live.Rows() {
			out = append(out, task.Title)
		}
		return out
	}
	if got := order(); len(got) != 2 || got[0] != a.Title {
		t.Fatalf("first result: %v", got)
	}
	// Moving Line check to the top re-sorts the live rows.
	if err := jazzapp.Tasks.Update(ctx, db, b.ID, jazzapp.TaskPatch{Rank: jazz.Set(0.5)}); err != nil {
		t.Fatal(err)
	}
	for {
		if got := order(); got[0] == b.Title {
			break
		}
		select {
		case <-live.Changes():
		case <-live.Done():
			t.Fatal(live.Err())
		case <-ctx.Done():
			t.Fatalf("never re-sorted: %v", order())
		}
	}
	cancel()
	<-live.Done()
}
