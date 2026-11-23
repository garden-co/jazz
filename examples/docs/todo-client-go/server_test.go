package todoclient

import (
	"bufio"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	jazz "github.com/garden-co/jazz/packages/jazz-go/v2"

	"github.com/garden-co/jazz/examples/docs/todo-client-go/jazzapp"
	orders "github.com/garden-co/jazz/examples/docs/todo-client-go/recipes/required-references/jazzapp"
)

type server struct{ appID, url string }

// testServer is a Jazz core (`jazz-tools server`, JAZZ_TOOLS) on a memory
// store, and in front of it jazz-app-support's test relay (JAZZ_TEST_RELAY), the
// stand-in for a TS main backend's /jazz: it publishes the todo app's files and
// signs "Bearer token-<user>" in as <user>.
func testServer(t *testing.T) server {
	t.Helper()
	tools := os.Getenv("JAZZ_TOOLS")
	if tools == "" {
		tools = "../../../target/debug/jazz-tools"
	}
	relay := os.Getenv("JAZZ_TEST_RELAY")
	if relay == "" {
		relay = "../../../target/debug/jazz-app-test-relay"
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
	db, err := openAsUser(ctx, srv.appID, srv.url, t.TempDir(), func(context.Context, jazz.TokenReason) (string, error) {
		return "token-" + user, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// ok is v, failing the test (by panicking) on err.
func ok[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func titles(todos []jazzapp.Todo) []string {
	out := make([]string, len(todos))
	for i, todo := range todos {
		out[i] = todo.Title
	}
	return out
}

// TestSnippets runs every region against a core behind the test relay.
func TestSnippets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	srv := testServer(t)
	db := openTest(t, srv, "alice")
	owner, err := db.Account(ctx)
	if err != nil || owner == "" {
		t.Fatalf("account %q: %v", owner, err)
	}
	project := ok(jazzapp.Projects.Insert(ctx, db, jazzapp.NewProject{Name: "Docs"}))
	insert := func(title string, done bool, parent *string, priority int32) jazzapp.Todo {
		return ok(jazzapp.Todos.Insert(ctx, db, jazzapp.NewTodo{
			Title: title, Done: done, ProjectID: &project.ID, ParentID: parent, Priority: &priority,
		}))
	}
	epic := insert("Write the docs", false, nil, 5)
	child := insert("Draft", false, &epic.ID, 1)
	finished := insert("Ship docs v1", true, nil, 12)

	open := ok(ReadOpenTodos(ctx, db))
	if len(open) != 2 {
		t.Fatalf("open todos: %v", titles(open))
	}
	if got := ok(ReadTodo(ctx, db, child.ID)); got == nil || got.Title != "Draft" {
		t.Fatalf("one: %+v", got)
	}
	if got := ok(ReadOpenDocsTodos(ctx, db)); len(got) != 1 || got[0].ID != epic.ID {
		t.Fatalf("filters: %v", titles(got))
	}
	if got := titles(ok(ReadOpenTodosByTitle(ctx, db))); strings.Join(got, ",") != "Draft,Write the docs" {
		t.Fatalf("sorting: %v", got)
	}
	if got := titles(ok(ReadTodoPage(ctx, db, 2, 1))); strings.Join(got, ",") != "Write the docs" {
		t.Fatalf("page 2 of 1: %v", got)
	}
	if got := ok(ReadOpenTodosFromServer(ctx, db)); len(got) != 2 {
		t.Fatalf("remote tier: %v", titles(got))
	}
	withProjects := ok(ReadOpenTodosWithProjects(ctx, db))
	for _, todo := range withProjects {
		if todo.Project == nil || todo.Project.Name != "Docs" {
			t.Fatalf("includes: %+v", todo)
		}
		if todo.ID == child.ID && (todo.Parent == nil || todo.Parent.Project == nil) {
			t.Fatalf("parent's project: %+v", todo)
		}
	}
	projects := ok(ReadProjectsWithOpenTodos(ctx, db))
	if len(projects) != 1 || len(projects[0].Todos) != 2 {
		t.Fatalf("reverse relation: %+v", projects)
	}
	selected := ok(ReadOpenTodoTitles(ctx, db))
	if len(selected) != 2 || selected[0].Project == nil || selected[0].Project.Name != "Docs" || selected[0].Title == "" {
		t.Fatalf("select: %+v", selected)
	}
	lineage := ok(TodoLineageQuery().All(ctx, db))
	if len(lineage) < 2 {
		t.Fatalf("recursive: %v", titles(lineage))
	}
	if err := whereOperators(ctx, db, "docs", epic.ID, child.ID); err != nil {
		t.Fatal(err)
	}
	live := ok(SubscribeOpenTodos(ctx, db))
	<-live.Ready()
	if len(live.Rows()) != 2 {
		t.Fatalf("where-subscription: %v", titles(live.Rows()))
	}

	// The subscription gets the whole result, then is unsubscribed.
	watchCtx, stop := context.WithCancel(ctx)
	results := make(chan []jazzapp.Todo, 8)
	done := make(chan error, 1)
	go func() { done <- WatchOpenTodos(watchCtx, db, func(todos []jazzapp.Todo) { results <- todos }) }()
	for got := <-results; len(got) != 2; got = <-results {
	}
	stop()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("watch ended with %v", err)
	}

	// Writes.
	if err := WriteTodoCrud(ctx, db, child.ID, owner, project.ID); err != nil {
		t.Fatal(err)
	}
	if err := ClearTodoOwner(ctx, db, epic.ID); err != nil {
		t.Fatal(err)
	}
	imported := randomUUID(t)
	if got := ok(UpsertTodo(ctx, db, imported)); got.ID != imported || got.Title != "Imported task" {
		t.Fatalf("upsert: %+v", got)
	}
	if got := ok(RestoreDeletedTodo(ctx, db, finished.ID, owner, project.ID)); got.Title != "Restored task" {
		t.Fatalf("restore: %+v", got)
	}
	if err := WriteTodoDurably(ctx, db, owner, project.ID); err != nil {
		t.Fatal(err)
	}
	if id := ok(InsertTodoAndWait(ctx, db, owner, project.ID)); id == "" {
		t.Fatal("mutation errors: no id")
	}
	created := ok(GroupTodoWrites(ctx, db, epic.ID, owner, project.ID))
	if got := ok(ReadTodo(ctx, db, created)); got == nil || got.Title != "Write transaction docs" {
		t.Fatalf("transaction: %+v", got)
	}
	if err := CompleteTodoInTransaction(ctx, db, created); err != nil {
		t.Fatal(err)
	}
	if id := ok(FinishTodoSerializably(ctx, db, created)); id != created {
		t.Fatalf("serializable: %q", id)
	}
	if _, err := FinishTodoSerializably(ctx, db, randomUUID(t)); err == nil {
		t.Fatal("serializable: a missing todo didn't fail")
	}

	// Permission advice (the todo app's permissions allow everything).
	if advice := ok(CanReadTodo(ctx, db, created)); advice == jazz.Denied {
		t.Fatalf("can read: %v", advice)
	}
	if advice := ok(CanCreateTodo(ctx, db, "New")); advice == jazz.Denied {
		t.Fatalf("can insert: %v", advice)
	}
	ok(TodosYouMayDelete(ctx, db))
	ok(TodosYouMayEdit(ctx, db))
}

// TestRequireIncludes runs the required-references recipe's region on a
// local app of the recipe.
func TestRequireIncludes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db := ok(orders.Open(ctx, jazz.Config{AppID: randomUUID(t), Storage: jazz.StorageMemory}))
	defer db.Close()
	customer := ok(orders.Customers.Insert(ctx, db, orders.NewCustomer{Name: "Ada"}))
	ok(orders.Orders.Insert(ctx, db, orders.NewOrder{CustomerID: customer.ID}))
	ok(orders.Orders.Insert(ctx, db, orders.NewOrder{CustomerID: randomUUID(t)}))
	got := ok(ReadOrdersWithTheirCustomer(ctx, db))
	if len(got) != 1 || got[0].Customer == nil || got[0].Customer.Name != "Ada" {
		t.Fatalf("required customer: %+v", got)
	}
}

// The app files are the docs todo app's, as npx jazz-tools generate writes
// them from schema.ts.
func TestAppFilesAreTheTodoApp(t *testing.T) {
	for _, pair := range [][2]string{
		{"jazz.app.json", "../todo-client-localfirst-ts/jazz.app.json"},
		{"jazz.permissions.json", "../todo-client-localfirst-ts/jazz.permissions.json"},
		{"recipes/required-references/jazz.app.json", "../todo-client-localfirst-ts/src/recipes/required-references/jazz.app.json"},
	} {
		ours, err1 := os.ReadFile(pair[0])
		theirs, err2 := os.ReadFile(pair[1])
		if err1 != nil || err2 != nil || string(ours) != string(theirs) {
			t.Errorf("%s differs from %s (%v, %v)", pair[0], pair[1], err1, err2)
		}
	}
}
