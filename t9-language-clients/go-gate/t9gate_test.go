package main

// t9 language gate: a Go backend client of the stock TS createJazz StagePlan backend.
// Env: JAZZ_SERVER_URL (with /jazz), JAZZ_APP_ID, T9_WORKLOAD_ISSUER (token endpoint base), T9_USER_A/B (have accounts).
import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	jazz "github.com/garden-co/jazz/packages/jazz-go/v2"
	"github.com/garden-co/jazz/examples/stage-plan/apps/go-cli/jazzapp"
)

func tok(t *testing.T, sub string) string {
	r, err := http.Get(os.Getenv("T9_WORKLOAD_ISSUER") + "/token?sub=" + sub)
	if err != nil { t.Fatal(err) }
	defer r.Body.Close(); b, _ := io.ReadAll(r.Body); return string(b)
}
func open(t *testing.T, sub string) *jazz.DB {
	ctx, c := context.WithTimeout(context.Background(), 30*time.Second); defer c()
	perms, _ := os.ReadFile("jazz.permissions.json")
	db, err := jazzapp.Open(ctx, jazz.Config{AppID: os.Getenv("JAZZ_APP_ID"), ServerURL: os.Getenv("JAZZ_SERVER_URL"), Storage: jazz.StorageMemory, Permissions: string(perms),
		Auth: jazz.Credential(func(context.Context, jazz.TokenReason) (string, error) { return tok(t, sub), nil })})
	if err != nil { t.Fatalf("open %s: %v", sub, err) }
	t.Cleanup(func() { db.Close() }); return db
}
func tasksIn(ctx context.Context, db *jazz.DB, show string) ([]jazzapp.Task, error) {
	return jazzapp.Tasks.Query().Where(jazzapp.Tasks.ShowID.Eq(show)).Tier(jazz.ReadRemote).All(ctx, db)
}

func TestT9Gate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second); defer cancel()
	A, B := os.Getenv("T9_USER_A"), os.Getenv("T9_USER_B")
	trusted := open(t, "gate-trusted-job")
	if k, err := trusted.BackendClient(ctx); err != nil || k != jazz.BackendTrusted { t.Fatalf("trusted kind %q %v", k, err) }
	t.Log("PASS trusted open (kind trusted)")
	asA, err := trusted.ForUser(ctx, A, nil); if err != nil { t.Fatalf("ForUser(A): %v", err) }
	show, err := ensureShow(ctx, asA, "Go gate A private"); if err != nil { t.Fatalf("A show: %v", err) }
	task, err := jazzapp.Tasks.Insert(ctx, asA, jazzapp.NewTask{ShowID: show.ID, Title: "Go secret", Status: jazzapp.TaskStatusTodo, Rank: 1}, jazz.WaitFor(jazz.WriteGlobal))
	if err != nil { t.Fatalf("A write: %v", err) }
	t.Logf("PASS forUser(A) writes show %s task %s", show.ID, task.ID)
	ts, err := tasksIn(ctx, asA, show.ID); found := false
	for _, x := range ts { if x.ID == task.ID { found = true } }
	if err != nil || !found { t.Fatalf("A read back: %v %v", ts, err) }; t.Log("PASS forUser(A) reads it")
	asB, err := trusted.ForUser(ctx, B, nil); if err != nil { t.Fatalf("ForUser(B): %v", err) }
	tb, err := tasksIn(ctx, asB, show.ID)
	if err != nil || len(tb) != 0 { t.Fatalf("FAIL B sees A's tasks: %v %v", tb, err) }; t.Log("PASS forUser(B) sees none of A's tasks")
	_, err = jazzapp.Tasks.Insert(ctx, asB, jazzapp.NewTask{ShowID: show.ID, Title: "Go sneaky", Status: jazzapp.TaskStatusTodo, Rank: 2}, jazz.WaitFor(jazz.WriteGlobal))
	if err == nil { t.Fatal("FAIL B's write into A's show was accepted") }
	if !errors.Is(err, jazz.ErrPermission) { t.Logf("note: B's refusal is %v (not ErrPermission)", err) }
	t.Logf("PASS forUser(B) write refused: %v", err)

	scoped := open(t, "gate-scoped-agent")
	if k, err := scoped.BackendClient(ctx); err != nil || k != jazz.BackendScoped { t.Fatalf("scoped kind %q %v", k, err) }
	if _, err := scoped.ForUser(ctx, A, nil); !errors.Is(err, jazz.ErrRefused) { t.Fatalf("scoped ForUser = %v, want ErrRefused", err) }
	own, err := ensureShow(ctx, scoped, "Go gate scoped"); if err != nil { t.Fatalf("scoped show: %v", err) }
	if _, err := jazzapp.Tasks.Insert(ctx, scoped, jazzapp.NewTask{ShowID: own.ID, Title: "scoped own", Status: jazzapp.TaskStatusTodo, Rank: 1}, jazz.WaitFor(jazz.WriteGlobal)); err != nil { t.Fatalf("scoped write: %v", err) }
	t.Log("PASS scoped writes its own show; ForUser refused")
	ts2, err := tasksIn(ctx, scoped, show.ID)
	if err != nil || len(ts2) != 0 { t.Fatalf("FAIL scoped sees A's tasks: %v %v", ts2, err) }
	_, err = jazzapp.Tasks.Insert(ctx, scoped, jazzapp.NewTask{ShowID: show.ID, Title: "scoped sneaky", Status: jazzapp.TaskStatusTodo, Rank: 3}, jazz.WaitFor(jazz.WriteGlobal))
	if err == nil { t.Fatal("FAIL scoped write into A's show accepted") }
	t.Logf("PASS scoped can't see or write A's show: %v", err)
	if err := trusted.Shutdown(ctx); err != nil { t.Errorf("Shutdown: %v", err) }
}
