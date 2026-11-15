package main

// Documentation snippet sources, compiled and run with the example's
// tests. The docs include each `#region` by name.

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	jazz "github.com/garden-co/jazz/packages/jazz-go"

	"github.com/garden-co/jazz/examples/stage-plan/apps/go-cli/jazzapp"
)

// #region context-setup-go-backend
func openAsBackend(ctx context.Context, appID, serverURL, userID string) (*jazz.DB, error) {
	// A backend publishes the app's permissions and acts as one user:
	// the server checks every write as that user's.
	return jazzapp.Open(ctx, jazz.Config{
		Permissions:   permissions, // the text of jazz.permissions.json
		AppID:         appID,
		ServerURL:     serverURL,
		BackendSecret: os.Getenv("JAZZ_BACKEND_SECRET"),
		AdminSecret:   os.Getenv("JAZZ_ADMIN_SECRET"),
		Storage:       jazz.StorageMemory,
		Session:       &jazz.Session{Subject: userID},
	})
}

// #endregion context-setup-go-backend

// #region context-setup-go-client
func openLocally(ctx context.Context, appID, dataDir string) (*jazz.DB, error) {
	// Without permissions and a server, the app is a local client: its rows
	// stay in dataDir. (Syncing as an end user isn't supported in Go yet.)
	return jazzapp.Open(ctx, jazz.Config{
		AppID:   appID,
		Storage: jazz.StoragePersistent,
		DataDir: dataDir,
	})
}

// #endregion context-setup-go-client

// #region reading-oneshot-go
func printBoard(ctx context.Context, db *jazz.DB, showID string) error {
	tasks, err := jazzapp.Tasks.Query().
		Where(jazzapp.Tasks.ShowID.Eq(showID)).
		All(ctx, db)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		fmt.Printf("[%s] %s\n", task.Status, task.Title)
	}
	return nil
}

// #endregion reading-oneshot-go

// #region reading-subscriptions-go
func waitUntilDone(ctx context.Context, db *jazz.DB, showID string) error {
	// Cancelling ctx (here, by returning) unsubscribes and closes the channel.
	ctx, unsubscribe := context.WithCancel(ctx)
	defer unsubscribe()
	deltas, err := jazzapp.Tasks.Query().
		Where(jazzapp.Tasks.ShowID.Eq(showID)).
		Subscribe(ctx, db)
	if err != nil {
		return err
	}
	for delta := range deltas {
		if delta.Err != nil {
			return delta.Err
		}
		// The first delta is Reset, with every task in Added.
		for _, task := range append(delta.Added, delta.Updated...) {
			if task.Status == jazzapp.TaskStatusDone {
				fmt.Println("done:", task.Title)
				return nil
			}
		}
	}
	return ctx.Err()
}

// #endregion reading-subscriptions-go

// #region reading-filters-go
func blockedTasks(ctx context.Context, db *jazz.DB, showID string) ([]jazzapp.Task, error) {
	// Conditions in one Where (and in chained ones) must all hold.
	return jazzapp.Tasks.Query().
		Where(
			jazzapp.Tasks.ShowID.Eq(showID),
			jazzapp.Tasks.Status.Eq(jazzapp.TaskStatusBlocked),
		).
		All(ctx, db)
}

// #endregion reading-filters-go

// #region reading-sorting-go
func tasksByRank(ctx context.Context, db *jazz.DB, showID string) ([]jazzapp.Task, error) {
	return jazzapp.Tasks.Query().
		Where(jazzapp.Tasks.ShowID.Eq(showID)).
		OrderBy(jazzapp.Tasks.Rank.Asc(), jazzapp.Tasks.Title.Asc()).
		All(ctx, db)
}

// #endregion reading-sorting-go

// #region reading-pagination-go
func taskPage(ctx context.Context, db *jazz.DB, showID string, page, perPage int) ([]jazzapp.Task, error) {
	return jazzapp.Tasks.Query().
		Where(jazzapp.Tasks.ShowID.Eq(showID)).
		OrderBy(jazzapp.Tasks.Rank.Asc()).
		Limit(perPage).
		Offset(page*perPage).
		All(ctx, db)
}

// #endregion reading-pagination-go

// #region where-eq-ne-go
func openTasks(ctx context.Context, db *jazz.DB) ([]jazzapp.Task, error) {
	return jazzapp.Tasks.Query().
		Where(jazzapp.Tasks.Status.Ne(jazzapp.TaskStatusDone)).
		All(ctx, db)
}

// #endregion where-eq-ne-go

// #region where-numeric-go
func topTasks(ctx context.Context, db *jazz.DB) ([]jazzapp.Task, error) {
	return jazzapp.Tasks.Query().
		Where(jazzapp.Tasks.Rank.Gte(1), jazzapp.Tasks.Rank.Lt(3)).
		All(ctx, db)
}

// #endregion where-numeric-go

// #region where-contains-go
func checkTasks(ctx context.Context, db *jazz.DB) ([]jazzapp.Task, error) {
	return jazzapp.Tasks.Query().
		Where(jazzapp.Tasks.Title.Contains("check")).
		All(ctx, db)
}

// #endregion where-contains-go

// #region where-null-go
func unassignedTasks(ctx context.Context, db *jazz.DB) ([]jazzapp.Task, error) {
	return jazzapp.Tasks.Query().
		Where(jazzapp.Tasks.AssigneeID.IsNull()).
		All(ctx, db)
}

// #endregion where-null-go

// #region writing-crud-go
func taskLifecycle(ctx context.Context, db *jazz.DB, showID string) error {
	task, err := jazzapp.Tasks.Insert(ctx, db, jazzapp.NewTask{
		ShowID: showID,
		Title:  "Soundcheck",
		Status: jazzapp.TaskStatusTodo,
		Rank:   1,
		// Nullable columns are pointers: nil leaves them null.
	})
	if err != nil {
		return err
	}
	// A patch changes only the fields it sets.
	err = jazzapp.Tasks.Update(ctx, db, task.ID, jazzapp.TaskPatch{
		Status: jazz.Set(jazzapp.TaskStatusDoing),
		Title:  jazz.Set("Soundcheck (main stage)"),
	})
	if err != nil {
		return err
	}
	return jazzapp.Tasks.Delete(ctx, db, task.ID)
}

// #endregion writing-crud-go

// #region writing-nullable-update-go
func unassign(ctx context.Context, db *jazz.DB, taskID string) error {
	// A nullable column's patch field holds a pointer: Set(nil) clears it.
	return jazzapp.Tasks.Update(ctx, db, taskID, jazzapp.TaskPatch{
		AssigneeID: jazz.Set[*string](nil),
	})
}

// #endregion writing-nullable-update-go

// #region reading-reverse-relation-go
func showWithItsTasks(ctx context.Context, db *jazz.DB, showID string) (*jazzapp.Show, error) {
	// Included relations fill the row's relation fields: Show.Tasks here.
	return jazzapp.Shows.Query().
		Where(jazzapp.Shows.ID.Eq(showID)).
		Include(jazzapp.Shows.Tasks.With(
			jazzapp.Tasks.Query().OrderBy(jazzapp.Tasks.Rank.Asc()),
		)).
		One(ctx, db)
}

// #endregion reading-reverse-relation-go

// #region writing-transaction-go
func addTwoTasks(ctx context.Context, db *jazz.DB, showID string) error {
	// Commits when the function returns nil; an error or a panic rolls
	// every write in it back.
	return db.Tx(ctx, func(tx *jazz.Tx) error {
		for i, title := range []string{"Load-in", "Line check"} {
			if _, err := jazzapp.Tasks.Insert(ctx, tx, jazzapp.NewTask{
				ShowID: showID, Title: title, Status: jazzapp.TaskStatusTodo, Rank: float64(i + 1),
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

// #endregion writing-transaction-go

// #region reading-durability-tier-go
func tasksFromTheServer(ctx context.Context, db *jazz.DB, showID string) ([]jazzapp.Task, error) {
	// ReadRemote waits for the server's result; the default reads locally.
	return jazzapp.Tasks.Query().
		Where(jazzapp.Tasks.ShowID.Eq(showID)).
		Tier(jazz.ReadRemote).
		All(ctx, db)
}

// #endregion reading-durability-tier-go

// #region writing-durability-tier-go
func addTaskDurably(ctx context.Context, db *jazz.DB, showID string) error {
	// Returns once the sync server has the task (or refuses it: then the
	// error is jazz.ErrPermission).
	_, err := jazzapp.Tasks.Insert(ctx, db, jazzapp.NewTask{
		ShowID: showID, Title: "Soundcheck", Status: jazzapp.TaskStatusTodo, Rank: 1,
	}, jazz.WaitFor(jazz.WriteGlobal))
	return err
}

// #endregion writing-durability-tier-go

func TestDocsSnippets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srv := testServer(t)
	t.Setenv("JAZZ_BACKEND_SECRET", srv.backendSecret)
	t.Setenv("JAZZ_ADMIN_SECRET", srv.adminSecret)
	db, err := openAsBackend(ctx, srv.appID, srv.url, "docs-user")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	show, err := ensureShow(ctx, db, "Docs Night")
	if err != nil {
		t.Fatal(err)
	}
	for _, title := range []string{"Load-in", "Line check", "Backline"} {
		if _, err := addTask(ctx, db, show.ID, title); err != nil {
			t.Fatal(err)
		}
	}
	if err := taskLifecycle(ctx, db, show.ID); err != nil {
		t.Fatal(err)
	}
	if err := printBoard(ctx, db, show.ID); err != nil {
		t.Fatal(err)
	}
	byRank, err := tasksByRank(ctx, db, show.ID)
	if err != nil || len(byRank) != 3 || byRank[0].Title != "Load-in" {
		t.Fatalf("by rank: %+v, %v", byRank, err)
	}
	page, err := taskPage(ctx, db, show.ID, 1, 2)
	if err != nil || len(page) != 1 || page[0].Title != "Backline" {
		t.Fatalf("page 2: %+v, %v", page, err)
	}
	if err := moveTask(ctx, db, byRank[1].ID, jazzapp.TaskStatusBlocked); err != nil {
		t.Fatal(err)
	}
	blocked, err := blockedTasks(ctx, db, show.ID)
	if err != nil || len(blocked) != 1 || blocked[0].Title != "Line check" {
		t.Fatalf("blocked: %+v, %v", blocked, err)
	}
	checks, err := checkTasks(ctx, db)
	if err != nil || len(checks) != 1 {
		t.Fatalf("contains: %+v, %v", checks, err)
	}
	for name, query := range map[string]func(context.Context, *jazz.DB) ([]jazzapp.Task, error){
		"ne": openTasks, "numeric": topTasks, "null": unassignedTasks,
	} {
		if rows, err := query(ctx, db); err != nil || len(rows) == 0 {
			t.Fatalf("%s: %+v, %v", name, rows, err)
		}
	}
	if err := unassign(ctx, db, byRank[0].ID); err != nil {
		t.Fatal(err)
	}

	if err := addTwoTasks(ctx, db, show.ID); err != nil {
		t.Fatal(err)
	}
	if err := addTaskDurably(ctx, db, show.ID); err != nil {
		t.Fatal(err)
	}
	if remote, err := tasksFromTheServer(ctx, db, show.ID); err != nil || len(remote) != 6 {
		t.Fatalf("remote: %d, %v", len(remote), err)
	}
	if withTasks, err := showWithItsTasks(ctx, db, show.ID); err != nil || withTasks == nil || len(withTasks.Tasks) != 6 {
		t.Fatalf("show with tasks: %+v, %v", withTasks, err)
	}

	// The subscription snippet returns once a task is done.
	waited := make(chan error, 1)
	go func() { waited <- waitUntilDone(ctx, db, show.ID) }()
	time.Sleep(200 * time.Millisecond)
	if err := moveTask(ctx, db, byRank[2].ID, jazzapp.TaskStatusDone); err != nil {
		t.Fatal(err)
	}
	if err := <-waited; err != nil {
		t.Fatal(err)
	}

	// A local client opens the app on a store of its own.
	client, err := openLocally(ctx, srv.appID, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}
