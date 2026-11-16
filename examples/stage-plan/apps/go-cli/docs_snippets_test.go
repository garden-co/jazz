package main

// Documentation snippet sources, compiled and run with the example's
// tests. The docs include each `#region` by name.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	jazz "github.com/garden-co/jazz/packages/jazz-go/v2"

	"github.com/garden-co/jazz/examples/stage-plan/apps/go-cli/jazzapp"
)

// #region context-setup-go-client
func openAsUser(ctx context.Context, appID, serverURL, dataDir string, token jazz.TokenProvider) (*jazz.DB, error) {
	// Sign in through your main backend's /jazz with a token from your auth
	// provider; rows stay in dataDir and sync through it. appID is your
	// app's (jazz.EndpointAppID(serverURL) if your clients derive it).
	return jazzapp.Open(ctx, jazz.Config{
		AppID:     appID,
		ServerURL: serverURL,
		Storage:   jazz.StoragePersistent,
		DataDir:   dataDir,
		Auth:      jazz.Token(token),
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

// #region reading-includes-go
func tasksWithTheirShow(ctx context.Context, db *jazz.DB, showID string) ([]jazzapp.Task, error) {
	// Each task's Show field is filled; relations not included stay nil.
	return jazzapp.Tasks.Query().
		Where(jazzapp.Tasks.ShowID.Eq(showID)).
		Include(jazzapp.Tasks.Show).
		All(ctx, db)
}

// #endregion reading-includes-go

// #region reading-require-includes-go
func tasksOfVisibleShows(ctx context.Context, db *jazz.DB) ([]jazzapp.Task, error) {
	// Only tasks whose show this user can read: Required() is
	// requireIncludes, for a relation through a non-optional column.
	return jazzapp.Tasks.Query().
		Include(jazzapp.Tasks.Show.Required()).
		All(ctx, db)
}

// #endregion reading-require-includes-go

// #region reading-select-go
type taskTitle struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

func taskTitles(ctx context.Context, db *jazz.DB, showID string) ([]taskTitle, error) {
	// Select reads only the given columns (and id) into a struct of yours.
	return jazz.Select[taskTitle](
		jazzapp.Tasks.Query().Where(jazzapp.Tasks.ShowID.Eq(showID)),
		jazzapp.Tasks.Title,
	).All(ctx, db)
}

// #endregion reading-select-go

// #region dry-run-permissions-go
func canAddTask(ctx context.Context, db *jazz.DB, showID string) (bool, error) {
	// Advice, not enforcement: Allowed, Denied, or Unknown (e.g. offline).
	advice, err := jazzapp.Tasks.CanInsert(ctx, db, jazzapp.NewTask{
		ShowID: showID, Title: "Encore", Status: jazzapp.TaskStatusTodo, Rank: 9,
	})
	return advice == jazz.Allowed, err
}

// #endregion dry-run-permissions-go

// #region writing-mutation-errors-go
func renameTask(ctx context.Context, db *jazz.DB, taskID, title string) error {
	err := jazzapp.Tasks.Update(ctx, db, taskID, jazzapp.TaskPatch{Title: jazz.Set(title)},
		jazz.WaitFor(jazz.WriteGlobal))
	switch {
	case errors.Is(err, jazz.ErrPermission):
		return fmt.Errorf("you can't rename this task: %w", err)
	case errors.Is(err, jazz.ErrRefused):
		return fmt.Errorf("that title isn't valid: %w", err)
	}
	return err
}

// #endregion writing-mutation-errors-go

// #region writing-serializable-transaction-go
func moveIfStillTodo(ctx context.Context, db *jazz.DB, taskID string) error {
	// A serializable transaction: if the task changed after it was read
	// here, the commit fails with jazz.ErrConflict.
	err := db.Tx(ctx, func(tx *jazz.Tx) error {
		task, err := jazzapp.Tasks.Query().Where(jazzapp.Tasks.ID.Eq(taskID)).One(ctx, tx)
		if err != nil || task == nil || task.Status != jazzapp.TaskStatusTodo {
			return err
		}
		return jazzapp.Tasks.Update(ctx, tx, taskID, jazzapp.TaskPatch{Status: jazz.Set(jazzapp.TaskStatusDoing)})
	}, jazz.Serializable())
	if errors.Is(err, jazz.ErrConflict) {
		return moveIfStillTodo(ctx, db, taskID) // read again and retry
	}
	return err
}

// #endregion writing-serializable-transaction-go

func TestDocsSnippets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srv := testServer(t)
	db, err := openAsUser(ctx, srv.appID, srv.url, t.TempDir(), func(context.Context, jazz.TokenReason) (string, error) {
		return "token-docs-user", nil
	})
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

	if rows, err := tasksWithTheirShow(ctx, db, show.ID); err != nil || len(rows) == 0 || rows[0].Show == nil {
		t.Fatalf("with show: %+v, %v", rows, err)
	}
	if rows, err := tasksOfVisibleShows(ctx, db); err != nil || len(rows) == 0 || rows[0].Show == nil {
		t.Fatalf("required show: %+v, %v", rows, err)
	}
	if rows, err := taskTitles(ctx, db, show.ID); err != nil || len(rows) == 0 || rows[0].Title == "" {
		t.Fatalf("titles: %+v, %v", rows, err)
	}
	if ok, err := canAddTask(ctx, db, show.ID); err != nil || !ok {
		t.Fatalf("can add: %v, %v", ok, err)
	}
	if err := renameTask(ctx, db, byRank[0].ID, "Load-in early"); err != nil {
		t.Fatal(err)
	}
	if err := moveIfStillTodo(ctx, db, byRank[0].ID); err != nil {
		t.Fatal(err)
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

}
