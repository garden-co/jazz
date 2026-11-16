package main

import (
	"context"
	"errors"

	jazz "github.com/garden-co/jazz/packages/jazz-go/v2"

	"github.com/garden-co/jazz/examples/stage-plan/apps/go-cli/jazzapp"
)

// server is the app's Jazz endpoint (its main backend's /jazz) and its
// app id.
type server struct {
	appID, url string
}

// openUser opens StagePlan as an end user signing in through the app's
// /jazz with bearer, a token from the app's auth provider: the main
// backend checks every write against the app's permissions as that
// user's. An empty dataDir keeps rows in memory.
func openUser(ctx context.Context, srv server, bearer, dataDir string) (*jazz.DB, error) {
	storage := jazz.StorageMemory
	if dataDir != "" {
		storage = jazz.StoragePersistent
	}
	return jazzapp.Open(ctx, jazz.Config{
		AppID:     srv.appID,
		ServerURL: srv.url,
		Storage:   storage,
		DataDir:   dataDir,
		Auth: jazz.Token(func(context.Context, jazz.TokenReason) (string, error) {
			return bearer, nil
		}),
	})
}

// ensureShow is the first show the user is chief of, created if there is
// none.
func ensureShow(ctx context.Context, db *jazz.DB, name string) (jazzapp.Show, error) {
	account, err := db.Account(ctx)
	if err != nil {
		return jazzapp.Show{}, err
	}
	if account == "" {
		return jazzapp.Show{}, errors.New("the app acts as no account")
	}
	show, err := jazzapp.Shows.Query().
		Where(jazzapp.Shows.ChiefAccount.Eq(account)).
		One(ctx, db)
	if err != nil || show != nil {
		return deref(show), err
	}
	return jazzapp.Shows.Insert(ctx, db, jazzapp.NewShow{
		Name:         name,
		Venue:        "The Jazz Club",
		Date:         "2026-12-31",
		Doors:        "19:00",
		ChiefAccount: account,
	})
}

func deref[T any](v *T) T {
	if v == nil {
		var zero T
		return zero
	}
	return *v
}

// listTasks is the show's tasks, ordered by rank.
func listTasks(ctx context.Context, db *jazz.DB, showID string) ([]jazzapp.Task, error) {
	return jazzapp.Tasks.Query().
		Where(jazzapp.Tasks.ShowID.Eq(showID)).
		OrderBy(jazzapp.Tasks.Rank.Asc()).
		All(ctx, db)
}

// addTask adds a task to the show's To do column, after its other tasks.
func addTask(ctx context.Context, db *jazz.DB, showID, title string) (jazzapp.Task, error) {
	last, err := jazzapp.Tasks.Query().
		Where(jazzapp.Tasks.ShowID.Eq(showID)).
		OrderBy(jazzapp.Tasks.Rank.Desc()).
		One(ctx, db)
	if err != nil {
		return jazzapp.Task{}, err
	}
	rank := 1.0
	if last != nil {
		rank = last.Rank + 1
	}
	return jazzapp.Tasks.Insert(ctx, db, jazzapp.NewTask{
		ShowID: showID,
		Title:  title,
		Status: jazzapp.TaskStatusTodo,
		Rank:   rank,
	})
}

// moveTask moves a task to another stage.
func moveTask(ctx context.Context, db *jazz.DB, taskID string, status jazzapp.TaskStatus) error {
	return jazzapp.Tasks.Update(ctx, db, taskID, jazzapp.TaskPatch{
		Status: jazz.Set(status),
	})
}

// statuses are the stages, in board order.
var statuses = []jazzapp.TaskStatus{
	jazzapp.TaskStatusTodo,
	jazzapp.TaskStatusDoing,
	jazzapp.TaskStatusDone,
	jazzapp.TaskStatusBlocked,
}
