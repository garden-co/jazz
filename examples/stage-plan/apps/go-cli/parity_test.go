package main

import (
	"context"
	"errors"
	"testing"
	"time"

	jazz "github.com/garden-co/jazz/packages/jazz-go"

	"github.com/garden-co/jazz/examples/stage-plan/apps/go-cli/jazzapp"
)

func TestIncludesAndSelect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srv := testServer(t)
	db := openTest(t, srv, "alice")
	show, err := ensureShow(ctx, db, "Include Night")
	if err != nil {
		t.Fatal(err)
	}
	task, err := addTask(ctx, db, show.ID, "Rig lights")
	if err != nil {
		t.Fatal(err)
	}

	// A forward include: each task with its show.
	tasks, err := jazzapp.Tasks.Query().
		Where(jazzapp.Tasks.ShowID.Eq(show.ID)).
		Include(jazzapp.Tasks.Show).
		All(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Show == nil || tasks[0].Show.Name != "Include Night" {
		t.Fatalf("tasks with show: %+v", tasks)
	}
	if tasks[0].Assignee != nil || tasks[0].Comments != nil {
		t.Fatalf("relations not included are nil: %+v", tasks[0])
	}

	// A reverse include, with a child query: a show with its tasks.
	shows, err := jazzapp.Shows.Query().
		Where(jazzapp.Shows.ID.Eq(show.ID)).
		Include(jazzapp.Shows.Tasks.With(
			jazzapp.Tasks.Query().OrderBy(jazzapp.Tasks.Rank.Asc()).Limit(10),
		)).
		All(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(shows) != 1 || len(shows[0].Tasks) != 1 || shows[0].Tasks[0].ID != task.ID {
		t.Fatalf("show with tasks: %+v", shows)
	}

	// Select: only the title, into a struct of your own.
	type title struct {
		ID    string `json:"id"`
		Title string `json:"title"`
	}
	titles, err := jazz.Select[title](
		jazzapp.Tasks.Query().Where(jazzapp.Tasks.ShowID.Eq(show.ID)),
		jazzapp.Tasks.Title,
	).All(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(titles) != 1 || titles[0].Title != "Rig lights" || titles[0].ID != task.ID {
		t.Fatalf("selected: %+v", titles)
	}
}

func TestTransactionsAndTiers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srv := testServer(t)
	db := openTest(t, srv, "alice")
	show, err := ensureShow(ctx, db, "Tx Night")
	if err != nil {
		t.Fatal(err)
	}

	// Commit: both tasks land, readable inside the transaction too.
	err = db.Tx(ctx, func(tx *jazz.Tx) error {
		for i, title := range []string{"Load-in", "Soundcheck"} {
			if _, err := jazzapp.Tasks.Insert(ctx, tx, jazzapp.NewTask{
				ShowID: show.ID, Title: title, Status: jazzapp.TaskStatusTodo, Rank: float64(i + 1),
			}); err != nil {
				return err
			}
		}
		inside, err := jazzapp.Tasks.Query().Where(jazzapp.Tasks.ShowID.Eq(show.ID)).All(ctx, tx)
		if err != nil {
			return err
		}
		if len(inside) != 2 {
			t.Errorf("read inside the transaction: %d tasks", len(inside))
		}
		return nil
	}, jazz.CommitAt(jazz.WriteGlobal))
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := listTasks(ctx, db, show.ID)
	if err != nil || len(tasks) != 2 {
		t.Fatalf("after commit: %+v, %v", tasks, err)
	}

	// An error rolls back.
	sentinel := errors.New("changed my mind")
	err = db.Tx(ctx, func(tx *jazz.Tx) error {
		if _, err := jazzapp.Tasks.Insert(ctx, tx, jazzapp.NewTask{
			ShowID: show.ID, Title: "Encore", Status: jazzapp.TaskStatusTodo, Rank: 3,
		}); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("rollback error: %v", err)
	}
	// A panic rolls back too, and panics on.
	func() {
		defer func() {
			if recover() == nil {
				t.Error("the panic didn't propagate")
			}
		}()
		_ = db.Tx(ctx, func(tx *jazz.Tx) error {
			_, _ = jazzapp.Tasks.Insert(ctx, tx, jazzapp.NewTask{ShowID: show.ID, Title: "Panic", Status: jazzapp.TaskStatusTodo, Rank: 4})
			panic("boom")
		})
	}()
	if tasks, err := listTasks(ctx, db, show.ID); err != nil || len(tasks) != 2 {
		t.Fatalf("after rollbacks: %+v, %v", tasks, err)
	}

	// Tiers: a write that waits for the server, a read from it, and
	// waiting for every write so far.
	if _, err := jazzapp.Tasks.Insert(ctx, db, jazzapp.NewTask{
		ShowID: show.ID, Title: "Backline", Status: jazzapp.TaskStatusTodo, Rank: 5,
	}, jazz.WaitFor(jazz.WriteGlobal)); err != nil {
		t.Fatal(err)
	}
	remote, err := jazzapp.Tasks.Query().Where(jazzapp.Tasks.ShowID.Eq(show.ID)).Tier(jazz.ReadRemote).All(ctx, db)
	if err != nil || len(remote) != 3 {
		t.Fatalf("remote read: %d, %v", len(remote), err)
	}
}
