package main

import (
	"context"
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
