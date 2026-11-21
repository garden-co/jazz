package main

import (
	"context"
	"testing"
	"time"

	jazz "github.com/garden-co/jazz/packages/jazz-go/v2"

	"github.com/garden-co/jazz/examples/stage-plan/apps/go-cli/jazzapp"
)

// waitRows waits until l has want rows.
func waitRows[R any](t *testing.T, ctx context.Context, l *jazz.Live[R], want int) []R {
	t.Helper()
	for {
		if rows := l.Rows(); len(rows) == want {
			return rows
		}
		select {
		case <-l.Changes():
		case <-l.Done():
			t.Fatalf("live ended: %v", l.Err())
		case <-time.After(10 * time.Second):
			t.Fatalf("live has %d rows, want %d", len(l.Rows()), want)
		}
	}
}

// One row can be several live results: a union's overlapping arms, and a
// history query's versions. Live keeps them apart by result key.
func TestLiveUnionAndHistoryByResultKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	srv := testServer(t)
	db := openTest(t, srv, "alice")
	show, err := ensureShow(ctx, db, "Keys Night")
	if err != nil {
		t.Fatal(err)
	}
	task, err := addTask(ctx, db, show.ID, "Soundcheck")
	if err != nil {
		t.Fatal(err)
	}

	union, err := jazz.Union(
		jazzapp.Tasks.Query().Where(jazzapp.Tasks.ShowID.Eq(show.ID)),
		jazzapp.Tasks.Query().Where(jazzapp.Tasks.ID.Eq(task.ID)),
	).Live(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	<-union.Ready()
	if len(union.Rows()) == 1 {
		t.Skip("this libjazz_ffi has no result keys: a union's overlapping results share an id")
	}
	waitRows(t, ctx, union, 2) // the task, from both arms
	if err := moveTask(ctx, db, task.ID, jazzapp.TaskStatusDoing); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); ; {
		rows := union.Rows()
		if len(rows) == 2 && rows[0].Status == jazzapp.TaskStatusDoing && rows[1].Status == jazzapp.TaskStatusDoing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the union's rows after the update: %+v", rows)
		}
		select {
		case <-union.Changes():
		case <-time.After(200 * time.Millisecond):
		}
	}
	if err := jazzapp.Tasks.Delete(ctx, db, task.ID); err != nil {
		t.Fatal(err)
	}
	waitRows(t, ctx, union, 0)

	again, err := addTask(ctx, db, show.ID, "Line check")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.WaitForSync(ctx, jazz.WriteGlobal); err != nil {
		t.Fatal(err)
	}
	history, err := jazzapp.Tasks.History().
		Where(jazz.NewColumn[jazz.HistoryRow[jazzapp.Task], string]("id").Eq(again.ID)).
		Live(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	<-history.Ready()
	waitRows(t, ctx, history, 1)
	if err := moveTask(ctx, db, again.ID, jazzapp.TaskStatusDoing); err != nil {
		t.Fatal(err)
	}
	if err := db.WaitForSync(ctx, jazz.WriteGlobal); err != nil {
		t.Fatal(err)
	}
	versions := waitRows(t, ctx, history, 2) // both versions, one id
	if versions[0].Row.ID != versions[1].Row.ID {
		t.Fatalf("versions of two rows: %+v", versions)
	}
}
