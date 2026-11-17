package main

import (
	"context"
	"errors"
	"testing"
	"time"

	jazz "github.com/garden-co/jazz/packages/jazz-go/v2"

	"github.com/garden-co/jazz/examples/stage-plan/apps/go-cli/jazzapp"
)

// interleaved runs two transactions on one task from two clients of the
// same user: the first reads the task and writes, then waits while the
// second reads, writes and commits; then the first commits. Both commit
// at the global tier, so the server decides.
func interleaved(t *testing.T, opts ...jazz.TxOption) (first, second error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srv := testServer(t)
	one := openTest(t, srv, "alice")
	two := openTest(t, srv, "alice")
	show, err := ensureShow(ctx, one, "Tx Night")
	if err != nil {
		t.Fatal(err)
	}
	task, err := addTask(ctx, one, show.ID, "Load-in")
	if err != nil {
		t.Fatal(err)
	}
	if err := one.WaitForSync(ctx, jazz.WriteGlobal); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		seen, err := jazzapp.Tasks.Query().Where(jazzapp.Tasks.ID.Eq(task.ID)).Tier(jazz.ReadRemote).One(ctx, two)
		if err == nil && seen != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the second client never saw the task: %v", err)
		}
	}
	change := func(tx *jazz.Tx, title string) error {
		if _, err := jazzapp.Tasks.Query().Where(jazzapp.Tasks.ID.Eq(task.ID)).One(ctx, tx); err != nil {
			return err
		}
		return jazzapp.Tasks.Update(ctx, tx, task.ID, jazzapp.TaskPatch{Title: jazz.Set(title)})
	}
	opts = append(opts, jazz.CommitAt(jazz.WriteGlobal))
	wrote, proceed, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- one.Tx(ctx, func(tx *jazz.Tx) error {
			err := change(tx, "From one")
			close(wrote)
			<-proceed
			return err
		}, opts...)
	}()
	<-wrote
	second = two.Tx(ctx, func(tx *jazz.Tx) error { return change(tx, "From two") }, opts...)
	close(proceed)
	return <-done, second
}

// Anselm's decision: a plain Tx is mergeable, as TS's db.transaction.
// Concurrent ones on the same row both commit; serializable ones don't.
func TestTxIsMergeableByDefault(t *testing.T) {
	first, second := interleaved(t)
	if first != nil || second != nil {
		t.Fatalf("plain Tx (mergeable by default): first %v, second %v; both should commit", first, second)
	}
}

func TestSerializableTxConflicts(t *testing.T) {
	first, second := interleaved(t, jazz.Serializable())
	if !errors.Is(first, jazz.ErrConflict) && !errors.Is(second, jazz.ErrConflict) {
		t.Fatalf("serializable Tx: first %v, second %v; one should fail with ErrConflict", first, second)
	}
	if first != nil && second != nil {
		t.Fatalf("serializable Tx: both failed: first %v, second %v", first, second)
	}
}
