package main

import (
	"context"
	"errors"
	"testing"
	"time"

	jazz "github.com/garden-co/jazz/packages/jazz-go/v2"

	"github.com/garden-co/jazz/examples/stage-plan/apps/go-cli/jazzapp"
)

// skipWithoutLifecycle skips while libjazz_ffi has no lifecycle calls.
func skipWithoutLifecycle(t *testing.T, err error) {
	t.Helper()
	if errors.Is(err, jazz.ErrNotSupported) || errors.Is(err, jazz.ErrRefused) {
		t.Skipf("this libjazz_ffi has no lifecycle calls yet: %v", err)
	}
}

// TS's lifecycle: mergeable transactions, disconnect and reconnect,
// refused writes nothing waited on, and deleting the local store.
func TestLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	srv := testServer(t)
	alice := openTest(t, srv, "alice")
	bob := openTest(t, srv, "bob")
	show, err := ensureShow(ctx, alice, "Lifecycle Night")
	if err != nil {
		t.Fatal(err)
	}

	// A mergeable transaction commits; it never conflicts.
	err = alice.Tx(ctx, func(tx *jazz.Tx) error {
		_, err := jazzapp.Tasks.Insert(ctx, tx, jazzapp.NewTask{
			ShowID: show.ID, Title: "Merged", Status: jazzapp.TaskStatusTodo, Rank: 1,
		})
		return err
	}, jazz.Mergeable())
	if err != nil {
		skipWithoutLifecycle(t, err)
		t.Fatalf("a mergeable transaction: %v", err)
	}

	// Writes while disconnected reach the server after Reconnect.
	if err := alice.Disconnect(ctx); err != nil {
		t.Fatal(err)
	}
	offline, err := addTask(ctx, alice, show.ID, "Written offline")
	if err != nil {
		t.Fatal(err)
	}
	if err := alice.Reconnect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := alice.WaitForSync(ctx, jazz.WriteGlobal); err != nil {
		t.Fatal(err)
	}
	again := openTest(t, srv, "alice")
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		task, err := jazzapp.Tasks.Query().Where(jazzapp.Tasks.ID.Eq(offline.ID)).Tier(jazz.ReadRemote).One(ctx, again)
		if err == nil && task != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the offline write never reached the server: %v", err)
		}
	}

	// Bob's refused write, which nothing waits on, is reported.
	refused, err := bob.OnMutationError(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := jazzapp.Tasks.Insert(ctx, bob, jazzapp.NewTask{
		ShowID: show.ID, Title: "Sneaky", Status: jazzapp.TaskStatusTodo, Rank: 9,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-refused:
		if !m.Denied || m.Transaction == "" || m.Message == "" {
			t.Fatalf("mutation error %+v", m)
		}
	case <-ctx.Done():
		t.Fatal("bob's refused write was never reported")
	}

	// Deleting a local app's store: the next open starts empty.
	dir := t.TempDir()
	appID := randomUUID(t)
	local, err := jazzapp.Open(ctx, jazz.Config{AppID: appID, Storage: jazz.StoragePersistent, DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := jazzapp.Shows.Insert(ctx, local, jazzapp.NewShow{
		Name: "Local", Venue: "Home", Date: "2026-12-31", Doors: "19:00", ChiefAccount: randomUUID(t),
	}); err != nil {
		t.Fatal(err)
	}
	if err := local.DeleteClientStorage(ctx); err != nil {
		t.Fatal(err)
	}
	reopened, err := jazzapp.Open(ctx, jazz.Config{AppID: appID, Storage: jazz.StoragePersistent, DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	shows, err := jazzapp.Shows.Query().All(ctx, reopened)
	if err != nil || len(shows) != 0 {
		t.Fatalf("after DeleteClientStorage: %v, %v", shows, err)
	}
}
