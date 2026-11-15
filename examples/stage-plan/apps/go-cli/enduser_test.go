package main

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	jazz "github.com/garden-co/jazz/packages/jazz-go"

	"github.com/garden-co/jazz/examples/stage-plan/apps/go-cli/jazzapp"
)

func TestEndUsersSignInAndSyncThroughTheApp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	srv := testServer(t)
	backend := openTest(t, srv, "stage-plan")
	bundle, err := backend.Bundle(ctx)
	if err != nil || bundle == "" {
		t.Fatalf("bundle: %q %v", bundle, err)
	}
	app := startEndpoint(t, srv, bundle)

	// Carol: a device account, created on first open.
	keyFile := filepath.Join(t.TempDir(), "device-key")
	carol, err := jazzapp.Open(ctx, jazz.Config{
		AppID: srv.appID, ServerURL: app.url, Storage: jazz.StorageMemory,
		Auth: jazz.DeviceAccount(jazz.FileStore(keyFile)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer carol.Close()
	state, err := carol.Session(ctx)
	if err != nil || state == nil || state.Status != jazz.SignedIn || state.User.Kind != "device" {
		t.Fatalf("carol's session: %+v %v", state, err)
	}
	if _, err := jazzapp.Shows.Insert(ctx, carol, jazzapp.NewShow{
		Name: "Carol's gig", Venue: "Hall", Date: "2026-12-01", Doors: "20:00", ChiefAccount: state.User.Account,
	}, jazz.WaitFor(jazz.WriteGlobal)); err != nil {
		t.Fatal(err)
	}
	if s, err := carol.SyncStatus(ctx); err != nil || len(s.Link) == 0 {
		t.Fatalf("sync status: %+v %v", s, err)
	}

	// Alice: a bearer from "her auth provider".
	var mu sync.Mutex
	var asked []jazz.TokenReason
	alice, err := jazzapp.Open(ctx, jazz.Config{
		AppID: srv.appID, ServerURL: app.url, Storage: jazz.StorageMemory,
		Auth: jazz.Token(func(ctx context.Context, reason jazz.TokenReason) (string, error) {
			mu.Lock()
			asked = append(asked, reason)
			mu.Unlock()
			return "token-alice", nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer alice.Close()
	state, err = alice.Session(ctx)
	if err != nil || state == nil || state.Status != jazz.SignedIn || state.User.Subject != "alice" {
		t.Fatalf("alice's session: %+v %v", state, err)
	}
	states, err := alice.OnSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first := <-states; first.Status != jazz.SignedIn {
		t.Fatalf("first state: %+v", first)
	}
	if _, err := jazzapp.Shows.Insert(ctx, alice, jazzapp.NewShow{
		Name: "Alice's gig", Venue: "Club", Date: "2026-12-02", Doors: "21:00", ChiefAccount: state.User.Account,
	}, jazz.WaitFor(jazz.WriteGlobal)); err != nil {
		t.Fatal(err)
	}
	if err := alice.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	refreshed := len(asked) > 0 && asked[0] == jazz.TokenConnect
	sawRefresh := false
	for _, r := range asked {
		sawRefresh = sawRefresh || r == jazz.TokenRefresh
	}
	mu.Unlock()
	if !refreshed || !sawRefresh {
		t.Fatalf("asked for tokens: %v", asked)
	}
	if err := alice.SignOut(ctx); err != nil {
		t.Fatal(err)
	}
	last := jazz.SessionState{}
	for s := range states {
		last = s
		if s.Status == jazz.SignedOut {
			break
		}
	}
	if last.Status != jazz.SignedOut {
		t.Fatalf("last state: %+v", last)
	}
	if _, err := jazzapp.Shows.Query().All(ctx, alice); err == nil {
		t.Fatal("a signed-out app still reads")
	}
}
