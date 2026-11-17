package main

// The Go snippets of the Backend Client docs page, run by
// TestBackendClientSnippets against StagePlan's TS main backend.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	jazz "github.com/garden-co/jazz/packages/jazz-go/v2"

	"github.com/garden-co/jazz/examples/stage-plan/apps/go-cli/jazzapp"
)

// #region backend-client-connect-go
// connectBackendClient opens a Jazz session against the main backend's
// /jazz as a backend client. credential returns a fresh OIDC token of
// this machine's workload: Jazz asks for every request and socket.
func connectBackendClient(ctx context.Context, appID, serverURL string, credential jazz.TokenProvider) (*jazz.DB, error) {
	permissions, err := os.ReadFile("jazz.permissions.json")
	if err != nil {
		return nil, err
	}
	return jazzapp.Open(ctx, jazz.Config{
		AppID:       appID,
		ServerURL:   serverURL, // e.g. "https://my-app.com/jazz"
		Storage:     jazz.StorageMemory,
		Permissions: string(permissions),
		Auth:        jazz.Credential(credential),
	})
}

// #endregion backend-client-connect-go

// #region backend-client-trusted-go
// requireTrusted checks that the main backend admitted the client as
// trusted (acting as the system, with ForUser), not scoped to one user.
func requireTrusted(ctx context.Context, client *jazz.DB) error {
	kind, err := client.BackendClient(ctx)
	if err != nil {
		return err
	}
	if kind != jazz.BackendTrusted {
		return fmt.Errorf("this job needs a trusted backend client, not %q", kind)
	}
	return nil
}

// #endregion backend-client-trusted-go

// #region backend-client-for-user-go
// scheduleRehearsal adds a show for userID, as that user: their
// permissions apply, with claims as their claims.
func scheduleRehearsal(ctx context.Context, client *jazz.DB, userID string) error {
	db, err := client.ForUser(ctx, userID, map[string]any{"org": "acme"})
	if errors.Is(err, jazz.ErrNoAccount) {
		return nil // They haven't signed in yet, so they have no account to act as.
	}
	if err != nil {
		return err
	}
	defer db.Close()
	account, err := db.Account(ctx)
	if err != nil {
		return err
	}
	_, err = jazzapp.Shows.Insert(ctx, db, jazzapp.NewShow{
		Name:         "Rehearsal",
		Venue:        "The Jazz Club",
		Date:         "2026-12-30",
		Doors:        "18:00",
		ChiefAccount: account,
	})
	return err
}

// #endregion backend-client-for-user-go

// #region backend-client-close-go
// closeBackendClient uploads the writes the client holds, for up to 10
// seconds, then closes it. With memory storage, writes that haven't
// arrived by then are lost.
func closeBackendClient(client *jazz.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := client.Shutdown(ctx)
	var unsynced *jazz.Error
	if errors.As(err, &unsynced) && errors.Is(err, jazz.ErrUnsynced) {
		log.Printf("closing lost %d writes", unsynced.Pending)
	}
	return err
}

// #endregion backend-client-close-go

func TestBackendClientSnippets(t *testing.T) {
	if os.Getenv("JAZZ_MAIN_BACKEND") != "ts" {
		t.Skip("needs the TS main backend (JAZZ_MAIN_BACKEND=ts)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	srv := testServer(t)
	v, _ := workloads.Load(srv.url)
	tokens := v.(workloadTokens)
	// Skips while libjazz_ffi has no backend-client mode.
	openBackendClient(t, srv, tokens.Trusted)

	client, err := connectBackendClient(ctx, srv.appID, srv.url, func(context.Context, jazz.TokenReason) (string, error) {
		return tokens.Trusted, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := requireTrusted(ctx, client); err != nil {
		t.Fatal(err)
	}
	if err := scheduleRehearsal(ctx, client, "bob"); err != nil {
		t.Fatalf("for a user with no account: %v", err)
	}
	bob := openTest(t, srv, "bob")
	bobAccount, err := bob.Account(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduleRehearsal(ctx, client, "bob"); err != nil {
		t.Fatal(err)
	}
	if err := closeBackendClient(client); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		shows, err := jazzapp.Shows.Query().Where(jazzapp.Shows.ChiefAccount.Eq(bobAccount)).Tier(jazz.ReadRemote).All(ctx, bob)
		if err == nil && len(shows) == 1 && shows[0].Name == "Rehearsal" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("bob's app never saw the rehearsal: %v, %v", shows, err)
		}
	}
}
