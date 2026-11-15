package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	jazz "github.com/garden-co/jazz/packages/jazz-go/v2"

	"github.com/garden-co/jazz/examples/stage-plan/apps/go-cli/jazzapp"
)

// A Go backend serves its app's /jazz endpoint itself, as TS's createJazz
// does: end users sign in through it with their bearer or a device
// account, and a route reads as the request's user.
func TestAGoBackendServesItsEndUsers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	srv := testServer(t)
	backend := openTest(t, srv, "stage-plan")
	endpoint, err := jazz.NewEndpoint(backend, func(r *http.Request) (*jazz.EndpointUser, error) {
		user, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer token-")
		if !ok {
			return jazz.AnyDeviceAccount, nil
		}
		return &jazz.EndpointUser{UserID: user, ExpiresAt: time.Now().Add(time.Hour)}, nil
	}, jazz.EndpointOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()
	mux := http.NewServeMux()
	mux.Handle("/jazz/", endpoint)
	mux.HandleFunc("/my-shows", func(w http.ResponseWriter, r *http.Request) {
		db, err := endpoint.ForUser(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		defer db.Close()
		shows, err := jazzapp.Shows.Query().Tier(jazz.ReadRemote).All(r.Context(), db)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, s := range shows {
			w.Write([]byte(s.Name + "\n"))
		}
	})
	app := httptest.NewServer(mux)
	defer app.Close()

	// Alice signs in with her bearer and writes a show.
	alice, err := jazzapp.Open(ctx, jazz.Config{
		AppID: srv.appID, ServerURL: app.URL + "/jazz", Storage: jazz.StorageMemory,
		Auth: jazz.Token(func(context.Context, jazz.TokenReason) (string, error) { return "token-alice", nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer alice.Close()
	state, err := alice.Session(ctx)
	if err != nil || state == nil || state.Status != jazz.SignedIn || state.User.Subject != "alice" {
		t.Fatalf("alice: %+v %v", state, err)
	}
	if _, err := jazzapp.Shows.Insert(ctx, alice, jazzapp.NewShow{
		Name: "Alice's gig", Venue: "Club", Date: "2026-12-02", Doors: "21:00", ChiefAccount: state.User.Account,
	}, jazz.WaitFor(jazz.WriteGlobal)); err != nil {
		t.Fatal(err)
	}

	// A route of the backend reads as Alice.
	req, _ := http.NewRequest("GET", app.URL+"/my-shows", nil)
	req.Header.Set("Authorization", "Bearer token-alice")
	var body strings.Builder
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body.Reset()
		buf := make([]byte, 4096)
		n, _ := resp.Body.Read(buf)
		resp.Body.Close()
		body.Write(buf[:n])
		if resp.StatusCode == 200 && strings.Contains(body.String(), "Alice's gig") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("my-shows: %d %q", resp.StatusCode, body.String())
		}
	}

	// Dan signs in as a device account.
	dan, err := jazzapp.Open(ctx, jazz.Config{
		AppID: srv.appID, ServerURL: app.URL + "/jazz", Storage: jazz.StorageMemory,
		Auth: jazz.DeviceAccount(jazz.FileStore(t.TempDir() + "/key")),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer dan.Close()
	if s, err := dan.Session(ctx); err != nil || s == nil || s.Status != jazz.SignedIn || s.User.Kind != "device" {
		t.Fatalf("dan: %+v %v", s, err)
	}
}

// A token provider that fails (a network blip at the auth provider)
// doesn't sign the user out; one that answers "" does.
func TestATokenProviderErrorKeepsTheSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	srv, app := goBackend(t)
	var mu sync.Mutex
	answer, fail := "token-alice", false
	alice, err := jazzapp.Open(ctx, jazz.Config{
		AppID: srv.appID, ServerURL: app.URL + "/jazz", Storage: jazz.StorageMemory,
		Auth: jazz.Token(func(context.Context, jazz.TokenReason) (string, error) {
			mu.Lock()
			defer mu.Unlock()
			if fail {
				return "", errors.New("auth provider unreachable")
			}
			return answer, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer alice.Close()
	mu.Lock()
	fail = true
	mu.Unlock()
	alice.Refresh(ctx) // may report the failure; the session must stay
	if s, err := alice.Session(ctx); err != nil || s == nil || s.Status != jazz.SignedIn {
		t.Fatalf("after a failed fetch: %+v %v", s, err)
	}
}
