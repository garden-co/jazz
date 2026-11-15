package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	jazz "github.com/garden-co/jazz/packages/jazz-go/v2"

	"github.com/garden-co/jazz/examples/stage-plan/apps/go-cli/jazzapp"
)

// The checks a Go backend's /jazz endpoint must pass (the .65 rc gate):
// bad credentials are refused, users see and write only what the
// permissions allow and never another user's rows, and malformed or
// oversized frames don't take the endpoint down.

// goBackend serves StagePlan's /jazz from a Go backend. Its authenticate
// stands in for a JWT check: "token-<user>" is valid, "expired-<user>"
// names a user whose credential has expired, anything else (a wrong
// issuer, a bad signature, none) names no one.
func goBackend(t *testing.T) (server, *httptest.Server) {
	t.Helper()
	srv := testServer(t)
	backend := openTest(t, srv, "stage-plan")
	endpoint, err := jazz.NewEndpoint(backend, func(r *http.Request) (*jazz.EndpointUser, error) {
		bearer, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if user, ok := strings.CutPrefix(bearer, "token-"); ok && user != "" {
			return &jazz.EndpointUser{UserID: user, ExpiresAt: time.Now().Add(time.Hour)}, nil
		}
		if user, ok := strings.CutPrefix(bearer, "expired-"); ok && user != "" {
			return &jazz.EndpointUser{UserID: user, ExpiresAt: time.Now().Add(-time.Minute)}, nil
		}
		return nil, nil
	}, jazz.EndpointOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(endpoint.Close)
	mux := http.NewServeMux()
	mux.Handle("/jazz/", endpoint)
	app := httptest.NewServer(mux)
	t.Cleanup(app.Close)
	return srv, app
}

func openUser(ctx context.Context, srv server, app *httptest.Server, token string) (*jazz.DB, error) {
	return jazzapp.Open(ctx, jazz.Config{
		AppID: srv.appID, ServerURL: app.URL + "/jazz", Storage: jazz.StorageMemory,
		Auth: jazz.Token(func(context.Context, jazz.TokenReason) (string, error) { return token, nil }),
	})
}

func TestEndpointRefusesBadCredentials(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	srv, app := goBackend(t)
	for _, token := range []string{"", "wrong-issuer-alice", "expired-alice"} {
		db, err := openUser(ctx, srv, app, token)
		if err != nil {
			continue // refused at open
		}
		state, err := db.Session(ctx)
		db.Close()
		if err == nil && state != nil && state.Status == jazz.SignedIn {
			t.Errorf("token %q signed in as %+v", token, state.User)
		}
	}
	// And the bearer sign-in route refuses them too.
	for _, token := range []string{"", "wrong-issuer-alice", "expired-alice"} {
		req, _ := http.NewRequest("POST", fmt.Sprintf("%s/jazz/apps/%s/accounts/backend-sign-in", app.URL, srv.appID), nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Errorf("backend-sign-in with %q: 200", token)
		}
	}
}

func TestEndpointUsersSeeAndWriteOnlyWhatPermissionsAllow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	srv, app := goBackend(t)
	alice, err := openUser(ctx, srv, app, "token-alice")
	if err != nil {
		t.Fatal(err)
	}
	defer alice.Close()
	bob, err := openUser(ctx, srv, app, "token-bob")
	if err != nil {
		t.Fatal(err)
	}
	defer bob.Close()
	aliceState, _ := alice.Session(ctx)
	bobState, _ := bob.Session(ctx)
	if aliceState == nil || bobState == nil || aliceState.User.Account == bobState.User.Account {
		t.Fatalf("sessions: %+v %+v", aliceState, bobState)
	}

	// Alice may make a show of her own and a task in it.
	show, err := jazzapp.Shows.Insert(ctx, alice, jazzapp.NewShow{
		Name: "Alice's private gig", Venue: "Club", Date: "2026-12-02", Doors: "21:00", ChiefAccount: aliceState.User.Account,
	}, jazz.WaitFor(jazz.WriteGlobal))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := jazzapp.Tasks.Insert(ctx, alice, jazzapp.NewTask{
		ShowID: show.ID, Title: "Private task", Status: jazzapp.TaskStatusTodo, Rank: 1,
	}, jazz.WaitFor(jazz.WriteGlobal)); err != nil {
		t.Fatal(err)
	}
	// She can't make a show whose chief is Bob.
	if _, err := jazzapp.Shows.Insert(ctx, alice, jazzapp.NewShow{
		Name: "Forged", Venue: "X", Date: "2026-12-03", Doors: "20:00", ChiefAccount: bobState.User.Account,
	}, jazz.WaitFor(jazz.WriteGlobal)); !errors.Is(err, jazz.ErrPermission) {
		t.Fatalf("a show chiefed by bob: %v", err)
	}

	// Bob, neither chief nor crew of Alice's show, reads none of it and
	// can't add to it.
	shows, err := jazzapp.Shows.Query().Where(jazzapp.Shows.ID.Eq(show.ID)).Tier(jazz.ReadRemote).All(ctx, bob)
	if err != nil || len(shows) != 0 {
		t.Fatalf("bob reads alice's show: %+v %v", shows, err)
	}
	tasks, err := jazzapp.Tasks.Query().Where(jazzapp.Tasks.ShowID.Eq(show.ID)).Tier(jazz.ReadRemote).All(ctx, bob)
	if err != nil || len(tasks) != 0 {
		t.Fatalf("bob reads alice's tasks: %+v %v", tasks, err)
	}
	if _, err := jazzapp.Tasks.Insert(ctx, bob, jazzapp.NewTask{
		ShowID: show.ID, Title: "Sneaky", Status: jazzapp.TaskStatusTodo, Rank: 2,
	}, jazz.WaitFor(jazz.WriteGlobal)); !errors.Is(err, jazz.ErrPermission) {
		t.Fatalf("bob writes into alice's show: %v", err)
	}
	// Alice still reads her own.
	mine, err := jazzapp.Tasks.Query().Where(jazzapp.Tasks.ShowID.Eq(show.ID)).Tier(jazz.ReadRemote).All(ctx, alice)
	if err != nil || len(mine) != 1 {
		t.Fatalf("alice's tasks: %+v %v", mine, err)
	}
}

// rawSocket opens the endpoint's sync socket by hand, as a hostile client.
func rawSocket(t *testing.T, app *httptest.Server, appID string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.Dial("tcp", strings.TrimPrefix(app.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(conn, "GET /jazz/apps/%s/ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n", appID)
	r := bufio.NewReader(conn)
	resp, err := http.ReadResponse(r, nil)
	if err != nil || resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade: %v %v", resp, err)
	}
	return conn, r
}

// closeCodeFrom reads frames until the server's close, and returns its code.
func closeCodeFrom(t *testing.T, conn net.Conn, r *bufio.Reader) uint16 {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for {
		var head [2]byte
		if _, err := r.Read(head[:1]); err != nil {
			return 0
		}
		if _, err := r.Read(head[1:]); err != nil {
			return 0
		}
		n := int(head[1] & 0x7f)
		payload := make([]byte, n)
		for got := 0; got < n; {
			m, err := r.Read(payload[got:])
			if err != nil {
				return 0
			}
			got += m
		}
		if head[0]&0x0f == 0x8 && n >= 2 {
			return binary.BigEndian.Uint16(payload)
		}
	}
}

func TestEndpointSurvivesMalformedAndOversizedFrames(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	srv, app := goBackend(t)
	for _, c := range []struct {
		name  string
		frame []byte
		code  uint16
	}{
		{"unmasked", []byte{0x82, 0x02, 'h', 'i'}, 1002},
		{"reserved bits", []byte{0xc2, 0x80, 1, 2, 3, 4}, 1002},
		{"oversized", []byte{0x82, 0x80 | 127, 0, 0, 0, 1, 0, 0, 0, 0}, 1009},
		{"text", []byte{0x81, 0x80, 1, 2, 3, 4}, 1003},
	} {
		conn, r := rawSocket(t, app, srv.appID)
		conn.Write(c.frame)
		if got := closeCodeFrom(t, conn, r); got != c.code {
			t.Errorf("%s: closed with %d, want %d", c.name, got, c.code)
		}
		conn.Close()
	}
	// The endpoint still serves a well-behaved user.
	alice, err := openUser(ctx, srv, app, "token-alice")
	if err != nil {
		t.Fatal(err)
	}
	defer alice.Close()
	if state, err := alice.Session(ctx); err != nil || state == nil || state.Status != jazz.SignedIn {
		t.Fatalf("after the bad frames: %+v %v", state, err)
	}
}
