package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	jazz "github.com/garden-co/jazz/packages/jazz-go/v2"

	"github.com/garden-co/jazz/examples/stage-plan/apps/go-cli/jazzapp"
)

// A stock TypeScript client (jazz-tools in Node) signs in through the Go
// backend's /jazz with a bearer, writes a show at tier global and reads it
// back; the backend then reads it as that user. JAZZ_TS_CLIENT names a
// directory with ts-client/client.ts's dependencies installed (jazz-tools,
// tsx); without it, the test is skipped.
func TestATypeScriptClientSyncsThroughTheGoEndpoint(t *testing.T) {
	dir := os.Getenv("JAZZ_TS_CLIENT")
	if dir == "" {
		t.Skip("JAZZ_TS_CLIENT is unset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	srv, app := goBackend(t)
	cmd := exec.CommandContext(ctx, filepath.Join(dir, "node_modules/.bin/tsx"), filepath.Join(dir, "client.ts"),
		app.URL+"/jazz", "token-carol")
	cmd.Dir = dir
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("ts client: %v\n%s", err, out)
	}
	var got struct {
		Status, Subject, Account, ShowID string
		Read                             int
	}
	if err := json.Unmarshal(out[len(out)-lastLine(out):], &got); err != nil {
		t.Fatalf("ts client output %q: %v", out, err)
	}
	if got.Status != "ready" || got.Subject != "carol" || got.Read != 1 {
		t.Fatalf("ts client: %+v", got)
	}
	// Carol's show reached the server: Carol, signed in from Go, reads it.
	carol, err := openUser(ctx, srv, app, "token-carol")
	if err != nil {
		t.Fatal(err)
	}
	defer carol.Close()
	shows, err := jazzapp.Shows.Query().Where(jazzapp.Shows.ID.Eq(got.ShowID)).Tier(jazz.ReadRemote).All(ctx, carol)
	if err != nil || len(shows) != 1 || shows[0].Name != "TS gig" {
		t.Fatalf("carol's show from Go: %+v %v", shows, err)
	}
}

func lastLine(out []byte) int {
	n := len(out)
	for n > 0 && out[n-1] == '\n' {
		n--
	}
	i := n
	for i > 0 && out[i-1] != '\n' {
		i--
	}
	return len(out) - i
}
