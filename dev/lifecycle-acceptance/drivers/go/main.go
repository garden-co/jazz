// The Go lifecycle driver: drivers/PROTOCOL.md on jazz-go's public API.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	jazz "github.com/garden-co/jazz/packages/jazz-go/v2"

	"example.com/lifecycle/jazzapp"
)

type request struct {
	ID     uint64 `json:"id"`
	Op     string `json:"op"`
	H      string `json:"h"`
	As     string `json:"as"`
	Dir    string `json:"dir"`
	URL    string `json:"url"`
	App    string `json:"app"`
	Key    string `json:"key"`
	Token  string `json:"token"`
	Start  string `json:"start"`
	Title  string `json:"title"`
	Owner  string `json:"owner"`
	Tier   string `json:"tier"`
	Target uint64 `json:"target"`
	Ms     *int64 `json:"ms"`
	Store  string `json:"storage"`
	Local  bool   `json:"local"`
}

type session struct {
	s  *jazz.Session
	mu *sync.Mutex
	t  *string
}

var (
	mu       sync.Mutex
	clients  = map[string]*jazz.DB{}
	sessions = map[string]session{}
	running  = map[uint64]context.CancelFunc{}
	output   sync.Mutex
)

var errUnsupported = errors.New("driver.unsupported")

type badRequest string

func (b badRequest) Error() string { return string(b) }

func kindOf(err error) string {
	var typed *jazz.Error
	var bad badRequest
	switch {
	case errors.Is(err, context.Canceled):
		return "driver.cancelled"
	case errors.Is(err, errUnsupported):
		return "driver.unsupported"
	case errors.As(err, &bad):
		return "driver.bad"
	case errors.As(err, &typed):
		return typed.Kind
	}
	return fmt.Sprintf("%T", err)
}

func answer(line map[string]any) {
	encoded, err := json.Marshal(line)
	if err != nil {
		fmt.Fprintln(os.Stderr, "encode:", err)
		return
	}
	output.Lock()
	defer output.Unlock()
	fmt.Fprintln(os.Stdout, string(encoded))
}

func perform(ctx context.Context, r request) (any, error) {
	mu.Lock()
	db, hasDB := clients[r.H]
	ses, hasSession := sessions[r.H]
	mu.Unlock()

	switch r.Op {
	case "account", "write", "read", "sync", "close", "delete", "disconnect", "reconnect", "signout", "hrefresh":
		if !hasDB {
			return nil, badRequest("no handle")
		}
	case "stoken", "ssnap", "sjazz", "screate", "sreopen", "srefresh", "ssignout", "sclose":
		if !hasSession {
			return nil, badRequest("no session")
		}
	}

	switch r.Op {
	case "genkey":
		return jazz.GenerateDeviceKey()
	case "open":
		cfg := jazz.Config{AppID: r.App, ServerURL: r.URL, DataDir: r.Dir, Storage: jazz.StoragePersistent}
		if r.Store == "memory" {
			cfg.Storage, cfg.DataDir = jazz.StorageMemory, ""
		}
		if r.Local {
			cfg.ServerURL = ""
		} else if r.Key != "" {
			cfg.Auth = jazz.DeviceKey(r.Key)
		} else {
			token := r.Token
			cfg.Auth = jazz.Token(func(context.Context, jazz.TokenReason) (string, error) { return token, nil })
		}
		opened, err := jazzapp.Open(ctx, cfg)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		clients[r.H] = opened
		mu.Unlock()
		return nil, nil
	case "account":
		return db.Account(ctx)
	case "write":
		var opts []jazz.WriteOption
		if r.Tier == "global" {
			opts = append(opts, jazz.WaitFor(jazz.WriteGlobal))
		}
		row, err := jazzapp.Notes.Insert(ctx, db, jazzapp.NewNote{Title: r.Title, Owner: r.Owner}, opts...)
		if err != nil {
			return nil, err
		}
		return row.ID, nil
	case "read":
		tier := jazz.ReadLocal
		if r.Tier == "remote" {
			tier = jazz.ReadRemote
		}
		rows, err := jazzapp.Notes.Query().Tier(tier).All(ctx, db)
		if err != nil {
			return nil, err
		}
		titles := []string{}
		for _, row := range rows {
			titles = append(titles, row.Title)
		}
		sort.Strings(titles)
		return titles, nil
	case "sync":
		return nil, db.WaitForSync(ctx, jazz.WriteGlobal)
	case "close":
		if r.Ms != nil {
			// Shutdown waits for queued writes until its context's deadline.
			deadline, cancel := context.WithTimeout(ctx, time.Duration(*r.Ms)*time.Millisecond)
			defer cancel()
			return nil, db.Shutdown(deadline)
		}
		return nil, db.Close()
	case "delete":
		return nil, db.DeleteClientStorage(ctx)
	case "disconnect":
		return nil, db.Disconnect(ctx)
	case "reconnect":
		return nil, db.Reconnect(ctx)
	case "signout":
		return nil, db.SignOut(ctx)
	case "hrefresh":
		return nil, db.Refresh(ctx)
	case "sopen":
		tokenMu, token := &sync.Mutex{}, new(string)
		*token = r.Token
		cfg := jazz.Config{App: jazzapp.App, AppID: r.App, ServerURL: r.URL, DataDir: r.Dir, Storage: jazz.StoragePersistent,
			Auth: jazz.Token(func(context.Context, jazz.TokenReason) (string, error) {
				tokenMu.Lock()
				defer tokenMu.Unlock()
				return *token, nil
			})}
		start := jazz.StartWithBackend
		switch r.Start {
		case "device":
			start = jazz.StartWithDevice
		case "signed-out":
			start = jazz.StartSignedOut
		}
		opened, err := jazz.OpenSession(ctx, cfg, jazz.SessionOptions{Start: start})
		if err != nil {
			return nil, err
		}
		mu.Lock()
		sessions[r.H] = session{opened, tokenMu, token}
		mu.Unlock()
		return nil, nil
	case "stoken":
		ses.mu.Lock()
		*ses.t = r.Token
		ses.mu.Unlock()
		return nil, nil
	case "ssnap":
		snap, err := ses.s.Snapshot(ctx)
		if err != nil {
			return nil, err
		}
		return map[string]any{"status": snap.Status, "clientEpoch": snap.ClientEpoch, "error": snap.Error}, nil
	case "sjazz":
		client, err := ses.s.DB(ctx)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		clients[r.As] = client
		mu.Unlock()
		return nil, nil
	case "screate":
		return nil, ses.s.CreateDeviceAccount(ctx)
	case "sreopen":
		return nil, ses.s.ReopenDeviceAccount(ctx)
	case "srefresh":
		return nil, ses.s.Refresh(ctx)
	case "ssignout":
		return nil, ses.s.SignOut(ctx)
	case "sclose":
		return nil, ses.s.Close()
	case "cancel":
		mu.Lock()
		cancel := running[r.Target]
		mu.Unlock()
		if cancel != nil {
			cancel()
		}
		return nil, nil
	}
	return nil, errUnsupported
}

func main() {
	answer(map[string]any{"ready": true, "pid": os.Getpid()})
	input := bufio.NewScanner(os.Stdin)
	input.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for input.Scan() {
		var r request
		if err := json.Unmarshal(input.Bytes(), &r); err != nil {
			fmt.Fprintln(os.Stderr, "bad request:", err)
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		mu.Lock()
		running[r.ID] = cancel
		mu.Unlock()
		go func() {
			defer cancel()
			value, err := perform(ctx, r)
			mu.Lock()
			delete(running, r.ID)
			mu.Unlock()
			if err != nil {
				answer(map[string]any{"id": r.ID, "ok": false, "kind": kindOf(err), "msg": err.Error()})
			} else {
				answer(map[string]any{"id": r.ID, "ok": true, "v": value})
			}
		}()
	}
}
