package jazz

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"
)

// holdCall makes jazz_call's method wait, after running (or before, if
// !after), until release is closed; got has what it returned.
func holdCall(t *testing.T, method string, after bool) (entered, release chan struct{}, got chan string) {
	t.Helper()
	real := ffi.call
	entered, release, got = make(chan struct{}, 8), make(chan struct{}), make(chan string, 8)
	ffi.call = func(h uintptr, m, a *byte, e *uintptr) uintptr {
		if goString(uintptr(unsafe.Pointer(m))) != method {
			return real(h, m, a, e)
		}
		var r uintptr
		if after {
			r = real(h, m, a, e)
		}
		entered <- struct{}{}
		<-release
		if !after {
			r = real(h, m, a, e)
		}
		text := ""
		if r != 0 {
			text = goString(r)
		}
		got <- text
		return r
	}
	t.Cleanup(func() { ffi.call = real })
	return
}

// cancelOnEntry is a ctx cancelled once the held call is entered.
func cancelOnEntry(entered chan struct{}) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-entered; cancel() }()
	return ctx
}

// ended says, within a few seconds, that what probe uses was ended:
// probe touches it without ending it, failing "not open" once it has.
func ended(t *testing.T, what string, probe func() error) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := probe()
		if err != nil && strings.Contains(err.Error(), "not open") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s is still open (%v)", what, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// txEnded and uploadEnded touch a transaction or upload without ending it.
func txEnded(t *testing.T, db *DB, id string) {
	t.Helper()
	ended(t, "transaction "+id, func() error {
		row := map[string]any{"title": "probe", "done": false}
		return db.call(context.Background(), "insert", map[string]any{"table": "todos", "row": row, "tx": id}, nil)
	})
}

func uploadEnded(t *testing.T, db *DB, id string) {
	t.Helper()
	ended(t, "upload "+id, func() error {
		return db.call(context.Background(), "uploadPush", map[string]any{"upload": id, "bytes": ""}, nil)
	})
}

func TestACancelledBeginRollsBack(t *testing.T) {
	db, _ := openOffline(t)
	defer db.Close()
	entered, release, got := holdCall(t, "txBegin", true)
	ctx := cancelOnEntry(entered)
	ran := false
	err := db.Tx(ctx, func(*Tx) error { ran = true; return nil })
	if !errors.Is(err, context.Canceled) || ran {
		t.Fatalf("Tx: %v, fn ran: %v", err, ran)
	}
	close(release)
	var begun struct{ Tx string }
	json.Unmarshal([]byte(<-got), &begun)
	txEnded(t, db, begun.Tx)
}

func TestGoexitInTxRollsBack(t *testing.T) {
	db, _ := openOffline(t)
	defer db.Close()
	ids := make(chan string, 1)
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		db.Tx(context.Background(), func(tx *Tx) error { ids <- tx.id; runtime.Goexit(); return nil })
	}()
	<-exited
	txEnded(t, db, <-ids)
}

func openUploads(t *testing.T) *DB {
	t.Helper()
	cfg := offlineConfig(t)
	cfg.App = filesApp
	key, err := GenerateDeviceKey()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Auth = DeviceKey(key)
	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestACancelledUploadBeginAborts(t *testing.T) {
	db := openUploads(t)
	defer db.Close()
	entered, release, got := holdCall(t, "uploadBegin", true)
	ctx := cancelOnEntry(entered)
	_, err := upload(ctx, db, uploadBegin(), strings.NewReader("x"), nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("upload: %v", err)
	}
	close(release)
	var begun struct{ Upload string }
	json.Unmarshal([]byte(<-got), &begun)
	if begun.Upload == "" {
		t.Fatal("uploadBegin failed")
	}
	uploadEnded(t, db, begun.Upload)
}

type cancelAtEnd struct{ cancel func() }

func (r cancelAtEnd) Read([]byte) (int, error) { r.cancel(); return 0, io.EOF }

func TestAnUploadCancelledBeforeFinishAborts(t *testing.T) {
	db := openUploads(t)
	defer db.Close()
	_, release, got := holdCall(t, "uploadBegin", true)
	close(release)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := upload(ctx, db, uploadBegin(), cancelAtEnd{cancel}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("upload: %v", err)
	}
	var begun struct{ Upload string }
	json.Unmarshal([]byte(<-got), &begun)
	uploadEnded(t, db, begun.Upload)
}

func TestACancelledOpenFreesItsStoreAndProvider(t *testing.T) {
	cfg := offlineConfig(t)
	asked, ended := make(chan struct{}, 8), make(chan struct{}, 8)
	cfg.Auth = Token(func(c context.Context, _ TokenReason) (string, error) {
		asked <- struct{}{}
		<-c.Done()
		ended <- struct{}{}
		return "", c.Err()
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := returnsWithin(t, "a cancelled Open", closeBudget, func() error {
		_, err := Open(ctx, cfg)
		return err
	}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Open: %v", err)
	}
	select {
	case <-asked:
		select {
		case <-ended:
		default:
			t.Fatal("the provider was still running when Open returned")
		}
	default:
	}
	// Nothing holds the store: deleting it works.
	key, err := GenerateDeviceKey()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Auth = DeviceKey(key)
	again, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := again.DeleteClientStorage(context.Background()); err != nil {
		t.Fatalf("deleting the store after a cancelled Open: %v", err)
	}
}

func TestACancelledSignOutStillFrees(t *testing.T) {
	db, _ := openOffline(t)
	entered, release, got := holdCall(t, "signOut", true)
	ctx := cancelOnEntry(entered)
	if err := db.SignOut(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("SignOut: %v", err)
	}
	close(release)
	<-got
	deadline := time.Now().Add(5 * time.Second)
	for {
		db.mu.RLock()
		freed := db.handle == 0
		db.mu.RUnlock()
		if freed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the signed-out DB was never freed")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestACancelledSessionDBReturnsAtOnce(t *testing.T) {
	session, _ := openOfflineSession(t)
	defer session.Close()
	real := ffi.sessionJazz
	release, returned := make(chan struct{}), make(chan struct{})
	ffi.sessionJazz = func(s uintptr, e *uintptr) uintptr {
		defer close(returned)
		<-release
		return real(s, e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := returnsWithin(t, "a cancelled Session.DB", time.Second, func() error {
		_, err := session.DB(ctx)
		return err
	}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Session.DB: %v", err)
	}
	close(release)
	<-returned
	ffi.sessionJazz = real
}

func uploadBegin() map[string]any {
	return map[string]any{"table": "files", "column": "content", "kind": "insert", "row": map[string]any{"name": "a"}}
}
