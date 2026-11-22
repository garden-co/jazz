package jazz

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// stallingBackend is a backend client whose provider, once stall is set,
// runs inside and then waits for its ctx or release.
func stallingBackend(t *testing.T, inside func(context.Context, *DB)) (db *DB, asked <-chan context.Context, stall func()) {
	t.Helper()
	var self atomic.Pointer[DB]
	var stalling atomic.Bool
	var once sync.Once
	ask := make(chan context.Context, 1)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	db = openBackendOffline(t, func(ctx context.Context, _ TokenReason) (string, error) {
		if !stalling.Load() {
			return "machine", nil
		}
		first := false
		once.Do(func() { first = true })
		if first {
			ask <- ctx
			if inside != nil {
				inside(ctx, self.Load())
			}
		}
		select {
		case <-ctx.Done():
		case <-release:
		}
		return "", context.Canceled
	})
	self.Store(db)
	return db, ask, func() { stalling.Store(true) }
}

// startForUser starts a ForUser, which asks the provider for a token.
func startForUser(t *testing.T, db *DB, asked <-chan context.Context) (context.Context, <-chan error) {
	t.Helper()
	result := make(chan error, 1)
	go func() {
		_, err := db.ForUser(context.Background(), "alice", nil)
		result <- err
	}()
	select {
	case ctx := <-asked:
		return ctx, result
	case <-time.After(10 * time.Second):
		t.Fatal("the provider was never asked")
		return nil, nil
	}
}

func freed(db *DB) bool {
	_, ok := providers.Load(db.provider)
	return !ok
}

func TestDBCloseFromItsTokenProvider(t *testing.T) {
	t.Parallel()
	closed := make(chan error, 1)
	db, asked, stall := stallingBackend(t, func(_ context.Context, db *DB) { closed <- db.Close() })
	stall()
	_, pending := startForUser(t, db, asked)
	_ = returnsWithin(t, "Close in the provider", closeBudget, func() error { return <-closed })
	if err := returnsWithin(t, "the ForUser it served", closeBudget, func() error { return <-pending }); !errors.Is(err, ErrClosed) {
		t.Fatalf("the ForUser it served: %v, want ErrClosed", err)
	}
	for deadline := time.Now().Add(closeBudget); !freed(db); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the handle was never freed after the provider returned")
		}
	}
}

// A provider closing while another goroutine's Close waits for it.
func TestCloseFromTheProviderWhileClosing(t *testing.T) {
	t.Parallel()
	inner := make(chan error, 1)
	db, asked, stall := stallingBackend(t, func(ctx context.Context, db *DB) {
		<-ctx.Done() // the other Close cancelled it
		inner <- db.Close()
	})
	stall()
	startForUser(t, db, asked)
	_ = returnsWithin(t, "Close", closeBudget, db.Close)
	_ = returnsWithin(t, "Close in the provider", closeBudget, func() error { return <-inner })
	if !freed(db) {
		t.Fatal("Close returned before freeing")
	}
}

// A second Close returns once the app is closed and freed, like the first.
func TestClosingAgainWaitsForTheFirstClose(t *testing.T) {
	t.Parallel()
	db, _ := openOffline(t)
	pendingSync(t, context.Background(), db)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go db.Shutdown(ctx)
	time.Sleep(200 * time.Millisecond)
	start := time.Now()
	_ = returnsWithin(t, "Close during Shutdown", closeBudget, db.Close)
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.handle != 0 || time.Since(start) < time.Second {
		t.Fatal("Close returned before the Shutdown it joined ended")
	}
}

// Draining may need a token: the provider keeps its ctx until the deadline.
func TestShutdownCancelsTheProviderAtItsDeadline(t *testing.T) {
	t.Parallel()
	db, asked, stall := stallingBackend(t, nil)
	row := map[string]any{"title": "to drain", "done": false}
	if _, err := Insert[map[string]any, map[string]any](context.Background(), db, "todos", row); err != nil {
		t.Fatal(err)
	}
	stall()
	fetch, _ := startForUser(t, db, asked)
	const drain = 1500 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), drain)
	defer cancel()
	start := time.Now()
	go db.Shutdown(ctx)
	select {
	case <-fetch.Done():
		if after := time.Since(start); after < drain-200*time.Millisecond {
			t.Fatalf("the provider was cancelled %v into a %v drain", after, drain)
		}
	case <-time.After(closeBudget):
		t.Fatal("the provider wasn't cancelled at Shutdown's deadline")
	}
}

func TestShutdownReportsUnsyncedWrites(t *testing.T) {
	t.Parallel()
	db := openBackendOffline(t, func(context.Context, TokenReason) (string, error) { return "machine", nil })
	waited := pendingSync(t, context.Background(), db)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := returnsWithin(t, "Shutdown", closeBudget, func() error { return db.Shutdown(ctx) })
	var e *Error
	if !errors.Is(err, ErrUnsynced) || !errors.As(err, &e) || e.Pending != 1 {
		t.Fatalf("Shutdown with a write left: %#v, want ErrUnsynced with 1 pending", err)
	}
	endsClosed(t, waited)
}

func TestCallsRacingClose(t *testing.T) {
	t.Parallel()
	for round := 0; round < 20; round++ {
		db, _ := openOffline(t)
		ctx, cancel := context.WithCancel(context.Background())
		start := make(chan struct{})
		var wg sync.WaitGroup
		failed := make(chan error, 64)
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for n := 0; n < 1000; n++ {
					var err error
					if i%4 == 0 {
						var states <-chan SyncStatus
						if states, err = db.OnSyncStatus(ctx); err == nil {
							go func() {
								for range states {
								}
							}()
						}
					} else {
						_, err = db.Account(ctx)
					}
					if errors.Is(err, ErrClosed) {
						return
					}
					if err != nil {
						failed <- err
						return
					}
				}
			}()
		}
		close(start)
		time.Sleep(time.Duration(round%5) * time.Millisecond)
		var closes sync.WaitGroup
		for i := 0; i < 3; i++ {
			closes.Add(1)
			go func() {
				defer closes.Done()
				_ = db.Close()
			}()
		}
		_ = returnsWithin(t, "racing closes", closeBudget, func() error { closes.Wait(); return nil })
		_ = returnsWithin(t, "racing calls", closeBudget, func() error { wg.Wait(); return nil })
		cancel()
		select {
		case err := <-failed:
			t.Fatalf("a call racing Close: %v, want ErrClosed", err)
		default:
		}
		db.streamsMu.Lock()
		left := len(db.streams)
		db.streamsMu.Unlock()
		if left != 0 {
			t.Fatalf("%d streams left after Close", left)
		}
	}
}

// Cancelled by closing, a provider's "" is a failed fetch: it doesn't sign
// the user out.
func TestAProviderCancelledByClosingDoesntSignOut(t *testing.T) {
	if err := loadFFI(); err != nil {
		t.Skipf("no libjazz_ffi: %v", err)
	}
	id := nextProvider.Add(1)
	asked := make(chan struct{})
	providers.Store(id, TokenProvider(func(ctx context.Context, _ TokenReason) (string, error) {
		close(asked)
		<-ctx.Done()
		return "", nil
	}))
	defer releaseProvider(id)
	reason := cstr("renew")
	status := make(chan int32, 1)
	ask := func() {
		var token uintptr
		status <- onToken(uintptr(id), uintptr(unsafe.Pointer(reason)), uintptr(unsafe.Pointer(&token)))
	}
	go ask()
	<-asked
	cancelFetches(id)
	if got := <-status; got != 1 {
		t.Fatalf("a cancelled fetch: status %d, want 1", got)
	}
	// And it isn't asked again (asked would close twice).
	go ask()
	if got := <-status; got != 1 {
		t.Fatalf("a fetch while closing: status %d, want 1", got)
	}
}

// Freeing under a call is a use after free, which racing rarely shows: a
// call in flight holds the free up.
func TestCloseFreesAfterTheLastCallInFlight(t *testing.T) {
	t.Parallel()
	db, _ := openOffline(t)
	_, release, err := db.acquire(nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- db.Close() }()
	select {
	case <-done:
		t.Fatal("Close freed the handle with a call in flight")
	case <-time.After(500 * time.Millisecond):
	}
	release()
	_ = returnsWithin(t, "Close after the last call", closeBudget, func() error { return <-done })
}

// A close that fails still frees, and the caller gets its error.
func TestAFailedCloseFreesAndReturnsItsError(t *testing.T) {
	failed := &Error{Kind: KindIO, Message: "flushing failed"}
	db := &DB{handle: 1} // never reaches jazz-ffi
	var freedHandle uintptr
	err := db.closeAndFree(0, func(uintptr) error { return failed }, func(h uintptr) { freedHandle = h })
	if !errors.Is(err, ErrIO) || freedHandle != 1 || db.handle != 0 {
		t.Fatalf("a failed close: err %v, freed %d, handle %d", err, freedHandle, db.handle)
	}
	if _, _, err := db.acquire(nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("a call after a failed close: %v, want ErrClosed", err)
	}
}
