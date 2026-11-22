package jazz

import (
	"runtime"
	"sync"
	"time"
)

// Closing follows jazz-ffi's host sequence (include/jazz_ffi.h): stop
// admitting calls, "close" (which ends the ones waiting for the server),
// wait for those in flight, free, and only then forget the token provider.

// acquire is the handle for one call into Jazz, until release: the handle
// isn't freed before. admitted, if any, runs before a close can begin.
func (db *DB) acquire(admitted func()) (h uintptr, release func(), err error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.handle == 0 || db.closed != nil {
		return 0, nil, &Error{Kind: KindClosed, Message: "closed"}
	}
	if admitted != nil {
		admitted()
	}
	db.calls.Add(1)
	return db.handle, db.calls.Done, nil
}

// closeAndFree closes the handle with end, giving the token provider
// drain to keep answering, and frees it with free once no call is in
// flight. It frees, and returns end's error, also when end fails.
//
// Closing again waits for the first close. From inside the DB's own token
// provider neither waits, as what they would wait for waits on the
// provider: the handle is then freed once it returns.
func (db *DB) closeAndFree(drain time.Duration, end func(h uintptr) error, free func(h uintptr)) error {
	inside := insideProvider(db.provider)
	db.mu.Lock()
	if closed := db.closed; closed != nil {
		db.mu.Unlock()
		if !inside {
			<-closed
		}
		return nil
	}
	h := db.handle
	if h == 0 {
		db.mu.Unlock()
		return nil
	}
	closed := make(chan struct{})
	db.closed = closed
	db.mu.Unlock()

	var deadline *time.Timer
	if drain > 0 {
		deadline = time.AfterFunc(drain, func() { cancelFetches(db.provider) })
	} else {
		cancelFetches(db.provider)
	}
	db.endStreams()
	err := end(h)
	if deadline != nil {
		deadline.Stop() // not after the provider is released
	}
	cancelFetches(db.provider)
	finish := func() {
		defer close(closed)
		db.calls.Wait()
		free(h)
		db.mu.Lock()
		db.handle = 0
		db.mu.Unlock()
		releaseProvider(db.provider)
	}
	if inside {
		go finish()
	} else {
		finish()
	}
	return err
}

// callClose is jazz_call's or jazz_session_call's "close".
func callClose(call func(h uintptr, method, args *byte, err *uintptr) uintptr, h uintptr, argsJSON string) error {
	var cerr uintptr
	takeString(call(h, cstr("close"), cstr(argsJSON), &cerr))
	return takeError(cerr)
}

// tokenFetches are one provider's fetches in progress.
type tokenFetches struct {
	mu      sync.Mutex
	closing bool
	running map[*tokenFetch]struct{}
}

type tokenFetch struct {
	// thread is the OS thread the provider runs on. A goroutine in a
	// callback from C stays on its thread, and no other runs there.
	thread uintptr
	cancel func()
}

var fetches sync.Map // a provider's id → *tokenFetches

func fetchesOf(id uint64) *tokenFetches {
	value, _ := fetches.LoadOrStore(id, &tokenFetches{running: map[*tokenFetch]struct{}{}})
	return value.(*tokenFetches)
}

// begin records a fetch on this thread; false once closing.
func (f *tokenFetches) begin(cancel func()) (*tokenFetch, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closing {
		return nil, false
	}
	fetch := &tokenFetch{thread: ffi.threadSelf(), cancel: cancel}
	f.running[fetch] = struct{}{}
	return fetch, true
}

func (f *tokenFetches) done(fetch *tokenFetch) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.running, fetch)
}

func (f *tokenFetches) isClosing() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closing
}

// cancelFetches cancels the provider's ctx, and fails its later fetches
// without asking it: closing waits for a fetch in progress.
func cancelFetches(id uint64) {
	if id == 0 {
		return
	}
	f := fetchesOf(id)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closing = true
	for fetch := range f.running {
		fetch.cancel()
	}
}

// insideProvider says whether the caller runs in a fetch of the provider.
func insideProvider(id uint64) bool {
	if id == 0 {
		return false
	}
	value, ok := fetches.Load(id)
	if !ok {
		return false
	}
	// Stay on the thread whose identity is compared.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	thread := ffi.threadSelf()
	f := value.(*tokenFetches)
	f.mu.Lock()
	defer f.mu.Unlock()
	for fetch := range f.running {
		if fetch.thread == thread {
			return true
		}
	}
	return false
}

func releaseProvider(id uint64) {
	providers.Delete(id)
	fetches.Delete(id)
}
