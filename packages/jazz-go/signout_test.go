package jazz

import (
	"context"
	"runtime"
	"testing"
	"time"
	"unsafe"
)

// deliver calls onSubscription as jazz-ffi's watch thread would.
func deliver(id uint64, kind, text string) {
	k, j := cstr(kind), cstr(text)
	onSubscription(uintptr(id), uintptr(unsafe.Pointer(k)), uintptr(unsafe.Pointer(j)))
	runtime.KeepAlive(k)
	runtime.KeepAlive(j)
}

// After signOut, jazz-ffi's watch thread delivers the session's last
// state and then its closed callback, a little later than signOut
// returns. Settling the streams before Close keeps that state: the
// channel gets signedOut, then closes.
func TestSignOutKeepsTheFinalSessionState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := &DB{handle: 1} // never reaches jazz-ffi: the stream is a stand-in
	started := make(chan uint64, 1)
	states, err := stream(ctx, db, watchStates(decodeSessionState), func(id uint64) (func(), error) {
		started <- id
		return func() {}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	id := <-started
	go func() {
		time.Sleep(100 * time.Millisecond)
		deliver(id, "session", `{"status":"signedOut"}`)
		deliver(id, "error", `{"kind":"closed","message":"closed"}`)
	}()

	db.settleStreams() // what SignOut does once signOut returns, before Close

	var last SessionState
	for s := range states {
		last = s
	}
	if last.Status != SignedOut {
		t.Fatalf("the stream closed without the final signedOut state (last %+v)", last)
	}
}
