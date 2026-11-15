package jazz

/*
#include <stdint.h>
#include <stdlib.h>
#include "jazz_ffi.h"
extern JazzSubscription* jazz_go_subscribe(JazzHandle* h, const char* q, uintptr_t ctx, char** err);
*/
import "C"

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Delta is a change of a subscribed query's result. The first is Reset,
// with the whole result in Added.
type Delta[R any] struct {
	Added   []R      `json:"added"`
	Updated []R      `json:"updated"`
	Removed []string `json:"removed"`
	// Reset says the result is Added alone: start over from it.
	Reset bool `json:"reset"`
	// Coalesced says several changes were folded into this one.
	Coalesced bool `json:"coalesced"`
	// Err is set on the last delta when the subscription failed; the
	// channel closes after it.
	Err error `json:"-"`
}

// subscriber is what a jazz-ffi callback reaches through its cgo.Handle.
type subscriber struct {
	mu     sync.Mutex
	closed bool
	// deliver queues the raw callback; it never blocks Jazz's thread.
	queue []event
	wake  chan struct{}
}

type event struct{ kind, json string }

// subscribers are the live subscriptions, by the id their callbacks get
// as context. A callback whose id is gone (one still running as its
// subscription ends) is dropped.
var (
	subscribers    sync.Map
	nextSubscriber atomic.Uint64
)

//export jazzGoSubscriptionCallback
func jazzGoSubscriptionCallback(ctx C.uintptr_t, kind *C.char, text *C.char) {
	value, ok := subscribers.Load(uint64(ctx))
	if !ok {
		return
	}
	s := value.(*subscriber)
	// The strings are borrowed for this call only: copy them.
	e := event{C.GoString(kind), C.GoString(text)}
	s.mu.Lock()
	if !s.closed {
		s.queue = append(s.queue, e)
	}
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Subscribe follows the query's result: the channel gets a Reset delta
// with the current rows, then one delta per change, until ctx is done
// (the subscription is then cancelled and the channel closed) or the
// subscription fails (a last delta with Err, then the channel closes).
// Deltas are buffered in order; a slow reader never blocks Jazz.
func (q *Query[R]) Subscribe(ctx context.Context, db *DB) (<-chan Delta[R], error) {
	query, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}
	s := &subscriber{wake: make(chan struct{}, 1)}
	id := nextSubscriber.Add(1)
	subscribers.Store(id, s)
	forget := func() { subscribers.Delete(id) }
	db.mu.RLock()
	if db.handle == nil {
		db.mu.RUnlock()
		forget()
		return nil, &Error{Kind: "closed", Message: "closed"}
	}
	cquery := C.CString(string(query))
	var cerr *C.char
	sub := C.jazz_go_subscribe(db.handle, cquery, C.uintptr_t(id), &cerr)
	C.free(unsafe.Pointer(cquery))
	db.mu.RUnlock()
	if err := takeError(cerr); err != nil || sub == nil {
		forget()
		if err == nil {
			err = &Error{Kind: "refused", Message: "subscribe failed"}
		}
		return nil, err
	}
	out := make(chan Delta[R])
	go func() {
		defer close(out)
		stop := func() {
			s.mu.Lock()
			s.closed = true
			s.mu.Unlock()
			C.jazz_unsubscribe(sub)
			// No callback starts after jazz_unsubscribe returns.
			forget()
		}
		for {
			s.mu.Lock()
			events := s.queue
			s.queue = nil
			s.mu.Unlock()
			for _, e := range events {
				var d Delta[R]
				if e.kind == "error" {
					d.Err = parseError(e.json)
				} else if decoded, err := decodeDelta[R](q.table, e.json); err != nil {
					d = Delta[R]{Err: err}
				} else {
					d = decoded
				}
				select {
				case out <- d:
				case <-ctx.Done():
					stop()
					return
				}
				if d.Err != nil {
					stop()
					return
				}
			}
			select {
			case <-s.wake:
			case <-ctx.Done():
				stop()
				return
			}
		}
	}()
	return out, nil
}
