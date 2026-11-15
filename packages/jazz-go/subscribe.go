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

// subscriber is what a jazz-ffi callback reaches, through the integer id
// it gets as its context (see subscribers).
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
	return subscribe(ctx, db, q.subscription(), func(e event) (Delta[R], bool) {
		if e.kind == "error" {
			return Delta[R]{Err: parseError(e.json)}, true
		}
		d, err := decodeDelta[R](db.computed(), q.table, e.json)
		if err != nil {
			return Delta[R]{Err: err}, true
		}
		return d, false
	})
}

// subscribe runs a jazz-ffi subscription of query, sending what decode
// makes of each callback, in order, until ctx is done or decode says the
// value it made is the last (an error).
func subscribe[T any](ctx context.Context, db *DB, query any, decode func(event) (T, bool)) (<-chan T, error) {
	queryJSON, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}
	return stream(ctx, db, decode, func(id uint64) (func(), error) {
		cquery := C.CString(string(queryJSON))
		defer C.free(unsafe.Pointer(cquery))
		var cerr *C.char
		sub := C.jazz_go_subscribe(db.handle, cquery, C.uintptr_t(id), &cerr)
		if err := takeError(cerr); err != nil || sub == nil {
			if err == nil {
				err = &Error{Kind: KindRefused, Message: "subscribe failed"}
			}
			return nil, err
		}
		return func() { C.jazz_unsubscribe(sub) }, nil
	})
}

// stream runs a jazz-ffi callback stream that start begins (with the id
// its callbacks get as context) and whose stop func ends it, sending what
// decode makes of each callback, in order, until ctx is done or decode
// says the value it made is the last (an error).
func stream[T any](ctx context.Context, db *DB, decode func(event) (T, bool), start func(id uint64) (func(), error)) (<-chan T, error) {
	s := &subscriber{wake: make(chan struct{}, 1)}
	id := nextSubscriber.Add(1)
	subscribers.Store(id, s)
	forget := func() { subscribers.Delete(id) }
	db.mu.RLock()
	if db.handle == nil {
		db.mu.RUnlock()
		forget()
		return nil, &Error{Kind: KindClosed, Message: "closed"}
	}
	end, err := start(id)
	db.mu.RUnlock()
	if err != nil {
		forget()
		return nil, err
	}
	out := make(chan T)
	go func() {
		defer close(out)
		stop := func() {
			s.mu.Lock()
			s.closed = true
			s.mu.Unlock()
			end()
			// No callback runs after the stream's end returns.
			forget()
		}
		for {
			s.mu.Lock()
			events := s.queue
			s.queue = nil
			s.mu.Unlock()
			for _, e := range events {
				v, last := decode(e)
				select {
				case out <- v:
				case <-ctx.Done():
					stop()
					return
				}
				if last {
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
