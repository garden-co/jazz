package jazz

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
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

// onSubscription is jazz-ffi's subscription and watch callback.
func onSubscription(ctx, kind, text uintptr) {
	// A panic must not cross into jazz-ffi (it would kill the process).
	defer func() { _ = recover() }()
	value, ok := subscribers.Load(uint64(ctx))
	if !ok {
		return
	}
	s := value.(*subscriber)
	// The strings are borrowed for this call only: copy them.
	e := event{goString(kind), goString(text)}
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
	sendAll := func(e event) (T, bool, bool) { v, last := decode(e); return v, last, false }
	return stream(ctx, db, sendAll, func(id uint64) (func(), error) {
		var cerr uintptr
		sub := ffi.subscribe(db.handle, cstr(string(queryJSON)), ffi.subscriptionCallback, uintptr(id), &cerr)
		if err := takeError(cerr); err != nil || sub == 0 {
			if err == nil {
				err = &Error{Kind: KindRefused, Message: "subscribe failed"}
			}
			return nil, err
		}
		return func() { ffi.unsubscribe(sub) }, nil
	})
}

// stream runs a jazz-ffi callback stream that start begins (with the id
// its callbacks get as context) and whose stop func ends it, sending what
// decode makes of each callback, in order, until ctx is done, the DB
// closes, or decode says the value it made is the last (an error); skip
// ends the stream without sending it.
func stream[T any](ctx context.Context, db *DB, decode func(event) (v T, last, skip bool), start func(id uint64) (func(), error)) (<-chan T, error) {
	s := &subscriber{wake: make(chan struct{}, 1)}
	id := nextSubscriber.Add(1)
	subscribers.Store(id, s)
	forget := func() { subscribers.Delete(id) }
	// stop ends jazz-ffi's side, once: no callback runs after it returns.
	var (
		endMu   sync.Mutex
		end     func()
		stopped bool
	)
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			s.mu.Lock()
			s.closed = true
			s.mu.Unlock()
			endMu.Lock()
			stopped = true
			e := end
			endMu.Unlock()
			if e != nil {
				e()
			}
			forget()
		})
	}
	// Close ends the stream too: it stops jazz-ffi's side at once (before
	// the handle closes), and the stream then sends what was already
	// queued and closes its channel. It is registered before it starts, so
	// a Close meanwhile ends it.
	closing := make(chan struct{})
	var closeOnce sync.Once
	unregister := func() {
		db.streamsMu.Lock()
		delete(db.streams, id)
		db.streamsMu.Unlock()
	}
	db.mu.RLock()
	if db.handle == 0 {
		db.mu.RUnlock()
		forget()
		return nil, &Error{Kind: KindClosed, Message: "closed"}
	}
	db.streamsMu.Lock()
	if db.streams == nil {
		db.streams = map[uint64]func(){}
	}
	db.streams[id] = func() {
		stop()
		closeOnce.Do(func() { close(closing) })
	}
	db.streamsMu.Unlock()
	started, err := start(id)
	db.mu.RUnlock()
	if err != nil {
		unregister()
		forget()
		return nil, err
	}
	endMu.Lock()
	if stopped {
		// Closed while starting: end what just started.
		endMu.Unlock()
		started()
		return nil, &Error{Kind: KindClosed, Message: "closed"}
	}
	end = started
	endMu.Unlock()
	out := make(chan T)
	go func() {
		defer close(out)
		defer func() {
			stop()
			unregister()
		}()
		draining := false
		for {
			s.mu.Lock()
			events := s.queue
			s.queue = nil
			s.mu.Unlock()
			for _, e := range events {
				v, last, skip := decode(e)
				if skip {
					return
				}
				select {
				case out <- v:
				case <-ctx.Done():
					return
				}
				if last {
					return
				}
			}
			if draining {
				return
			}
			select {
			case <-s.wake:
			case <-closing:
				// Nothing more arrives: send what is queued, then end.
				draining = true
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}
