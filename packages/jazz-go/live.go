package jazz

import (
	"bytes"
	"context"
	"encoding/json"
	"math/big"
	"reflect"
	"sort"
	"strings"
	"sync"
)

// Live is a query's current result, kept up to date: the rows in the
// query's order (re-sorted locally, as deltas carry no positions).
// It is safe for concurrent use.
type Live[R any] struct {
	table  string
	order  []Order[R]
	bigint map[string]bool // columns whose decimal strings order as numbers
	ready  chan struct{}
	change chan struct{}
	done   chan struct{}

	mu    sync.Mutex
	rows  map[string]liveRow[R]
	seq   uint64 // arrival order, for queries without OrderBy
	err   error
	reset bool
}

type liveRow[R any] struct {
	row    R
	fields map[string]json.RawMessage
	seq    uint64
}

// Live follows the query like Subscribe, keeping its rows: Rows is the
// current result in the query's order. It ends when ctx is done or the
// subscription fails (then Err says why).
func (q *Query[R]) Live(ctx context.Context, db *DB) (*Live[R], error) {
	l := &Live[R]{
		table:  q.table,
		order:  append([]Order[R](nil), q.order...),
		ready:  make(chan struct{}),
		change: make(chan struct{}, 1),
		done:   make(chan struct{}),
		rows:   map[string]liveRow[R]{},
		bigint: bigintColumns(reflect.TypeOf((*R)(nil)).Elem()),
	}
	events, err := subscribe(ctx, db, q, func(e event) (event, bool) {
		return e, e.kind == "error"
	})
	if err != nil {
		return nil, err
	}
	go func() {
		defer close(l.done)
		for e := range events {
			if err := l.apply(e); err != nil {
				l.mu.Lock()
				l.err = err
				l.mu.Unlock()
				l.signal()
				return
			}
			l.signal()
		}
		l.mu.Lock()
		if l.err == nil {
			l.err = ctx.Err()
		}
		l.mu.Unlock()
	}()
	return l, nil
}

func (l *Live[R]) signal() {
	select {
	case l.change <- struct{}{}:
	default:
	}
}

func (l *Live[R]) apply(e event) error {
	if e.kind == "error" {
		return parseError(e.json)
	}
	var raw struct {
		Added   []json.RawMessage `json:"added"`
		Updated []json.RawMessage `json:"updated"`
		Removed []string          `json:"removed"`
		Reset   bool              `json:"reset"`
	}
	if err := json.Unmarshal([]byte(e.json), &raw); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if raw.Reset {
		l.rows = map[string]liveRow[R]{}
	}
	for _, id := range raw.Removed {
		delete(l.rows, id)
	}
	for _, rows := range [][]json.RawMessage{raw.Added, raw.Updated} {
		for _, text := range rows {
			var r liveRow[R]
			if err := decodeRow(l.table, text, &r.row); err != nil {
				return err
			}
			if err := json.Unmarshal(text, &r.fields); err != nil {
				return err
			}
			var id string
			_ = json.Unmarshal(r.fields["id"], &id)
			if old, ok := l.rows[id]; ok {
				r.seq = old.seq
			} else {
				l.seq++
				r.seq = l.seq
			}
			l.rows[id] = r
		}
	}
	if raw.Reset && !l.reset {
		l.reset = true
		close(l.ready)
	}
	return nil
}

// Ready is closed once the first result has arrived.
func (l *Live[R]) Ready() <-chan struct{} { return l.ready }

// Changes gets a value after the rows change (several changes may fold
// into one). Read Rows after it.
func (l *Live[R]) Changes() <-chan struct{} { return l.change }

// Done is closed when the Live has ended: its ctx is done, or it failed.
func (l *Live[R]) Done() <-chan struct{} { return l.done }

// Err is why the Live ended, or nil while it runs.
func (l *Live[R]) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

// Rows is the current result, in the query's order.
func (l *Live[R]) Rows() []R {
	l.mu.Lock()
	rows := make([]liveRow[R], 0, len(l.rows))
	for _, r := range l.rows {
		rows = append(rows, r)
	}
	l.mu.Unlock()
	sort.SliceStable(rows, func(i, j int) bool { return l.less(rows[i], rows[j]) })
	out := make([]R, len(rows))
	for i, r := range rows {
		out[i] = r.row
	}
	return out
}

func (l *Live[R]) less(a, b liveRow[R]) bool {
	for _, o := range l.order {
		c := compareJSON(a.fields[o.column], b.fields[o.column], l.bigint[o.column])
		if o.direction == "desc" {
			c = -c
		}
		if c != 0 {
			return c < 0
		}
	}
	return a.seq < b.seq
}

// bigintColumns is the JSON names of t's BigInt fields.
func bigintColumns(t reflect.Type) map[string]bool {
	cols := map[string]bool{}
	if t.Kind() != reflect.Struct {
		return cols
	}
	bigint := reflect.TypeOf(BigInt(0))
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		ft := f.Type
		if ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		if ft != bigint {
			continue
		}
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if name == "" {
			name = f.Name
		}
		cols[name] = true
	}
	return cols
}

// compareJSON orders two column values as Jazz does: null first, then
// booleans (false < true), numbers (and, for a bigint column, its decimal
// strings) numerically, strings bytewise.
func compareJSON(a, b json.RawMessage, bigint bool) int {
	ka, va := jsonKey(a, bigint)
	kb, vb := jsonKey(b, bigint)
	if ka != kb {
		return ka - kb
	}
	switch ka {
	case 1:
		return compareBool(va.(bool), vb.(bool))
	case 2:
		return va.(*big.Float).Cmp(vb.(*big.Float))
	case 3:
		return compareStrings(va.(string), vb.(string))
	}
	return bytes.Compare(a, b)
}

func jsonKey(raw json.RawMessage, bigint bool) (int, any) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return 0, nil
	}
	var v any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if dec.Decode(&v) != nil {
		return 4, nil
	}
	switch v := v.(type) {
	case bool:
		return 1, v
	case json.Number:
		f, _, err := big.ParseFloat(string(v), 10, 256, big.ToNearestEven)
		if err == nil {
			return 2, f
		}
	case string:
		// A bigint is a decimal string: order it as a number.
		if bigint && isDecimalInteger(v) {
			if f, ok := new(big.Float).SetPrec(256).SetString(v); ok {
				return 2, f
			}
		}
		return 3, v
	}
	return 4, nil
}

func isDecimalInteger(s string) bool {
	if s == "" {
		return false
	}
	if s[0] == '-' {
		s = s[1:]
	}
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func compareBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case !a:
		return -1
	}
	return 1
}

func compareStrings(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
