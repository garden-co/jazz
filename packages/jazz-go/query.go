package jazz

import (
	"context"
	"encoding/json"
)

// Cond is a condition on rows of R, from one of its table's columns.
type Cond[R any] struct {
	column string
	op     string
	value  any
	hasVal bool
}

// Order is an order of rows of R, from one of its table's columns.
type Order[R any] struct {
	column    string
	direction string
}

// Column is a column of rows of R whose values are V: what generated
// table values hold, to build conditions and orders from.
type Column[R, V any] struct{ name string }

// NewColumn is the column name of rows of R.
func NewColumn[R, V any](name string) Column[R, V] { return Column[R, V]{name: name} }

// Name is the column's authored name.
func (c Column[R, V]) Name() string { return c.name }

func (c Column[R, V]) cond(op string, v any) Cond[R] {
	return Cond[R]{column: c.name, op: op, value: v, hasVal: true}
}

// Eq holds where the column equals v.
func (c Column[R, V]) Eq(v V) Cond[R] { return c.cond("eq", v) }

// Ne holds where the column doesn't equal v.
func (c Column[R, V]) Ne(v V) Cond[R] { return c.cond("ne", v) }

// Lt holds where the column is less than v.
func (c Column[R, V]) Lt(v V) Cond[R] { return c.cond("lt", v) }

// Lte holds where the column is at most v.
func (c Column[R, V]) Lte(v V) Cond[R] { return c.cond("lte", v) }

// Gt holds where the column is greater than v.
func (c Column[R, V]) Gt(v V) Cond[R] { return c.cond("gt", v) }

// Gte holds where the column is at least v.
func (c Column[R, V]) Gte(v V) Cond[R] { return c.cond("gte", v) }

// In holds where the column is one of vs.
func (c Column[R, V]) In(vs ...V) Cond[R] {
	if vs == nil {
		vs = []V{}
	}
	return c.cond("in", vs)
}

// Contains holds where the column (a string, or an array) contains v.
func (c Column[R, V]) Contains(v any) Cond[R] { return c.cond("contains", v) }

// IsNull holds where the column is null.
func (c Column[R, V]) IsNull() Cond[R] { return Cond[R]{column: c.name, op: "isNull"} }

// IsNotNull holds where the column isn't null.
func (c Column[R, V]) IsNotNull() Cond[R] { return Cond[R]{column: c.name, op: "isNotNull"} }

// Asc orders by the column, smallest first.
func (c Column[R, V]) Asc() Order[R] { return Order[R]{column: c.name, direction: "asc"} }

// Desc orders by the column, largest first.
func (c Column[R, V]) Desc() Order[R] { return Order[R]{column: c.name, direction: "desc"} }

// Query is a query of rows of R. Its methods return a new query and leave
// the receiver as it is, so a query can be shared and extended.
type Query[R any] struct {
	table       string
	where       []Cond[R]
	order       []Order[R]
	limit       *int
	offset      *int
	include     []Include[R]
	selected    []string
	selectItems []any
	tier        ReadTier
	// afterHop: the typed parts apply to the rows a hop (raw.HopTo)
	// reached, so they go under "after".
	afterHop bool
	// raw holds the parts of a query Select made from a query of another
	// row type; the typed parts above add to it.
	raw *queryJSON
}

// NewQuery is a query of every row of table, read as R.
func NewQuery[R any](table string) *Query[R] { return &Query[R]{table: table} }

func (q *Query[R]) clone() *Query[R] {
	c := *q
	c.where = append([]Cond[R](nil), q.where...)
	c.order = append([]Order[R](nil), q.order...)
	c.include = append([]Include[R](nil), q.include...)
	c.selectItems = append([]any(nil), q.selectItems...)
	return &c
}

// Where keeps the rows every condition holds for (ANDed with earlier ones).
func (q *Query[R]) Where(conds ...Cond[R]) *Query[R] {
	c := q.clone()
	c.where = append(c.where, conds...)
	return c
}

// OrderBy orders the rows, by each order in turn (after earlier ones).
func (q *Query[R]) OrderBy(orders ...Order[R]) *Query[R] {
	c := q.clone()
	c.order = append(c.order, orders...)
	return c
}

// Limit keeps at most n rows.
func (q *Query[R]) Limit(n int) *Query[R] {
	c := q.clone()
	c.limit = &n
	return c
}

// Offset skips the first n rows.
func (q *Query[R]) Offset(n int) *Query[R] {
	c := q.clone()
	c.offset = &n
	return c
}

func (q *Query[R]) toJSON() queryJSON {
	typed := queryJSON{Limit: q.limit, Offset: q.offset, Select: q.selectItems}
	for _, w := range q.where {
		c := condJSON{Column: w.column, Op: w.op}
		if w.hasVal {
			raw, err := json.Marshal(w.value)
			if err != nil {
				raw = []byte("null")
			}
			c.Value = raw
		}
		typed.Where = append(typed.Where, c)
	}
	for _, o := range q.order {
		typed.OrderBy = append(typed.OrderBy, orderJSON{o.column, o.direction})
	}
	for _, i := range q.include {
		typed.Include = append(typed.Include, includeJSON{Relation: i.relation, Required: i.required, Query: i.child})
	}
	if q.raw == nil {
		typed.Table = q.table
		return typed
	}
	out := *q.raw.cloneParts()
	if q.afterHop {
		if !typed.empty() {
			after := typed
			if out.After != nil {
				after = out.After.merged(typed)
			}
			out.After = &after
		}
		return out
	}
	return out.merged(typed)
}

func (j queryJSON) empty() bool {
	return len(j.Where) == 0 && len(j.OrderBy) == 0 && j.Limit == nil && j.Offset == nil &&
		j.Select == nil && len(j.Include) == 0
}

// merged is j with typed's parts added: conditions, orders and includes
// after j's, typed's limit, offset and select over j's.
func (j queryJSON) merged(typed queryJSON) queryJSON {
	out := *j.cloneParts()
	out.Where = append(out.Where, typed.Where...)
	out.OrderBy = append(out.OrderBy, typed.OrderBy...)
	out.Include = append(out.Include, typed.Include...)
	if typed.Limit != nil {
		out.Limit = typed.Limit
	}
	if typed.Offset != nil {
		out.Offset = typed.Offset
	}
	if typed.Select != nil {
		out.Select = typed.Select
	}
	return out
}

// args is the query's jazz-ffi arguments: the query, and its tier.
func (q *Query[R]) args() map[string]any {
	args := map[string]any{"query": q}
	if q.tier != "" {
		args["tier"] = string(q.tier)
	}
	return args
}

// subscription is what jazz_subscribe takes: the query, or with a tier
// {"query", "tier"}.
func (q *Query[R]) subscription() any {
	if q.tier == "" {
		return q
	}
	return map[string]any{"query": q, "tier": string(q.tier)}
}

// MarshalJSON writes the query in the jazz-ffi query form.
func (q *Query[R]) MarshalJSON() ([]byte, error) {
	for _, w := range q.where {
		if w.hasVal {
			if _, err := json.Marshal(w.value); err != nil {
				return nil, err
			}
		}
	}
	return json.Marshal(q.toJSON())
}

// readable is what a Select's rows may have fields for: the selected
// columns and the included relations (nil without a Select).
func (q *Query[R]) readable() []string {
	if q.selected == nil {
		return nil
	}
	names := append([]string(nil), q.selected...)
	if q.raw != nil {
		for _, inc := range q.raw.Include {
			names = append(names, inc.Relation)
		}
	}
	for _, inc := range q.include {
		names = append(names, inc.relation)
	}
	return names
}

// All is every row the query matches, read through db (a *DB or a *Tx).
func (q *Query[R]) All(ctx context.Context, db Querier) ([]R, error) {
	if err := checkSelect[R](q.table, q.readable()); err != nil {
		return nil, err
	}
	var raws []json.RawMessage
	if err := db.call(ctx, "all", q.args(), &raws); err != nil {
		return nil, err
	}
	return decodeRows[R](db.computed(), q.table, raws)
}

// One is the first row the query matches, or nil if none does.
func (q *Query[R]) One(ctx context.Context, db Querier) (*R, error) {
	if err := checkSelect[R](q.table, q.readable()); err != nil {
		return nil, err
	}
	var raw json.RawMessage
	if err := db.call(ctx, "one", q.args(), &raw); err != nil {
		return nil, err
	}
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var row R
	if err := decodeRow(db.computed(), q.table, raw, &row); err != nil {
		return nil, err
	}
	return &row, nil
}

// Insert inserts row (an insert struct N) into table and returns the
// inserted row R, with its id.
func Insert[N, R any](ctx context.Context, db Querier, table string, row N, opts ...WriteOption) (R, error) {
	var inserted R
	var raw json.RawMessage
	encoded, err := encode(db, row)
	if err != nil {
		return inserted, err
	}
	if err := db.call(ctx, "insert", withOptions(map[string]any{"table": table, "row": encoded}, opts), &raw); err != nil {
		return inserted, err
	}
	err = decodeRow(db.computed(), table, raw, &inserted)
	return inserted, err
}
