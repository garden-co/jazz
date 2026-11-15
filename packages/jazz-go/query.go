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
	table    string
	where    []Cond[R]
	order    []Order[R]
	limit    *int
	offset   *int
	include  []Include[R]
	selected []string
	tier     ReadTier
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
	out := queryJSON{Table: q.table, Limit: q.limit, Offset: q.offset, Select: q.selected}
	if q.raw != nil {
		out = *q.raw.cloneParts()
		out.Limit, out.Offset = q.limit, q.offset
		if q.selected != nil {
			out.Select = q.selected
		}
	}
	for _, w := range q.where {
		c := condJSON{Column: w.column, Op: w.op}
		if w.hasVal {
			raw, err := json.Marshal(w.value)
			if err != nil {
				raw = []byte("null")
			}
			c.Value = raw
		}
		out.Where = append(out.Where, c)
	}
	for _, o := range q.order {
		out.OrderBy = append(out.OrderBy, orderJSON{o.column, o.direction})
	}
	for _, i := range q.include {
		out.Include = append(out.Include, includeJSON{Relation: i.relation, Required: i.required, Query: i.child})
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

// All is every row the query matches, read through db (a *DB or a *Tx).
func (q *Query[R]) All(ctx context.Context, db Querier) ([]R, error) {
	if err := checkSelect[R](q.table, q.selected); err != nil {
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
	if err := checkSelect[R](q.table, q.selected); err != nil {
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
