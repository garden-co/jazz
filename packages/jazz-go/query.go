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
	table  string
	where  []Cond[R]
	order  []Order[R]
	limit  *int
	offset *int
}

// NewQuery is a query of every row of table, read as R.
func NewQuery[R any](table string) *Query[R] { return &Query[R]{table: table} }

func (q *Query[R]) clone() *Query[R] {
	c := *q
	c.where = append([]Cond[R](nil), q.where...)
	c.order = append([]Order[R](nil), q.order...)
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

// MarshalJSON writes the query in the jazz-ffi query form.
func (q *Query[R]) MarshalJSON() ([]byte, error) {
	type cond struct {
		Column string `json:"column"`
		Op     string `json:"op"`
		Value  any    `json:"value,omitempty"`
	}
	type order struct {
		Column    string `json:"column"`
		Direction string `json:"direction"`
	}
	out := struct {
		Table   string  `json:"table"`
		Where   []cond  `json:"where,omitempty"`
		OrderBy []order `json:"orderBy,omitempty"`
		Limit   *int    `json:"limit,omitempty"`
		Offset  *int    `json:"offset,omitempty"`
	}{Table: q.table, Limit: q.limit, Offset: q.offset}
	for _, w := range q.where {
		c := cond{Column: w.column, Op: w.op}
		if w.hasVal {
			raw, err := json.Marshal(w.value)
			if err != nil {
				return nil, err
			}
			c.Value = json.RawMessage(raw)
		}
		out.Where = append(out.Where, c)
	}
	for _, o := range q.order {
		out.OrderBy = append(out.OrderBy, order{o.column, o.direction})
	}
	return json.Marshal(out)
}

// All is every row the query matches.
func (q *Query[R]) All(ctx context.Context, db *DB) ([]R, error) {
	var rows []R
	if err := db.call(ctx, "all", map[string]any{"query": q}, &rows); err != nil {
		return nil, err
	}
	if rows == nil {
		rows = []R{}
	}
	return rows, nil
}

// One is the first row the query matches, or nil if none does.
func (q *Query[R]) One(ctx context.Context, db *DB) (*R, error) {
	var row *R
	if err := db.call(ctx, "one", map[string]any{"query": q}, &row); err != nil {
		return nil, err
	}
	return row, nil
}

// Insert inserts row (an insert struct N) into table and returns the
// inserted row R, with its id.
func Insert[N, R any](ctx context.Context, db *DB, table string, row N) (R, error) {
	var inserted R
	err := db.call(ctx, "insert", map[string]any{"table": table, "row": row}, &inserted)
	return inserted, err
}
