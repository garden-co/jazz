package jazz

import (
	"context"
	"encoding/json"
)

// HopTo is the rows that rel leads to from the rows q keeps: a query of
// rel's target table. Conditions, orders and includes added to it apply
// to those target rows; HopTo again chains the next hop.
func HopTo[R, T any](q *Query[R], rel Relation[R, T]) *Query[T] {
	from := q.toJSON()
	if from.HopTo != nil && from.After == nil {
		// Chaining straight from a hop: the next relation joins the chain.
		from.HopTo = append(append([]string(nil), from.HopTo...), rel.name)
		return &Query[T]{table: q.table, raw: &from, afterHop: true}
	}
	if from.HopTo != nil {
		// A hop from filtered hop results isn't expressible as one chain.
		from.HopTo = append(append([]string(nil), from.HopTo...), rel.name)
	} else {
		from.HopTo = []string{rel.name}
	}
	return &Query[T]{table: q.table, raw: &from, afterHop: true}
}

// Gather is the rows reachable from the rows start keeps by following rel
// (a relation of the table to itself) again and again, up to maxDepth hops
// (0: only the start rows).
func Gather[R any](start *Query[R], rel Relation[R, R], maxDepth int) *Query[R] {
	from := start.toJSON()
	g := &Query[R]{table: start.table, raw: from.cloneParts()}
	g.raw.Gather = &gatherJSON{Relation: rel.name, MaxDepth: maxDepth}
	return g
}

// Union is the rows each of qs keeps (queries of one table), as TS's
// union: UNION ALL, so a row two of them keep comes back twice, and
// limits count every occurrence. Orders and limits added to it apply to
// the union. (A live union whose queries overlap can mis-apply removals.)
func Union[R any](qs ...*Query[R]) *Query[R] {
	u := &Query[R]{raw: &queryJSON{}}
	for _, q := range qs {
		u.raw.Union = append(u.raw.Union, q.toJSON())
		u.table = q.table
	}
	return u
}

// Version is a version of a row of R, from its table's history.
type Version[R any] struct {
	Row R
	// TransactionID is the transaction that wrote the version: with the
	// row's id, it tells versions apart.
	TransactionID string
	// AcceptedAt is when the server accepted the version.
	AcceptedAt Timestamp
}

// UnmarshalJSON reads the row and its $history metadata.
func (v *Version[R]) UnmarshalJSON(data []byte) error {
	var meta struct {
		History struct {
			TransactionID string    `json:"transactionId"`
			AcceptedAt    Timestamp `json:"acceptedAt"`
		} `json:"$history"`
	}
	if err := json.Unmarshal(data, &meta); err != nil {
		return err
	}
	if err := json.Unmarshal(data, &v.Row); err != nil {
		return err
	}
	v.TransactionID, v.AcceptedAt = meta.History.TransactionID, meta.History.AcceptedAt
	return nil
}

// History is q over its table's history: every accepted version of each
// row q keeps (each keeps its row's id; id and TransactionID tell
// versions apart). Order by AcceptedAt to list them as the server
// applied them.
func History[R any](q *Query[R]) *Query[Version[R]] {
	h := &Query[Version[R]]{table: q.table, raw: q.toJSON().cloneParts()}
	h.raw.History = true
	return h
}

// Magic columns Jazz keeps for every row, to filter, order and select by.
func CreatedAt[R any]() Column[R, Timestamp] { return Column[R, Timestamp]{name: "$createdAt"} }
func UpdatedAt[R any]() Column[R, Timestamp] { return Column[R, Timestamp]{name: "$updatedAt"} }
func CreatedBy[R any]() Column[R, string]    { return Column[R, string]{name: "$createdBy"} }
func UpdatedBy[R any]() Column[R, string]    { return Column[R, string]{name: "$updatedBy"} }

// Advice is what the app's permissions say an action would get: Allowed,
// Denied, or Unknown when Jazz can't tell yet. It is advice: the
// authority decides when the action reaches it.
type Advice string

const (
	Allowed Advice = "allowed"
	Denied  Advice = "denied"
	Unknown Advice = "unknown"
)

func advise(ctx context.Context, q Querier, args map[string]any) (Advice, error) {
	var allowed *bool
	if err := q.call(ctx, "can", args, &allowed); err != nil {
		return Unknown, err
	}
	switch {
	case allowed == nil:
		return Unknown, nil
	case *allowed:
		return Allowed, nil
	}
	return Denied, nil
}

// CanRead advises whether the row of table with id may be read.
func CanRead(ctx context.Context, q Querier, table, id string) (Advice, error) {
	return advise(ctx, q, map[string]any{"table": table, "op": "read", "id": id})
}

// CanInsert advises whether row may be inserted into table.
func CanInsert(ctx context.Context, q Querier, table string, row any) (Advice, error) {
	row, err := encode(q, row)
	if err != nil {
		return Unknown, err
	}
	return advise(ctx, q, map[string]any{"table": table, "op": "insert", "row": row})
}

// CanUpdate advises whether patch may be applied to the row of table
// with id.
func CanUpdate(ctx context.Context, q Querier, table, id string, patch any) (Advice, error) {
	patch, err := encode(q, patch)
	if err != nil {
		return Unknown, err
	}
	return advise(ctx, q, map[string]any{"table": table, "op": "update", "id": id, "row": patch})
}

// CanDelete advises whether the row of table with id may be deleted.
func CanDelete(ctx context.Context, q Querier, table, id string) (Advice, error) {
	return advise(ctx, q, map[string]any{"table": table, "op": "delete", "id": id})
}

// AcceptedAt is a history version's $acceptedAt, to order versions by.
func AcceptedAt[R any]() Column[Version[R], Timestamp] {
	return Column[Version[R], Timestamp]{name: "$acceptedAt"}
}

// IncludeDeleted is q reading deleted rows too (to show or Restore them).
func IncludeDeleted[R any](q *Query[R]) *Query[R] {
	c := &Query[R]{table: q.table, raw: q.toJSON().cloneParts()}
	c.raw.IncludeDeleted = true
	return c
}
