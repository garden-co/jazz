package jazz

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// Relation is a relation of rows of R to rows of T: what generated table
// values hold (jazzapp.Tasks.Show, jazzapp.Tasks.Comments), to include
// related rows in a query's results.
type Relation[R, T any] struct{ name string }

// NewRelation is the relation name of rows of R, leading to rows of T.
func NewRelation[R, T any](name string) Relation[R, T] { return Relation[R, T]{name: name} }

// Name is the relation's authored name.
func (r Relation[R, T]) Name() string { return r.name }

// Include is a relation to include in a query of rows of R, with what to
// include of the related rows.
type Include[R any] struct {
	relation string
	required bool
	child    *queryJSON
}

// Includer is a relation, or an Include built from one.
type Includer[R any] interface{ asInclude() Include[R] }

func (r Relation[R, T]) asInclude() Include[R] { return Include[R]{relation: r.name} }
func (i Include[R]) asInclude() Include[R]     { return i }

// With includes the related rows that child (a query of T: Where,
// OrderBy, Limit, Offset, Include) keeps.
func (r Relation[R, T]) With(child *Query[T]) Include[R] {
	c := child.toJSON()
	c.Table = ""
	return Include[R]{relation: r.name, child: &c}
}

// Include includes the related rows, and in each of them incs.
func (r Relation[R, T]) Include(incs ...Includer[T]) Include[R] {
	return r.With(NewQuery[T]("").Include(incs...))
}

// Required keeps only rows that have the related row (requireIncludes).
func (r Relation[R, T]) Required() Include[R] { return r.asInclude().Required() }

// Required keeps only rows that have the related row (requireIncludes).
func (i Include[R]) Required() Include[R] {
	i.required = true
	return i
}

// Include fills the given relation fields of each row (Task.Show,
// Task.Comments); relation fields not included stay nil.
func (q *Query[R]) Include(incs ...Includer[R]) *Query[R] {
	c := q.clone()
	for _, inc := range incs {
		c.include = append(c.include, inc.asInclude())
	}
	return c
}

// RequireIncludes keeps only rows that have every included relation.
func (q *Query[R]) RequireIncludes() *Query[R] {
	c := q.clone()
	for i := range c.include {
		c.include[i].required = true
	}
	return c
}

// Named is a column of rows of R, whatever its values: what Select takes.
type Named[R any] interface{ columnName() string }

func (c Column[R, V]) columnName() string { return c.name }

// Select is q reading only cols (and id), into rows of P: a struct you
// define whose json tags name the selected columns. A field of P that is
// neither id nor selected is refused when the query runs.
func Select[P, R any](q *Query[R], cols ...Named[R]) *Query[P] {
	p := &Query[P]{table: q.table, limit: q.limit, offset: q.offset, raw: q.toJSON().cloneParts()}
	p.selected = []string{}
	for _, c := range cols {
		p.selected = append(p.selected, c.columnName())
	}
	p.raw.Select = p.selected
	return p
}

// checkSelect refuses a P with fields the selection doesn't read.
func checkSelect[P any](table string, selected []string) error {
	if selected == nil {
		return nil
	}
	t := reflect.TypeOf((*P)(nil)).Elem()
	if t.Kind() != reflect.Struct {
		return nil
	}
	have := map[string]bool{"id": true}
	for _, s := range selected {
		have[s] = true
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if !f.IsExported() || name == "-" {
			continue
		}
		if name == "" {
			name = f.Name
		}
		if !have[name] {
			return &Error{Kind: KindRefused, Table: table, Column: name, Message: fmt.Sprintf(
				"%s.%s is a field of %s but not among the selected columns", table, name, t.Name())}
		}
	}
	return nil
}

// queryJSON is the jazz-ffi query form.
type queryJSON struct {
	Table   string        `json:"table,omitempty"`
	Where   []condJSON    `json:"where,omitempty"`
	OrderBy []orderJSON   `json:"orderBy,omitempty"`
	Limit   *int          `json:"limit,omitempty"`
	Offset  *int          `json:"offset,omitempty"`
	Select  []string      `json:"select,omitempty"`
	Include []includeJSON `json:"include,omitempty"`
}

type condJSON struct {
	Column string          `json:"column"`
	Op     string          `json:"op"`
	Value  json.RawMessage `json:"value,omitempty"`
}

type orderJSON struct {
	Column    string `json:"column"`
	Direction string `json:"direction"`
}

type includeJSON struct {
	Relation string     `json:"relation"`
	Required bool       `json:"required,omitempty"`
	Query    *queryJSON `json:"query,omitempty"`
}

func (j queryJSON) cloneParts() *queryJSON {
	c := j
	c.Where = append([]condJSON(nil), j.Where...)
	c.OrderBy = append([]orderJSON(nil), j.OrderBy...)
	c.Include = append([]includeJSON(nil), j.Include...)
	return &c
}
