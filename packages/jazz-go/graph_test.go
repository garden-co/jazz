package jazz

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

type gTodo struct {
	ID     string  `json:"id"`
	Title  string  `json:"title"`
	Parent *string `json:"parent"`
}

func TestGraphQueriesJSON(t *testing.T) {
	done := NewColumn[gTodo, bool]("done")
	parent := NewRelation[gTodo, gTodo]("parentRelation")
	show := NewRelation[incTask, incShow]("show")
	rank := NewColumn[incTask, float64]("rank")
	for _, c := range []struct {
		q    any
		want string
	}{
		{Gather(NewQuery[gTodo]("todos").Where(done.Eq(false)), parent, 10),
			`{"table":"todos","where":[{"column":"done","op":"eq","value":false}],"gather":{"relation":"parentRelation","maxDepth":10}}`},
		{HopTo(NewQuery[incTask]("tasks").Where(rank.Lt(2)), show).Where(NewColumn[incShow, string]("name").Ne("")).Limit(3),
			`{"table":"tasks","where":[{"column":"rank","op":"lt","value":2}],"hopTo":["show"],"after":{"where":[{"column":"name","op":"ne","value":""}],"limit":3}}`},
		{Union(NewQuery[incTask]("tasks").Where(rank.Lt(1)), NewQuery[incTask]("tasks").Where(rank.Gt(5))),
			`{"union":[{"table":"tasks","where":[{"column":"rank","op":"lt","value":1}]},{"table":"tasks","where":[{"column":"rank","op":"gt","value":5}]}]}`},
		{History(NewQuery[incTask]("tasks").Where(rank.Eq(1))).Limit(2),
			`{"table":"tasks","where":[{"column":"rank","op":"eq","value":1}],"limit":2,"history":true}`},
	} {
		got, err := json.Marshal(c.q)
		if err != nil || string(got) != c.want {
			t.Errorf("got  %s\nwant %s (%v)", got, c.want, err)
		}
	}
	var v HistoryRow[incTask]
	if err := json.Unmarshal([]byte(`{"id":"t","title":"T","rank":2,"$history":{"transactionId":"x","acceptedAt":5}}`), &v); err != nil || v.Row.Title != "T" || v.AcceptedAt != 5 || v.TransactionID != "x" {
		t.Fatalf("%+v %v", v, err)
	}
}

const todosApp = `{ "format": 1, "tables": { "todos": {
  "columns": { "title": "string", "done": "boolean", "parent": "uuid?" },
  "relations": { "parentRelation": { "rel": "todos", "column": "parent" } } } } }`

func TestGatherFollowsASelfRelation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := Open(ctx, Config{App: todosApp, Storage: StorageMemory})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	type newTodo struct {
		Title  string  `json:"title"`
		Done   bool    `json:"done"`
		Parent *string `json:"parent,omitempty"`
	}
	root, err := Insert[newTodo, gTodo](ctx, db, "todos", newTodo{Title: "root", Done: true})
	if err != nil {
		t.Fatal(err)
	}
	mid, _ := Insert[newTodo, gTodo](ctx, db, "todos", newTodo{Title: "mid", Done: true, Parent: &root.ID})
	if _, err := Insert[newTodo, gTodo](ctx, db, "todos", newTodo{Title: "leaf", Parent: &mid.ID}); err != nil {
		t.Fatal(err)
	}
	done := NewColumn[gTodo, bool]("done")
	parent := NewRelation[gTodo, gTodo]("parentRelation")
	// From the open todo, its lineage: leaf, mid, root.
	lineage, err := Gather(NewQuery[gTodo]("todos").Where(done.Eq(false)), parent, 10).All(ctx, db)
	if err != nil || len(lineage) != 3 {
		t.Fatalf("lineage: %+v %v", lineage, err)
	}
	if one, err := Gather(NewQuery[gTodo]("todos").Where(done.Eq(false)), parent, 1).All(ctx, db); err != nil || len(one) != 2 {
		t.Fatalf("one hop: %+v %v", one, err)
	}
}
