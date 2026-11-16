package jazz

import (
	"encoding/json"
	"errors"
	"testing"
)

type incShow struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type incComment struct {
	ID     string   `json:"id"`
	Body   string   `json:"body"`
	Author *incCrew `json:"author,omitempty"`
}

type incCrew struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type incTask struct {
	ID       string       `json:"id"`
	Title    string       `json:"title"`
	Rank     float64      `json:"rank"`
	Show     *incShow     `json:"show,omitempty"`
	Comments []incComment `json:"comments,omitempty"`
}

type taskTitle struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

func TestIncludesNestAsTheContractSays(t *testing.T) {
	show := NewRelation[incTask, incShow]("show")
	comments := NewRelation[incTask, incComment]("comments")
	author := NewRelation[incComment, incCrew]("author")
	body := NewColumn[incComment, string]("body")
	q := NewQuery[incTask]("tasks").Include(
		show.Required(),
		comments.With(NewQuery[incComment]("").Where(body.Ne("")).Limit(3).Include(author)),
	)
	got, _ := json.Marshal(q)
	want := `{"table":"tasks","include":[{"relation":"show","required":true},{"relation":"comments","query":{"where":[{"column":"body","op":"ne","value":""}],"limit":3,"include":[{"relation":"author"}]}}]}`
	if string(got) != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	// Rows decode with their included relations; a forward relation that
	// isn't there is nil.
	var row incTask
	err := decodeRow(nil, "tasks", json.RawMessage(`{"id":"t","title":"T","rank":1,"show":null,"comments":[{"id":"c","body":"hi","author":{"id":"a","name":"Ann"}}]}`), &row)
	if err != nil || row.Show != nil || row.Comments[0].Author.Name != "Ann" {
		t.Fatalf("%+v %v", row, err)
	}
}

func TestSelectReadsIntoYourStruct(t *testing.T) {
	title := NewColumn[incTask, string]("title")
	rank := NewColumn[incTask, float64]("rank")
	q := Select[taskTitle](NewQuery[incTask]("tasks").Where(rank.Gt(1)).OrderBy(rank.Asc()).Limit(5), title)
	got, _ := json.Marshal(q)
	want := `{"table":"tasks","where":[{"column":"rank","op":"gt","value":1}],"orderBy":[{"column":"rank","direction":"asc"}],"limit":5,"select":["title"]}`
	if string(got) != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	if err := checkSelect[taskTitle]("tasks", q.selected); err != nil {
		t.Fatal(err)
	}
	// A field the selection doesn't read is refused.
	err := checkSelect[incTask]("tasks", []string{"title"})
	var jerr *Error
	if !errors.As(err, &jerr) || !errors.Is(err, ErrRefused) || jerr.Column != "rank" {
		t.Fatalf("%v", err)
	}
}

type hookCrew struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Upper string `json:"-"`
}

func (c *hookCrew) JazzDecode(app any, fields map[string]json.RawMessage) error {
	c.Upper = app.(string) + c.Name
	return nil
}

type hookComment struct {
	ID     string    `json:"id"`
	Author *hookCrew `json:"author,omitempty"`
}

type hookTask struct {
	ID       string        `json:"id"`
	Comments []hookComment `json:"comments,omitempty"`
	Owner    *hookCrew     `json:"owner,omitempty"`
}

func TestHooksRunOnIncludedRows(t *testing.T) {
	var row hookTask
	err := decodeRow("hi ", "tasks", json.RawMessage(`{"id":"t","owner":{"id":"o","name":"Olu"},"comments":[{"id":"c","author":{"id":"a","name":"Ann"}},{"id":"d","author":null}]}`), &row)
	if err != nil {
		t.Fatal(err)
	}
	if row.Owner.Upper != "hi Olu" || row.Comments[0].Author.Upper != "hi Ann" || row.Comments[1].Author != nil {
		t.Fatalf("%+v", row)
	}
}
