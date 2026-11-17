package jazz

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type row struct {
	ID string `json:"id"`
}

func TestQueryJSON(t *testing.T) {
	title := NewColumn[row, string]("title")
	rank := NewColumn[row, float64]("rank")
	base := NewQuery[row]("tasks").Where(title.Eq("a"))
	q := base.Where(rank.Gte(1), title.IsNull(), title.In("x", "y")).
		OrderBy(rank.Desc()).Limit(10).Offset(20)
	got, err := json.Marshal(q)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"table":"tasks","where":[{"column":"title","op":"eq","value":"a"},{"column":"rank","op":"gte","value":1},{"column":"title","op":"isNull"},{"column":"title","op":"in","value":["x","y"]}],"orderBy":[{"column":"rank","direction":"desc"}],"limit":10,"offset":20}`
	if string(got) != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	// Extending a query leaves it as it was.
	if b, _ := json.Marshal(base); string(b) != `{"table":"tasks","where":[{"column":"title","op":"eq","value":"a"}]}` {
		t.Fatalf("base changed: %s", b)
	}
}

func TestPatchAndValues(t *testing.T) {
	got, err := MarshalPatch(
		Set("x").Entry("title"),
		Patch[int32]{}.Entry("score"),
		Set[*string](nil).Entry("notes"),
		Set(BigInt(1<<62)).Entry("big"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"big":"4611686018427387904","notes":null,"title":"x"}` {
		t.Fatalf("patch: %s", got)
	}
	var b BigInt
	if err := json.Unmarshal([]byte(`"-12"`), &b); err != nil || b != -12 {
		t.Fatalf("bigint: %v %v", b, err)
	}
	if ts := Timestamp(1700000000123); TimestampOf(ts.Time()) != ts {
		t.Fatal("timestamp round trip")
	}
}

type floatRow struct {
	ID   string   `json:"id"`
	Rank float64  `json:"rank"`
	Opt  *float64 `json:"opt"`
	Tags []string `json:"tags"`
}

func TestNullInANonPointerFieldIsRefused(t *testing.T) {
	var row floatRow
	err := decodeRow(nil, "tasks", json.RawMessage(`{"id":"a","rank":null}`), &row)
	var jerr *Error
	if !errors.As(err, &jerr) || jerr.Kind != "refused" || !strings.Contains(jerr.Message, "tasks.rank") {
		t.Fatalf("null rank: %v", err)
	}
	if err := decodeRow(nil, "tasks", json.RawMessage(`{"id":"a","rank":1.5,"opt":null,"tags":null}`), &row); err != nil || row.Rank != 1.5 || row.Opt != nil {
		t.Fatalf("nullable fields: %+v %v", row, err)
	}
	d, err := decodeDelta[floatRow](nil, "tasks", `{"added":[],"updated":[{"id":"a","rank":null}],"removed":[],"reset":false}`)
	if err == nil {
		t.Fatalf("a delta with a null rank: %+v", d)
	}
}
