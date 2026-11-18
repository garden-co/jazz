package jazz

import (
	"encoding/json"
	"testing"
)

type liveTask struct {
	ID    string  `json:"id"`
	Title string  `json:"title"`
	Rank  float64 `json:"rank"`
	Big   BigInt  `json:"big"`
	Code  string  `json:"code"`
}

func newTestLive(order ...Order[liveTask]) *Live[liveTask] {
	q := NewQuery[liveTask]("tasks").OrderBy(order...)
	return &Live[liveTask]{
		table: "tasks", order: q.order, ready: make(chan struct{}), change: make(chan struct{}, 1),
		done: make(chan struct{}), rows: map[string]liveRow[liveTask]{}, bigint: map[string]bool{"big": true},
	}
}

func titles(rows []liveTask) (out []string) {
	for _, r := range rows {
		out = append(out, r.Title)
	}
	return
}

func delta(t *testing.T, l *Live[liveTask], v any) {
	t.Helper()
	b, _ := json.Marshal(v)
	if err := l.apply(event{kind: "delta", json: string(b)}); err != nil {
		t.Fatal(err)
	}
}

func liveRowJSON(id, title string, rank float64, big string, code string) map[string]any {
	return map[string]any{"id": id, "title": title, "rank": rank, "big": big, "code": code}
}

func TestLiveKeepsTheQueryOrder(t *testing.T) {
	rank := NewColumn[liveTask, float64]("rank")
	l := newTestLive(rank.Asc())
	delta(t, l, map[string]any{"reset": true, "added": []any{liveRowJSON("a", "A", 2, "1", "9"), liveRowJSON("b", "B", 1, "1", "10")}})
	select {
	case <-l.Ready():
	default:
		t.Fatal("not ready after reset")
	}
	if got := titles(l.Rows()); len(got) != 2 || got[0] != "B" {
		t.Fatalf("by rank: %v", got)
	}
	// Moving A before B: the delta has no position, the order follows rank.
	delta(t, l, map[string]any{"updated": []any{liveRowJSON("a", "A", 0.5, "1", "9")}})
	delta(t, l, map[string]any{"added": []any{liveRowJSON("c", "C", 1.5, "1", "8")}, "removed": []string{"b"}})
	if got := titles(l.Rows()); len(got) != 2 || got[0] != "A" || got[1] != "C" {
		t.Fatalf("after moves: %v", got)
	}
}

func TestLiveOrdersBigintsNumericallyAndStringsAsText(t *testing.T) {
	big := NewColumn[liveTask, BigInt]("big")
	code := NewColumn[liveTask, string]("code")
	l := newTestLive(big.Desc())
	delta(t, l, map[string]any{"reset": true, "added": []any{
		liveRowJSON("a", "A", 0, "9", "9"), liveRowJSON("b", "B", 0, "10", "10"), liveRowJSON("c", "C", 0, "-1", "-1"),
	}})
	if got := titles(l.Rows()); got[0] != "B" || got[1] != "A" || got[2] != "C" {
		t.Fatalf("bigint desc: %v", got)
	}
	l.order = []Order[liveTask]{code.Asc()}
	if got := titles(l.Rows()); got[0] != "C" || got[1] != "B" || got[2] != "A" {
		t.Fatalf("text asc (\"-1\" < \"10\" < \"9\"): %v", got)
	}
}
