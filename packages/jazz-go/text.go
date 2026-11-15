package jazz

import (
	"context"
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

// TextEdit is an edit of a mergeable text column (merge: "text",
// "markdown" or "html"): concurrent edits merge three-way, character by
// character.
type TextEdit struct {
	// Pos is a byte offset in the text as the edits before this one left
	// it, as strings.Index and slicing count; an offset inside a
	// character is refused.
	Pos int
	// Delete is how many bytes from Pos to remove.
	Delete int
	// Insert is inserted at Pos.
	Insert string
}

// Splice is the edit replacing deleteLen bytes at pos with insert.
func Splice(pos, deleteLen int, insert string) TextEdit {
	return TextEdit{Pos: pos, Delete: deleteLen, Insert: insert}
}

// ApplyEdits is text with edits applied in order, or why it can't be.
func ApplyEdits(text string, edits ...TextEdit) (string, error) {
	for i, e := range edits {
		end := e.Pos + e.Delete
		if e.Pos < 0 || e.Delete < 0 || end > len(text) {
			return "", &Error{Kind: KindRefused, Message: fmt.Sprintf(
				"edit %d (at %d, deleting %d) is outside the text (%d bytes)", i, e.Pos, e.Delete, len(text))}
		}
		if !boundary(text, e.Pos) || !boundary(text, end) {
			return "", &Error{Kind: KindRefused, Message: fmt.Sprintf(
				"edit %d (at %d, deleting %d) splits a character", i, e.Pos, e.Delete)}
		}
		text = text[:e.Pos] + e.Insert + text[end:]
	}
	return text, nil
}

func boundary(text string, at int) bool {
	return at == len(text) || utf8.RuneStart(text[at])
}

// EditText applies edits to column (a mergeable text) of the row of table
// with id. The edits apply to the text as this app has it now; Jazz merges
// them with edits others made meanwhile.
func EditText(ctx context.Context, q Querier, table, id, column string, edits ...TextEdit) error {
	var row map[string]json.RawMessage
	query := map[string]any{
		"table":  table,
		"where":  []map[string]any{{"column": "id", "op": "eq", "value": id}},
		"select": []string{column},
	}
	if err := q.call(ctx, "one", map[string]any{"query": query}, &row); err != nil {
		return err
	}
	if row == nil {
		return &Error{Kind: KindRefused, Table: table, Message: fmt.Sprintf("%s has no row %s", table, id)}
	}
	var current *string
	if err := json.Unmarshal(row[column], &current); err != nil {
		return err
	}
	from := ""
	if current != nil {
		from = *current
	}
	text, err := ApplyEdits(from, edits...)
	if err != nil {
		return err
	}
	return q.call(ctx, "editText", map[string]any{
		"table": table, "id": id, "column": column, "editedFrom": from, "text": text,
	}, nil)
}

// SetText makes column (a mergeable text) of the row of table with id
// text, merged with edits others made since this app last read editedFrom.
func SetText(ctx context.Context, q Querier, table, id, column, editedFrom, text string) error {
	return q.call(ctx, "editText", map[string]any{
		"table": table, "id": id, "column": column, "editedFrom": editedFrom, "text": text,
	}, nil)
}

// JSONSet sets the value at a JSON pointer (RFC 6901, e.g. "/links/0")
// inside a json column, leaving the rest of the document as it is.
type JSONSet struct {
	At    string `json:"at"`
	Value any    `json:"value"`
}

// EditJSON applies sets to column (a json column) of the row of table
// with id: concurrent sets to other paths of the document merge.
func EditJSON(ctx context.Context, q Querier, table, id, column string, sets ...JSONSet) error {
	if sets == nil {
		sets = []JSONSet{}
	}
	return q.call(ctx, "edit", map[string]any{
		"table": table, "id": id,
		"edits": []map[string]any{{"kind": "jsonSet", "column": column, "edits": sets}},
	}, nil)
}
