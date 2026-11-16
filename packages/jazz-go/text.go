package jazz

import (
	"context"
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

// EditText applies edits, in order, to column (a string column; for a
// mergeable text, they merge with edits others made meanwhile) of the row
// of table with id.
func EditText(ctx context.Context, q Querier, table, id, column string, edits ...TextEdit) error {
	splices := make([]map[string]any, len(edits))
	for i, e := range edits {
		if e.Pos < 0 || e.Delete < 0 {
			return &Error{Kind: KindRefused, Table: table, Column: column, Message: fmt.Sprintf(
				"edit %d (at %d, deleting %d) is outside the text", i, e.Pos, e.Delete)}
		}
		splices[i] = map[string]any{"pos": e.Pos, "delete": e.Delete, "insert": e.Insert}
	}
	return q.call(ctx, "textEdit", map[string]any{
		"table": table, "id": id, "column": column, "units": "utf8", "edits": splices,
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
