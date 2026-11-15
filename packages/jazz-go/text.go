package jazz

import "context"

// TextEdit is an edit of a mergeable text column (merge: "text",
// "markdown" or "html"): concurrent edits merge character by character.
type TextEdit struct {
	// Pos is a byte offset in the current UTF-8 text, as strings.Index and
	// slicing count; an offset inside a character is refused.
	Pos int `json:"pos"`
	// Delete is how many bytes from Pos to remove.
	Delete int `json:"delete"`
	// Insert is inserted at Pos.
	Insert string `json:"insert"`
}

// Splice is the edit replacing deleteLen bytes at pos with insert.
func Splice(pos, deleteLen int, insert string) TextEdit {
	return TextEdit{Pos: pos, Delete: deleteLen, Insert: insert}
}

// EditText applies edits, in order, to column of the row of table with
// id: each position counts in the text as the edits before it left it.
func EditText(ctx context.Context, q Querier, table, id, column string, edits ...TextEdit) error {
	if edits == nil {
		edits = []TextEdit{}
	}
	return q.call(ctx, "textEdit", map[string]any{
		"table": table, "id": id, "column": column, "edits": edits, "units": "utf8",
	}, nil)
}
