package jazz

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
)

// StreamChunk is how many bytes a streaming write sends per call.
const StreamChunk = 256 << 10

// InsertStreaming inserts row into table, then streams r into its bytes
// column, a chunk at a time, so a large file is never held in memory
// whole. If reading r or a write fails, the row is deleted again.
func InsertStreaming[N, R any](ctx context.Context, q Querier, table string, row N, column string, r io.Reader) (R, error) {
	encoded, err := encode(q, row)
	if err != nil {
		var zero R
		return zero, err
	}
	fields, err := Fields(encoded)
	if err != nil {
		var zero R
		return zero, err
	}
	fields[column] = []byte{}
	inserted, err := Insert[map[string]any, R](ctx, q, table, fields)
	if err != nil {
		return inserted, err
	}
	var id struct {
		ID string `json:"id"`
	}
	if b, err := json.Marshal(inserted); err == nil {
		_ = json.Unmarshal(b, &id)
	}
	if err := appendStream(ctx, q, table, id.ID, column, 0, r); err != nil {
		_ = remove(context.WithoutCancel(ctx), q, table, id.ID, nil)
		var zero R
		return zero, err
	}
	return inserted, nil
}

// UpdateStreaming replaces the bytes column of the row of table with id
// by what r holds, streamed a chunk at a time.
func UpdateStreaming(ctx context.Context, q Querier, table, id, column string, r io.Reader) error {
	if err := update(ctx, q, table, id, map[string]any{column: []byte{}}, nil); err != nil {
		return err
	}
	return appendStream(ctx, q, table, id, column, 0, r)
}

func appendStream(ctx context.Context, q Querier, table, id, column string, at int64, r io.Reader) error {
	buf := make([]byte, StreamChunk)
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			edit := map[string]any{
				"kind":    "splice",
				"column":  column,
				"within":  map[string]any{"units": "bytes", "from": at, "to": at},
				"splices": []map[string]any{{"at": 0, "delete": 0, "insert": base64.StdEncoding.EncodeToString(buf[:n])}},
			}
			if err := q.call(ctx, "edit", map[string]any{"table": table, "id": id, "edits": []any{edit}}, nil); err != nil {
				return err
			}
			at += int64(n)
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// ReadBytes is the bytes column of the row of table with id, as a reader.
func ReadBytes(ctx context.Context, q Querier, table, id, column string) (io.ReadCloser, error) {
	var row map[string]json.RawMessage
	query := map[string]any{
		"table":  table,
		"where":  []map[string]any{{"column": "id", "op": "eq", "value": id}},
		"select": []string{column},
	}
	if err := q.call(ctx, "one", map[string]any{"query": query}, &row); err != nil {
		return nil, err
	}
	if row == nil {
		return nil, &Error{Kind: KindRefused, Table: table, Message: table + " has no row " + id}
	}
	var content []byte
	if err := json.Unmarshal(row[column], &content); err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(content)), nil
}

// Upsert writes row (an insert struct) as the row of table with id:
// inserted if there is none, updated if there is. It returns the row.
func Upsert[N, R any](ctx context.Context, q Querier, table, id string, row N, opts ...WriteOption) (R, error) {
	var out R
	encoded, err := encode(q, row)
	if err != nil {
		return out, err
	}
	var raw json.RawMessage
	if err := q.call(ctx, "upsert", withOptions(map[string]any{"table": table, "id": id, "row": encoded}, opts), &raw); err != nil {
		return out, err
	}
	err = decodeRow(q.computed(), table, raw, &out)
	return out, err
}

// Restore brings back the deleted row of table with id (with patch's
// fields set, if patch isn't nil), and returns it.
func Restore[R any](ctx context.Context, q Querier, table, id string, patch any) (R, error) {
	var out R
	args := map[string]any{"table": table, "id": id}
	if patch != nil {
		encoded, err := encode(q, patch)
		if err != nil {
			return out, err
		}
		args["patch"] = encoded
	}
	var raw json.RawMessage
	if err := q.call(ctx, "restore", args, &raw); err != nil {
		return out, err
	}
	err := decodeRow(q.computed(), table, raw, &out)
	return out, err
}

// ReadBytesRange is bytes [from, to) of the bytes column of the row of
// table with id (clamped to its end): a page of a large file.
func ReadBytesRange(ctx context.Context, q Querier, table, id, column string, from, to int64) ([]byte, error) {
	var row map[string]json.RawMessage
	query := map[string]any{
		"table":  table,
		"where":  []map[string]any{{"column": "id", "op": "eq", "value": id}},
		"select": []any{map[string]any{"column": column, "from": from, "to": to, "units": "bytes"}},
	}
	if err := q.call(ctx, "one", map[string]any{"query": query}, &row); err != nil {
		return nil, err
	}
	if row == nil {
		return nil, &Error{Kind: KindRefused, Table: table, Message: table + " has no row " + id}
	}
	var content []byte
	err := json.Unmarshal(row[column], &content)
	return content, err
}

// OpenBytes reads the bytes column of the row of table with id a page at
// a time (StreamChunk bytes per read from Jazz), for files too large to
// hold in memory.
func OpenBytes(ctx context.Context, q Querier, table, id, column string) io.ReadCloser {
	return &pageReader{ctx: ctx, q: q, table: table, id: id, column: column}
}

type pageReader struct {
	ctx               context.Context
	q                 Querier
	table, id, column string
	at                int64
	page              []byte
	done              bool
}

func (r *pageReader) Read(p []byte) (int, error) {
	if len(r.page) == 0 {
		if r.done {
			return 0, io.EOF
		}
		page, err := ReadBytesRange(r.ctx, r.q, r.table, r.id, r.column, r.at, r.at+StreamChunk)
		if err != nil {
			return 0, err
		}
		r.at += int64(len(page))
		r.page = page
		if len(page) < StreamChunk {
			r.done = true
		}
		if len(page) == 0 {
			return 0, io.EOF
		}
	}
	n := copy(p, r.page)
	r.page = r.page[n:]
	return n, nil
}

func (r *pageReader) Close() error { return nil }
