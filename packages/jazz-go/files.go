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

// InsertStreaming inserts row into table with its bytes column streamed
// from r, a chunk at a time, so a large file is never held in memory
// whole. The row appears whole once the upload finishes; if reading r
// fails, or ctx ends before r is read to its end, nothing is written.
func InsertStreaming[N, R any](ctx context.Context, q Querier, table string, row N, column string, r io.Reader, opts ...WriteOption) (R, error) {
	var out R
	encoded, err := encode(q, row)
	if err != nil {
		return out, err
	}
	raw, err := upload(ctx, q, map[string]any{"table": table, "column": column, "kind": "insert", "row": encoded}, r, opts)
	if err != nil {
		return out, err
	}
	err = decodeRow(q.computed(), table, raw, &out)
	return out, err
}

// UpdateStreaming replaces the bytes column of the row of table with id
// by what r holds, streamed a chunk at a time.
func UpdateStreaming(ctx context.Context, q Querier, table, id, column string, r io.Reader, opts ...WriteOption) error {
	_, err := upload(ctx, q, map[string]any{"table": table, "column": column, "kind": "update", "id": id}, r, opts)
	return err
}

// UpsertStreaming writes the row of table with id (inserting it with row's
// columns if there is none) with its bytes column streamed from r.
func UpsertStreaming[N any](ctx context.Context, q Querier, table, id string, row N, column string, r io.Reader, opts ...WriteOption) error {
	encoded, err := encode(q, row)
	if err != nil {
		return err
	}
	_, err = upload(ctx, q, map[string]any{"table": table, "column": column, "kind": "upsert", "id": id, "row": encoded}, r, opts)
	return err
}

func upload(ctx context.Context, q Querier, begin map[string]any, r io.Reader, opts []WriteOption) (json.RawMessage, error) {
	if _, ok := q.(*Tx); ok {
		return nil, &Error{Kind: KindNotSupported, Message: "streaming uploads don't run inside a transaction"}
	}
	var begun struct {
		Upload string `json:"upload"`
	}
	abortLate := func(text string, err error) {
		var late struct {
			Upload string `json:"upload"`
		}
		if err == nil && json.Unmarshal([]byte(text), &late) == nil && late.Upload != "" {
			_ = q.call(context.Background(), "uploadAbort", map[string]any{"upload": late.Upload}, nil)
		}
	}
	if err := callOr(ctx, q, "uploadBegin", begin, &begun, abortLate); err != nil {
		return nil, err
	}
	abort := func() {
		_ = q.call(context.WithoutCancel(ctx), "uploadAbort", map[string]any{"upload": begun.Upload}, nil)
	}
	buf := make([]byte, StreamChunk)
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			chunk := base64.StdEncoding.EncodeToString(buf[:n])
			if err := q.call(ctx, "uploadPush", map[string]any{"upload": begun.Upload, "bytes": chunk}, nil); err != nil {
				abort()
				return nil, err
			}
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			abort()
			return nil, err
		}
	}
	// Nothing is written if ctx ends before the stream is complete.
	if err := ctx.Err(); err != nil {
		abort()
		return nil, err
	}
	var raw json.RawMessage
	err := q.call(ctx, "uploadFinish", withOptions(map[string]any{"upload": begun.Upload}, opts), &raw)
	return raw, err
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

// callOr is q's call, handing a result ctx's end returned from to
// abandoned (see DB.callOr).
func callOr(ctx context.Context, q Querier, method string, args map[string]any, out any, abandoned func(string, error)) error {
	if db, ok := q.(*DB); ok {
		return db.callOr(ctx, method, args, out, abandoned)
	}
	return q.call(ctx, method, args, out)
}
