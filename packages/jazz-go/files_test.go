package jazz

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

const filesApp = `{ "format": 1, "tables": { "files": { "columns": { "name": "string", "content": "bytes" } } } }`

type fileRow struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Content []byte `json:"content"`
}

func TestStreamingFiles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := Open(ctx, Config{App: filesApp, Storage: StorageMemory})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Three chunks and a bit, so the appends meet at chunk boundaries.
	content := bytes.Repeat([]byte("0123456789abcdef"), (3*StreamChunk+100)/16)
	row, err := InsertStreaming[map[string]any, fileRow](ctx, db, "files", map[string]any{"name": "big.bin"}, "content", bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	r, err := ReadBytes(ctx, db, "files", row.ID, "content")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	if !bytes.Equal(got, content) {
		t.Fatalf("read %d bytes, wrote %d", len(got), len(content))
	}
	if err := UpdateStreaming(ctx, db, "files", row.ID, "content", bytes.NewReader([]byte("small"))); err != nil {
		t.Fatal(err)
	}
	// A reader that fails part-way writes nothing.
	before, _ := NewQuery[fileRow]("files").All(ctx, db)
	_, err = InsertStreaming[map[string]any, fileRow](ctx, db, "files", map[string]any{"name": "broken"}, "content",
		io.MultiReader(bytes.NewReader(content[:StreamChunk+1]), errReader{}))
	if err == nil {
		t.Fatal("a failing reader inserted")
	}
	if after, _ := NewQuery[fileRow]("files").All(ctx, db); len(after) != len(before) {
		t.Fatalf("rows: %d before, %d after a failed upload", len(before), len(after))
	}
	r, _ = ReadBytes(ctx, db, "files", row.ID, "content")
	if got, _ := io.ReadAll(r); string(got) != "small" {
		t.Fatalf("after update: %q", got)
	}
}

func TestFilesReadAPageAtATime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := Open(ctx, Config{App: filesApp, Storage: StorageMemory})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	content := bytes.Repeat([]byte("abcdefghij"), (2*StreamChunk+7)/10)
	row, err := InsertStreaming[map[string]any, fileRow](ctx, db, "files", map[string]any{"name": "f"}, "content", bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	page, err := ReadBytesRange(ctx, db, "files", row.ID, "content", 10, 20)
	if err != nil || string(page) != "abcdefghij" {
		t.Fatalf("page: %q %v", page, err)
	}
	got, err := io.ReadAll(OpenBytes(ctx, db, "files", row.ID, "content"))
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("paged read: %d of %d bytes, %v", len(got), len(content), err)
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, io.ErrClosedPipe }
