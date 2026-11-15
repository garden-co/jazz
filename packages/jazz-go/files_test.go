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
	r, _ = ReadBytes(ctx, db, "files", row.ID, "content")
	if got, _ := io.ReadAll(r); string(got) != "small" {
		t.Fatalf("after update: %q", got)
	}
}
