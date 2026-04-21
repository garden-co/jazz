package jazzgo

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClientHealthAndSchemaRoutes(t *testing.T) {
	t.Helper()

	var sawSchemas bool
	var sawSchemaHash bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":     true,
				"status": "ok",
			})
		case "/apps/demo-app/schemas":
			sawSchemas = true
			_ = json.NewEncoder(w).Encode(SchemaHashesResponse{Hashes: []string{"hash-a", "hash-b"}})
		case "/apps/demo-app/schema/hash-a":
			sawSchemaHash = true
			_ = json.NewEncoder(w).Encode(StoredSchemaResponse{
				Schema: map[string]TableSchema{
					"trace_events": {
						Columns: []ColumnDescriptor{
							{Name: "session_id", ColumnType: ColumnType{Type: "Text"}},
						},
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "demo-app")
	ctx := context.Background()

	health, err := client.Health(ctx)
	if err != nil {
		t.Fatalf("Health returned error: %v", err)
	}
	if !health.OK || health.Status != "ok" {
		t.Fatalf("unexpected health response %#v", health)
	}

	hashes, err := client.SchemaHashes(ctx)
	if err != nil {
		t.Fatalf("SchemaHashes returned error: %v", err)
	}
	if len(hashes.Hashes) != 2 || hashes.Hashes[0] != "hash-a" {
		t.Fatalf("unexpected hashes %#v", hashes)
	}
	if !sawSchemas {
		t.Fatal("expected schemas endpoint to be called")
	}

	stored, err := client.SchemaByHash(ctx, "hash-a")
	if err != nil {
		t.Fatalf("SchemaByHash returned error: %v", err)
	}
	if _, ok := stored.Schema["trace_events"]; !ok {
		t.Fatalf("unexpected stored schema %#v", stored.Schema)
	}
	if !sawSchemaHash {
		t.Fatal("expected schema-by-hash endpoint to be called")
	}

	if got := client.AppWSURL(); got != "ws"+strings.TrimPrefix(server.URL, "http")+"/apps/demo-app/ws" {
		t.Fatalf("unexpected ws url %q", got)
	}
}
