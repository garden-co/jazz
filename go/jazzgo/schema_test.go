package jazzgo

import (
	"strings"
	"testing"
)

const rawSchemaFixture = `{
  "trace_events": {
    "columns": [
      { "name": "session_id", "column_type": { "type": "Text" }, "nullable": false },
      { "name": "turn_index", "column_type": { "type": "Integer" }, "nullable": false },
      { "name": "source", "column_type": { "type": "Enum", "variants": ["cursor", "codex", "claude"] }, "nullable": false },
      { "name": "payload", "column_type": { "type": "Json" }, "nullable": false },
      { "name": "created_at", "column_type": { "type": "Timestamp" }, "nullable": false },
      { "name": "branch_ids", "column_type": { "type": "Array", "element": { "type": "Uuid" } }, "nullable": true }
    ]
  }
}`

func TestParseSchemaJSONAcceptsRawSchema(t *testing.T) {
	schema, err := ParseSchemaJSON([]byte(rawSchemaFixture))
	if err != nil {
		t.Fatalf("ParseSchemaJSON(raw) returned error: %v", err)
	}

	table, ok := schema["trace_events"]
	if !ok {
		t.Fatalf("expected trace_events table, got %#v", schema)
	}
	if len(table.Columns) != 6 {
		t.Fatalf("expected 6 columns, got %d", len(table.Columns))
	}
	if got := table.Columns[2].ColumnType.Type; got != "Enum" {
		t.Fatalf("expected enum column type, got %q", got)
	}
	if got := strings.Join(table.Columns[2].ColumnType.Variants, ","); got != "cursor,codex,claude" {
		t.Fatalf("unexpected enum variants %q", got)
	}
	if got := table.Columns[5].ColumnType.Type; got != "Array" {
		t.Fatalf("expected array type, got %q", got)
	}
	if table.Columns[5].ColumnType.Element == nil || table.Columns[5].ColumnType.Element.Type != "Uuid" {
		t.Fatalf("expected uuid array element, got %#v", table.Columns[5].ColumnType.Element)
	}
}

func TestParseSchemaJSONAcceptsRuntimeEnvelope(t *testing.T) {
	enveloped := `{"__jazzRuntimeSchema":1,"loadedPolicyBundle":false,"schema":` + rawSchemaFixture + `}`
	schema, err := ParseSchemaJSON([]byte(enveloped))
	if err != nil {
		t.Fatalf("ParseSchemaJSON(envelope) returned error: %v", err)
	}

	if _, ok := schema["trace_events"]; !ok {
		t.Fatalf("expected trace_events table, got %#v", schema)
	}
}
