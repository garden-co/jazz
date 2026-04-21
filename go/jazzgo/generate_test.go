package jazzgo

import (
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func TestGenerateBuildsTypedGoModels(t *testing.T) {
	schema, err := ParseSchemaJSON([]byte(rawSchemaFixture))
	if err != nil {
		t.Fatalf("ParseSchemaJSON returned error: %v", err)
	}

	code, err := Generate(GenerateOptions{
		PackageName: "traceapp",
		Schema:      schema,
	})
	if err != nil {
		t.Fatalf("Generate returned error: %v", err)
	}

	wants := []string{
		"type TraceEvent struct {",
		"type TraceEventInsert struct {",
		"type TraceEventSource string",
		`TraceEventSourceCursor TraceEventSource = "cursor"`,
		"SessionID string",
		"Payload   json.RawMessage",
		"var TraceEventsTable = TableDefinition{",
	}
	for _, want := range wants {
		if !strings.Contains(code, want) {
			t.Fatalf("generated code missing %q\n%s", want, code)
		}
	}

	if _, err := parser.ParseFile(token.NewFileSet(), "generated.go", code, parser.AllErrors); err != nil {
		t.Fatalf("generated code does not parse: %v\n%s", err, code)
	}
}
