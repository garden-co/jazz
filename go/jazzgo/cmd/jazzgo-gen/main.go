package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/garden-co/jazz2/go/jazzgo"
)

func main() {
	var schemaPath string
	var packageName string
	var outPath string

	flag.StringVar(&schemaPath, "schema", "", "Path to exported Jazz schema JSON")
	flag.StringVar(&packageName, "pkg", "jazzapp", "Go package name for generated code")
	flag.StringVar(&outPath, "out", "", "Output file path (defaults to stdout)")
	flag.Parse()

	if schemaPath == "" {
		fmt.Fprintln(os.Stderr, "jazzgo-gen: -schema is required")
		os.Exit(2)
	}

	input, err := os.ReadFile(schemaPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "jazzgo-gen: read schema: %v\n", err)
		os.Exit(1)
	}

	schema, err := jazzgo.ParseSchemaJSON(input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "jazzgo-gen: parse schema: %v\n", err)
		os.Exit(1)
	}

	code, err := jazzgo.Generate(jazzgo.GenerateOptions{
		PackageName: packageName,
		Schema:      schema,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "jazzgo-gen: generate: %v\n", err)
		os.Exit(1)
	}

	if outPath == "" {
		_, _ = os.Stdout.WriteString(code)
		return
	}
	if err := os.WriteFile(outPath, []byte(code), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "jazzgo-gen: write output: %v\n", err)
		os.Exit(1)
	}
}
