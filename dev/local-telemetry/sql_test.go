package main

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/marcboeker/go-duckdb/v2"
)

func TestRefreshViewsCreatesEmptyLogsViewWithoutLogFiles(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	refreshViews(context.Background(), db, t.TempDir())

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM logs").Scan(&count); err != nil {
		t.Fatalf("query empty logs view: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected empty logs view, got %d rows", count)
	}
}
