# Attempt 2: Schema-Driven SQLite Engine

Started: 2026-05-25 11:10 PDT.

Goal: build a small working system around schemas, layouts, plans, effects, and
whole-system tests. The implementation should discover architecture, not only
feature behavior.

## Guardrails

- Product-shaped integration tests first.
- Detailed decision/discovery log while context is fresh.
- Native Rust SQLite via `rusqlite`.
- Mutable fate on `jazz_tx` as baseline.
- Per-column conflict metadata from the start.
- Keep attempt1 under `reference/attempt1` for comparison.

## Decisions And Discoveries

### 2026-05-25 11:10 PDT

Starting attempt2 from a clean prep commit.

First target: a schema-driven local engine slice with a public-ish Rust API:

- define `projects` and `todos`
- open a durable client store
- apply one write transaction through schema-derived plans
- query open todos with a required project include
- capture result and dependency scope
- rebuild current projections byte-for-byte
- reopen the same SQLite file and reproduce the query

This is deliberately bigger than "insert a todo" because it pressures the
first abstractions immediately: layouts, row codecs, query plans, scope, and
projection rebuilds.

### 2026-05-25 11:11 PDT

First red test failed before compile due to workspace SQLite linkage:

```text
rusqlite 0.37 -> libsqlite3-sys 0.35
jazz-tools uses rusqlite 0.34 -> libsqlite3-sys 0.32
```

Cargo only allows one crate with `links = "sqlite3"` in the workspace graph.
Decision: attempt2 uses `rusqlite 0.34` for now, matching the existing
workspace. This is a workspace hygiene constraint, not an architectural choice.

### 2026-05-25 11:14 PDT

First product-shaped local test is green:

- schema builder defines `projects` and `todos`
- schema-derived DDL creates history/current tables for both
- one write call creates one `jazz_tx` and two row history/current entries
- joined query reads `todos` with required `project`
- query returns result scope plus dependency scope
- current projections rebuild byte-for-byte from history
- durable reopen reproduces the query and scope

Discovery: the first useful boundary is not a component object. It is the
schema-derived table plan: field list, physical tables, generated DDL, row
codecs, current projection columns, and fingerprint/rebuild shape all want to
come from one data artifact.

Discovery: even the tiny DDL generator caught a real layout bug. Quoted table
identifiers cannot be used as string prefixes for index identifiers. Physical
names and quoted SQL identifiers need to stay separate in the layout layer.

Discovery: the public-ish API is already doing useful pressure work. The test
did not call `insert_todo` or `query_open_todos_with_projects`; fixture tables
are concrete, but the engine path is schema-driven.
