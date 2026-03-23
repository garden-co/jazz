# Schema-Level Default Values — TODO (MVP)

## Goal

Make default values a first-class part of Jazz 2 table schemas, so a single default declared in the schema is used consistently for:

- TypeScript schema definitions.
- Generated `CREATE TABLE` SQL.
- Auto-generated lens defaults when a column is added or removed across schema versions.
- Typed insert APIs when a caller omits a field.

Today, defaults exist only in the migration DSL (`col.add(... { default })` / `col.drop(... { backwardsDefault })`). Base table schemas do not carry defaults, `CREATE TABLE ... DEFAULT ...` is parsed but discarded, and TS inserts map omitted fields to `Null`.

## User-facing API

### TypeScript DSL

Add schema-level defaults on column builders:

```ts
table("todos", {
  title: col.string(),
  done: col.boolean().default(false),
  priority: col.int().default(0),
  status: col.enum("todo", "done").default("todo"),
  archivedAt: col.timestamp().optional().default(null),
  tags: col.array(col.string()).default([]),
  ownerId: col.ref("users").default("00000000-0000-0000-0000-000000000001"),
});
```

Recommended shape:

- Add `.default(value)` to schema-context builders (`ScalarBuilder`, `EnumBuilder`, `JsonBuilder`, `RefBuilder`, `ArrayBuilder`).
- Preserve `.optional()` chaining.
- Type rule:
  - non-nullable builder: `.default(value: T)`
  - optional builder: `.default(value: T | null)`

This keeps migration defaults and schema defaults separate:

- schema default: part of steady-state schema and insert behavior
- lens default: per-migration row-transform behavior

### Semantics

- Omitted field on insert: use schema default if present.
- `undefined` on insert: treated the same as omitted.
- Explicit `null`: only allowed for nullable columns; it does not trigger the default.
- Explicit value: always wins over the default.
- Reads never synthesize defaults for stored rows except through schema-evolution lenses.

## TypeScript Changes

### Schema IR

Extend `packages/jazz-tools/src/schema.ts`:

- `Column` gains `default?: unknown`.

The DSL builders should store the declared default on the built column.

### Wasm/driver boundary

Extend `packages/jazz-tools/src/drivers/types.ts`:

- `ColumnDescriptor` gains `default?: Value`.

`schemaToWasm()` should serialize schema defaults into boundary `Value`s. This needs a dedicated helper; policy-literal conversion is too narrow.

Notes:

- `Timestamp` defaults normalize to `{ type: "Timestamp", value: number }`.
- `Json` defaults should be accepted as JS JSON values in the DSL, then serialized to the existing Rust/TS JSON representation (`Value::Text` with canonical JSON text).
- Container defaults (`Array`, `Bytea`) must be cloned when consumed so inserts do not reuse shared mutable references.

### Generated TS types

Update `packages/jazz-tools/src/codegen/type-generator.ts`:

- Row interfaces stay unchanged: a non-nullable defaulted column is still required in the row type.
- `Init` interfaces make defaulted columns optional, even when the stored column is non-nullable.

Example:

```ts
export interface Todo {
  id: string;
  done: boolean;
}

export interface TodoInit {
  done?: boolean;
}
```

### Insert conversion

Update `packages/jazz-tools/src/runtime/value-converter.ts`:

- When a column is omitted or `undefined`:
  - use `col.default` if present
  - else use `Null` for nullable columns
  - else throw a clear `"Missing required field 'x'"` error before calling into the runtime

This is the MVP write-path behavior for the main TS API (`db.insert(table, { ... })`).

## Rust Changes

### Schema types

Extend `crates/jazz-tools/src/query_manager/types/schema.rs`:

- `ColumnDescriptor` gains `default: Option<Value>`.
- Add a builder/helper like `.default(Value)` for Rust-side schema construction.

This field is schema metadata, not stored row data.

### Schema hashing

Extend `crates/jazz-tools/src/query_manager/types/branch.rs`:

- Schema hash must include the presence and value of column defaults.

Changing a default changes insert semantics, so it must produce a new `SchemaHash` even if row shape is unchanged.

### SQL parser and writer

Update `crates/jazz-tools/src/schema_manager/sql.rs`:

- `parse_column_def()` should stop discarding `DEFAULT` in `CREATE TABLE`.
- Reuse the existing default coercion logic used for lens parsing.
- `column_descriptor_to_sql()` should emit `DEFAULT ...` for schema columns.

This makes `CREATE TABLE ... DEFAULT ...` round-trip through the Rust schema parser/writer.

### Schema serialization / catalogue export

Because `crates/jazz-wasm/src/types.rs` re-exports Rust schema types directly, adding `default: Option<Value>` to `ColumnDescriptor` will surface it through:

- WASM
- NAPI
- schema catalogue responses

Old schemas must continue to deserialize with `default: None`.

### Auto-lens generation

Update `crates/jazz-tools/src/schema_manager/auto_lens.rs` and `diff.rs`:

- When a column is added and the new schema declares an explicit default, use that value for the generated `AddColumn`.
- When a column is removed and the old schema declared an explicit default, prefer that value for the generated `RemoveColumn` backward default.
- If no explicit schema default exists, keep current fallback heuristics in MVP to avoid breaking existing auto-lens behavior.

This preserves current behavior while making schema defaults authoritative when present.

### Lens application

`LensOp::AddColumn { default, .. }` already injects a value during row transformation. No semantic change is needed there; the change is where the default comes from.

### SchemaManager / runtime

Rust still needs to carry defaults in the active schema for:

- schema hashing
- catalogue export
- lens generation
- `CREATE TABLE` round-trip

MVP does not change the low-level positional Rust insert APIs. Schema-default application for omitted fields happens in the TS typed insert path.

If we later want defaults enforced uniformly for direct WASM/NAPI or Rust callers, we should add a named/partial insert boundary instead of overloading the positional `Vec<Value>` API.

## Default changes on existing columns

Changing a default on an existing column affects future inserts, not existing row contents.

MVP behavior:

- The schema hash changes.
- Generated TS init types and insert behavior use the new default.
- No row-transform lens op is required, because stored rows are unchanged.

This means default-only schema changes are metadata-only in MVP. A later follow-up can add dedicated default-alter SQL/lens ops if we need migration SQL parity.

## Interaction with manual migrations

Manual migration defaults still matter.

- Auto-generated lenses should use schema defaults when available.
- Hand-written migration DSL can still override a transform default for a specific schema edge.
- If both exist:
  - manual lens default wins for row transformation on that edge
  - schema default wins for future inserts under the new schema

This lets users backfill old rows one way while using a different default for newly created rows.

## Testing Strategy

### TypeScript

- DSL tests for `.default(...)` typing and builder output.
- `schemaToWasm()` tests for scalar, enum, array, bytea, json, timestamp, and nullable `null` defaults.
- `type-generator` tests verifying defaulted columns become optional in `Init` but not in row types.
- `value-converter` tests for:
  - omitted field uses schema default
  - `undefined` uses schema default
  - explicit `null` bypasses default on nullable column
  - missing required non-defaulted field throws

### Rust

- `schema_manager/sql.rs` tests for parsing and emitting `CREATE TABLE ... DEFAULT ...`.
- serde/catalogue tests verifying schema export includes defaults.
- schema hash tests verifying default changes alter the hash.
- auto-lens tests verifying explicit schema defaults override heuristic defaults.
- transformer/integration tests verifying old rows pick up the explicit schema default when a new column is added.

### End-to-end

- TS runtime integration test: create a row via `db.insert()` while omitting a defaulted non-nullable field; query result must contain the defaulted value.
- Cross-schema integration test: v1 row materialized in v2 after adding a defaulted column should see the schema default through the generated lens.

## Questions

1. Should schema defaults immediately replace the current heuristic auto-defaults for non-nullable added columns?

- No. Keep heuristics as fallback in MVP for compatibility, but prefer explicit schema defaults when present.

2. Do we want raw positional insert APIs to support defaults too?

- No in MVP. Keep schema-default application in the typed/named TS path and add a separate named Rust boundary later if needed.

3. Should we support default-only migration SQL (`ALTER COLUMN SET/DROP DEFAULT`)?

- Not in MVP. Treat it as metadata-only first; add a dedicated lens op later if operationally necessary.
