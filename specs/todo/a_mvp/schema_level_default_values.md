# Schema-Level Default Values — TODO (MVP)

## Goal

Make default values a first-class part of Jazz 2 table schemas, so a single default declared in the schema is used consistently for:

- TypeScript schema definitions.
- Generated `CREATE TABLE` SQL.
- Auto-generated lens defaults when a column is added or removed across schema versions.
- Insert APIs when a caller omits a field.

Today, defaults exist only in the migration DSL (`col.add(... { default })` / `col.drop(... { backwardsDefault })`). Base table schemas do not carry defaults, `CREATE TABLE ... DEFAULT ...` is parsed but discarded, and the current insert flow materializes missing TS fields as `Null` before Rust can distinguish omission from an explicit null.

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

### Runtime bridge

Update the TS runtime bridge so `db.insert(table, { ... })` preserves omission information and delegates default materialization to Rust.

The TS side should not eagerly turn a missing field into `Null` for inserts. It should pass a plain JS object keyed by field name to a schema-aware Rust insert path, which should deserialize to `HashMap<String, Value>` and distinguish:

- omitted / `undefined`
- explicit `null`
- explicit concrete value

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

### Insert path

Use omission-preserving named input in the higher-level Rust insert APIs above `QueryManager::insert(table, &[Value])`.

This path should:

- accept omission-preserving named/partial input as `HashMap<String, Value>`
- materialize schema defaults for omitted or `undefined` fields
- allow explicit `null` only for nullable columns
- reject missing required non-defaulted fields before row encoding

The low-level positional `QueryManager::insert(table, &[Value])` can remain exact-value oriented. Default application should happen one layer above it, where the schema and field-presence information are both available.

### SchemaManager / runtime

Rust still needs to carry defaults in the active schema for:

- schema hashing
- catalogue export
- lens generation
- `CREATE TABLE` round-trip
- schema-aware inserts

MVP should apply defaults in Rust so the behavior is shared across WASM, NAPI, and any future Rust-native callers that use the schema-aware insert path.

## Default changes on existing columns

Changing a default on an existing column affects future inserts, not existing row contents.

MVP behavior:

- The schema hash changes.
- Generated TS init types and Rust insert behavior use the new default.
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
- TS runtime bridge tests verifying omitted fields remain omitted across the TS -> Rust insert boundary, while explicit `null` remains explicit.

### Rust

- `schema_manager/sql.rs` tests for parsing and emitting `CREATE TABLE ... DEFAULT ...`.
- serde/catalogue tests verifying schema export includes defaults.
- schema hash tests verifying default changes alter the hash.
- auto-lens tests verifying explicit schema defaults override heuristic defaults.
- transformer/integration tests verifying old rows pick up the explicit schema default when a new column is added.
- runtime/schema-manager insert tests for:
  - omitted field uses schema default
  - `undefined` uses schema default
  - explicit `null` bypasses the default and is validated against nullability
  - missing required non-defaulted field throws

### End-to-end

- TS runtime integration test: create a row via `db.insert()` while omitting a defaulted non-nullable field; query result must contain the defaulted value.
- Cross-schema integration test: v1 row materialized in v2 after adding a defaulted column should see the schema default through the generated lens.

## Questions

1. Should schema defaults immediately replace the current heuristic auto-defaults for non-nullable added columns?

- No. Keep heuristics as fallback in MVP for compatibility, but prefer explicit schema defaults when present.

2. Do we want raw positional insert APIs to support defaults too?

- No. Apply defaults in the schema-aware Rust insert path, not in `QueryManager::insert(table, &[Value])`. If we ever need low-level positional defaults, that should be a separate API rather than implicit behavior on `Vec<Value>`.

3. Should we support default-only migration SQL (`ALTER COLUMN SET/DROP DEFAULT`)?

- Not in MVP. Treat it as metadata-only first; add a dedicated lens op later if operationally necessary.

## Implementation Tasks

Recommended execution order: start in Rust so schema defaults become real schema metadata first, then add the schema-aware Rust insert path, then expose that through the runtime boundary, and then add the TypeScript DSL and typed API changes on top.

- [x] Rust schema core:
      Add `default: Option<Value>` to `ColumnDescriptor`, add a builder/helper for setting it, and ensure old serialized schemas still deserialize with `default: None`.

- [x] Rust schema hashing:
      Update schema hashing so column defaults affect `SchemaHash`, and add tests proving a default-only change produces a new hash.

- [x] Rust SQL parser and writer:
      Stop discarding `DEFAULT` in `CREATE TABLE`, reuse default coercion logic for schema columns, emit schema defaults from `column_descriptor_to_sql()`, and add round-trip tests.

- [x] Rust schema serialization and boundaries:
      Verify `ColumnDescriptor.default` flows through WASM, NAPI, and catalogue export, and add serde/catalogue tests for explicit defaults and absent defaults.

- [x] Rust schema-aware insert path:
      Use omission-preserving `HashMap<String, Value>` input in the higher-level Rust insert APIs above `QueryManager::insert(table, &[Value])`, materialize defaults, validate explicit nulls, and error on missing required non-defaulted fields.

- [x] Rust auto-lens and diffing:
      Update `auto_lens.rs` and `diff.rs` so explicit schema defaults are used for generated `AddColumn` and `RemoveColumn` defaults before falling back to the current heuristics.

- [x] TypeScript shared schema/value boundary:
      Add `default?: Value` to `packages/jazz-tools/src/drivers/types.ts` and implement a dedicated schema-default-to-`Value` conversion path in `schema-reader.ts`.

- [x] TypeScript schema IR:
      Add `default?: unknown` to `packages/jazz-tools/src/schema.ts` and thread it through built columns.

- [x] TypeScript DSL builders:
      Add `.default(...)` to schema-context builders (`ScalarBuilder`, `EnumBuilder`, `JsonBuilder`, `RefBuilder`, `ArrayBuilder`), preserve `.optional()` chaining, and add DSL typing/runtime tests.

- [ ] TypeScript codegen:
      Update generated `Init` interfaces so defaulted columns are optional while row/result interfaces remain unchanged, and add codegen assertions for that behavior.

- [ ] End-to-end tests:
      Add integration coverage showing `db.insert()` can omit a defaulted non-nullable field and still persist the defaulted value, plus a cross-schema evolution test covering defaulted added columns.

- [ ] Documentation cleanup:
      Update schema docs/examples so schema defaults and migration/lens defaults are clearly distinguished, including which one affects future inserts versus schema-evolution transforms.
