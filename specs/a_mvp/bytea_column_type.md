# `bytea` Column Type — TODO (MVP)

Implements gap #1 from [`built_in_file_storage.md`](../todo/a_mvp/built_in_file_storage.md): first-class binary column support for `file_parts.data`.

## Goal

Add a native `bytea` type across SQL parser, schema IR, runtime values, and WASM/TS bridges so file chunks can be stored as row data without text/base64 workarounds.

## Scope

- Add `bytea` to SQL schema parsing and generation.
- Add binary type/value variants in Rust core and WASM boundary.
- Add JS runtime conversion helpers for binary values.
- Enforce per-cell size limit: `<= 1 MiB`.
- Cover core read/write/filter semantics and migration/lens compatibility.

Out of scope:

- File chunking, content addressing, upload/download helpers (tracked by `built_in_file_storage.md`).
- Compression/encryption policy.

## Semantics

### Type and Value Model

- New column type: `ColumnType::Bytea`.
- New value type: `Value::Bytea(Vec<u8>)`.
- WASM bridge adds matching `WasmColumnType::Bytea` and `WasmValue::Bytea`.
- TS runtime exposes binary values as `Uint8Array` in API-facing transforms.

### SQL

- Accept `BYTEA` in `CREATE TABLE` and lens parsing.
- Preserve `BYTEA` in SQL generation/round-trip.
- Arrays are valid (`BYTEA[]`) via existing array suffix handling.

### Constraints

- Hard limit on encoded payload length per `bytea` cell: `1_048_576` bytes.
- Enforced on insert/update and any schema-transform write path.
- Over-limit writes fail deterministically with a dedicated error.

### Query/Policy Behavior

- Equality/inequality comparisons on `bytea` are supported (`=`/`!=`) by exact byte match.
- Ordering operators (`<`, `>`, etc.) are unsupported for `bytea` in MVP and must fail clearly.
- Null semantics match existing nullable columns.

## Invariants

- Binary round-trip is lossless, including `0x00` bytes.
- Stored bytes are never transcoded (no UTF-8 or base64 mutation in core storage).
- Size limit is enforced uniformly regardless of entry point (Rust API, WASM API, schema manager).

## Testing Strategy

### Parser/Schema Tests

- Parse `CREATE TABLE file_parts (data BYTEA NOT NULL);`.
- Parse/generate round-trip for `BYTEA` and `BYTEA[]`.
- Ensure references are unaffected by `BYTEA` introduction.

### Encoding/Runtime Tests

- Encode/decode `Value::Bytea` across row encoding and decoding.
- Verify null + nullable behavior.
- Reject payload `> 1 MiB`.

### WASM/TS Bridge Tests

- `WasmValue::Bytea` round-trip between JS and Rust.
- `value-converter` and `row-transformer` preserve byte equality and type shape.

### Query/Policy Tests

- `Eq`/`Ne` predicates on `bytea` values.
- Attempting range comparisons on `bytea` returns an explicit unsupported-type error.

## Dependency on Built-in File Storage

This feature is a prerequisite for `file_parts.data bytea` in [`built_in_file_storage.md`](../todo/a_mvp/built_in_file_storage.md). Without it, chunk rows cannot be represented as typed binary columns.
