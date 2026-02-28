# Enum Column Type Design

## Overview

Add a first-class enum column type to the TypeScript DSL:

```ts
col.enum("variant1", "variant2");
```

The feature must propagate end-to-end:

- TS DSL/schema AST
- SQL generation
- Rust SQL parsing and schema representation
- native schema encoding/hash/diff/lens logic
- WASM/NAPI schema bridges
- runtime value validation
- generated TypeScript types/query APIs

## Architecture / Components

### 1. TypeScript DSL and schema model

Files to touch:

- `packages/jazz-tools/src/dsl.ts`
- `packages/jazz-tools/src/schema.ts`
- `packages/jazz-tools/src/sql-gen.ts`
- `packages/jazz-tools/src/index.ts` (type exports only if needed)

Changes:

- Add enum sql type shape in TS schema model, e.g.:
  - `type EnumSqlType = { kind: "ENUM"; variants: string[] }`
  - include in `SqlType` union.
- Add `col.enum(...variants)` in schema DSL and migration DSL (`col.add().enum(...)` / `col.drop().enum(...)`) with parity across schema and lens workflows.
- Add validation in DSL:
  - at least one variant
  - unique variants
  - no empty-string variants
- SQL generation path emits enum type syntax: `ENUM('a','b',...)`.

Core TS model sketch:

```ts
export interface EnumSqlType {
  kind: "ENUM";
  variants: readonly string[];
}

export type SqlType = ScalarSqlType | ArraySqlType | EnumSqlType;
```

### 2. TS codegen/runtime surface

Files to touch:

- `packages/jazz-tools/src/codegen/schema-reader.ts`
- `packages/jazz-tools/src/codegen/type-generator.ts`
- `packages/jazz-tools/src/codegen/query-builder-generator.ts`
- `packages/jazz-tools/src/runtime/value-converter.ts`
- `packages/jazz-tools/src/runtime/query-adapter.ts`
- `packages/jazz-tools/src/drivers/types.ts` (consumed generated type union from `jazz-wasm`)

Changes:

- Map DSL enum sql type to wasm/native column type (new enum variant, not plain text).
- Generate string-literal unions in app types:
  - `status: "draft" | "published"`
- Where-input generation should stay string-based but narrow to enum union.
- Value conversion must enforce enum membership on insert/update before crossing into native runtime.

### 3. Rust core schema type and runtime compatibility

Files to touch:

- `crates/jazz-tools/src/query_manager/types/schema.rs`
- `crates/jazz-tools/src/query_manager/types/value.rs`
- `crates/jazz-tools/src/query_manager/types/branch.rs`
- `crates/jazz-tools/src/query_manager/encoding.rs`
- `crates/jazz-tools/src/query_manager/types/tests.rs`

Changes:

- Add `ColumnType::Enum(Vec<String>)`.
- Preserve enum metadata in schema hash with normalized variant ordering (order-insensitive hash behavior).
- Keep storage representation text-compatible (encode/decode as string bytes), but enforce type compatibility:
  - `Value::Text("draft")` is accepted for `ColumnType::Enum(["draft", ...])`.
  - reject text not present in variant list.

Core Rust sketch:

```rust
pub enum ColumnType {
    Integer,
    BigInt,
    Boolean,
    Text,
    Timestamp,
    Uuid,
    Enum(Vec<String>),
    Array(Box<ColumnType>),
    Row(Box<RowDescriptor>),
}
```

### 4. Rust SQL parser/emitter and schema tooling

Files to touch:

- `crates/jazz-tools/src/schema_manager/sql.rs`
- `crates/jazz-tools/src/schema_manager/files.rs`
- `crates/jazz-tools/src/schema_manager/encoding.rs`
- `crates/jazz-tools/src/schema_manager/diff.rs`
- `crates/jazz-tools/src/schema_manager/auto_lens.rs`

Changes:

- Parse enum SQL type and variants into `ColumnType::Enum`.
- Emit enum SQL from schema/lens generation.
- Add catalogue encoding tags for enum type + variant payload.
- Include enum handling in defaults/draft decisions for auto-generated lenses.
- Update migration TS stub rendering in `files.rs` to output enum builder forms.

### 5. WASM/NAPI bridge types

Files to touch:

- `crates/jazz-wasm/src/types.rs`
- `crates/jazz-napi/src/lib.rs`

Changes:

- Add bridge type variant:
  - `WasmColumnType::Enum { variants: Vec<String> }`
  - NAPI JSON bridge equivalent.
- Implement both directions conversion between bridge types and `ColumnType::Enum`.

## Data Models

Proposed schema-level enum model:

- schema type owns canonical list of variants.
- value-level representation remains string (`Text`) for row payloads.
- enum constraints are enforced by schema-aware validation layers in both TS runtime and native Rust runtime.

This keeps wire/value formats stable while adding stricter type semantics.

## Testing Strategy

### TypeScript tests

- `packages/jazz-tools/src/sql-gen.test.ts`
  - enum column SQL output
  - array(enum) output if supported
- `packages/jazz-tools/src/codegen/codegen.test.ts`
  - wasm conversion and generated TS unions
  - where-input enum narrowing
- `packages/jazz-tools/src/runtime/value-converter.test.ts`
  - accepts known variants, rejects unknown values
- `packages/jazz-tools/src/runtime/query-adapter.test.ts`
  - enum conditions serialize to text literals correctly

### Rust tests

- `crates/jazz-tools/src/schema_manager/sql.rs` tests
  - parse/emit/roundtrip enum column definitions
- `crates/jazz-tools/src/schema_manager/encoding.rs` tests
  - encode/decode schema with enum metadata
- `crates/jazz-tools/src/query_manager/encoding.rs` tests
  - insert/update validation for enum membership
- `crates/jazz-tools/src/query_manager/types/tests.rs`
  - fixed/variable typing expectations and hash stability behavior

### Integration

- Existing TS DSL -> `current.sql` -> native build pipeline test path (`packages/jazz-tools/src/cli.test.ts`) with enum schema fixture.

## Missing Coverage Check

Coverage appears complete for request scope (TS DSL to native runtime) with all policy questions resolved.

## Decisions

1. Use first-class `ENUM('a','b',...)` SQL syntax.
2. Support `col.add().enum(...)` and `col.drop().enum(...)` now.
3. Normalize variant ordering for schema hashing.
4. Fail fast in both TypeScript and native runtime validation paths.
