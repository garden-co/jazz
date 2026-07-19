# Core-Supported Public Schema Subset

## Problem

The public schema/catalogue vocabulary is broader than the current core runtime
can execute. That is useful for migration planning, but dangerous when publish,
dev server, runtime open, and server conversion disagree about which shapes are
actually supported.

## Current Rule

Until the core supports the full public schema vocabulary, every path that
accepts a schema for execution should share one validation gate for the
**core-supported public schema subset**. Unsupported executable features should
fail before catalogue persistence or runtime open, not later in a server-specific
conversion path.

The gate should be used by:

- CLI/dev schema publish
- dev server schema loading
- native/WASM runtime open
- server public-schema conversion

## Default Lowering Boundary

Column defaults are accepted public schema metadata. They are not represented in
`jazz::schema::JazzSchema`, and the core runtime does not evaluate them during
insert validation.

For the TypeScript public DSL, literal column defaults are applied by the typed
client row-creation path before submission to the native/WASM runtime. An omitted
column with `s.<type>().default(value)` is expanded into an explicit cell in the
runtime-facing write record. Explicit values are preserved. Explicit `null` is
preserved for nullable columns and still rejected for non-nullable columns.

This keeps the wire/storage write explicit and keeps core validation strict:
core validates the cells it receives, but does not invent cells from public schema
metadata.

🔶 Open question: dynamic defaults such as `now()` are not part of the current
literal default surface. If added, the spec must choose their evaluation point and
clock/session semantics before exposing them in public schema metadata.

## Known Unsupported Runtime Features

The subset currently excludes at least:

- `BigInt`
- `BatchId`
- `Json`
- nested `Row`
- `GSet` merge strategy

These features may still exist in catalogue or migration-oriented types, but
that must be explicit. If a feature is catalogue-only, it should not be accepted
as an executable runtime schema without a clear downgrade or lowering rule.

## Desired Shape

One validator should produce the same structured error everywhere. The schema
format boundaries should be named and tested separately:

- catalogue schema payload
- runtime schema JSON/envelope
- native core postcard schema

Each format may have different serialization concerns, but the executable
feature subset must be shared.

## Open Questions

🔶 Open question: literal-vs-column-type coercion should be specified as a
general query and authorization lowering rule, not case-by-case. For example,
when a policy predicate compares a UUID literal against a `String` column, the
runtime lowering contract should say whether and where that literal is coerced
before evaluation.
