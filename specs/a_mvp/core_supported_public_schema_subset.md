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

Column defaults are accepted public schema metadata and lower into
`jazz::schema::ColumnSchema`. Core applies literal defaults at the write origin
for row-creation writes before policy dry-runs and before sealing the mutation
record. The sealed transaction remains self-contained: omitted defaulted columns
become explicit cells in the committed row/version payload, so the wire and
storage layers do not need an implicit-default rule.

The TypeScript public DSL keeps the type-level insert surface where defaulted
columns may be omitted, but it does not expand defaults itself. The same core
implementation therefore covers typed TS inserts, direct NAPI/WASM consumers,
server-side inserts, importers, and future bindings.

Explicit values are preserved. Explicit `null` is distinct from omission:
nullable columns store `null`, and non-nullable columns reject `null`.

🔶 Open question: dynamic defaults such as `now()` are not part of the current
literal default surface. If added, the spec must choose their evaluation point and
clock/session semantics before exposing them in public schema metadata.

## BIGINT Representation

Public `BIGINT` is executable as a signed 64-bit integer, matching PostgreSQL
`bigint` range semantics (`-9223372036854775808` through
`9223372036854775807`). The core storage value is `I64`; TypeScript client
values round-trip as JavaScript `bigint` when read from native row codecs, so
values outside `Number.MAX_SAFE_INTEGER` stay lossless.

Unsigned `U64` remains a distinct core type for timestamps and internal counters.
Public `BIGINT` must not lower to `U64`, and negative values are valid unless a
schema/policy constraint rejects them.

🔶 Open question: arbitrary-precision integer columns are not part of the
current executable subset. If needed later, they require a separate public type
and explicit storage, ordering, and wire semantics instead of widening
`BIGINT`.

## Known Unsupported Runtime Features

The subset currently excludes at least:

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
