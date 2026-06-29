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

## Known Unsupported Runtime Features

The subset currently excludes at least:

- column defaults
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
