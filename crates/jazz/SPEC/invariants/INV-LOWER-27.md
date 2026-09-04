# INV-LOWER-27

- Status: target
- Coverage: untested

## Invariant

A scalar enum's authored discriminant is scoped to its row `SchemaVersionId`; lowering translates it through the persistent case identity of its physical column occurrence before using a local storage tag, predicate, grouping key, ordering key, or projection. Concurrent sibling authored ordinal allocations cannot alias.

## Enforced by (tests)

NONE-FOUND

## Implementation

`jazz/src/node/physical.rs::physical_version_storage_tables`; enum case identity lowering
