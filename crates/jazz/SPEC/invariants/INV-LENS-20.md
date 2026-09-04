# INV-LENS-20

- Status: now
- Coverage: ✓

## Invariant

Published physical lineages and authored schema variants MUST NOT be automatically garbage-collected.

## Enforced by (tests)

`jazz::node::tests::catalogue_lenses::physical_schema_variants_survive_pointer_changes_and_reopen`

## Implementation

durable schema mappings and physical variant registries
