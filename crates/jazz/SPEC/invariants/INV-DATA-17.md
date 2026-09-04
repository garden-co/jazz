# INV-DATA-17

- Status: now
- Coverage: ✓

## Invariant

A stored row version MUST belong to exactly one physical layer: content with user cells or deletion-register state with `_deletion` and no user cells.

## Enforced by (tests)

`jazz::schema::tests::storage_lowering_declares_system_columns_by_shape`; `jazz::node::tests::catalogue_lenses::physical_deletion_register_spans_renamed_schemas_and_reopens`

## Implementation

`jazz/src/node/physical.rs::physical_version_storage_tables`; `jazz/src/node/codec.rs::{HistoryRowRecord,RegisterRowRecord}`
