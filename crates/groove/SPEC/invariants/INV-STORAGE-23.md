# INV-STORAGE-23

- Status: now
- Coverage: ✓

## Invariant

Durable unique indices MUST reject writing a positive delta for an index key already associated with a different record.

## Enforced by (tests)

`groove::db::tests::durable_unique_indices_reject_positive_delta_for_existing_different_record`

## Implementation

`ivm/runtime/persist.rs::apply_persist_delta`
