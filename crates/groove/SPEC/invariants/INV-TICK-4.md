# INV-TICK-4

- Status: now
- Coverage: ✓

## Invariant

Same-key operations in one `DatabaseBatch` MUST compute deltas against prior operations in that batch, not only against pre-batch storage, and table deltas MUST be consolidated before ticking.

## Enforced by (tests)

`groove::db::tests::same_key_writes_in_one_batch_emit_deltas_against_earlier_batch_writes`; `groove::db::tests::same_batch_same_key_operations_emit_only_the_consolidated_final_delta`

## Implementation

groove/src/db/storage_helpers.rs::compute_table_deltas
