# INV-TICK-13

- Status: now
- Coverage: ✓

## Invariant

A `Persist` node MUST consolidate all same-tick deltas by durable key before writing storage. A unique persist target MAY replace a stored owner only when that owner's net weight is fully retracted in the same tick and MUST reject the write unless at most one positive owner remains; same-tick reads MUST see staged durable writes through the tick overlay.

## Enforced by (tests)

`groove::db::tests::{durable_unique_indices_allow_atomic_replacement_within_one_batch,persist_consolidates_same_tick_deltas_and_rejects_unique_conflicts}`; `groove::storage::tests::staged_overlay_reads_staged_sets_and_deletes_before_base_storage`

## Implementation

groove/src/ivm/runtime/persist.rs::apply_persist_delta; groove/src/storage/mod.rs::StagedWriteOverlay
