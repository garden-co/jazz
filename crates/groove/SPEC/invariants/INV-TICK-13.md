# INV-TICK-13

- Status: now
- Coverage: ✓

## Invariant

A `Persist` node MUST consolidate all same-tick deltas by durable key before writing storage, and a unique persist target MUST reject a positive delta that conflicts with another stored record for the same key; same-tick reads MUST see staged durable writes through the tick overlay.

## Enforced by (tests)

`groove::db::tests::persist_consolidates_same_tick_deltas_and_rejects_unique_conflicts`; `groove::storage::tests::staged_overlay_reads_staged_sets_and_deletes_before_base_storage`

## Implementation

groove/src/ivm/runtime/persist.rs::apply_persist_delta; groove/src/storage/mod.rs::StagedWriteOverlay
