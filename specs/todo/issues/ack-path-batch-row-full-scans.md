# Ack-path batch row full scans

## What

Processing a confirmed batch fate can fall back to scanning the entire database. `local_batch_rows` tries sealed submission → record cache → stored record, and when all come up empty falls back to `scan_row_locators()` over every object plus `scan_history_row_batches` per object. `apply_received_batch_fate` calls it for every confirmed fate, so fate storms after bulk writes do O(db size) work per ack (~20% of client CPU in the bare-insert stress profile, alongside the recovery-scan issue fixed by the bare-insert scalability spec).

## Priority

medium

## Notes

- Where:
  - `crates/jazz-tools/src/runtime_core/ticks.rs` (`local_batch_rows`, fallback at ~lines 138-168; `apply_received_batch_fate` at ~line 231)
- Also on the ack path: per-fate `mark_subscriptions_visibility_recompute_for_tier` and per-row `scan_history_row_batches` costs.
- Direction: bound or index the fallback (e.g. batch-id → row-ids index, or drop the last-resort full scan and rely on the retained bookkeeping), and dedupe visibility recomputes across a burst of fates in one tick.
- Context: found while profiling the expo stress app freeze; see `specs/todo/a_mvp/bare_insert_scalability_fixes.md` for the incident evidence and repro test.
