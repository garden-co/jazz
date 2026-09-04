# INV-QUERY-26

- Status: now
- Coverage: ✓

## Invariant

Per touched partition TopBy MUST emit the minimal consolidated weighted diff of retained windows; unchanged in-window copy counts MUST NOT emit, including rank-only moves, unless rank metadata is declared.

## Enforced by (tests)

`groove::db::tests::top_by_emits_weighted_diff_when_duplicate_copy_enters_window`; `groove::db::tests::top_by_suppresses_outside_window_changes`; `groove::db::tests::top_by_boundary_insert_and_delete_updates_window`

## Implementation

`groove/src/ivm/runtime/mod.rs::TickEvaluator::update_top_by`; `groove/src/ivm/runtime/mod.rs::top_by_window_before_from_deltas`; `groove/src/ivm/runtime/mod.rs::diff_record_windows`
