# Incremental Window Maintenance

Date: 2026-07-22
Branch: `track/incremental-window-maintenance`

## Findings

- CLEARLY-GOOD: `TopBy`, `ArgMinBy`, and `ArgMaxBy` no longer derive each
  touched group's before/after answer by cloning all positive records from the
  shared arrangement and sorting/scanning the full group for every input batch.
- CLEARLY-GOOD: The runtime now keeps operator-owned per-group ordered state.
  `TopByState` is keyed by `(sort key, full record bytes)` and `ArgByState` is
  keyed by `(primary key, full record bytes)`, preserving passenger-column
  update semantics where an in-window value-only update emits the same
  retract+insert pair as the previous full-record reconstruction.
- CLEARLY-GOOD: The shared arrangement is still maintained for other operators
  and reusable graph state, but the window output delta is computed from the
  operator state by taking the previous window, applying only the current input
  deltas, and diffing the new window.
- CLEARLY-GOOD: Hydration in 256-record batches becomes incremental across
  batches because each batch inserts into the already-sorted per-group state
  instead of rebuilding the complete group window from the arrangement.
- SPECULATIVE: The state currently keeps the full positive per-group ordered
  multiset rather than a bounded top-window plus margin. This avoids a margin
  sizing policy decision and never falls back to an O(group) recompute on
  boundary removals, but it duplicates record bytes already present in the
  arrangement. A bounded-margin variant can reduce memory later if there is a
  measured need and an explicit policy for fallback frequency.

## Scale Receipt

Harness:

```text
cargo test -p groove maintained_top_by_one_group_60k_in_256_record_batches_receipt -j 2 -- --ignored --nocapture
```

Scenario: one unpartitioned `history` group represented as `row = 1`, 60,000
records inserted through a maintained `TopBy` subscription with `limit(100)`,
committed in 256-record batches. The receipt test asserts the final top-100
window is rows `stamp = 0..99` and reports elapsed wall time for the batched
commits plus subscription delivery.

Before, temporary baseline worktree at `e8e6aa7b5` with only the receipt test
added:

```text
maintained_top_by_one_group_60k_in_256_record_batches rows=60000 batch=256 limit=100 elapsed_ms=48209.948 delivered_delta_records=100
```

After, this worktree:

```text
maintained_top_by_one_group_60k_in_256_record_batches rows=60000 batch=256 limit=100 elapsed_ms=1389.558 delivered_delta_records=100
```

Result: about 34.7x faster on this native receipt (`48209.948 / 1389.558`).

## Validation Notes

- The receipt test is ignored because it is timing-only and too large for the
  ordinary groove suite.
- Exact output-delta semantics are covered by the existing top-by and arg-by
  regression tests in `cargo test -p groove -j 2`; the full-record state key is
  the relevant preservation mechanism for value-only in-window updates.

Tooling-friction: a checked-in native microbench harness that can run old/new
code against the same compiled target and emit JSON receipts would have saved
the temporary baseline worktree and manual timing capture.
