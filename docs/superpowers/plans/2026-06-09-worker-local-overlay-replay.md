# Worker Local Overlay Replay Implementation Plan

> **For Guido:** REQUIRED SUB-SKILL: Use executing-plans to implement this plan task-by-task.

**Goal:** Replace retained-batch replay/reconciliation between worker and main with direct row-level local overlay hydration, without startup scans on main and without the `LocalBatchRecord` hydration path.

**Architecture:** The worker remains the owner of retained sealed submission metadata. During startup it sends only the row overlay entries needed by main. Main hydrates those entries into the existing query manager local overlay state (`pending_local_row_batches` plus `visible_rows_by_batch`) and lets normal row sync and fates clear them.

**Correctness Invariant:** Every row that still needs optimistic local overlay on main also still has a retained `SealedBatchSubmission` on the worker. This is true because sealed submission pruning is gated on either rejection or `confirmed_tier >= GlobalServer`, so local-only batches retain their submissions and the worker submission list is a complete source for rows that matter. Fate-only retained batches without submissions are inconsistent metadata for this path and must not trigger a startup scan.

## Preconditions

- Work on branch `guido/worker-local-overlay-replay`.
- Do not revert existing unrelated dirty workspace changes.
- Keep the implementation narrow: no index, no metadata migration, no generalized replay protocol.
- Preserve worker-side forensic helpers unless directly replaced by this path.

## Task 1: Add Query Manager Overlay Hydration Helpers

**Files:**

- `crates/jazz-tools/src/query_manager/manager.rs`
- `crates/jazz-tools/src/runtime_core/mod.rs`

**Changes:**

1. Add a query manager helper that hydrates one retained local overlay row by inserting a `RowBatchKey` into `pending_local_row_batches` and registering the table/row under `visible_rows_by_batch`.
2. Mark local subscriptions dirty for the table and row when hydrating.
3. Add a helper that returns pending overlay rows for a batch by intersecting `visible_rows_by_batch` with `pending_local_row_batches`.
4. Add a helper that clears pending overlay rows for a batch when a terminal fate or globally confirmed row makes the overlay obsolete.
5. Expose a `RuntimeCore` wrapper around overlay hydration using a small `RetainedLocalOverlayRow` struct.

**Acceptance:**

- No new parallel cache beyond existing query manager maps.
- Main can represent retained worker overlays without persisting `LocalBatchRecord`.

## Task 2: Replace Worker Retained Batch Payload With Overlay Entries

**Files:**

- `crates/jazz-tools/src/runtime_core/writes.rs`

**Changes:**

1. Add `retained_local_overlay_rows_for_worker_sync()`.
2. Build entries from retained `LocalBatchRecord` members, when present.
3. Build entries from retained sealed submissions by loading each member row locator with `load_row_locator`.
4. Do not inspect fate-only batches and do not call `local_batch_rows()` for this startup payload.
5. De-duplicate entries by `(table_name, object_id, branch_name, batch_id)` and sort deterministically.
6. Remove or stop using `local_batch_records_for_worker_sync()` from the worker startup path.

**Acceptance:**

- `scan_row_locators` is not called.
- `scan_history_row_batches` is not called.
- `load_row_locator` calls scale with retained submission members, not unrelated stored rows.

## Task 3: Switch The Worker Wire Protocol

**Files:**

- `crates/jazz-wasm/src/worker_protocol.rs`
- `crates/jazz-wasm/src/worker_host.rs`
- `crates/jazz-wasm/src/worker_bridge.rs`

**Changes:**

1. Replace `LocalBatchRecordsSync` with `LocalOverlaySync { entries }`.
2. Define a wire entry carrying table name, object id, branch name, and batch id.
3. Worker host sends `LocalOverlaySync` from `retained_local_overlay_rows_for_worker_sync()`.
4. Worker bridge hydrates each entry through the `RuntimeCore` overlay helper.
5. Remove bridge-side `LocalBatchRecord` decoding, hydration, rejection replay, and reconciliation from this retained startup path.

**Acceptance:**

- Main startup receives row overlays only.
- Main does not persist retained worker `LocalBatchRecord` values.
- Main does not call `reconcile_local_batch_with_server()` from worker retained startup sync.

## Task 4: Make Fate Handling Respect Overlay-Only State

**Files:**

- `crates/jazz-tools/src/runtime_core/ticks.rs`
- `crates/jazz-tools/src/query_manager/manager.rs`

**Changes:**

1. When a rejected fate arrives and pending overlay rows exist for that batch, clear those overlays directly instead of calling scan-backed local batch row lookup.
2. When a missing fate arrives and pending overlay rows exist for that batch, do not retransmit from main; the worker is the owner of the submission.
3. When a globally confirmed fate arrives and pending overlay rows exist for that batch, clear those overlays and dirty affected subscriptions.
4. Preserve existing local-batch behavior when no overlay-only rows are present.

**Acceptance:**

- Main does not need retained batch records to remove worker overlays.
- Existing worker/server reconciliation behavior remains available outside this startup overlay path.

## Task 5: Update And Add Tests

**Files:**

- `crates/jazz-tools/src/runtime_core/tests/write_batch/rejection_recovery.rs`
- `crates/jazz-tools/src/runtime_core/tests.rs`
- `crates/jazz-wasm/tests/worker_bridge.rs`

**Changes:**

1. Replace retained local-batch sync assertions with retained overlay entry assertions.
2. Add a no-scan test that fails if `scan_row_locators` or `scan_history_row_batches` are called.
3. In the no-scan test, assert `load_row_locator` calls equal the retained member count and do not grow after unrelated rows are inserted.
4. Add an assertion that fate-only batches without retained submissions do not produce overlay entries.
5. Update worker bridge tests to send `LocalOverlaySync` and assert main gets the optimistic overlay without a persisted local batch record.

**Acceptance:**

- The no-scan test forbids both scan functions.
- Point lookup counts scale with retained members, not total rows.
- Existing retained local UX remains covered through black-box runtime or bridge behavior.

## Task 6: Verification

Run targeted checks:

```bash
cargo test -p jazz-tools retained_local_overlay
cargo test -p jazz-tools rc_worker
cargo test -p jazz-wasm local_overlay
```

If a filter matches no tests, run the nearest package-level focused test command that compiles the touched crate and exercises the changed path.

Before finishing:

- Inspect `git diff --stat`.
- Inspect the touched-file diff.
- Ensure no unrelated dirty files were staged or modified.
