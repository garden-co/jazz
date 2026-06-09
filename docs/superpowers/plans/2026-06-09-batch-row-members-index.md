# Batch Row Members Index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace retained-batch row discovery with a storage-maintained `batch_id -> rows` index so replay and startup reconciliation do not call broad row-history scans.

**Architecture:** Every history-row write records a compact batch membership row keyed by batch id. Retained-batch replay loads indexed members, then exact-loads `(table, branch, row_id, batch_id)` history rows; sealed submissions and local batch records can enrich metadata, but they no longer require `scan_history_row_batches` to locate rows. Startup reconciliation scans the membership index instead of row locators plus per-row history.

**Tech Stack:** Rust storage trait, in-memory storage, SQLite/RocksDB/OPFS raw table backends, existing runtime-core integration tests.

---

### Task 1: Storage Membership Index

**Files:**

- Modify: `crates/jazz-tools/src/storage/mod.rs`
- Modify: `crates/jazz-tools/src/storage/storage_trait.rs`
- Modify: `crates/jazz-tools/src/storage/memory.rs`
- Modify: `crates/jazz-tools/src/storage/rocksdb.rs`
- Modify: `crates/jazz-tools/src/storage/sqlite.rs`
- Modify: `crates/jazz-tools/src/storage/opfs_btree/mod.rs`
- Test: `crates/jazz-tools/src/storage/conformance.rs`

- [ ] **Step 1: Add the membership row type and raw table helpers**

Add a `BatchRowMember` struct and a `__batch_row_member` system table in `storage/mod.rs`. Use keys of the form `batch:<batch-hex>:table:<table-hex>:branch:<branch-hex>:row:<row-hex>` so `scan_batch_row_members(batch_id)` is a prefix scan and `scan_all_batch_row_members()` is a system-table scan.

- [ ] **Step 2: Add Storage trait methods**

Add `put_batch_row_member`, `scan_batch_row_members`, and `scan_all_batch_row_members` default methods in `storage_trait.rs`. The write method ensures a `batch_row_member` raw table header and inserts an empty row value; the scan methods decode keys and sort by `(batch_id, table, branch, object_id)`.

- [ ] **Step 3: Maintain the index on history writes**

Call `put_batch_row_member(table, branch, row_id, batch_id)` for each history row written through `append_history_region_rows`, `append_history_region_row_bytes`, and each backend `apply_encoded_row_mutation` override. The SQLite and RocksDB overrides must write index rows inside their existing transaction/savepoint. The OPFS raw append path can use the trait-level default helper after its insert loop.

- [ ] **Step 4: Add conformance coverage**

Add a storage conformance test that appends two rows in one batch and one row in another batch, then asserts `scan_batch_row_members(first_batch)` returns only the two first-batch rows and `scan_all_batch_row_members()` returns all three rows.

Run: `cargo test -p jazz-tools storage::`

Expected: storage tests pass.

### Task 2: Indexed Retained-Batch Replay

**Files:**

- Modify: `crates/jazz-tools/src/runtime_core/ticks.rs`
- Test: `crates/jazz-tools/src/runtime_core/tests.rs`

- [ ] **Step 1: Rewrite local batch row discovery**

Change `RuntimeCore::local_batch_rows` to first load `self.storage.scan_batch_row_members(batch_id)`. For each member, exact-load the row with `load_history_row_batch(table, branch, object_id, batch_id)`, get schema metadata via existing `local_batch_member_schema_hash`, and build `LocalBatchMember` from the row digest. Do not call `scan_history_row_batches` from this method.

- [ ] **Step 2: Keep non-scanning compatibility paths**

If the index is empty, use cached or stored `LocalBatchRecord` entries to exact-load rows with `load_history_row_batch_for_schema_hash`, then `load_history_row_batch`. If a sealed submission exists, use its member list with `load_row_locator` and exact `load_history_row_batch` on the target branch. These paths are point lookup only and do not call `scan_history_row_batches`.

- [ ] **Step 3: Add a no-scan replay test**

Add a test that writes a local batch, removes its sealed submission and local record, keeps only the row-history membership index plus fate, then calls `local_batch_rows(batch_id)` through the existing counting storage wrapper. Assert the returned row count matches the batch member count, `scan_row_locators_calls == 0`, `scan_history_row_batches_calls == 0`, and exact-load calls scale with member count.

Run: `cargo test -p jazz-tools local_batch_rows_uses_batch_member_index_without_history_scan`

Expected: the test passes and the broad scan counters stay at zero.

### Task 3: Indexed Startup Reconciliation

**Files:**

- Modify: `crates/jazz-tools/src/runtime_core/sync.rs`
- Test: `crates/jazz-tools/src/runtime_core/tests.rs`

- [ ] **Step 1: Replace row-locator reconciliation scans**

Change `pending_batch_ids_needing_reconciliation` so the row-store-only discovery path iterates `scan_all_batch_row_members()`, exact-loads each row, and selects `VisibleDirect` rows whose authoritative fate still needs edge reconciliation. Remove the `scan_row_locators` plus `scan_history_row_batches` loop from this method.

- [ ] **Step 2: Add a no-scan reconciliation test**

Add a test that writes one pending batch with a retained visible-direct row and many unrelated rows, then invokes startup/server reconciliation through the counting storage wrapper. Assert both broad scan counters stay at zero. Assert exact row loads increase by the retained batch member count and do not increase with unrelated row count.

Run: `cargo test -p jazz-tools retained_batch_reconciliation_uses_batch_member_index_without_row_store_scans`

Expected: the test passes and row-history scan counters stay at zero.

### Task 4: Verification And Publish

**Files:**

- Modify only files changed by Tasks 1-3.

- [ ] **Step 1: Run focused tests**

Run:

```bash
cargo test -p jazz-tools local_batch_rows_uses_batch_member_index_without_history_scan
cargo test -p jazz-tools retained_batch_reconciliation_uses_batch_member_index_without_row_store_scans
cargo test -p jazz-tools storage::
cargo test -p jazz-tools rc_worker
cargo check -p jazz-wasm --target wasm32-unknown-unknown
```

Expected: all commands pass.

- [ ] **Step 2: Review scan references**

Run:

```bash
rg -n "scan_row_locators|scan_history_row_batches" crates/jazz-tools/src/runtime_core/ticks.rs crates/jazz-tools/src/runtime_core/sync.rs
```

Expected: `local_batch_rows` and `pending_batch_ids_needing_reconciliation` do not contain calls to `scan_row_locators` or `scan_history_row_batches`.

- [ ] **Step 3: Commit, push, and open stacked PR**

Commit on `guido/batch-row-members-index`, push it, and open a draft PR with base `guido/worker-local-overlay-replay`.
