# Batch Settlement Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unify the four divergent "is this batch settled?" predicates behind one per-runtime settlement target, retire settled batch bookkeeping so the pending set stays bounded, and make `wait(tier)` error fast when no producer for the requested tier exists.

**Architecture:** All changes live in the Rust core (`crates/jazz-tools`) plus one small gate fix in `crates/jazz-wasm`. A new `SyncManager::settlement_target()` derives the terminal tier from configuration (`GlobalServer` when an upstream server is registered, else the runtime's own max durability tier, else `Local`). A new instance predicate `RuntimeCore::batch_needs_settlement(fate)` replaces the hard-coded `< EdgeServer` static gate everywhere in core. Sealed submissions become the durable membership store for pending batches: authorities stop deleting them before the batch reaches the settlement target, and clients delete them (plus the local batch record) the moment a fate at/above the target arrives or is self-confirmed. `wait_for_batch` rejects tiers the runtime has no configured path to instead of registering an unresolvable waiter.

**Tech Stack:** Rust (cargo), existing `runtime_core/tests` harness (`new_test_core`, `create_3tier_rc`, `pump_a_to_b`, `MemoryStorage`, `NoopScheduler`).

**Spec:** `specs/todo/a_mvp/batch_settlement_lifecycle.md` (committed on this branch). Scope notes:

- Spec §3 (persisted membership) is satisfied by _retaining sealed submissions until settlement_ rather than introducing a second persisted record — submissions already carry the member list, and `local_batch_rows` already reads them first. No new storage table.
- Spec §6 (promotion watermark) is deferred: promotion when a server appears later reuses the existing `add_server` reconciliation scan, which already regenerates seals from row history. The watermark optimization (skip the scan when the target hasn't changed) is a follow-up.
- The TS-side `serverUrl` config validation from spec §1 is out of scope here (it lives in the vite plugin / TS runtime, tracked by the spec's open questions).

**Behavioural changes that existing tests may encode (surface these in the final report, per project CLAUDE.md):**

1. A serverless commit no longer persists a sealed submission (it is settled at `Local` immediately). `rc_sealed_direct_batch_replays_row_and_seal_after_offline_write` asserts the stored submission is replayed verbatim after `add_server`; after this change the seal is regenerated from row history, so the assertion must compare against the regenerated submission.
2. An authority with an upstream (worker between main and a server) now _retains_ the submission it settles at `Local` until the upstream confirms, where it used to delete it immediately (`inbox.rs:908`).
3. `wait_for_batch` returns `Err` immediately for unattainable tiers instead of hanging.

---

## Task 0: Branch setup (already done)

Branch `batch-settlement-lifecycle` exists; the spec is committed as `5c2e3d844`. Plan is committed alongside.

---

## Task 1: `settlement_target()` + `batch_needs_settlement()` predicate

**Files:**

- Modify: `crates/jazz-tools/src/sync_manager/mod.rs` (~line 315, next to `max_local_durability_tier`)
- Modify: `crates/jazz-tools/src/runtime_core/sync.rs:4-101`
- Modify: `crates/jazz-tools/src/runtime_core/writes.rs:803-883`
- Test: `crates/jazz-tools/src/runtime_core/tests/write_batch/direct.rs`

- [x] **Step 1: Write the failing test**

Append to `crates/jazz-tools/src/runtime_core/tests/write_batch/direct.rs`:

```rust
#[test]
fn rc_local_only_runtime_settles_direct_batches_and_replays_nothing_on_worker_sync() {
    // A durable Local-tier runtime with no upstream server — the serverless
    // browser worker. Spec invariant 2: after commit, nothing is pending.
    let app_id = AppId::from_name("local-only-settlement");
    let sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mgr = SchemaManager::new(sm, test_schema(), app_id, "dev", "main").unwrap();
    let mut core = new_test_core(mgr, MemoryStorage::new(), NoopScheduler);
    core.immediate_tick();

    let ((_row_id, _), batch_id) = core
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();
    core.commit_batch(batch_id).unwrap();

    let replayed = core.local_batch_records_for_worker_sync().unwrap();
    assert!(
        replayed.is_empty(),
        "a local-only runtime settles at Local; worker sync must replay nothing, got {replayed:?}"
    );
    assert!(
        core.pending_batch_ids_needing_reconciliation_for_test().is_empty(),
        "no batch should pend reconciliation on a serverless runtime"
    );
}
```

`pending_batch_ids_needing_reconciliation` is private; expose a `#[cfg(test)]` shim in `runtime_core/sync.rs`:

```rust
#[cfg(test)]
pub(crate) fn pending_batch_ids_needing_reconciliation_for_test(
    &self,
) -> Vec<crate::row_histories::BatchId> {
    self.pending_batch_ids_needing_reconciliation()
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p jazz-tools rc_local_only_runtime_settles -- --nocapture`
Expected: FAIL — the worker-sync fate sweep (`writes.rs:857`) includes the `DurableDirect { Local }` fate because `sealed_batch_still_needs_edge_reconciliation` hard-codes `< EdgeServer`.

- [x] **Step 3: Implement the predicate**

In `crates/jazz-tools/src/sync_manager/mod.rs`, after `max_local_durability_tier` (line 317):

```rust
/// The durability tier at which this runtime considers a batch settled:
/// batches confirmed at or above this tier need no further reconciliation,
/// replay, or retained bookkeeping on this node.
///
/// With an upstream server registered the target is `GlobalServer` (local
/// fates are provisional until the upstream confirms). Without one, this
/// node's own strongest tier is terminal — there is nobody else to wait for.
pub fn settlement_target(&self) -> DurabilityTier {
    if self.has_connected_servers() {
        DurabilityTier::GlobalServer
    } else {
        self.max_local_durability_tier()
            .unwrap_or(DurabilityTier::Local)
    }
}
```

In `crates/jazz-tools/src/runtime_core/sync.rs`, replace `retained_batch_terminal_tier` (lines 4-14) with a delegation, and add the instance predicate:

```rust
pub(crate) fn settlement_target(&self) -> DurabilityTier {
    self.schema_manager
        .query_manager()
        .sync_manager()
        .settlement_target()
}

/// One terminal predicate for batch settlement (spec: batch_settlement_lifecycle §1).
/// `Rejected` and `Missing` fates are handled by their own paths and never
/// retained as pending; anything below the settlement target still pends.
pub(crate) fn batch_needs_settlement(&self, fate: Option<&crate::batch_fate::BatchFate>) -> bool {
    match fate {
        Some(
            crate::batch_fate::BatchFate::Rejected { .. }
            | crate::batch_fate::BatchFate::Missing { .. },
        ) => false,
        Some(
            crate::batch_fate::BatchFate::DurableDirect { confirmed_tier, .. }
            | crate::batch_fate::BatchFate::AcceptedTransaction { confirmed_tier, .. },
        ) => *confirmed_tier < self.settlement_target(),
        None => true,
    }
}
```

- [x] **Step 4: Route every core call site through the predicate**

Find all uses: `grep -rn "sealed_batch_still_needs_edge_reconciliation\|retained_batch_terminal_tier" crates/jazz-tools/src`

In `runtime_core/sync.rs` `pending_batch_ids_needing_reconciliation`:

- first loop (submissions, lines 23-42): replace the whole `match` body with `self.batch_needs_settlement(... .as_ref())`.
- second loop (fates, lines 51-56): replace `Self::sealed_batch_still_needs_edge_reconciliation(Some(fate)) && fate.confirmed_tier().is_some_and(|tier| tier < terminal_tier)` with `self.batch_needs_settlement(Some(fate))`.
- third loop (row sweep, line 76): `Self::sealed_batch_still_needs_edge_reconciliation(fate.as_ref())` → `self.batch_needs_settlement(fate.as_ref())`.
- cache loop (lines 81-96): replace the `sealed_batch_still_needs_edge_reconciliation` + tier check pair with a single `self.batch_needs_settlement(latest_fate)`.
- delete the now-unused `terminal_tier` local.

In `runtime_core/writes.rs` `local_batch_records_for_worker_sync`:

- line 839: `!Self::sealed_batch_still_needs_edge_reconciliation(fate.as_ref())` → `!self.batch_needs_settlement(fate.as_ref())`
- line 861: `!Self::sealed_batch_still_needs_edge_reconciliation(Some(&fate))` → `!self.batch_needs_settlement(Some(&fate))`

Delete `sealed_batch_still_needs_edge_reconciliation` (writes.rs:803-812) once no callers remain (check tests too; update any test caller to the new predicate).

- [x] **Step 5: Run the test and the touched suites**

Run: `cargo test -p jazz-tools rc_local_only_runtime_settles && cargo test -p jazz-tools write_batch`
Expected: new test PASSES; pre-existing write_batch tests still pass (the predicate is equivalent to the old gates whenever a server is registered, which is the case in `create_3tier_rc`).

- [x] **Step 6: Commit**

```bash
git add -A crates/jazz-tools
git commit -m "feat(core): unify batch settlement behind per-runtime settlement target"
```

---

## Task 2: Authorities retain submissions until the settlement target is reached

**Files:**

- Modify: `crates/jazz-tools/src/sync_manager/inbox.rs:908-912` (`settle_sealed_batch`) and `:965-968` (`try_accept_completed_sealed_batch_from_client`)
- Test: `crates/jazz-tools/src/runtime_core/tests/write_batch/direct.rs`

The worker currently deletes the sealed submission the moment it settles a client batch at `Local` (`inbox.rs:908`), leaving a fate-only batch whose membership later requires a full row-store scan. Retain the submission while the fate is below the sync manager's settlement target; keep deleting it when the target is reached (serverless worker → still deletes immediately, same as today).

- [x] **Step 1: Write the failing test**

Append to `direct.rs`:

```rust
#[test]
fn rc_worker_with_upstream_retains_settled_submission_until_target_tier() {
    // B is a Local-tier worker with upstream C. When B settles A's batch at
    // Local, the submission is B's only durable membership record for a batch
    // that still owes upstream reconciliation — it must survive (spec §2/§3).
    let mut s = create_3tier_rc();
    let ((_row_id, _), batch_id) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();
    pump_a_to_b(&mut s);

    assert_eq!(
        s.b.storage()
            .load_authoritative_batch_fate(batch_id)
            .unwrap()
            .and_then(|fate| fate.confirmed_tier()),
        Some(DurabilityTier::Local),
        "worker should settle the client batch at Local"
    );
    assert!(
        s.b.storage()
            .load_sealed_batch_submission(batch_id)
            .unwrap()
            .is_some(),
        "worker must retain the submission while the batch is below its settlement target"
    );
}

#[test]
fn rc_serverless_authority_prunes_submission_at_local_settlement() {
    // A Local-tier authority with no upstream settles at its own tier — the
    // submission retires immediately.
    let app_id = AppId::from_name("serverless-authority-prune");
    let sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mgr = SchemaManager::new(sm, test_schema(), app_id, "dev", "main").unwrap();
    let mut b = new_test_core(mgr, MemoryStorage::new(), NoopScheduler);
    b.immediate_tick();

    let client_id = ClientId::new();
    b.add_client(client_id, None);
    b.schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(client_id, ClientRole::Peer);

    // Client-side runtime that ships its batch to B.
    let app_id = AppId::from_name("serverless-authority-prune");
    let sm_a = SyncManager::new();
    let mgr_a = SchemaManager::new(sm_a, test_schema(), app_id, "dev", "main").unwrap();
    let mut a = new_test_core(mgr_a, MemoryStorage::new(), NoopScheduler);
    a.immediate_tick();
    let server_id = ServerId::new();
    a.add_server(server_id);
    a.batched_tick();
    a.sync_sender().take();

    let ((_row_id, _), batch_id) =
        a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();
    a.batched_tick();
    for entry in a.sync_sender().take() {
        b.push_sync_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: entry.payload,
        });
    }
    b.batched_tick();

    assert!(
        b.storage()
            .load_sealed_batch_submission(batch_id)
            .unwrap()
            .is_none(),
        "serverless authority settles at Local (its target) and retires the submission"
    );
}
```

(If the harness exposes an existing two-node pump helper, use it instead of the manual outbox loop — check `grep -n "fn pump_" crates/jazz-tools/src/runtime_core/tests.rs` first.)

- [x] **Step 2: Run to verify the first test fails**

Run: `cargo test -p jazz-tools rc_worker_with_upstream_retains_settled_submission -- --nocapture`
Expected: FAIL — `settle_sealed_batch` deletes the submission for any non-`Missing` fate.

- [x] **Step 3: Implement**

In `crates/jazz-tools/src/sync_manager/inbox.rs`, `settle_sealed_batch` (lines 908-912), replace:

```rust
if !matches!(fate, BatchFate::Missing { .. })
    && let Err(error) = storage.delete_sealed_batch_submission(batch_id)
{
    tracing::warn!(?batch_id, %error, "failed to delete sealed batch submission");
}
```

with:

```rust
let settled_at_target = matches!(fate, BatchFate::Rejected { .. })
    || fate
        .confirmed_tier()
        .is_some_and(|tier| tier >= self.settlement_target());
if settled_at_target
    && !matches!(fate, BatchFate::Missing { .. })
    && let Err(error) = storage.delete_sealed_batch_submission(batch_id)
{
    tracing::warn!(?batch_id, %error, "failed to delete sealed batch submission");
}
```

In `try_accept_completed_sealed_batch_from_client` (lines 965-968), replace:

```rust
let should_prune_submission = matches!(fate, BatchFate::Rejected { .. })
    || fate
        .confirmed_tier()
        .is_some_and(|tier| tier >= DurabilityTier::GlobalServer);
```

with:

```rust
let should_prune_submission = matches!(fate, BatchFate::Rejected { .. })
    || fate
        .confirmed_tier()
        .is_some_and(|tier| tier >= self.settlement_target());
```

Also add the explicit warning for the silent no-tier return (`inbox.rs:879-881`), spec §4:

```rust
let Some(confirmed_tier) = self.my_tiers.iter().copied().max() else {
    tracing::warn!(
        ?batch_id,
        "received a sealed batch but this node has no durability tier; \
         dropping settlement — the origin's wait() can only resolve via another peer"
    );
    return;
};
```

- [x] **Step 4: Run tests**

Run: `cargo test -p jazz-tools rc_worker_with_upstream_retains rc_serverless_authority_prunes && cargo test -p jazz-tools sync_manager`
Expected: PASS. Then run the wider batch suites: `cargo test -p jazz-tools write_batch` — if any existing test asserts the old early deletion, stop and surface it before changing it.

- [x] **Step 5: Commit**

```bash
git add -A crates/jazz-tools
git commit -m "feat(sync): retain sealed submissions until the settlement target is reached"
```

---

## Task 3: Client-side retirement when a settled fate arrives or is self-confirmed

**Files:**

- Modify: `crates/jazz-tools/src/runtime_core/ticks.rs:231-293` (`apply_received_batch_fate`)
- Modify: `crates/jazz-tools/src/runtime_core/writes.rs:1087-1117` (`commit_batch`)
- Test: `crates/jazz-tools/src/runtime_core/tests/write_batch/direct.rs`

- [x] **Step 1: Write the failing tests**

```rust
#[test]
fn rc_client_retires_batch_bookkeeping_when_fate_reaches_settlement_target() {
    let mut s = create_3tier_rc();
    let ((_row_id, _), batch_id) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();
    assert!(
        s.a.storage()
            .load_sealed_batch_submission(batch_id)
            .unwrap()
            .is_some(),
        "submission pends while below the target"
    );

    s.a.push_sync_inbox(InboxEntry {
        source: Source::Server(s.b_server_for_a),
        payload: SyncPayload::BatchFate {
            fate: crate::batch_fate::BatchFate::DurableDirect {
                batch_id,
                confirmed_tier: DurabilityTier::GlobalServer,
            },
        },
    });
    s.a.batched_tick();

    assert!(
        s.a.storage()
            .load_sealed_batch_submission(batch_id)
            .unwrap()
            .is_none(),
        "global fate retires the submission"
    );
    assert!(
        s.a.storage()
            .load_local_batch_record(batch_id)
            .unwrap()
            .is_none(),
        "global fate retires the local batch record"
    );
    assert!(
        s.a.storage()
            .load_authoritative_batch_fate(batch_id)
            .unwrap()
            .is_some(),
        "the fate itself stays as a terminal tombstone"
    );
    assert!(
        s.a.pending_batch_ids_needing_reconciliation_for_test()
            .is_empty()
    );
}

#[test]
fn rc_serverless_commit_retires_submission_immediately() {
    // Spec invariant 1+2: with no upstream, Local is the target — commit
    // settles and retires in one step; nothing is left to replay.
    let mut core = create_runtime_with_boxed_storage(
        test_schema(),
        "serverless-commit-retires",
        Box::new(MemoryStorage::new()),
    );
    let ((_row_id, _), batch_id) = core
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();
    core.commit_batch(batch_id).unwrap();

    assert!(
        core.storage()
            .load_sealed_batch_submission(batch_id)
            .unwrap()
            .is_none(),
        "serverless commit settles at Local and must not retain the submission"
    );
    // The data itself stays visible.
    let results = execute_query(&mut core, Query::new("users"));
    assert_eq!(results.len(), 1);
}
```

Note: `create_runtime_with_boxed_storage` returns `BoxedStorageTestCore`; if `execute_query` is typed for `TestCore`, query via a fresh subscription inline (copy the four lines from `execute_query`).

- [x] **Step 2: Run to verify both fail**

Run: `cargo test -p jazz-tools rc_client_retires_batch_bookkeeping rc_serverless_commit_retires -- --nocapture`
Expected: FAIL — nothing deletes client-side submissions today; serverless commit persists one.

- [x] **Step 3: Implement retirement**

In `runtime_core/sync.rs` (next to the predicate), add the shared retirement routine:

```rust
/// Retire a batch's pending bookkeeping once its fate reached the settlement
/// target (spec: batch_settlement_lifecycle §2). The fate row stays behind as
/// the terminal tombstone; rows are untouched.
pub(crate) fn retire_settled_batch(&mut self, batch_id: crate::row_histories::BatchId) {
    if let Err(error) = self.storage.delete_sealed_batch_submission(batch_id) {
        tracing::warn!(?batch_id, %error, "failed to retire sealed batch submission");
    }
    if let Err(error) = self.storage.delete_local_batch_record(batch_id) {
        tracing::warn!(?batch_id, %error, "failed to retire local batch record");
    }
    self.local_batch_record_cache.remove(&batch_id);
    self.mark_storage_write_pending_flush();
}
```

In `apply_received_batch_fate` (`ticks.rs`), at the very end of the function (after `record_batch_ack`, because `local_batch_rows` at line 283 still needs the submission for membership):

```rust
if !self.batch_needs_settlement(Some(&fate)) {
    self.retire_settled_batch(batch_id);
}
```

In `commit_batch` (`writes.rs`), the direct-batch self-confirmation block currently persists the submission unconditionally. Change the tail of the function (lines 1087-1117) so a settlement that already reaches the target skips persisting and retires instead:

```rust
let submission = self.sealed_batch_submission(&record)?;

record.mark_sealed(submission.clone());
let mut settled_at_commit = false;
if record.mode == BatchMode::Direct {
    if let Some(confirmed_tier) = self.local_write_confirmed_tier() {
        let settlement = BatchFate::DurableDirect {
            batch_id,
            confirmed_tier,
        };
        record.apply_fate(settlement.clone());
        self.storage
            .upsert_authoritative_batch_fate(&settlement)
            .map_err(|err| {
                RuntimeError::WriteError(format!("persist batch fate: {err}"))
            })?;
        settled_at_commit = !self.batch_needs_settlement(Some(&settlement));
        if let Some(acked_tier) = settlement.confirmed_tier() {
            self.durability.record_batch_ack(batch_id, acked_tier);
        }
    }
    self.publish_direct_batch_rows(&record)?;
}
if !settled_at_commit {
    self.storage
        .upsert_sealed_batch_submission(&submission)
        .map_err(|err| {
            RuntimeError::WriteError(format!("persist sealed batch submission: {err}"))
        })?;
}
self.local_batch_record_cache.insert(batch_id, record);
self.schema_manager
    .query_manager_mut()
    .sync_manager_mut()
    .seal_batch_to_servers(submission);
self.finish_batch(batch_id, RuntimeBatchStatus::Committed);
self.mark_storage_write_pending_flush();
self.immediate_tick();
Ok(())
```

(Keep the cache insert — same-session membership lookups still use it; it is in-memory only and dies with the runtime. `seal_batch_to_servers` is a no-op with no servers registered, which is the only configuration where `settled_at_commit` can be true.)

- [x] **Step 4: Verify late-server promotion still works**

The offline-replay test `rc_sealed_direct_batch_replays_row_and_seal_after_offline_write` (direct.rs:69) commits serverless, then `add_server`s. Under the new semantics the stored submission is gone, so the replay seal must be regenerated from row history. Check `retransmit_local_batch_to_servers` (in `ticks.rs`, near `direct_sealed_submission_from_local_batch_rows`): confirm it falls back to `direct_sealed_submission_from_local_batch_rows` when `load_sealed_batch_submission` returns `None`. If it does not, add that fallback there:

```rust
let submission = match self.storage.load_sealed_batch_submission(batch_id) {
    Ok(Some(submission)) => Some(submission),
    _ => Self::direct_sealed_submission_from_local_batch_rows(
        batch_id,
        &self.local_batch_rows(batch_id),
    ),
};
```

Then run: `cargo test -p jazz-tools rc_sealed_direct_batch_replays_row_and_seal_after_offline_write -- --nocapture`

If it fails only on the submission-equality assertion (stored vs regenerated), update the test to load no stored submission and assert the outbox `SealBatch` carries the same `batch_id` + members instead — and record this in the final report as behaviour change 1.

- [x] **Step 5: Run the suites**

Run: `cargo test -p jazz-tools write_batch && cargo test -p jazz-tools runtime_core`
Expected: PASS (modulo the documented assertion update in Step 4).

- [x] **Step 6: Commit**

```bash
git add -A crates/jazz-tools
git commit -m "feat(core): retire batch bookkeeping when fates reach the settlement target"
```

---

## Task 4: `wait_for_batch` fails fast on unattainable tiers

**Files:**

- Modify: `crates/jazz-tools/src/runtime_core/writes.rs:900-926` (`wait_for_batch`), `:164-176` (next to `local_write_confirmed_tier`)
- Test: `crates/jazz-tools/src/runtime_core/tests/write_batch/direct.rs`

- [x] **Step 1: Write the failing tests**

```rust
#[test]
fn rc_wait_for_unattainable_tier_errors_instead_of_hanging() {
    // Spec invariant 4: wait(global) with no server has no producer — error now.
    let mut core = create_test_runtime();
    let ((_row_id, _), batch_id) = core
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();
    core.commit_batch(batch_id).unwrap();

    assert!(
        core.wait_for_batch(batch_id, DurabilityTier::GlobalServer)
            .is_err(),
        "waiting on an unattainable tier must error immediately"
    );
    // Local is this runtime's own settlement target — still resolvable.
    let mut receiver = core
        .wait_for_batch(batch_id, DurabilityTier::Local)
        .unwrap();
    assert_eq!(receiver.try_recv(), Ok(Some(Ok(()))));
}

#[test]
fn rc_non_durable_client_without_server_errors_on_local_wait() {
    // The exact shape of the reported "stuck forever" repro: a non-durable
    // client with no tiered peer has no fate producer at all.
    let app_id = AppId::from_name("non-durable-no-server-wait");
    let mgr =
        SchemaManager::new(SyncManager::new(), test_schema(), app_id, "dev", "main").unwrap();
    let mut core = new_test_core(mgr, MemoryStorage::new(), NoopScheduler);
    core.set_non_durable_client_runtime();
    core.immediate_tick();

    let ((_row_id, _), batch_id) = core
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();
    core.commit_batch(batch_id).unwrap();

    assert!(
        core.wait_for_batch(batch_id, DurabilityTier::Local).is_err(),
        "a non-durable client with no upstream has no producer for any tier"
    );
}
```

- [x] **Step 2: Run to verify both fail**

Run: `cargo test -p jazz-tools rc_wait_for_unattainable rc_non_durable_client_without_server -- --nocapture`
Expected: FAIL — `wait_for_batch` registers a waiter unconditionally.

- [x] **Step 3: Implement**

In `writes.rs`, next to `local_write_confirmed_tier` (line 164):

```rust
/// The strongest durability tier some producer can still confirm for this
/// runtime's batches: a registered upstream can return up to GlobalServer; a
/// self-confirming runtime can attest its own tier; a non-durable client with
/// no upstream has no producer at all.
fn max_attainable_wait_tier(&self) -> Option<DurabilityTier> {
    let sync_manager = self.schema_manager.query_manager().sync_manager();
    if sync_manager.has_connected_servers() {
        return Some(DurabilityTier::GlobalServer);
    }
    if self.synthesize_direct_write_fate {
        return Some(
            sync_manager
                .max_local_durability_tier()
                .unwrap_or(DurabilityTier::Local),
        );
    }
    sync_manager.max_local_durability_tier()
}
```

In `wait_for_batch`, immediately before `Ok(self.register_batch_waiter(batch_id, tier))` (after all the already-resolved checks, so completed batches and recorded acks still resolve):

```rust
let attainable = self.max_attainable_wait_tier();
if attainable.is_none_or(|max_tier| tier > max_tier) {
    return Err(RuntimeError::WriteError(format!(
        "cannot wait for durability tier {tier:?}: no configured server or local \
         durability tier can produce it (max attainable: {attainable:?})"
    )));
}
```

- [x] **Step 4: Run tests**

Run: `cargo test -p jazz-tools rc_wait_for_unattainable rc_non_durable_client_without_server && cargo test -p jazz-tools wait`
Expected: PASS, including `rc_non_durable_client_seals_direct_batch_without_self_confirming_local_fate` (its runtime has server B registered, so `Local` stays attainable and pending).

- [x] **Step 5: Commit**

```bash
git add -A crates/jazz-tools
git commit -m "feat(core): reject wait_for_batch on tiers with no configured producer"
```

---

## Task 5: Worker-bridge reconciliation gate follows upstream expectation

**Files:**

- Modify: `crates/jazz-wasm/src/worker_bridge.rs:64-73` and its call sites (`:617` area, `:791`)

The main-side gate `local_batch_record_needs_fate_reconciliation` hard-codes `< EdgeServer`. The bridge already knows whether an upstream exists (`expects_upstream`, captured at construction). Without an upstream, a `Local` fate is terminal on main too.

- [x] **Step 1: Implement**

```rust
fn local_batch_record_needs_fate_reconciliation(
    record: &LocalBatchRecord,
    expects_upstream: bool,
) -> bool {
    let terminal_tier = if expects_upstream {
        DurabilityTier::EdgeServer
    } else {
        DurabilityTier::Local
    };
    match record.latest_fate.as_ref() {
        None => true,
        Some(BatchFate::DurableDirect { confirmed_tier, .. })
        | Some(BatchFate::AcceptedTransaction { confirmed_tier, .. }) => {
            *confirmed_tier < terminal_tier
        }
        Some(BatchFate::Missing { .. } | BatchFate::Rejected { .. }) => false,
    }
}
```

Update both call sites to pass the bridge's `expects_upstream` flag (check how the inner struct stores it: `grep -n "expects_upstream" crates/jazz-wasm/src/worker_bridge.rs`).

- [x] **Step 2: Check it compiles**

Run: `cargo check -p jazz-wasm --target wasm32-unknown-unknown` (fall back to `cargo check -p jazz-wasm` if the target is not installed, and note it).
Expected: clean.

- [x] **Step 3: Commit**

```bash
git add -A crates/jazz-wasm
git commit -m "fix(wasm): main-side fate reconciliation gate respects upstream expectation"
```

---

## Task 6: Full suite + fallout review

- [x] **Step 1: Run the full Rust suite**

Run: `cargo test -p jazz-tools`
Expected: PASS. For every failure, classify before touching it:

- encodes one of the three documented behaviour changes → update the assertion and note it in the final report;
- anything else → stop and surface it to the user (project rule: failing tests may be right and the implementation wrong).

- [x] **Step 2: Check the other Rust crates compile**

Run: `cargo check --workspace` (or `cargo check -p jazz-napi -p jazz-rn` plus the wasm check from Task 5 if the workspace includes platform-gated crates).

- [x] **Step 3: Final commit + report**

```bash
git add -A
git commit -m "test: cover batch settlement lifecycle invariants"
```

Report: behaviour changes shipped, tests updated (with justification), spec items deferred (§6 watermark, TS config validation, §5 payload slimming), and suggested follow-up issues.

---

## Self-review notes

- **Spec coverage:** §1 → Tasks 1, 5; §2 → Tasks 2, 3; §3 → satisfied via submission retention (Task 2) + retirement (Task 3), no new table; §4 → Task 4 + the warn in Task 2 Step 3; §5 → payload shrinks because the pending set shrinks (Tasks 1-3); §6 → promotion preserved via the existing `add_server` scan + seal regeneration (Task 3 Step 4); watermark deferred. Migration section → covered implicitly: legacy settled-at-Local fates are excluded by the Task 1 predicate when the target is `Local`; legacy fate-only batches under a `GlobalServer` target still hit the old fallback scan once — deferred with the watermark.
- **Invariants:** 1 → Task 3; 2 → Task 1; 3 → partially (fallback scan retained for legacy data, no longer on the hot path); 4 → Task 4; 5 → Task 3 Step 4; 6 → fate tombstone retained in `retire_settled_batch` + `try_accept_completed_sealed_batch_from_client` early-return path.
- **Type consistency:** `settlement_target()` (SyncManager + RuntimeCore delegation), `batch_needs_settlement(Option<&BatchFate>) -> bool`, `retire_settled_batch(BatchId)`, `max_attainable_wait_tier() -> Option<DurabilityTier>` — names used consistently across tasks.
