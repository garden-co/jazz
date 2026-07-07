# Bare-Insert Scalability: Event-Driven Sealed-Batch Recovery + RN Scheduler Rework — TODO (MVP)

This spec is written for readers who know the status quo on `main`.

Bulk streams of bare `db.insert(...)` calls (no explicit `db.batch(...)`) currently wedge Jazz
clients. The expo stress app freezes around 3,000 inserts on an iPhone simulator; a Node
reproduction against a local server stalls around 12,500. Two independent defects compound:

1. **Core, all platforms:** every write runs a full scan-and-decode of all stored sealed batch
   submissions (quadratic in the number of unsettled writes; profiled at ~65% of client CPU).
2. **jazz-rn only:** the tick scheduler spawns one OS thread per tick request; a busy JS thread
   makes blocked threads pile up until `pthread_create` fails with `EAGAIN` and the write panics
   (`panic in insert: failed to spawn thread: Os { code: 35 ... }`).

This spec fixes both, plus a free 2× on the write path (each bare write currently ticks twice).

## Related

- [Batches — Status Quo](../../status-quo/batches.md)
- [Sync Manager — Status Quo](../../status-quo/sync_manager.md)
- [Batched Tick Orchestration — Status Quo](../../status-quo/batched_tick_orchestration.md)
- [Issue: ack-path batch row full scans](../issues/ack-path-batch-row-full-scans.md) (deliberately out of scope here)
- [Issue: napi tick notifier takes core lock](../issues/napi-tick-notifier-takes-core-lock.md) (same scheduler family, not ported in this spec)
- [Issue: duplicated batch bookkeeping storage](../issues/duplicated-batch-bookkeeping-storage.md)

## Incident summary (evidence)

- Expo stress app (`dev/stress-tests/stress-test-expo`): generation of 15k todos freezes the whole
  UI around batch 6–7 (~3,000–3,350 inserts); with a try/catch around the loop the failure
  surfaces as the `EAGAIN` thread-spawn panic at insert #3347.
- Node repro (`packages/jazz-tools/src/runtime/stress-expo-stall.repro.test.ts`): per-500-insert
  batch times climb 0.9s → 15s, then a hard stall at ~12.5k inserts. Offline (no `serverUrl`):
  15k inserts complete in ~750ms. With explicit `db.batch(500)`: completes.
- CPU profile of the Node repro (wasm keeps Rust symbols): ~65% of samples inside
  `Storage::scan_sealed_batch_submissions` (dominated by `decode_sealed_batch_submission_with_branch_ords`:
  blake3 digesting + row decode), reached via `insert → immediate_tick` and
  `insert → commit_batch → immediate_tick`.

## Root causes

### A. Per-tick sealed-batch recovery scan (quadratic)

`RuntimeCore::immediate_tick` (`crates/jazz-tools/src/runtime_core/ticks.rs:486-493`) calls
`SyncManager::recover_completed_sealed_batches_with_storage`
(`crates/jazz-tools/src/sync_manager/inbox.rs:1157`), which scans and decodes **every** stored
sealed batch submission. The scan early-returns only when `my_tiers` is empty — and direct
(non-worker) clients register durability tier `"local"`
(`packages/jazz-tools/src/runtime/wasm-runtime-module.ts:80`,
`crates/jazz-wasm/src/runtime.rs:332-336`, `crates/jazz-rn/rust/src/lib.rs:399`) so they can
self-confirm local durability. Every bare write is auto-sealed into its own single-row Direct
batch (`crates/jazz-tools/src/runtime_core/writes.rs:595`) and, while a server is connected,
persists a sealed submission until the settlement target (edge) acks it
(`writes.rs:1109-1138`). Submissions accumulate faster than acks retire them, so write #N decodes
N-1 submissions.

The scan is load-bearing for three cases, all of which must be preserved:

1. **Crash recovery:** a submission stored without a terminal fate (crash between accepting a
   seal and writing its fate).
2. **Seal-before-rows race:** the live seal path defers without scheduling any retry when member
   rows have not arrived yet (`inbox.rs:1117-1123`: `declared_rows_for_submission → None`,
   `infer_sealed_batch_mode → Ok(None)`). The per-tick scan is what eventually settles the batch.
3. **Direct-fate tier promotion:** `can_promote_direct_fate` (`inbox.rs:931`) lets a higher-tier
   authority re-validate and promote a lower-tier `DurableDirect` fate.

### B. Double tick per bare write

`insert_with_id` / `update` / `upsert` / `delete` (`writes.rs:563-682`) end with
`mark_storage_write_pending_flush(); immediate_tick();` — but for auto-sealed direct writes,
`commit_batch` (`writes.rs:1146`) already performed both. Every bare write pays the tick cost
twice.

### C. jazz-rn thread-per-tick scheduler

`RnScheduler::schedule_batched_tick` (`crates/jazz-rn/rust/src/lib.rs:294-322`) spawns a new OS
thread per request (1ms coalescing sleep, then a uniffi callback into JS). The debounce flag is
released _before_ the callback completes (`lib.rs:313`), so while the JS thread is busy (e.g. a
synchronous 500-insert loop, made long by defect A) each subsequent write spawns another thread
that blocks waiting for the JS thread. Blocked threads accumulate until iOS refuses new threads
(`EAGAIN`), which panics inside the next write. `schedule_mutation_error_delivery`
(`lib.rs:324-360`) has the same shape.

## Design

### Component A — event-driven sealed-batch recovery (crates/jazz-tools)

Recovery inputs only change when sync messages are applied (rows / seals / fates), at startup, or
on server add. So: replace the per-tick full scan with an in-memory pending set plus point
lookups, re-examined only when messages were applied.

**New state** on `SyncManager` (`sync_manager/mod.rs`):

```rust
/// Sealed batches that may need authority attention: seal persisted but not
/// yet settled (rows missing / mode not yet inferable / promotable fate).
/// In-memory only — reseeded from storage at runtime startup.
pub(super) pending_sealed_recovery: BTreeSet<BatchId>,
```

`BTreeSet` keeps iteration order deterministic, matching the sorted behavior of the scan it
replaces.

**Population — four entry points:**

1. **Startup seed** (crash recovery). In `RuntimeCore::new`, when
   `sync_manager.has_durability_identity()`, run the full scan **once** and insert every
   submission whose fate is missing or promotable:

   ```rust
   fn seed_sealed_batch_recovery(&mut self) {
       let sm = self.schema_manager.query_manager().sync_manager();
       if !sm.has_durability_identity() { return; }
       let Ok(submissions) = self.storage.scan_sealed_batch_submissions() else { return; };
       for submission in submissions {
           let fate = self.storage
               .load_authoritative_batch_fate(submission.batch_id).ok().flatten();
           let needs_attention = match fate {
               None => true,
               Some(fate) => sm.can_promote_direct_fate(&fate),
           };
           if needs_attention {
               self.schema_manager.query_manager_mut().sync_manager_mut()
                   .pending_sealed_recovery.insert(submission.batch_id);
           }
       }
   }
   ```

2. **Seal persisted from sync input:** `persist_sealed_batch_submission` (`inbox.rs:207`)
   unconditionally inserts `submission.batch_id` into the set. If the live path settles the batch
   moments later, settlement removes it; if the live path defers (`inbox.rs:1117-1123`), the id
   stays pending — exactly the seal-before-rows race.

3. **Promotable fate persisted from sync input:** where authoritative fates are persisted from
   inbox processing (`inbox.rs:~195`) and in `apply_received_batch_fate` (`ticks.rs:231`): if
   `can_promote_direct_fate(&fate)` and a sealed submission exists for that batch, insert the id.

4. **Local `commit_batch` requires no hook:** self-confirming runtimes write their own fate at
   commit (`writes.rs:1110-1124`); runtimes without a durability identity never ran recovery in
   the first place. A client's own bare writes therefore never enter the set — per-write recovery
   cost drops to zero, not merely O(small).

**Removal:** `settle_sealed_batch`, `reject_sealed_transactional_batch`,
`retire_settled_batch` (`runtime_core/sync.rs:86`), and the "fate present, not promotable" branch
during a recovery pass.

**Recovery function:** keep the existing validation/settlement body
(`inbox.rs:1192-1239`) verbatim, but drive it from the set with per-id point lookups:

```rust
pub(crate) fn recover_pending_sealed_batches_with_storage<H: Storage>(
    &mut self,
    storage: &mut H,
) -> bool {
    if self.my_tiers.is_empty() || self.pending_sealed_recovery.is_empty() {
        return false;
    }
    let pending: Vec<BatchId> = self.pending_sealed_recovery.iter().copied().collect();
    let mut recovered_any = false;
    for batch_id in pending {
        let Ok(Some(submission)) = storage.load_sealed_batch_submission(batch_id) else {
            self.pending_sealed_recovery.remove(&batch_id); // retired/rejected elsewhere
            continue;
        };
        match storage.load_authoritative_batch_fate(batch_id) {
            Ok(Some(fate)) if self.can_promote_direct_fate(&fate) => {}
            Ok(Some(_)) => { self.pending_sealed_recovery.remove(&batch_id); continue; }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(?batch_id, %error, "failed to load batch fate during recovery");
                continue; // keep pending
            }
        }
        // ... identical validation + settle/reject body as today, with settle/reject
        // also removing batch_id from pending_sealed_recovery. A batch that still
        // cannot complete (rows not yet arrived) stays in the set.
    }
    recovered_any
}
```

**Call-site changes:**

- `immediate_tick` (`ticks.rs:486-493`) stops calling recovery entirely.
- `batched_tick` calls the new function right after `handle_sync_messages()` reports applied
  messages — the only moment recovery inputs can have changed. This preserves same-tick
  settlement when a deferred seal's missing rows arrive in the same drain:

  ```rust
  let drained_any = self.handle_sync_messages();
  if drained_any {
      let recovered = self.schema_manager.query_manager_mut().sync_manager_mut()
          .recover_pending_sealed_batches_with_storage(&mut self.storage);
      if recovered {
          self.mark_storage_write_pending_flush();
          self.immediate_tick();
      }
  }
  ```

- `add_server_*` keeps its existing connect-time reconciliation
  (`pending_batch_ids_needing_reconciliation`, `runtime_core/sync.rs:24`) unchanged as the
  backstop of last resort.

**Complexity after the change:** writes perform zero recovery work; message-arrival ticks perform
O(|pending|) point lookups with |pending| ≈ 0 in steady state; the full scan happens exactly once
per runtime startup.

### Component B — one tick per write (crates/jazz-tools)

In `insert_with_id`, `update`, `upsert`, `delete` (`writes.rs`), tick once:

```rust
if Self::should_auto_seal_direct_write(batch_mode, write_context) {
    self.commit_batch(batch_id)?; // flushes + ticks internally (writes.rs:1145-1146)
} else {
    self.mark_storage_write_pending_flush();
    self.immediate_tick();
}
```

Explicit-batch writes keep their tick; auto-sealed bare writes rely on `commit_batch`'s.

### Component C — persistent scheduler thread (crates/jazz-rn)

Replace spawn-per-call with one long-lived worker thread per runtime, fed by an mpsc channel.
Debounce semantics are preserved verbatim (flag cleared after the 1ms coalescing sleep, before
the callback fires, so a schedule arriving during a blocked callback still wins a follow-up
tick) — but the thread count is constant.

```rust
enum SchedulerJob {
    Tick,
    DeliverMutationErrors,
}

#[derive(Clone, Default)]
struct RnScheduler {
    tick_scheduled: Arc<AtomicBool>,
    errors_scheduled: Arc<AtomicBool>,
    core_ref: Arc<Mutex<Option<Weak<Mutex<RnCoreType>>>>>,
    callback: Arc<Mutex<Option<Box<dyn BatchedTickCallback>>>>,
    worker: Arc<Mutex<Option<SchedulerWorker>>>,
}

struct SchedulerWorker {
    sender: std::sync::mpsc::Sender<SchedulerJob>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Scheduler for RnScheduler {
    fn schedule_batched_tick(&self) {
        if self.tick_scheduled.swap(true, Ordering::SeqCst) {
            return; // debounce: at most one pending tick
        }
        self.send_job(SchedulerJob::Tick);
    }

    fn schedule_mutation_error_delivery(&self) {
        if self.errors_scheduled.swap(true, Ordering::SeqCst) {
            return;
        }
        self.send_job(SchedulerJob::DeliverMutationErrors);
    }
}

impl RnScheduler {
    fn send_job(&self, job: SchedulerJob) {
        let mut slot = match self.worker.lock() { Ok(s) => s, Err(_) => return };
        let worker = slot.get_or_insert_with(|| Self::spawn_worker(
            Arc::clone(&self.tick_scheduled),
            Arc::clone(&self.errors_scheduled),
            Arc::clone(&self.core_ref),
            Arc::clone(&self.callback),
        ));
        // Send failure means the worker exited (shutdown); drop the job.
        let _ = worker.sender.send(job);
    }

    fn spawn_worker(
        tick_scheduled: Arc<AtomicBool>,
        errors_scheduled: Arc<AtomicBool>,
        core_ref: Arc<Mutex<Option<Weak<Mutex<RnCoreType>>>>>,
        callback: Arc<Mutex<Option<Box<dyn BatchedTickCallback>>>>,
    ) -> SchedulerWorker {
        let (sender, receiver) = std::sync::mpsc::channel::<SchedulerJob>();
        let handle = std::thread::Builder::new()
            .name("jazz-rn-scheduler".into())
            .spawn(move || {
                while let Ok(job) = receiver.recv() {
                    // Same 1ms coalescing delay as before: collapses bursts of
                    // schedule calls into one callback and avoids synchronous
                    // re-entry into batched_tick.
                    std::thread::sleep(Duration::from_millis(1));
                    match job {
                        SchedulerJob::Tick => {
                            tick_scheduled.store(false, Ordering::SeqCst);
                            // fire request_batched_tick() under catch_unwind
                            // (today's lib.rs:314-319 body)
                        }
                        SchedulerJob::DeliverMutationErrors => {
                            errors_scheduled.store(false, Ordering::SeqCst);
                            // lock core, drain + deliver errors
                            // (today's lib.rs:332-360 body)
                        }
                    }
                }
                // Channel closed → runtime shut down → thread exits.
            })
            .expect("spawn jazz-rn scheduler thread");
        SchedulerWorker { sender, handle: Some(handle) }
    }

    fn shutdown(&self) {
        if let Ok(mut slot) = self.worker.lock()
            && let Some(mut worker) = slot.take()
        {
            drop(worker.sender);
            if let Some(handle) = worker.handle.take() {
                let _ = handle.join();
            }
        }
    }
}
```

- **Boundedness:** per debounce flag, at most one job sits in the channel while one is in flight,
  so the channel never exceeds ~2 entries and the extra thread count is exactly 1 per runtime, no
  matter how long the JS thread stays busy.
- **Failure mode:** if thread spawn ever fails, it fails once at first use with a clear error —
  not nondeterministically at write #3347.
- **Lifecycle:** `RnRuntime::close()` calls `scheduler.shutdown()` (drop sender → worker loop
  ends → join). `set_callback(None)` in `close` (already present) prevents late callbacks.
- The same pattern is portable to `NapiScheduler`
  ([napi tick notifier issue](../issues/napi-tick-notifier-takes-core-lock.md)); that port is out
  of scope here.

## Behavior preserved (invariants)

- All three recovery semantics (crash recovery, seal-before-rows, fate promotion) still hold —
  moved in time from "every tick" to "startup / relevant message arrival", never dropped.
- Runtimes without a durability identity (worker-bridged browser clients) behave exactly as
  today: no seed, empty set, no recovery work.
- Scheduler debounce/coalescing timing (1ms) and callback panic-swallowing are unchanged.
- No storage format changes; the pending set is in-memory and rebuilt from storage on startup.

## Edge cases

- **Batch stuck pending forever** (rows never arrive): stays in the set (a few bytes), retried
  only when messages are applied; server-connect reconciliation remains the last resort — same
  eventual visibility as today, without the per-tick cost.
- **Storage errors during a recovery pass:** keep the id pending, `tracing::warn!` (mirrors
  today).
- **Restart:** in-memory set lost by design; startup seed rebuilds it.
- **Multiple servers:** unchanged — recovery operates on storage state, not per-server state.

## Testing

Black-box through public APIs; read `crates/jazz-tools/TESTING_GUIDELINES.md` in full before
writing the Rust tests.

1. **Recovery behavior preservation (core, Rust integration tests):**
   - seal delivered before its member rows settles once the rows arrive;
   - a runtime opened over storage containing an unsettled sealed submission settles it at
     startup;
   - a lower-tier `DurableDirect` fate is promoted by a higher-tier authority.
2. **Performance guard (core):** a counting `Storage` wrapper asserts
   `scan_sealed_batch_submissions` is called once at startup and never during a stream of N bare
   inserts. Call-count assertions instead of wall-clock keep CI stable.
3. **Scheduler (jazz-rn, native Rust tests):** with a counting/blocking `BatchedTickCallback`:
   bursts coalesce to ≤1 pending job; a deliberately blocking callback does not grow thread count
   or channel depth; `shutdown()` joins cleanly.
4. **End-to-end regression:** promote
   `packages/jazz-tools/src/runtime/stress-expo-stall.repro.test.ts` into the suite — 15k bare
   inserts against a local server must complete (generous timeout, no time assertions). Manual
   validation: expo stress app generates 15k todos on the iOS simulator with a responsive UI and
   no `EAGAIN` panic.

## Out of scope

- Ack-path costs: `local_batch_rows` empty-result fallback full-scan (`ticks.rs:138-168`) and
  per-ack `scan_history_row_batches` — filed as
  [ack-path batch row full scans](../issues/ack-path-batch-row-full-scans.md).
- Porting the scheduler rework to `jazz-napi`.
- Changing the stress app to use `db.batch(...)` — it must keep exercising the bare-insert path.
