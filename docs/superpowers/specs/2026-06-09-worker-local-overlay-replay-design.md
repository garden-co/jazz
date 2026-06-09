# Worker local overlay replay design

## Purpose

Browser reload currently rebuilds main-thread query visibility by replaying
worker-retained `LocalBatchRecord` metadata into main. That makes main duplicate
worker batch cache state and lets main trigger manual batch reconciliation. It
also couples a query visibility concern to sync/storage recovery metadata.

This design replaces retained batch-record replay with retained local overlay
replay. The worker remains the owner of durable browser storage and batch
reconciliation. Main receives only the volatile row-level facts its query manager
needs to render locally visible rows after reload.

## Scope

In scope:

- Replace worker-to-main `LocalBatchRecordsSync` startup replay with a row-level
  local overlay replay.
- Stop hydrating worker `LocalBatchRecord`s into the main runtime.
- Stop main from calling batch reconciliation as a result of worker startup
  replay.
- Make main batch-fate processing tolerate overlay-only batch state without
  falling back to full row-locator/history scans.
- Clear hydrated overlay entries when durable row or fate processing makes them
  obsolete.
- Prove retained overlay replay does not use row-locator or row-history
  forensic scans.

Out of scope:

- Adding a `batch_id -> rows` storage index.
- Terminal retained-batch garbage collection.
- Removing the main runtime entirely or serving all queries through worker RPC.
- Reworking rejected mutation-error replay. That remains a separate path.

## Architecture

Worker remains the durable runtime. Main remains the UI-facing runtime for now,
but it no longer mirrors worker batch metadata.

The new boundary is:

- Worker owns storage, sealed submissions, authoritative fates, retained local
  batch records, and upstream reconciliation.
- Main owns volatile query overlay state for rendering.
- Worker sends main a startup snapshot of retained local overlay rows.
- Main applies those rows to query overlay state only. It does not persist them
  as local batch records and does not reconstruct sealed submissions or fates.

This is intentionally smaller than a full one-runtime architecture rewrite while
removing the duplicated batch cache and manual reconciliation responsibility from
main.

The implementation should reuse the existing query overlay mechanism. Main's
overlay hydration API is a thin runtime wrapper around
`QueryManager::pending_local_row_batches` / `local_overlay_rows` behavior, using
the same semantics as `maybe_track_local_pending_batch_overlay` or its exact
equivalent. It must not introduce a parallel overlay state machine.

## Wire Shape

Add a worker-to-main wire payload like:

```rust
pub struct LocalOverlayEntryWire {
    pub table_name: String,
    pub object_id: String,
    pub branch_name: String,
    pub batch_id: String,
}

pub enum WorkerToMainWire {
    LocalOverlaySync {
        entries: Vec<LocalOverlayEntryWire>,
    },
}
```

The exact Rust types can use existing binary encodings where convenient, but the
semantic payload is row-level overlay state, not batch metadata.

## Worker Data Flow

On worker startup:

1. Open durable storage as today.
2. Build retained local overlay entries from trusted retained metadata:
   persisted `LocalBatchRecord`s and memberful sealed submissions. Sealed
   submission members may resolve table names with per-object `load_row_locator`
   point lookups.
3. Do not derive overlay entries by scanning all row locators or all row
   histories.
4. Post `LocalOverlaySync` before `InitOk`.
5. Continue normal worker startup and drain buffered sync messages.

Worker-side reconciliation stays in worker:

- Worker asks upstream for batch fates.
- Worker retransmits local rows/seals to upstream when needed.
- Worker forwards normal row and fate sync messages to main through the existing
  sync channel.

Fate-only batches without a retained local batch record or sealed submission are
not replayed as overlays. The assumption is that well-formed committed local
batches retain a sealed submission even if their `LocalBatchRecord` is pruned.
The existing commit path persists sealed submissions and does not delete them on
ack. A fate-only retained batch therefore indicates incomplete or inconsistent
metadata, not a normal startup overlay source.

## Main Data Flow

On `LocalOverlaySync`:

1. Decode each overlay entry.
2. Insert or replace a volatile `RowBatchKey` for that row in main query overlay
   state.
3. Mark affected subscriptions dirty as local overlay updates.
4. Do not call `hydrate_local_batch_record`.
5. Do not persist a `LocalBatchRecord`.
6. Do not call `reconcile_local_batch_with_server`.

Main batch-fate processing must handle overlay-only state. Before invoking any
scan-enabled row reconstruction helper for a batch, fate handling should consult
overlay entries for that batch. This covers fate-before-seal ordering and
prevents one full database scan per fate after reload.

Main sync processing must also clear stale overlay entries. When the
corresponding row arrives through `RowBatchCreated`, or when a batch fate settles
the row so the visible region can answer the subscription without local overlay,
main removes the matching `RowBatchKey` and dirties affected subscriptions. This
prevents replayed overlay state from shadowing durable rows indefinitely.

## Runtime API

Add a narrow main-runtime API such as:

```rust
pub fn hydrate_retained_local_overlay_row(
    &mut self,
    table_name: &str,
    object_id: ObjectId,
    branch_name: BranchName,
    batch_id: BatchId,
) -> Result<(), RuntimeError>
```

This API should update query overlay state only. It should not touch local batch
record storage and should not start sync reconciliation.

Add a matching query-manager/runtime helper for clearing overlay rows by
`RowBatchKey` or `(object_id, batch_id)` so row and fate sync processing can
evict retained overlay entries once durable state supersedes them.

## Error Handling

- Worker logs and skips malformed or incomplete retained metadata.
- Main logs and skips invalid overlay entries.
- Overlay hydration is idempotent. Replaying the same row replaces the same
  `RowBatchKey`.
- Startup continues if overlay replay has partial failures.

## Testing

Add focused tests that prove the boundary:

- Worker retained overlay payload includes entries for retained local records
  and memberful sealed submissions.
- Worker retained overlay payload generation does not call `scan_row_locators`.
- Main bridge hydration updates query overlay state without persisting a
  `LocalBatchRecord`.
- Main bridge hydration does not call `reconcile_local_batch_with_server`.
- Main fate processing with overlay-only state marks subscriptions dirty without
  scanning row locators or requiring a local batch record.
- Main clears a retained overlay entry when the same row arrives through normal
  sync or when its batch fate makes the overlay obsolete.
- Existing rejected mutation-error replay tests remain separate.

## Non-Goals And Follow-Ups

This design does not solve retained-batch accumulation. A later pass should add
a lifecycle rule for pruning terminal batch envelopes once their retention
boundary is satisfied.

This design also does not fix worker-side
`pending_batch_ids_needing_reconciliation`. The worker may still scan visible
rows while discovering batches that need upstream reconciliation. That remaining
cost delays sync/reconciliation rather than forcing main to hydrate duplicate
batch metadata, and should be addressed as a separate worker reconciliation
cleanup.

This design also does not collapse the two runtimes. If that becomes the goal,
main should become an RPC client for worker-owned query execution. That is a
larger architecture project and is not needed for this fix.
