# Y-CRDT as Object Layer

Replace the commit DAG (Object/Branch/Commit) with Yrs documents. Each row becomes a Yrs `Doc` with columns as Map keys. Automatic per-field CRDT merge replaces explicit merge commits and last-write-wins row-level conflict resolution.

## Motivation

1. **Finer-grained merge.** The current model is LWW per-row. Two clients editing different columns on the same row conflict unnecessarily. Yrs gives per-field LWW, and character-level CRDT merge for collaborative text.
2. **Simpler foundation.** The commit DAG (content-addressed BLAKE3 commits, tip/tail tracking, topological sort for sync, explicit merge commits) is significant machinery. Yrs handles causal consistency, deduplication, and convergence internally.
3. **Yjs ecosystem compatibility.** Yrs is the Rust port of Yjs. Adopting it opens the door to the Yjs awareness protocol, existing editor bindings, and a large community.

## Core Data Model

### RowDoc (replaces Object)

Each database row is a Yrs `Doc`. Columns are keys in the Doc's root `MapRef`.

```
RowDoc
  id: ObjectId (UUIDv7 -- unchanged)
  doc: yrs::Doc
  root_map: MapRef  (doc.get_or_insert_map("row"))
    "column_a" -> YValue
    "column_b" -> YValue
    ...
  metadata: HashMap<String, String>  (table name, storage class, etc. -- stored outside the Doc, not CRDT-merged)
  branches: HashMap<BranchName, ObjectId>  (points to branch RowDocs -- see Branches section)
  origin: Option<(ObjectId, Vec<u8>)>      (parent doc id + state vector at fork time -- None for non-branch docs)
```

### Column type mapping

| Jazz2 column type    | Yrs value type                                                       |
| -------------------- | -------------------------------------------------------------------- |
| TEXT, UUID           | `YValue::Any(String)`                                                |
| INTEGER, BIGINT      | `YValue::Any(i64)`                                                   |
| FLOAT                | `YValue::Any(f64)`                                                   |
| BOOLEAN              | `YValue::Any(bool)`                                                  |
| BYTEA                | `YValue::Any(Vec<u8>)`                                               |
| UUID[] (FK arrays)   | `MapRef` (nested shared type -- FK values as keys for set semantics) |
| TEXT (collaborative) | `TextRef` (nested shared type -- character-level CRDT)               |

### Conflict resolution

Automatic, per-field:

- Two clients update different columns on the same row: no conflict, both apply.
- Two clients update the same column concurrently: resolved deterministically by Yrs using client ID seniority (higher ID wins). Not wall-clock LWW.
- Collaborative text fields: character-level CRDT merge (insertions interleave correctly).

No explicit merge commits. No tip selection. The Doc has exactly one current state at all times.

### Client ID assignment

Yrs conflict resolution depends on client IDs -- higher ID systematically wins concurrent conflicts. Client IDs are assigned randomly per `Doc::new()` (u64). Because each RowDoc is a separate Doc, client IDs vary across rows, so no single peer wins everywhere. Within a single row-Doc, the asymmetry exists for the Doc's lifetime but is bounded to that row. This is acceptable because concurrent same-field conflicts are rare in practice, and the resolution is deterministic across all peers.

Client IDs are stable per Doc instance. When a Doc is loaded from storage, it gets a new client ID for future writes (old writes retain their original client IDs in the CRDT history).

### DocManager (replaces ObjectManager)

```rust
pub struct DocManager {
    docs: HashMap<ObjectId, RowDoc>,
    storage: Box<dyn Storage>,
    subscribers: HashMap<ObjectId, Vec<SubscriberId>>,
    all_subscribers: Vec<AllSubscriberId>,
}
```

Public API:

| Method                              | Purpose                                                  |
| ----------------------------------- | -------------------------------------------------------- |
| `create() -> ObjectId`              | Create new RowDoc with fresh UUIDv7                      |
| `create_with_id(id)`                | Deterministic ID (index roots, etc.)                     |
| `get(id) -> Option<&RowDoc>`        | Read from memory                                         |
| `get_or_load(id)`                   | Lazy load from storage                                   |
| `apply_update(id, update)`          | Apply a Yrs Update (from local write or sync)            |
| `get_state_vector(id)`              | Get Yrs StateVector for sync                             |
| `encode_diff(id, sv)`               | Encode changes since a remote StateVector                |
| `transact(id) -> ReadTxn`           | Read-only access to Doc                                  |
| `transact_mut(id) -> WriteTxn`      | Read-write access, auto-commits on drop                  |
| `subscribe(id)` / `unsubscribe()`   | Per-doc change notifications                             |
| `subscribe_all()`                   | Global change notifications (for QueryManager)           |
| `fork(id, branch_name) -> ObjectId` | Snapshot Doc, create branch RowDoc (see Branches)        |
| `merge(source_id, target_id)`       | Apply source's changes into target (see Branches)        |
| `list_branches(id)`                 | List named branches of a doc                             |
| `delete_branch(id, name)`           | Delete a branch RowDoc                                   |
| `evict(id)`                         | Unload Doc from memory (can be reloaded via get_or_load) |

Removed: `add_commit`, `receive_commit`, `replace_content`, `get_tip_ids`, `get_tips`, `get_commits`, `truncate_branch`, `next_timestamp`.

### Memory management

One Yrs Doc per row means potentially many in-memory Doc instances. The DocManager uses lazy loading (`get_or_load`) and explicit eviction (`evict`) to bound memory:

- Docs are loaded from storage on first access and held in memory.
- `evict(id)` drops the in-memory Doc (and its `observe_deep` subscriptions). The Doc can be reloaded later from storage.
- An LRU eviction policy can be layered on top: when the doc count exceeds a threshold, evict least-recently-accessed docs.
- When a Doc is evicted and reloaded, `observe_deep` subscriptions for index maintenance must be re-established. The DocManager handles this transparently inside `get_or_load`.

## Storage

The Storage trait drops all commit/branch/tail operations and stores two things per row: a snapshot (full Yrs Doc state) and an update log (incremental deltas since last snapshot).

### Storage trait (revised)

```rust
pub trait Storage {
    // Documents
    fn create_doc(&mut self, id: ObjectId, metadata: HashMap<String, String>);
    fn load_doc(&self, id: ObjectId) -> Option<StoredDoc>;
    fn save_snapshot(&mut self, id: ObjectId, snapshot: Vec<u8>);
    fn append_updates(&mut self, id: ObjectId, updates: Vec<Vec<u8>>);
    fn load_snapshot(&self, id: ObjectId) -> Option<Vec<u8>>;
    fn load_updates(&self, id: ObjectId) -> Vec<Vec<u8>>;
    fn clear_updates(&mut self, id: ObjectId);  // after compaction
    fn delete_doc(&mut self, id: ObjectId);     // hard delete

    // Indices (unchanged)
    fn index_insert(&mut self, key: &[u8], value: &[u8]);
    fn index_remove(&mut self, key: &[u8]);
    fn index_lookup(&self, key: &[u8]) -> Option<Vec<u8>>;
    fn index_range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)>;
    fn index_scan_all(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)>;

    // Lifecycle (unchanged)
    fn flush(&mut self);
    fn flush_wal(&mut self);
    fn close(&mut self);
}
```

### Load path

```
1. Load snapshot -> Doc::new() + txn.apply_update(snapshot)
2. Load updates -> apply each in order
3. Doc is at current state
4. Re-establish observe_deep subscriptions for index maintenance
```

### Compaction (replaces truncate_branch)

The update log grows unboundedly. Periodic compaction bounds it:

```
1. Encode full Doc state -> new snapshot
2. save_snapshot(id, snapshot)
3. clear_updates(id)
```

Trigger: update count exceeds threshold or on a timer.

Crash safety: if the process crashes between `save_snapshot` and `clear_updates`, the next load replays stale updates on top of the snapshot. This is safe -- Yrs deduplicates by client ID + sequence number, so redundant updates are no-ops. The update log just contains waste until the next compaction.

### Storage key layout

```
doc:{object_id}:meta              -> metadata (serialized HashMap, not CRDT-merged)
doc:{object_id}:snapshot          -> latest Yrs snapshot (encoded v1)
doc:{object_id}:log:{seq}         -> individual update (seq = monotonic counter)
doc:{object_id}:branches:{name}   -> branch ObjectId
doc:{object_id}:origin            -> (parent ObjectId, state vector at fork)
idx:{table}:{column}:{value}:{row_id} -> index entries (unchanged)
```

Wire format: v1 encoding throughout (`encode_state_as_update_v1`, `encode_diff_v1`). v2 is more compact but less mature in Yrs. Can be reconsidered as an optimization later.

All operations remain synchronous. No async gaps.

## Sync

The SyncManager keeps its current structure -- connection management, upstream/downstream roles, subscription filtering, durability tiers, ack tracking. The payload format changes and topological ordering is no longer needed.

### Payload change

Before: Commits shipped parent-before-child (topological sort required). Receiver calls `receive_commit()` and rebuilds the DAG.

After: Yrs encoded updates shipped as opaque blobs. Receiver calls `doc_manager.apply_update()`. Order doesn't matter -- Yrs handles causal consistency internally via state vectors.

### Per-doc sync protocol

```
Initial sync:
  1. Client sends StateVector for doc (what it has)
  2. Server encodes diff (what client is missing)
  3. Server sends diff as single Update blob
  4. Client applies Update

Ongoing (via observe_update_v1 callback -- distinct from observe_deep used for index maintenance):
  1. Local write -> Yrs observe_update_v1 callback fires with raw update bytes
  2. Ship update bytes to connected peers
  3. Peers call doc_manager.apply_update() with received bytes
```

### What stays the same

- Connection types (upstream server / downstream client)
- Subscription filtering (clients only get docs they subscribe to, filtered by ObjectId)
- Durability tiers (Worker -> Edge -> Global)
- QuerySettled / PersistenceAck signals (triggered after update applied + stored)
- Role-based auth (User/Admin/Peer)

### What simplifies

- No topological sort required by the sender. Yrs tolerates out-of-order delivery by buffering pending blocks internally until causal predecessors arrive. In-order delivery is still preferred to minimize receiver-side buffering.
- No merge commits over the wire. Diverged state converges automatically.
- Smaller diff encoding. Yrs state vectors are compact (one clock entry per client). Diffs contain only missing operations.
- Idempotency is built-in. Applying the same Yrs update twice is a no-op (deduplication by client ID + sequence number).

### Ack tracking

Currently acks reference CommitId. Now acks reference the state vector after application -- the server confirms "I've persisted all operations up to this state vector." More informative than per-commit acks since it represents cumulative state.

Client-side matching: when a local write occurs, the client captures the Doc's state vector immediately after the write transaction commits. This becomes the "write token." When a server ack arrives with a state vector that subsumes the write token (every clock entry >= the token's), the write is considered durable at that tier. This replaces the per-commit `ack_state.confirmed_tiers` model -- durability is tracked per state-vector threshold, not per individual operation.

## Policy & Permissions

The current policy system validates operations against schema-defined rules. It now inspects decoded Yrs updates instead of commits.

### Inspection point

```
Client sends Update
  -> SyncManager receives
  -> Decode update -> extract changed keys + values
  -> PolicyEngine.validate(doc_id, author, changes)
  -> If allowed: apply_update() + relay to other peers
  -> If denied: reject, send error back to client
```

### What the policy engine sees

Each decoded operation maps to:

```rust
struct PolicyChange {
    doc_id: ObjectId,
    key: String,        // column name
    value: YValue,      // new value (or delete marker)
    author: ClientId,   // who made the change
}
```

This is more granular than today (whole row blob). Enables per-column policies in the future.

### What stays the same

- Schema-declared policies (defined in schema files)
- Role-based access (User/Admin/Peer)
- Row-level policies via FK declarations
- Policy evaluation on the server (enforcement point)

### Optimistic updates

Client applies updates locally immediately. If the server rejects, the client rolls back via Yrs `UndoManager`. Cleaner than removing rejected commits from a DAG.

### Limitation

Decoding Yrs updates for policy inspection has a cost. For high-throughput scenarios (real-time text with many small edits), the policy engine may need to batch-validate or use coarser doc-level checks. Optimization concern, not architectural.

## QueryManager

The QueryManager reads from Yrs Doc state instead of deserializing commit content.

### Reading row data

Before: `get_tips()` -> pick newest -> deserialize content bytes -> columns.

After: `doc_manager.transact(id)` -> `root_map.get(txn, "column_name")` -> value. The Doc is the current state.

### Index maintenance

Yrs `observe_deep()` on the root MapRef delivers change events. The callback receives `&Events` (a `Vec<&Event>`); each event must be pattern-matched on `Event::Map` to access `MapEvent::keys(txn)` which yields `EntryChange` variants. Index deltas are computed directly from these events.

```rust
root_map.observe_deep(move |txn, events| {
    for event in events.iter() {
        if let Event::Map(map_event) = event {
            for (key, change) in map_event.keys(txn) {
                match change {
                    EntryChange::Inserted(value) => index_insert(..),
                    EntryChange::Updated(old, new) => {
                        index_remove(old..);
                        index_insert(new..);
                    },
                    EntryChange::Removed(old) => index_remove(..),
                }
            }
        }
    }
});
```

These subscriptions are established when a Doc is loaded (`get_or_load`) and torn down on `evict`. They fire during `transact_mut` drop (transaction commit) -- callbacks cannot create nested write transactions.

### Query evaluation

No changes to query semantics. SELECT, WHERE, JOIN, index lookups -- all work the same, reading from Yrs Maps instead of deserialized commit bytes.

### What simplifies

- No tip selection logic. No "newest timestamp wins."
- No merge-aware query planning. Diverged branches don't exist.
- Precise change events. Field-level deltas vs. whole-row diffing.
- No content deserialization. Values are already typed in the Yrs Map.

## Deletion Semantics

### Soft delete

Set a reserved key `_deleted` in the root MapRef to `"soft"`. The row remains in the Doc, all column values preserved. The QueryManager filters soft-deleted rows from queries by default (same as today's `_id_deleted` index check). Undelete by removing the `_deleted` key.

Concurrent edit + soft delete: Yrs merges both -- the `_deleted` key appears alongside the edit. QueryManager sees `_deleted` and excludes the row. If undeleted later, the concurrent edit is preserved.

### Hard delete

Set `_deleted` to `"hard"` and clear all column values (set each to null/remove). The Doc still exists (Yrs tombstones are permanent), but content is gone. Hard delete is authoritative -- if a peer concurrently edits and another hard-deletes, the hard delete wins at the QueryManager level (check `_deleted` key before returning any row).

To reclaim storage, `delete_doc()` on the Storage trait removes all storage artifacts (snapshot, update log, metadata). The DocManager evicts the Doc from memory. Sync propagates the hard-delete marker before the storage-level cleanup.

### Index interaction

- Soft delete: remove row from all column indices, add to `_id_deleted` index.
- Hard delete: remove from all indices including `_id_deleted`.
- Undelete: re-add to column indices, remove from `_id_deleted`.

Triggered via `observe_deep` on the `_deleted` key, same as any other field change.

## Schema Versioning

Schema versioning and lenses are being redesigned separately. The current branch-based schema version coexistence (different branches per schema version) does not apply in a branchless model. This section will be addressed in a dedicated spec.

For MVP: single schema version per table, no lenses, no cross-version indexing. Schema migrations are breaking changes (acceptable per the project's pre-launch backcompat policy).

## Branches

Branches are Doc forks. Each branch is a separate Yrs Doc that shares causal history with its origin.

Every RowDoc can have named branches. Branch RowDocs are full RowDocs themselves (stored, synced, subscribable), with an `origin` that records where they forked from. The `branches` and `origin` fields on RowDoc (defined in Core Data Model above) support this.

### Fork

```
fork(row_id, "draft"):
  1. state = row_doc.doc.transact().encode_state_as_update_v1(&StateVector::default())
  2. branch_doc = Doc::new()
  3. branch_doc.transact_mut().apply_update(Update::decode_v1(state))
  4. branch_id = new ObjectId
  5. Store branch RowDoc with origin = (row_id, row_doc.state_vector())
  6. Register branch_id under row_doc.branches["draft"]
  7. Persist both docs to Storage
```

`encode_state_as_update_v1` captures the full Doc state including any pending blocks. This is correct for fork -- the branch should start with everything the origin has.

The branch Doc starts with identical state and evolves independently. Both the origin and the branch can receive concurrent edits.

### Merge

```
merge(branch_id, main_id):
  1. sv = main_doc.doc.transact().state_vector()
  2. diff = branch_doc.doc.transact().encode_diff_v1(sv)
  3. main_doc.doc.transact_mut().apply_update(Update::decode_v1(diff))
  4. Optionally: delete branch RowDoc, or keep as archive
```

`encode_diff_v1` encodes only the operations the main doc does not have (excluding pending blocks on the branch, which is correct -- pending blocks lack causal predecessors and should not be transferred).

Yrs handles all conflict resolution automatically:

- Fields edited only on branch: applied cleanly.
- Fields edited only on main since fork: preserved.
- Fields edited on both: resolved by client ID seniority (same as concurrent edits).
- Collaborative text edited on both: character-level CRDT merge.

### Sync

Branch RowDocs sync like any other RowDoc -- the SyncManager ships their updates independently. The `origin` metadata syncs as part of the doc's metadata so all peers know the branch relationship.

### Index interaction

Branch RowDocs can be indexed independently. The QueryManager needs to know whether to include branch rows in queries:

- By default, queries read from "main" docs only (no `origin`).
- Branch-aware queries (e.g., for preview/diff UIs) can opt in to reading branch docs.
- When a branch is merged, the main doc's indices update via `observe_deep` as usual.

### Limitations vs. git-style branches

- No commit-by-commit history within a branch (unless snapshots are layered on top).
- No cherry-pick (merge is whole-branch, not individual changes).
- No three-way diff view -- Yrs merge is opaque, you see the result but not "what conflicted."
- Merge is always full-state, not selective.

## What Gets Deleted

### Types removed

- `Object` (branches, metadata container) -- replaced by RowDoc
- `Branch` (commits, tips, tails, loaded_state) -- the git-like Branch type is removed; branching as a concept is preserved via Doc forks (see Branches section above)
- `Commit` (parents, content, BLAKE3 hash) -- no commits, Yrs updates instead
- `CommitId` ([u8; 32]) -- no content addressing
- `BranchLoadedState` -- replaced by DocManager eviction/reload
- `TruncateResult` / `TruncateError` -- replaced by compaction
- `SubscriptionUpdate` (frontier/tips) -- replaced by Yrs observe callbacks
- `AllObjectUpdate` (old_content, previous_commit_ids) -- replaced by Yrs change events

### ObjectManager methods removed

`add_commit`, `receive_commit`, `replace_content`, `get_tip_ids`, `get_tips`, `get_commits`, `truncate_branch`, `next_timestamp`.

### Storage methods removed

`append_commit`, `delete_commit`, `set_branch_tails`, `load_branch`.

### Sync logic removed

Topological sorting of commits, CommitId-based deduplication, parent validation, merge commit creation/relay.

### New dependency

`yrs` crate (already in repo at `y-crdt/yrs`) becomes a core dependency across `jazz-tools`, `jazz-wasm`, and `jazz-napi`.
