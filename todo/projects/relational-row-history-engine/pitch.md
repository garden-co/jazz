# Relational Row History Engine

Jazz currently has a deep mismatch at its core: the system that stores data thinks in objects, branches, commits, tips, and merge structure, while the system that answers queries thinks in rows, columns, indices, and relational visibility. On `main`, we pay that cost every time we turn object history back into rows. In the ongoing batch-branch work, we are still trying to optimize a non-relational substrate underneath a relational engine. The project proposed here is to stop optimizing that seam and remove it: replace the current row-as-object substrate with a row-version storage engine, then add authority-driven transactions on top of that cleaner base.

## Solution

The project has three slices, and the first two are intentionally large:

1. replace all current functionality with the new row-region engine
2. add cross-row transactions, staging, authorities, and fate
3. expose public history, time-travel, and branch-view query APIs

The point of the split is not review convenience. It is conceptual clarity.

- Slice 1 proves that Jazz can do all of today's jobs with a simpler storage model
- Slice 2 then adds the stronger write semantics without also trying to invent the storage model at the same time
- Slice 3 turns storage capabilities we already have by then into explicit user-facing query features

### Slice 1: Replace the Current Engine with Row Regions

Slice 1 replaces the current user-row path end to end:

- reads
- writes
- persistence
- reconnect/restart behavior
- multi-tier sync
- better settled semantics for direct writes

The base model is one physical keyed table space per user table with two regions:

- `visible`: the current user-meaningful state for ordinary reads
- `history`: all row versions over time, using the same row shape

The key split is intentional:

- `visible` is keyed by `($branch, $row_id)`, so all current rows for one logical branch are packed together again
- `history` is keyed by `($row_id, $version_id)`, so one logical row's full version DAG stays tightly grouped

Slice 1 keeps commit-like ancestry, but at row granularity:

- one DAG per logical row
- branches are visible heads over that DAG, not separate ancestry universes
- merge meaning, ancestor checks, and MRCA logic stay available without dragging the old object layer along

Each row version stores user columns and reserved system columns side by side using the existing row encoding machinery. There is not a separate hot-path metadata struct for system columns. The canonical representation should be one encoded row buffer. The reserved system columns should include at least:

- `$row_id`
- `$branch`
- `$version_id`
- `$created_by`
- `$updated_by`
- `$parents`
- `$generation`
- `$state`
- `$confirmed_tier`
- `$is_deleted`
- `$metadata`

To keep the implementation honest, the code structure should follow the design directly:

- `row_histories` for row-local DAG semantics and visible-entry transitions
- `row_format` for the generic binary row codec used by both user data and system data
- `storage` for physical key layout and backend-specific persistence

That means the row-history subsystem should not own the generic codec, and the generic codec should not stay buried under `query_manager`.

Slice 1 should also reserve a history-only optional `$tx_id` placeholder:

- direct writes leave `$tx_id = null`
- Slice 2 reuses that field as the real cross-row transaction id
- visible rows do not need to pay that overhead yet
- this also means we should invest in better optional-column encoding rather than assuming "nullable" is automatically compact enough

This matters for more than storage layout:

- column access should look the same for user and system columns everywhere
- on-disk, in-memory, and sync payloads should share the same canonical row format as much as possible
- the existing fast reproject logic should be reused to encode subsets of columns for sync, subscriptions, and internal metadata-only paths
- the shared binary codec should become a neutral `row_format` module rather than living under query planning code
- `$updated_at` should be derived from `$version_id` rather than stored as a separate hot-path field
- parent pointers should reference only version ids; same-row ancestry is enforced by the enclosing history region rather than repeating row ids in every parent edge

Direct writes become simple:

- append one encoded row version to the history region
- upsert the current winning row version into the visible region for that branch
- update visible-region indices

The visible region should not duplicate whole rows per durability tier. Instead, one visible entry should carry:

- `current_version_id`
- optional `worker_version_id`
- optional `edge_version_id`
- optional `global_version_id`
- `current_data`

with chained defaults:

- `worker_version_id` defaults to `current_version_id`
- `edge_version_id` defaults to the worker winner
- `global_version_id` defaults to the edge winner

That keeps globally settled rows especially cheap while still allowing one branch to serve queries with different required durability tiers.

Ordinary queries become simple:

- compile against the visible region by default
- keep history-region access internal in Slice 1; public history/time-travel/branch-view APIs are deferred to Slice 3

Sync becomes row-oriented instead of commit-oriented:

- ship row-version appends in the same encoded row format
- ship visible-region upserts/removals in the same encoded row format
- use reprojected column subsets when a payload does not need the full row
- ship row-state or durability changes as updates to system columns rather than as a second structural metadata channel
- keep row-version parent relationships in the sync model so ancestry remains precise across devices

As part of the pre-Slice-2 cleanup, the remaining compatibility tissue from the old model should go away:

- legacy `Commit`
- legacy `StoredState`
- legacy `CommitAckState`
- legacy object/branch containers for user rows
- production `ObjectManager` ownership of row state

The runtime should instead rely on:

- storage-backed row apply / patch helpers
- pure row-history reducer logic
- a small monotonic clock in `RuntimeCore`

Settled semantics improve already in Slice 1 because the runtime no longer has to reconstruct direct-write state from commit ids, branch frontiers, and separate ack state. The persisted row-version metadata itself becomes the durable write record. But Slice 1 deliberately keeps the external product semantics for direct writes close to today:

- direct writes still feel like ordinary direct writes plus durability tiers
- reconnect/restart gets much more robust internally
- we do not introduce explicit public transaction-fate APIs for direct writes yet
- the richer accepted/rejected settlement model becomes part of Slice 2 with authorities and transactions

#### Breadboards

```text
alice updates a todo
  -> append a new encoded row version to todos.history
  -> row version includes version id + parent version ids inside one row-local DAG
  -> upsert the current winning version for that branch to todos.visible
  -> update visible-region indices
  -> sync ships the row version and its current confirmed tier
  -> bob's ordinary query reads only todos.visible
```

```text
alice reconnects after going offline
  -> local engine loads todos.visible directly for current reads
  -> sync resumes from row-version and row-metadata state, not object tips
  -> direct-write durability state is recovered internally without changing the public write API yet
```

#### Fat Marker Sketch

```text
Before:
  QueryManager
    -> rows from ObjectManager
    -> ObjectManager loads objects/branches/commits from Storage
    -> SyncManager talks in object/commit terms

After Slice 1:
  QueryManager
    -> ordinary queries read visible regions
    -> history regions exist but remain primarily internal
    -> Storage persists row versions directly
    -> SyncManager talks in row-version and row-metadata terms
```

#### Core shape

```rust
pub enum RowState {
    Rejected,
    VisibleDirect,
    StagingPending,
    VisibleTransactional,
}

pub struct StoredRowVersion {
    pub history_key: (ObjectId, VersionId),
    pub encoded_row: Vec<u8>, // system columns + user columns in one canonical row encoding
}

pub struct VisibleRowEntry {
    pub visible_key: (BranchId, ObjectId),
    pub current_version_id: VersionId,
    pub worker_version_id: Option<VersionId>,
    pub edge_version_id: Option<VersionId>,
    pub global_version_id: Option<VersionId>,
    pub current_data: Vec<u8>,
}
```

System columns inside `encoded_row`:

- `$row_id`
- `$branch`
- `$version_id`
- `$created_by`
- `$updated_by`
- `$parents`
- `$generation`
- `$tx_id` // optional, history-only in Slice 1
- `$state`
- `$confirmed_tier`
- `$is_deleted`
- `$metadata`

### Slice 2: Add Batches, Transactions, Authorities, and Fate

Once Slice 1 is real, Slice 2 adds the stronger semantics the current system struggles to express cleanly:

- cross-row transactions
- opt-in transactions
- staging writes before visibility
- authority acceptance/rejection
- explicit transaction fate
- replayable reconnect semantics for pending work

The storage shape does not change. The meaning of the row metadata gets richer. The row-level version DAG also does not change. Slice 2 adds cross-row transaction identity and fate on top of the same per-row ancestry model.

Transactional writes use the history region first:

- append staging row versions with `$state = staging_pending` inside the same canonical encoded row format
- set the same non-null `$tx_id` on every row version participating in that transaction
- do not touch the visible region yet

Authorities then decide fate:

- if accepted, patch history-row metadata in place and upsert visible-region rows
- if rejected, patch history-row metadata to `rejected` and leave visible unchanged

That gives us one coherent model:

- the history region records what was attempted
- the visible region records what is currently visible
- authority-driven fate is expressed as row metadata, not as a second bespoke mechanism

#### Breadboards

```text
alice starts a cross-row transaction touching todo/1 and project/9
  -> append staging row versions to history only
  -> each row version keeps its own per-row parent pointers
  -> both rows share the same $tx_id
  -> visible state does not change yet

authority accepts the transaction
  -> patch those history rows from staging_pending -> visible_transactional
  -> upsert the corresponding visible rows
  -> bob sees the transaction through ordinary visible queries
```

```text
authority rejects the transaction
  -> patch those history rows from staging_pending -> rejected
  -> visible state stays unchanged
  -> alice can still inspect the rejected attempt through history and outcome APIs
```

#### Fat Marker Sketch

```text
Slice 1:
  direct visible rows only

Slice 2:
  direct visible rows
  + staging rows
  + authorities
  + accepted/rejected transaction fate
  + transaction-aware visibility
```

#### Core shape

```rust
pub enum TxSettlement {
    Missing,
    Rejected { reason: String },
    Accepted { confirmed_tier: DurabilityTier },
}
```

### Slice 3: Public History, Time-Travel, and Branch Views

Once Slice 1 and Slice 2 are both real, Slice 3 exposes the query capabilities that the row-region engine was designed to make natural:

- `query.history()`
- `query.as_of(ts)`
- `query.branch_view(branch)`

By this point, the underlying model already exists:

- the history region already stores row versions in the canonical row format
- the visible region already stores current visible state
- transactional acceptance/rejection semantics already exist

That means Slice 3 is not a new storage project. It is a query-surface and execution-planning project.

#### Breadboards

```text
alice runs query.history()
  -> QueryManager targets the history region
  -> rows come back with the same encoded-row format as ordinary reads
  -> system columns make state, transaction identity, and durability visible when requested
```

```text
alice runs query.as_of(ts).branch_view("draft")
  -> QueryManager targets history rows for branch "draft"
  -> executor finds the latest visible row versions at or before ts
  -> result looks like an ordinary relational query over a past branch image
```

#### Fat Marker Sketch

```text
Slice 1:
  row-region engine exists
  public queries still focus on current visible state

Slice 2:
  transactional write semantics exist

Slice 3:
  public history/time-travel/branch-view APIs sit on top of the same engine
```

### Why This Split

This is not a gradual migration plan. It is a replacement plan with two major semantic steps on top:

- Slice 1 says: all of today's engine responsibilities should already work on row regions
- Slice 2 says: now that the substrate is relational, transactions become a metadata/state problem rather than a storage-graph problem
- Slice 3 says: once the storage and transactional semantics are stable, public historical query APIs can land cleanly on top

That is the main bet of the project: the new system is simpler because the storage model and the query model finally agree on what a row is.

## Rabbit Holes

- Slice 1 is still a large rewrite. If we keep too much object-manager compatibility around, we will lose the simplicity benefit and pay for two engines at once.
- The visible region is authoritative. If visible/history drift can occur without immediate detection, the design becomes dangerous rather than simplifying.
- Multi-tier sync currently depends on commit ids, object metadata, and branch-frontier reasoning. Slice 1 needs equally crisp idempotence and replay rules in row-region terms.
- Query execution is only simpler if ordinary reads really stay visible-first. If too many queries fall back to reconstructing current state from history, we will rebuild MVCC cost in the hot path.
- Transaction-level confirmed tiers and fate patching may be expensive for wide transactions. Slice 2 must prove the fan-out cost is acceptable.
- Time-travel and branch-view queries are conceptually clean here, but Slice 3 still needs access paths that are fast enough to be real features rather than debugging tools.

## No-gos

- No hybrid forever architecture where some user rows stay object-backed and others become row-region-backed indefinitely.
- No reintroducing a table-global object DAG under new names. The only preserved commit-like structure should be the row-local version DAG itself.
- No second write-fate mechanism beside row metadata and row-region settlement state.
- No optimizing for tiny review diffs at the cost of a distorted architecture.
- No appetite-driven compromise where Slice 1 lands as only a toy storage experiment without replacing the real runtime path.

## Testing Strategy

Use integration-first tests and benchmark comparisons, with all three slices tested against realistic scenarios.

For Slice 1:

- SchemaManager and RuntimeCore tests where `alice` writes direct visible rows and `bob` queries/subscribes through the visible region
- restart tests where current reads and direct-write durability recovery come from persisted visible/history state
- multi-tier sync tests where row versions and row-metadata updates replay cleanly across client, edge, and server
- deletion tests verifying visible reads omit tombstones while history preserves them
- benchmark comparisons against `main` for point reads, visible scans, direct writes, restart cost, sync payload size, and on-disk size

For Slice 2:

- transaction acceptance/rejection tests using realistic multi-row writes by `alice`, visible reads by `bob`, and replay/restart behavior after drops
- outcome tests verifying staged, accepted, and rejected history rows are all inspectable with correct visible state
- durability/fate tests verifying accepted transactions advance tier correctly and only then satisfy strict visibility requirements
- reconnect tests where pending transactional work resumes from persisted row metadata without reconstructing object frontiers

For Slice 3:

- public API tests for `query.history()`, `query.as_of(ts)`, and `query.branch_view(branch)`
- query-planning tests proving those APIs target the correct region and branch image
- performance tests for history scans and as-of reconstruction on realistic row histories
