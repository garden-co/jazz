# Relational Row History with Visible and History Regions

## Problem

Jazz currently has a deep architectural seam:

- the storage/object layer thinks in objects, branches, commits, tips, and merge structure
- the query layer thinks in rows, columns, indices, and live relational results

That seam is expensive. On `main`, we pay for a git-like object model and then reinterpret it as relational state. In the ongoing batch-branch work, we are trying to make that lower layer more explicit and more scalable, but we are still carrying a large amount of non-relational structure below a relational query engine.

The concern behind this spec is not just "can we make `#415` faster?" It is "are we optimizing the wrong boundary?" If the system's real semantic unit is a versioned row in a branch-scoped table, then object DAGs, branch-tip sets, and commit-specific hot paths may be the wrong primitive to optimize around. We may be paying complexity twice: once in the storage/object layer, and again in the query layer that has to turn that back into rows.

This spec explores a radical replacement: make the base substrate of Jazz a lower-level database of tables that contain both user columns and system columns. Branching, staging, durability, and visibility become data expressed directly in row versions, and the hot read path becomes an explicitly maintained visible region rather than a reconstruction from object history.

## Solution

### Chosen approach

This spec recommends replacing the "row = object with branch DAG history" model with a relational version-storage model built from one keyed storage space per user table, split into two table regions:

1. a per-user-table **history region**
2. a per-user-table **visible region**

The engine still has version history, staging, transactions, and durability tiers. The difference is where those concepts live:

- not inside object-manager commit graphs
- not in a separate bespoke metadata structure
- but in rows and reserved system columns inside one lower-level table engine

### Alternatives considered

1. Keep the current object/commit substrate and keep optimizing it.

   This is the lowest-risk option, but it preserves the seam between object history and relational queries. It may improve constants without removing the core mismatch.

2. Land batch branches (`#415`) and add more query-visible metadata beside them.

   This is better than `main`, but it still keeps two hot-path mental models: object/batch history below, relational rows above.

3. Replace the substrate with relational row history plus visible/history regions.

   This is the most radical option, but it is the only one that collapses the storage, visibility, and query models into one consistent data shape. This spec chooses that option.

### Design principles

- The hot path for ordinary reads must be **visible-region first**.
- The visible region must be authoritative for current reads.
- History must be **append-friendly** and compress well.
- The history and visible regions should stay structurally parallel so history/time-travel features can be real product features from the beginning.
- Visibility and settlement rules should be expressible in table terms, even if the engine materializes optimized keyed regions for speed.
- We should have one primary data management system to optimize on disk, in memory, and on the wire.
- Prefer write-time fan-out over read-time joins on the hot path.

### Core model

The system distinguishes five concepts:

- **logical row**: the stable application row identity
- **row version**: one concrete version of a logical row
- **row version DAG**: the per-row commit-like ancestry graph formed by parent version ids
- **logical transaction**: an optional cross-row write unit identified by one `TxId`
- **visible-region row**: the current visible copy of a row version used by ordinary reads

The old object boundary disappears for user rows. A row version is not wrapped in a commit object. It is already a row in a lower-level storage table. The commit-like semantics we keep are row-local:

- one DAG per logical row
- branches are labels and visible heads over that DAG
- ancestry is not branch-local, even if visibility is

### Physical layout

For each user table `todos`, the engine stores one physical keyed table space with two table regions:

- a `visible` region
- a `history` region

The two regions use the same logical column layout:

- user-defined columns
- reserved system columns

The difference is purpose and key shape:

- the `visible` region is the hot read region: one current visible row version per logical row per branch
- the `history` region is the append-heavy history log: all staged, rejected, and visible row versions over time, grouped by logical row so the row-local DAG stays contiguous

Suggested primary key layout:

```text
0 / $branch / $row_id
1 / $row_id / $version_id
```

This gives us both locality goals:

- all current visible rows for a branch sort first and stay tightly packed
- all historical versions of one logical row stay tightly grouped in the history region

Branch-oriented history scans and later `branch_view` / `as_of` features may use secondary raw-table access paths such as `($branch, $row_id, $version_id)`, but those are not the primary history clustering.

The engine may still have shared metadata tables for schema, catalog, and index management, but transaction settlement for user rows is not modeled as a required third hot-path relation.

### Reserved system columns

Every user table's visible and history regions reserve a set of engine columns. The exact names can change, but the model needs at least:

```text
$row_id           stable logical row id, engine-generated as UUIDv7
$branch           user-meaningful branch view
$version_id       row-version id, preferably UUIDv7; decoded timestamp is this version's updated_at
$created_by       actor that created the logical row
$updated_by       actor that produced this row version
$parents          parent version ids for this row version
$generation       ancestor depth/generation number for DAG algorithms
$tx_id            optional cross-row transaction id; null for direct writes in Slice 1
$state            staging_pending | rejected | visible_direct | visible_transactional
$confirmed_tier   worker | edge | global
$is_deleted       tombstone marker
$metadata         engine/user metadata blob
```

Notes:

- `$row_id` encodes creation time. If an application wants a semantically meaningful "created at" field, it should declare an ordinary user column for that purpose.
- `$branch` is important enough to be explicit data. It identifies which user-meaningful branch view this row version belongs to and is used by `query.branch_view(...)`.
- `$version_id` should do double duty as identity plus timestamp source. We should derive `updated_at` from it rather than carrying a separate always-hot timestamp column.
- Parent pointers reference only version ids. They never need to repeat row ids, because same-row ancestry is enforced by the enclosing history region.
- We intentionally keep one variable-sized `$parents` column instead of splitting `parent_1` vs merge-overflow. The common one-parent case should still encode compactly, and the simpler shape matters more right now.
- `$tx_id` is reserved in Slice 1 so we feel its real storage cost early. Direct writes leave it null. Slice 2 gives it real meaning for multi-row transactions.
- The same concern applies to sparse visible-tier pointers. Slice 1 should explicitly explore better optional-column encoding rather than assuming nullable fixed-width fields are compact enough.
- Transaction-wide settlement is deliberately denormalized onto row versions and visible rows. That makes write-time fate/tier propagation more expensive, but keeps ordinary reads and ordinary queries one-region only.
- History is append-only for user columns: each history row is one concrete row version and should not be rewritten to change user data.
- System metadata attached to that row version may still be updated in place. In particular, fields such as `$state`, `$confirmed_tier`, and selected `$metadata` entries are "about" the row version rather than part of the user payload.

### Visible-entry shape

The visible region should store one compact visible entry per `($branch, $row_id)`, not one duplicated full row per durability tier.

Suggested visible-entry fields:

```text
current_version_id   current visible winner for this branch/row
worker_version_id    optional; defaults to current_version_id
edge_version_id      optional; defaults to worker winner
global_version_id    optional; defaults to edge winner
current_data         encoded row payload for current_version_id
```

The intent is:

- ordinary current reads use `current_data` directly
- a worker-tier query resolves `worker_version_id` through the fallback chain
- an edge-tier query resolves `edge_version_id` through the fallback chain
- a global-tier query resolves `global_version_id` through the fallback chain
- if the resolved version id matches `current_version_id`, the visible entry alone is enough
- otherwise the engine performs one history lookup for that older settled version

This keeps globally settled rows especially cheap:

- if `current_version_id` is already globally confirmed, all three tier pointers can be omitted
- if only `global` lags, only `global_version_id` needs to be stored
- if `edge` and `global` both lag to the same older winner, only `edge_version_id` needs to be stored and `global` can default to it

### Breadboards

#### Direct visible write

```text
alice updates todo/1
  -> append new row version to todos.history
     with $state = visible_direct, a new $version_id, and parent version ids
  -> upsert the winning row version for that branch into todos.visible
  -> update visible-region indices
```

#### Transactional write

```text
alice starts transaction T7 touching todo/1 and project/9
  -> append staging row versions to todos.history and projects.history
     with $state = staging_pending and $tx_id = T7
     while each row version still keeps its own per-row parents
  -> no change to todos.visible or projects.visible yet
```

#### Transaction accepted

```text
authority accepts T7
  -> patch $state and $confirmed_tier on the affected history rows in place
       from staging_pending -> visible_transactional
  -> upsert those same row versions into todos.visible and projects.visible
  -> update visible-region indices
```

#### Transaction rejected

```text
authority rejects T7
  -> patch $state = rejected on the affected history rows
  -> visible regions unchanged
  -> staged history remains available for local rollback/debug/restart
```

### Fat marker sketch

```text
Application query
  -> QueryManager compiles against visible regions by default
  -> visible-region indices serve hot scans and lookups
  -> visible rows already include system columns needed for ordinary policy/filtering

History / debug / time-travel query
  -> QueryManager reads history regions with the same row shape
  -> can inspect staged rows, accepted rows, rejected rows, and older visible states
```

```text
+------------------------------------------------------------+
| todos keyed table space                                    |
|                                                            |
|  visible region: current visible rows, hot scans/indices   |
|  history region: all row versions, append/compression      |
|  same row shape in both regions                            |
+------------------------------------------------------------+
```

### Query semantics

Ordinary user queries should not solve version-selection themselves. They compile against the visible region.

That is the central optimization rule of this design:

- **do not** make every user query compute "latest visible version per row"
- **do** maintain a visible region that already answers that question

This means:

- table scans operate over current visible rows only
- ordinary secondary indices index the visible region, not the history region
- joins, filters, sorting, and subscriptions all work over the visible region by default

History-aware and time-travel paths may query the history region, but those are not the default UI path.

### Visibility and settlement semantics

The visible region is defined by two rules:

1. only row versions with `$state = visible_direct` or `$state = visible_transactional` may appear in the visible region
2. row versions with `$state = staging_pending` or `$state = rejected` stay history-only

Visible states are:

- `visible_direct`
- `visible_transactional`

History-only states are:

- `staging_pending`
- `rejected`

`confirmed_tier` is transaction-wide when `$tx_id` is present. If one logical transaction touches many rows, its effective tier is the minimum confirmed tier across all visible members in that transaction.

This is intentionally transaction-wide. If an app wants two updates to become independently visible or independently durable, it should emit two separate transactions or direct writes.

The visible region is authoritative for "what is the current visible state now?" Reads should not have to consult history to answer that question.

That statement still leaves room for stricter durability-tier queries:

- the visible region is authoritative for the current head version and tier winner pointers
- stricter tiered reads may resolve to an older settled version via those pointers
- history is only consulted when the required tier winner is older than the current visible head

### Storage and locality strategy

The two-region layout is how we answer the read-locality concern.

The visible region is optimized for:

- contiguous scans of current visible rows
- point lookups and secondary-index probes
- cache locality for the current branch-scoped table image

Suggested clustering:

- primary locality by `($branch, $row_id)`

The history region is optimized for:

- append-heavy writes
- grouped versions of the same logical row
- high redundancy compression across repeated user and system columns

Suggested clustering:

- primary locality by `($row_id, $version_id)`
- secondary access path by `($branch, $row_id, $version_id)` when branch-oriented history scans or as-of reconstruction need it
- optional secondary access path by `($tx_id, $row_id, $version_id)` once Slice 2 needs efficient transaction publish/replay

Because both regions share the same row codec and the same row shape, any columnar, dictionary, delta, page, or wire compression work benefits both:

- user columns
- system columns
- history redundancy
- current visible redundancy

### Compression strategy

This design intentionally leans into redundancy instead of fighting it structurally.

We should assume:

- history rows will duplicate many user columns
- visible rows duplicate the current version already present in history

The bet is that one unified compression strategy beats two unrelated data structure managers.

Compression opportunities:

- repeated column names and type layouts
- repeated unchanged user values across row versions
- repeated branches / authors
- repeated null transaction ids in Slice 1 and repeated shared transaction ids in Slice 2
- repeated small parent vectors in the common one-parent case
- sparse omitted worker/edge/global winner pointers for already-settled visible rows
- compressed pages in memory for cold rows
- stream compression over sync payloads that repeat the same system columns

### Sync implications

Sync stops shipping object/commit wrappers for user rows.

Instead, it ships row-version rows and row-metadata changes:

- history-row appends for the history region
- visible-row upserts/removals for the visible region
- state/tier fan-out changes for affected rows when transaction fate advances

The row-version payload itself still carries the precise DAG data:

- `version_id`
- parent version ids
- branch label
- optional `tx_id`

Downstream correctness comes from the same relational state the local engine uses. Reconnect and restart no longer need to reconstruct write fate from commit IDs and object frontiers; they can recover it from persisted history/visible rows directly.

Ordinary downstream reads should still be one visible-entry lookup. Tier-specific queries may require one extra history lookup only when the relevant settled winner is older than the current head.

### Query-manager implications

This spec intentionally breaks the current `Row = Object` assumption from [query_manager.md](/Users/anselm/.codex/worktrees/3ceb/jazz2/specs/status-quo/query_manager.md).

Major consequences:

- MaterializeNode no longer lazy-loads row objects from ObjectManager for ordinary user tables.
- Table indices point to visible row ids / physical row locations, not ObjectIds.
- Branch awareness becomes `$branch` columns and indices, not object-branch lookups.
- Query/sync scope becomes row/table oriented rather than object oriented for user data.

This is a rewrite of the substrate, not a compatibility layer over the existing object manager.

### Storage-trait implications

This spec also intentionally breaks the current object-centric `Storage` trait from [storage.md](/Users/anselm/.codex/worktrees/3ceb/jazz2/specs/status-quo/storage.md).

The new lowest layer needs table-oriented operations such as:

```rust
trait Storage {
    fn append_history_region_rows(&mut self, table: TableId, rows: &[EncodedRow]) -> Result<()>;
    fn upsert_visible_region_rows(&mut self, table: TableId, rows: &[EncodedRow]) -> Result<()>;
    fn patch_rows_by_tx(&mut self, table: TableId, tx_id: TxId, patch: RowPatch) -> Result<()>;
    fn scan_visible_region_index(&self, table: TableId, index: IndexId, cond: ScanCond) -> Result<RowCursor>;
    fn scan_history_region(&self, table: TableId, cond: HistoryScanCond) -> Result<RowCursor>;
}
```

The exact trait surface can vary, but the shape must be table-first rather than object-first.

### Write atomicity

One write transaction inside one node must be able to update:

- history rows
- visible rows
- visible-region indices
- row metadata for affected rows when fate/tier advances

as one atomic storage operation.

If that is not possible, this design will produce visible/history drift and will not be viable.

### History and time-travel path

This design should support user-facing history and branch views from the beginning.

The intended shape is:

- ordinary queries compile against the visible region
- `query.history()` compiles against the history region with the same user-column shape plus reserved system columns
- `query.as_of(ts)` is implemented as a specialized history execution mode that finds the latest visible row version at or before `ts`
- `query.branch_view(branch)` is implemented as an explicit mode that reads the visible state for a specific branch rather than only the default current branch view
- `query.history().branch_view(branch)` and `query.as_of(ts).branch_view(branch)` are valid combinations

The reason this stays manageable is structural similarity:

- the same table schema
- the same user columns
- the same reserved system columns
- different key regions and default query target

That gives us one critical invariant:

- the visible region is authoritative for current state
- the history region is authoritative for chronological row history
- they should be similar enough that time-travel and history features do not require a second query language

## Rabbit Holes

- Making the visible region authoritative means we are choosing not to rely on "just rebuild it from history" as the normal escape hatch. If visible/history drift occurs, that is a serious correctness bug.
- Row-version duplication may still be too expensive if our compression strategy is weaker in practice than expected.
- Rewriting the substrate means query compilation, sync scope tracking, schema activation, and policy evaluation all need to lose their `Row = Object` assumptions cleanly rather than through shims.
- Denormalizing transaction-wide settlement and tier onto many rows may make fate reception and tier advancement too expensive for wide transactions.
- User-facing `as_of(...)` queries sound structurally simple here, but they still need a fast "latest visible row version before timestamp" execution path or they will be too slow.
- Deletes, undeletes, and historical tombstone semantics need to stay simple; otherwise we will just rebuild commit semantics under new names.
- Existing branch semantics include env/schemaHash/userBranch concerns. We need a clean `$branch` model that preserves schema/lens behavior without dragging the old branch-name machinery through every hot path.
- Multi-tier sync currently relies on commit IDs, object metadata, and topological ordering. The replacement protocol needs equally crisp invariants for idempotence, replay, and row-metadata fan-out.
- The browser worker/main-thread split may react differently to this model: visible scans should get cheaper, but write amplification into visible + history + indices may get worse.

## No-gos

- No attempt to preserve the current object-manager hot path for user rows.
- No "hybrid forever" architecture where ordinary user tables can use either object DAGs or relational history indefinitely.
- No separate transaction-settlement table on the ordinary read path.
- No attempt to preserve a table-global commit graph under new names. The only preserved commit-like structure should be the row-local version DAG itself.
- No assumption that duplicated rows are acceptable without proving it in benchmarks.
- No implicit user write access to reserved system columns.

## Testing Strategy

Use integration-first tests and benchmark gates. This is not a unit-test-sized change.

- Add SchemaManager / RuntimeCore integration tests where `alice` writes direct visible row versions and `bob` subscribes, verifying ordinary queries read only visible-region semantics.
- Add transaction acceptance/rejection flows where `alice` stages rows, the authority accepts or rejects the transaction, and `bob` only sees accepted visible-region rows.
- Add fate/tier fan-out tests where one transaction touches many rows and settlement advancement updates both the history and visible regions correctly.
- Add restart tests where a runtime reconstructs current reads directly from the persisted visible region, and history/time-travel queries directly from the persisted history region.
- Add deletion tests with realistic actors showing that tombstones do not leak back into ordinary visible queries but remain reconstructible from history.
- Add history-query tests from the beginning:
  - `query.history()` returns chronologically ordered row versions with system columns
  - `query.as_of(ts)` returns the expected past visible state
  - `query.branch_view(branch)` returns the expected visible state for a non-default branch
  - `query.as_of(ts).branch_view(branch)` returns the expected past visible state for that branch
- Add benchmark suites comparing `main`, `#415`, and this model for:
  - point read of current row
  - table scan of current visible rows
  - direct-write latency
  - transactional publish latency
  - transaction fate/tier fan-out cost
  - history append throughput
  - `as_of(ts)` query latency
  - on-disk size
  - sync payload size
- Treat the idea as viable only if visible reads stay at least competitive with `main`, history/time-travel queries are usable without heroic special cases, and write fan-out costs are plausibly recoverable through compression plus simpler engine boundaries.

## Confidence

6/10

The conceptual simplification is strong, and the two-region keyed layout still feels like the right answer. The remaining uncertainty is practical: whether denormalized fate/tier fan-out and authoritative visible-state maintenance are cheap enough that the rewrite buys real performance instead of just cleaner boundaries.
