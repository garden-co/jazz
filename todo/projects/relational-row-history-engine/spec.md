# Relational Row History and Visible Projection

## Problem

Jazz currently has a deep architectural seam:

- the storage/object layer thinks in objects, branches, commits, tips, and merge structure
- the query layer thinks in rows, columns, indices, and live relational results

That seam is expensive. On `main`, we pay for a git-like object model and then reinterpret it as relational state. In the ongoing batch-branch work, we are trying to make that lower layer more explicit and more scalable, but we are still carrying a large amount of non-relational structure below a relational query engine.

The concern behind this spec is not just "can we make `#415` faster?" It is "are we optimizing the wrong boundary?" If the system's real semantic unit is a versioned row in a branch-scoped table, then object DAGs, branch-tip sets, and commit-specific hot paths may be the wrong primitive to optimize around. We may be paying complexity twice: once in the storage/object layer, and again in the query layer that has to turn that back into rows.

This spec explores a radical replacement: make the base substrate of Jazz a lower-level database of tables that contain both user columns and system columns. Branching, staging, durability, and visibility become data expressed directly in row versions, and the hot read path becomes an explicitly maintained "latest visible" projection rather than a reconstruction from object history.

## Solution

### Chosen approach

This spec recommends replacing the "row = object with branch DAG history" model with a relational version-storage model built from two first-class storage relations for user data:

1. per-user-table **history tables**
2. per-user-table **visible tables**

The engine still has version history, staging, transactions, and durability tiers. The difference is where those concepts live:

- not inside object-manager commit graphs
- not in a separate bespoke metadata structure
- but in rows and reserved system columns inside one lower-level table engine

### Alternatives considered

1. Keep the current object/commit substrate and keep optimizing it.

   This is the lowest-risk option, but it preserves the seam between object history and relational queries. It may improve constants without removing the core mismatch.

2. Land batch branches (`#415`) and add more query-visible metadata beside them.

   This is better than `main`, but it still keeps two hot-path mental models: object/batch history below, relational rows above.

3. Replace the substrate with relational row history plus a visible projection.

   This is the most radical option, but it is the only one that collapses the storage, visibility, and query models into one consistent data shape. This spec chooses that option.

### Design principles

- The hot path for ordinary reads must be **visible-table first**.
- `__visible` must be authoritative for current reads.
- History must be **append-friendly** and compress well.
- `__history` and `__visible` should stay structurally parallel so history/time-travel features can be real product features from the beginning.
- Visibility and settlement rules should be expressible in table terms, even if the engine materializes optimized projections for speed.
- We should have one primary data management system to optimize on disk, in memory, and on the wire.
- Prefer write-time fan-out over read-time joins on the hot path.

### Core model

The system distinguishes four concepts:

- **logical row**: the stable application row identity
- **row version**: one concrete version of a logical row
- **logical batch**: one write unit identified by one `BatchId`
- **visible batch member**: one row version published into the visible state as part of a logical batch

The old object boundary disappears for user rows. A row version is not wrapped in a commit object. It is already a row in a lower-level storage table.

### Physical layout

For each user table `todos`, the engine stores:

- `todos__history`
- `todos__visible`

The two physical tables use the same logical column layout:

- user-defined columns
- reserved system columns

The difference is purpose and clustering:

- `todos__visible` is the hot read projection: one current visible row version per logical row per branch scope
- `todos__history` is the append-heavy history log: all staged and visible row versions over time

The engine may still have shared metadata tables for schema, catalog, and index management, but batch settlement for user rows is not modeled as a required third hot-path relation.

### Reserved system columns

Every user table's history and visible projections reserve a set of engine columns. The exact names can change, but the model needs at least:

```text
$row_id           stable logical row id
$branch_scope     encoded visible/staging branch scope
$version_at       actual timestamp for this row version
$created_at       logical row creation timestamp
$created_by       actor that created the logical row
$updated_by       actor that produced this row version
$batch_id         logical batch id
$batch_mode       direct | transactional
$settlement       pending | rejected | durable_direct | accepted_transaction
$confirmed_tier   worker | edge | global
$visibility       visible | staging
$is_deleted       tombstone marker
$metadata         engine/user metadata blob
```

Notes:

- `BatchId` remains the single logical write id.
- We do not overload `$created_at` to carry UUIDv7 identity. Instead, row identity is explicit in `$row_id`, which may itself be UUIDv7-backed.
- Batch-wide settlement is deliberately denormalized onto row versions and visible rows. That makes write-time fate/tier propagation more expensive, but keeps ordinary reads and ordinary queries one-table only.
- History is append-only for user columns: each history row is one concrete row version and should not be rewritten to change user data.
- System metadata attached to that row version may still be updated in place. In particular, fields such as `$settlement`, `$confirmed_tier`, and selected `$metadata` entries are "about" the row version rather than part of the user payload.

### Breadboards

#### Direct visible write

```text
alice updates todo/1
  -> append new row version to todos__history
     with $visibility = visible, $batch_id = B1,
          $settlement = durable_direct
  -> upsert same row version into todos__visible
  -> update visible-table indices
```

#### Transactional write

```text
alice starts batch B7 touching todo/1 and project/9
  -> append staging row versions to todos__history and projects__history
     with $visibility = staging, $batch_id = B7,
          $settlement = pending
  -> no change to todos__visible or projects__visible yet
```

#### Transaction accepted

```text
authority accepts B7
  -> fan out fate/tier updates to the affected history rows
  -> append corresponding accepted visible row versions to history
       with $visibility = visible,
            $settlement = accepted_transaction
  -> upsert those same row versions into todos__visible and projects__visible
  -> update visible-table indices
```

#### Transaction rejected

```text
authority rejects B7
  -> fan out $settlement = rejected to the affected history rows
  -> visible tables unchanged
  -> staged history remains available for local rollback/debug/restart
```

### Fat marker sketch

```text
Application query
  -> QueryManager compiles against visible tables by default
  -> visible-table indices serve hot scans and lookups
  -> visible rows already include system columns needed for ordinary policy/filtering

History / debug / time-travel query
  -> QueryManager reads history tables with the same row shape
  -> can inspect staged rows, accepted rows, rejected rows, and older visible states
```

```text
+--------------------+                   +--------------------+
| todos__history     |                   | todos__visible     |
| all row versions   |                   | current visible    |
| same row shape     |                   | same row shape     |
| append/compression |                   | hot reads/indices  |
+--------------------+                   +--------------------+
```

### Query semantics

Ordinary user queries should not solve version-selection themselves. They compile against `table__visible`.

That is the central optimization rule of this design:

- **do not** make every user query compute "latest visible version per row"
- **do** maintain a visible projection that already answers that question

This means:

- table scans operate over current visible rows only
- ordinary secondary indices index `__visible`, not `__history`
- joins, filters, sorting, and subscriptions all work over the visible projection by default

History-aware and time-travel paths may query `__history`, but those are not the default UI path.

### Visibility and settlement semantics

The visible projection is defined by two rules:

1. only row versions with `$visibility = visible` may appear in `table__visible`
2. only row versions whose own `$settlement` is `durable_direct` or `accepted_transaction` may appear in `table__visible`

Visible settlements are:

- `DurableDirect`
- `AcceptedTransaction`

Non-visible settlements are:

- `pending`
- `rejected`

`confirmed_tier` is batch-wide. If a logical batch touches many rows, its effective tier is the minimum confirmed tier across all visible batch members in that batch.

This is intentionally batch-wide. If an app wants two updates to become independently visible or independently durable, it should emit two batches.

`table__visible` is authoritative for "what is the current visible state now?" Reads should not have to consult history to answer that question.

### Storage and locality strategy

The two-table design is how we answer the read-locality concern.

`table__visible` is optimized for:

- contiguous scans of current visible rows
- point lookups and secondary-index probes
- cache locality for the current branch-scoped table image

Suggested clustering:

- primary locality by `(branch_scope, row_id)`

`table__history` is optimized for:

- append-heavy writes
- grouped versions of the same logical row
- high redundancy compression across repeated user and system columns

Suggested clustering:

- primary locality by `(branch_scope, row_id, version_at)`
- secondary access path by `(batch_id, row_id)` when publish/replay needs it

Because both tables share the same row codec and almost the same row shape, any columnar, dictionary, delta, page, or wire compression work benefits both:

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
- repeated batch ids / branch scopes / authors
- compressed pages in memory for cold rows
- stream compression over sync payloads that repeat the same system columns

### Sync implications

Sync stops shipping commit DAG structure for user rows.

Instead, it ships row-version rows and row-metadata changes:

- history rows for `__history`
- visible-row upserts/removals for `__visible`
- settlement/tier fan-out changes for affected rows when batch fate advances

Downstream correctness comes from the same relational state the local engine uses. Reconnect and restart no longer need to reconstruct write fate from commit IDs and object frontiers; they can recover it from persisted history/visible rows directly.

### Query-manager implications

This spec intentionally breaks the current `Row = Object` assumption from [query_manager.md](/Users/anselm/.codex/worktrees/3ceb/jazz2/specs/status-quo/query_manager.md).

Major consequences:

- MaterializeNode no longer lazy-loads row objects from ObjectManager for ordinary user tables.
- Table indices point to visible row ids / physical row locations, not ObjectIds.
- Branch awareness becomes branch-scope columns and indices, not object-branch lookups.
- Query/sync scope becomes row/table oriented rather than object oriented for user data.

This is a rewrite of the substrate, not a compatibility layer over the existing object manager.

### Storage-trait implications

This spec also intentionally breaks the current object-centric `Storage` trait from [storage.md](/Users/anselm/.codex/worktrees/3ceb/jazz2/specs/status-quo/storage.md).

The new lowest layer needs table-oriented operations such as:

```rust
trait Storage {
    fn append_history_rows(&mut self, table: TableId, rows: &[EncodedRow]) -> Result<()>;
    fn upsert_visible_rows(&mut self, table: TableId, rows: &[EncodedRow]) -> Result<()>;
    fn patch_rows_by_batch(&mut self, table: TableId, batch_id: BatchId, patch: RowPatch) -> Result<()>;
    fn scan_visible_index(&self, table: TableId, index: IndexId, cond: ScanCond) -> Result<RowCursor>;
    fn scan_history(&self, table: TableId, cond: HistoryScanCond) -> Result<RowCursor>;
}
```

The exact trait surface can vary, but the shape must be table-first rather than object-first.

### Write atomicity

One write transaction inside one node must be able to update:

- history rows
- visible rows
- visible-table indices
- row metadata for affected rows when fate/tier advances

as one atomic storage operation.

If that is not possible, this design will produce visible/history drift and will not be viable.

### History and time-travel path

This design should support user-facing history and branch views from the beginning.

The intended shape is:

- ordinary queries compile against `table__visible`
- `query.history()` compiles against `table__history` with the same user-column shape plus reserved system columns
- `query.as_of(ts)` is implemented as a specialized history execution mode that finds the latest visible row version at or before `ts`
- `query.branch_view(branch_scope)` is implemented as an explicit mode that reads the visible state for a specific branch scope rather than only the default current branch view
- `query.history().branch_view(branch_scope)` and `query.as_of(ts).branch_view(branch_scope)` are valid combinations

The reason this stays manageable is structural similarity:

- the same table schema
- the same user columns
- the same reserved system columns
- different clustering and default query target

That gives us one critical invariant:

- `table__visible` is authoritative for current state
- `table__history` is authoritative for chronological row history
- they should be similar enough that time-travel and history features do not require a second query language

## Rabbit Holes

- Making `__visible` authoritative means we are choosing not to rely on "just rebuild it from history" as the normal escape hatch. If visible/history drift occurs, that is a serious correctness bug.
- Row-version duplication may still be too expensive if our compression strategy is weaker in practice than expected.
- Rewriting the substrate means query compilation, sync scope tracking, schema activation, and policy evaluation all need to lose their `Row = Object` assumptions cleanly rather than through shims.
- Denormalizing batch-wide settlement and tier onto many rows may make fate reception and tier advancement too expensive for wide batches.
- User-facing `as_of(...)` queries sound structurally simple here, but they still need a fast "latest visible row version before timestamp" execution path or they will be too slow.
- Deletes, undeletes, and historical tombstone semantics need to stay simple; otherwise we will just rebuild commit semantics under new names.
- Existing branch semantics include env/schemaHash/userBranch concerns. We need a clean `branch_scope` model that preserves schema/lens behavior without dragging the old branch-name machinery through every hot path.
- Multi-tier sync currently relies on commit IDs, object metadata, and topological ordering. The replacement protocol needs equally crisp invariants for idempotence, replay, and row-metadata fan-out.
- The browser worker/main-thread split may react differently to this model: visible scans should get cheaper, but write amplification into visible + history + indices may get worse.

## No-gos

- No attempt to preserve the current object-manager hot path for user rows.
- No "hybrid forever" architecture where ordinary user tables can use either object DAGs or relational history indefinitely.
- No separate batch-settlement table on the ordinary read path.
- No attempt to preserve arbitrary commit-graph merge semantics if that reintroduces the very structure this spec is trying to remove.
- No assumption that duplicated rows are acceptable without proving it in benchmarks.
- No implicit user write access to reserved system columns.

## Testing Strategy

Use integration-first tests and benchmark gates. This is not a unit-test-sized change.

- Add SchemaManager / RuntimeCore integration tests where `alice` writes direct visible batches and `bob` subscribes, verifying ordinary queries read only `__visible` semantics.
- Add transaction acceptance/rejection flows where `alice` stages rows, the authority accepts or rejects the batch, and `bob` only sees accepted visible batch members.
- Add fate/tier fan-out tests where one batch touches many rows and settlement advancement updates both `__history` and `__visible` metadata correctly.
- Add restart tests where a runtime reconstructs current reads directly from persisted `__visible`, and history/time-travel queries directly from persisted `__history`.
- Add deletion tests with realistic actors showing that tombstones do not leak back into ordinary visible queries but remain reconstructible from history.
- Add history-query tests from the beginning:
  - `query.history()` returns chronologically ordered row versions with system columns
  - `query.as_of(ts)` returns the expected past visible state
  - `query.branch_view(branch_scope)` returns the expected visible state for a non-default branch scope
  - `query.as_of(ts).branch_view(branch_scope)` returns the expected past visible state for that branch scope
- Add benchmark suites comparing `main`, `#415`, and this model for:
  - point read of current row
  - table scan of current visible rows
  - direct-write latency
  - transactional publish latency
  - batch fate/tier fan-out cost
  - history append throughput
  - `as_of(ts)` query latency
  - on-disk size
  - sync payload size
- Treat the idea as viable only if visible reads stay at least competitive with `main`, history/time-travel queries are usable without heroic special cases, and write fan-out costs are plausibly recoverable through compression plus simpler engine boundaries.

## Confidence

6/10

The conceptual simplification is strong, and the two-table split still feels like the right answer. The remaining uncertainty is practical: whether denormalized fate/tier fan-out and authoritative visible-state maintenance are cheap enough that the rewrite buys real performance instead of just cleaner boundaries.
