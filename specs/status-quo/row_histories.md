# Row Histories — Status Quo

This is the simplest way to think about Jazz today:

- every application table is still a table
- every logical row has a stable row id
- edits create row batch entries
- current reads come from a compact visible entry
- history stays around so sync, reconnect, and replay can speak in row-batch terms, while
  durability and rejection fate are recorded at the batch level

For the full direct/transactional batch lifecycle, replayable settlement model, and app-facing
batch APIs, read this together with [Batches](batches.md).

If you are new to the internals, it helps to picture one user table as two families of
engine-managed raw table instances:

```text
todos
  visible raw tables:
    one raw table per (visible, todos, full_schema_hash)
    local key: (branch, row_id)

  history raw tables:
    one raw table per (history, todos, full_schema_hash)
    local key: (row_id, branch, batch_id)
```

The visible region is the hot path for ordinary queries. The history region is the source of truth for replay, ancestry, and tier-aware fallbacks.

## The Three Important Pieces

### 1. Logical row

The logical row is the stable identity your application thinks of as "the todo". It is identified by a row id and mapped back to its table through storage.

### 2. Stored row batch entry

A `StoredRowBatch` is one concrete stored entry for that logical row. It carries:

- row identity
- branch
- batch id
- parent batch ids
- state
- confirmed durability tier
- delete markers
- engine/user metadata
- the application row values

The stable identity is `(row_id, branch_name, batch_id)`. `batch_id` is the public row identity
for both direct visible rows and accepted transactional rows.

Physically, a stored row batch entry is one flat `row_format` record:

- reserved `_jazz_*` columns first
- user columns after that

The current Rust type still exposes the user-column slice through `data`, but that is just a
decoded view over the flat stored bytes.

### 3. Visible row entry

A `VisibleRowEntry` is the compact current answer for one `(branch, row_id)` pair. It stores:

- the current visible row values for that branch view
- the current branch frontier
- optional preview metadata batch ids for `local`, `edge`, and `global`
- an optional winner batch-id pool plus packed ordinal vectors when a preview is synthetic

Physically, the visible region is also stored as flat `row_format` rows with the same user columns
and a slightly larger `_jazz_*` prefix. That lets ordinary reads stay fast while still allowing
lower-tier queries to resolve older settled winners when needed.

The common visible-row case is still intentionally cheap:

- if the frontier is linear, the visible entry stores one ordinary winner row
- if every durability tier sees the same preview, tier state collapses to that one shared shape

Only true concurrent frontiers pay the extra merge cost. In that case the reducer stores one merged
visible body for the default head, plus:

- a compact pool of contributing winner batch ids
- one packed ordinal vector for the default merged preview when it is synthetic
- extra packed ordinal vectors only for tiers whose preview differs from the default head
- a reserved opaque merge-artifacts blob for future merge diagnostics and related sidecar metadata

The visible-row format now also keeps the common case compact by treating some fields as implicit:

- empty parents decode from `null` as `[]`
- empty metadata decode from `null` as `{}`
- a frontier that is only `[current_batch_id]` decodes from `null`

## Reserved Engine Fields

Conceptually, every user table has:

- the application columns you defined in `schema.ts`
- a reserved set of engine fields that explain how the row should behave

The important engine fields are:

- `(row_id, branch_name, batch_id)` — the stable identity of one stored row batch entry
- `_jazz_parents` — parent batch ids for row-local ancestry
- `_jazz_state` — whether the version is visible, staging, or rejected
- `_jazz_confirmed_tier` — legacy/derived per-row tier metadata; authoritative durability is now
  read from `BatchFate` where available
- `_jazz_is_deleted` — tombstone marker
- `_jazz_metadata` — engine/user metadata blob
- actor/provenance columns such as `_jazz_created_by` and `_jazz_updated_by`

History rows keep that full engine shape. Visible rows intentionally do not: they keep the current
batch id, durability/state/provenance columns, user data, and the frontier/tier winner pointers,
while parents/metadata remain history-owned.

When the visible entry is a synthetic merge preview rather than one historical row copy, row-level
metadata stays coarse on purpose:

- `created_*` comes from the original creator
- row-level `updated_*` and `batch_id` come from the latest contributing winner
- if the row is deleted, delete metadata comes from the winning delete batch

Exact per-column provenance is carried in that visible-entry sidecar rather than expanded into the
public row shape.

The visible entry also reserves one engine-owned `_jazz_merge_artifacts` slot:

- it lives only on visible rows, never on history rows
- `null` means "no merge artifacts"
- non-`null` bytes are a versioned opaque envelope owned by the engine
- the current release keeps this slot empty while stabilizing the storage format for future
  conflict diagnostics

For history rows, the identity now lives in the raw-table-local storage key rather than the payload
columns. For visible rows, `(branch_name, row_id)` comes from the raw-table-local key and the
current visible `batch_id` stays in the flat visible payload. Raw table headers carry the general
storage format version, full schema hash, and table name, so flat row decoding no longer needs to
discover descriptors by scanning all historical catalogue schemas.

Read paths resolve the exact raw table context first, then decode rows against that already-known
format. The header is part of resolving the table, not something that gets reread for every row.

## How a Direct Write Lands

For a normal row write, the engine treats that write as a one-member direct batch and does four things:

1. Upsert the batch entry into the history region.
2. Recompute the visible answer for that `(branch, row_id)`.
3. Upsert the `VisibleRowEntry` for the branch view.
4. Update the relevant indices and queue sync notifications.

For linear histories, step 2 is still just the cheap whole-row fast path.

For concurrent frontiers, step 2 now does an explicit MRCA-relative merge:

- pick the latest common ancestor of the current frontier
- compare each frontier tip against that ancestor
- for each user column, apply that column's schema-declared merge strategy to the set of
  frontier tips whose value differs from the ancestor value
- if no frontier tip changed that column, attribute the winner to the ancestor itself
- deletes win over updates; two soft deletes keep a soft-deleted merged body

The current column merge strategies are:

- implicit `lww` for all columns
- explicit `counter` for non-nullable integer columns

`lww` keeps the existing MRCA-relative "latest changed tip wins" behavior.

`counter` treats each conflicting snapshot as a delta from the MRCA value:

- compute `tip_value - ancestor_value` for each changed frontier tip
- sum those deltas
- apply the sum to the ancestor value
- raise an error if checked integer arithmetic overflows

The visible-entry sidecar intentionally stays coarse even for non-`lww` strategies:

- each visible column still records only the latest timestamp-ordered batch that contributed to
  that column's resolved value
- readers that want deeper provenance must walk row history directly

That work produces an `ApplyRowBatchResult`, including any `RowVisibilityChange` that downstream
systems care about.

Authority settlement remains batch-scoped even for direct writes. A direct row may be visible
optimistically before its batch settles, but the authoritative accepted/rejected answer is
`BatchFate::DurableDirect` or `BatchFate::Rejected`.
Successful fate is whole-batch truth; row-history readers should not need to scan legacy
`visible_members` to decide whether a known row in that batch has reached the fate's tier. If a
rejected direct update had replaced an older visible row, receivers mark the rejected history entry
non-visible and rebuild the visible entry from the remaining history instead of deleting the object
outright.

## How a Transactional Write Lands

Transactional writes reuse the same `StoredRowBatch` shape, but they stage first:

1. Write a `StoredRowBatch` with `RowState::StagingPending`.
2. Keep it out of ordinary visible reads.
3. Seal the batch explicitly when the writer is done.
4. If the authority accepts it, promote that same batch entry to `VisibleTransactional`.

The row shape stays the same across both paths. The distinction is lifecycle and settlement, not
row identity.

Two exclusion rules matter for merge behavior:

- accepted transactional rows can participate in visible-row merges just like direct rows
- conflicted transactional batches never participate in merging anywhere; rejected or still-staged
  rows do not affect merge previews, merge-on-write bases, or tier-specific visible resolution
- rejection is batch-wide: if any member of a transactional batch is rejected, no member in that
  batch becomes visible

Merge strategy selection is schema-relative rather than history-relative:

- row history stores the original snapshots only
- the consumer's current schema decides which merge strategy applies to each visible column
- after a schema change, old-schema consumers may resolve the same conflicting history differently
  from new-schema consumers, and that is expected

## Why Visible Entries Exist

The visible entry is the reason ordinary reads can stay simple.

When a query asks for current todos:

- index scans find candidate row ids
- materialization loads visible entries for those ids
- the runtime usually answers lower-tier previews directly from the visible-entry sidecar
- it only falls back to full history lookup when an older entry does not carry enough preview
  provenance to answer directly

This is why the current engine feels table-first even though it retains full row history underneath.

## Deletion Semantics

Deletes are row batch entries too.

- a delete creates a version marked deleted
- the visible entry may disappear from current live scans, or resolve as deleted when explicitly requested
- the history region still remembers that deletion happened

That gives the runtime a stable replay story without keeping deleted rows in a separate conceptual bucket.

## Separate Catalogue Lane

Schemas and lenses do not live inside user row histories. They travel through the separate `catalogue` lane.

That split keeps the mental model tidy:

- user tables use row histories + visible entries
- system metadata uses catalogue entries
- both reuse the same shared `row_format` encoding machinery

## Key Files

| File                                                             | Purpose                                       |
| ---------------------------------------------------------------- | --------------------------------------------- |
| `crates/jazz-tools/src/row_histories/mod.rs`                     | Row-history types and reducer logic           |
| `crates/jazz-tools/src/storage/mod.rs`                           | Storage-backed persistence and lookup helpers |
| `specs/status-quo/batches.md`                                    | Direct/transactional batch lifecycle summary  |
| `crates/jazz-tools/src/row_format.rs`                            | Shared binary row/value encoding              |
| `crates/jazz-tools/src/query_manager/graph_nodes/materialize.rs` | Visible-entry driven materialization          |
| `crates/jazz-tools/src/sync_manager/types.rs`                    | Row-batch oriented sync payloads              |
