# Row Histories — Status Quo

This is the simplest way to think about Jazz today:

- every application table is still a table
- every logical row has a stable row id
- edits create row batch entries
- current reads come from a compact visible entry
- history stays around so sync, reconnect, and replay can speak in row-batch terms

For the full direct/transactional batch lifecycle, replayable settlement model, and app-facing
batch APIs, read this together with [Batches](batches.md).

If you are new to the internals, it helps to picture one user table as two engine-managed regions:

```text
todos
  visible: (branch, row_id) -> current visible winner
  history: (row_id, branch, batch_id) -> every stored row batch entry
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

- the current winning batch id
- the current visible row values for that branch view
- optional tier-specific winner ids for `worker`, `edge`, and `global`

Physically, the visible region is also stored as flat `row_format` rows with the same user columns
and a slightly larger `_jazz_*` prefix. That lets ordinary reads stay fast while still allowing
lower-tier queries to resolve older settled winners when needed.

## Reserved Engine Fields

Conceptually, every user table has:

- the application columns you defined in `schema.ts`
- a reserved set of engine fields that explain how the row should behave

The important engine fields are:

- `(row_id, branch_name, batch_id)` — the stable identity of one stored row batch entry
- `_jazz_parents` — parent batch ids for row-local ancestry
- `_jazz_state` — whether the version is visible, staging, or rejected
- `_jazz_confirmed_tier` — highest durability tier known for that version
- `_jazz_is_deleted` — tombstone marker
- `_jazz_metadata` — engine/user metadata blob
- actor/provenance columns such as `_jazz_created_by` and `_jazz_updated_by`

For history rows, that identity now lives in the storage key rather than the payload columns. For
visible rows, `(branch_name, row_id)` comes from the key and the current visible `batch_id` stays
in the flat visible payload. The important idea is still that visibility, ancestry, durability,
and deletion are expressed directly in the engine-managed row shape without repeating the full
key-derived identity inside the payload.

## How a Direct Write Lands

For a normal row write, the engine treats that write as a one-member direct batch and does four things:

1. Upsert the batch entry into the history region.
2. Recompute the visible winner for that `(branch, row_id)`.
3. Upsert the `VisibleRowEntry` for the branch view.
4. Update the relevant indices and queue sync notifications.

That work produces an `ApplyRowBatchResult`, including any `RowVisibilityChange` that downstream systems care about.

## How a Transactional Write Lands

Transactional writes reuse the same `StoredRowBatch` shape, but they stage first:

1. Write a `StoredRowBatch` with `RowState::StagingPending`.
2. Keep it out of ordinary visible reads.
3. Seal the batch explicitly when the writer is done.
4. If the authority accepts it, promote that same batch entry to `VisibleTransactional`.

The row shape stays the same across both paths. The distinction is lifecycle and settlement, not
row identity.

## Why Visible Entries Exist

The visible entry is the reason ordinary reads can stay simple.

When a query asks for current todos:

- index scans find candidate row ids
- materialization loads visible entries for those ids
- the runtime only falls back to full history lookup when a lower-tier winner differs from the current winner

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
