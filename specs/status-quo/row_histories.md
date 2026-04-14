# Row Histories — Status Quo

This is the simplest way to think about Jazz today:

- every application table is still a table
- every logical row has a stable row id
- edits create row batch members
- current reads come from a compact visible entry
- history stays around so sync, reconnect, and replay can speak in row-batch terms

If you are new to the internals, it helps to picture one user table as two engine-managed regions:

```text
todos
  visible: (branch, row_id) -> current visible winner
  history: (row_id, batch_id) -> every stored row batch member
```

The visible region is the hot path for ordinary queries. The history region is the source of truth for replay, ancestry, and tier-aware fallbacks.

## The Three Important Pieces

### 1. Logical row

The logical row is the stable identity your application thinks of as "the todo". It is identified by a row id and mapped back to its table through storage.

### 2. Stored row batch member

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

Physically, a stored row batch member is one flat `row_format` record:

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

The important reserved columns are:

- `_jazz_row_id` — stable logical row identity
- `_jazz_branch` — the branch view this version belongs to
- `_jazz_batch_id` — identity of this concrete stored entry
- `_jazz_parents` — parent batch ids for row-local ancestry
- `_jazz_state` — whether the version is visible, staging, or rejected
- `_jazz_confirmed_tier` — highest durability tier known for that version
- `_jazz_is_deleted` — tombstone marker
- `_jazz_metadata` — engine/user metadata blob
- actor/provenance columns such as `_jazz_created_by` and `_jazz_updated_by`

The important idea is that visibility, ancestry, durability, and deletion are expressed directly as
table columns inside the engine's flat row format.

## How a Direct Write Lands

For a normal row write, the engine does four things:

1. Append a new `StoredRowBatch` to the history region.
2. Recompute the visible winner for that `(branch, row_id)`.
3. Upsert the `VisibleRowEntry` for the branch view.
4. Update the relevant indices and queue sync notifications.

That work produces an `ApplyRowBatchResult`, including any `RowVisibilityChange` that downstream systems care about.

## Why Visible Entries Exist

The visible entry is the reason ordinary reads can stay simple.

When a query asks for current todos:

- index scans find candidate row ids
- materialization loads visible entries for those ids
- the runtime only falls back to full history lookup when a lower-tier winner differs from the current winner

This is why the current engine feels table-first even though it retains full row history underneath.

## Deletion Semantics

Deletes are row batch members too.

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
| `crates/jazz-tools/src/row_format.rs`                            | Shared binary row/value encoding              |
| `crates/jazz-tools/src/query_manager/graph_nodes/materialize.rs` | Visible-entry driven materialization          |
| `crates/jazz-tools/src/sync_manager/types.rs`                    | Row-batch oriented sync payloads              |
