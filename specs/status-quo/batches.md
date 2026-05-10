# Batches — Status Quo

This doc is the current end-to-end description of Jazz's batch model.

If [Row Histories](row_histories.md) explains what a stored row batch entry is, this doc explains:

- how batch identity works
- how those entries are laid out in durable storage
- which in-memory types carry batch state through the runtime
- how direct and transactional batches move from local write to replayable fate
- which Rust and TypeScript APIs expose that model

## One Identity, Two Modes

The current runtime has one shared row identity rule:

- one concrete row batch entry is identified by `(row_id, branch_name, batch_id)`
- `batch_id` is a 16-byte UUIDv7-backed `BatchId`
- app-facing APIs expose that id as 32 lowercase hex characters
- `batch_id` is the visible row identity for both direct and transactional writes
- content freshness is tracked separately with 32-byte digests such as `row_digest` and `batch_digest`

That means direct and transactional writes are not two different row models. They share the same
stored row shape and the same sync identity. The difference lives in:

- `RowState`
- `BatchMode`
- `BatchFate` (legacy storage table still uses settlement naming)
- whether the batch requires an explicit seal/authority decision

The two modes are:

- `Direct`: staged until explicit commit/seal, then optimistically visible and settles as `DurableDirect`
- `Transactional`: staged first, explicitly sealed, authority-decided, settles as `AcceptedTransaction`, `Rejected`, or `Missing`

## Core Invariants

- Same-row rewrites within one batch overwrite the same `(row_id, branch_name, batch_id)` entry in place.
- Same-row rewrites within one batch keep the frozen pre-batch parent frontier instead of self-parenting through intermediate rewrites.
- Simple `insert` / `update` / `delete` calls are just one-member direct batches.
- Explicit direct-batch APIs exist so multiple writes can share one `BatchId`.
- Direct batches created with `beginBatch()` do not affect global reads until `commit()` seals them.
  `db.batch(cb)` commits only if the callback resolves; if the callback throws, the batch is
  rolled back as one unit.
- Transactional batches use the same `BatchId` for staging members, accepted visible members, replayable fate, and public handles.
- Visible resolution only merges visible rows. Staged or rejected transactional batches never
  participate in visible merges.
- A batch is one fate unit. If any member write in a batch is rejected by authority, the entire
  batch is rejected and every member is rolled back or left non-visible. Applications that need
  independent authorization, rollback, or durability fate must use separate batches.
- `BatchFate` is the active durability/rejection acknowledgement for the whole batch. Row-level
  membership and query visibility are derived from row delivery, sealed submissions, local batch
  records, and `QuerySettled.scope`; they are not encoded in fate payloads.
- Merge strategy is schema metadata, not batch metadata. The same stored conflicting history can
  therefore resolve differently under different schema versions.

## Durable Storage Format

### BatchId format

`BatchId` is stored as raw 16-byte `Bytea` in row payloads and encoded as 32 hex characters in
string keys and public API surfaces.

### Row raw table instances

Row storage is now split into schema-qualified raw table instances instead of one mixed raw table
per logical table.

Each row raw table instance is identified by:

- storage kind: `visible` or `history`
- logical table name
- full schema/layout hash

Conceptually that means one raw table instance per:

```text
(storage_kind, logical_table, full_schema_hash)
```

So one logical table can have several durable visible/history raw tables at once during schema
evolution, but every individual raw table has exactly one row layout.

Each row raw table has a durable header containing at least:

- `storage_kind`
- `storage_format_version`
- full schema hash
- logical table name

That header is enough to recover the exact row descriptor from the catalogue. In practice, read
paths resolve that raw table context once and then decode rows against the already-known format,
instead of rereading header state for each individual row.

### Row keys inside a row raw table

Within one raw table, the row keys only carry row identity for that layout:

```text
visible raw table:
  <branch>:<row_id_hex>

history raw table:
  <row_id_hex>:<branch>:<batch_id_hex>
```

The raw table header already says which storage kind, logical table, and full schema/layout those
keys belong to, so the per-row key no longer needs to repeat that context.

Lookup uses that raw-table model directly:

- exact point loads prefer the row locator's persisted full `schema_hash`
- branch scans union all raw tables for that logical table and filter on the branch key

So ordinary storage reads no longer need branch-name short-hash matching to find the right row raw
table.

### Flat history rows

History rows are encoded with `row_format` as one flat record:

- reserved `_jazz_*` system columns first
- nullable application columns after that

The current history system columns are:

- `_jazz_parents`
- `_jazz_updated_at`
- `_jazz_created_by`
- `_jazz_created_at`
- `_jazz_updated_by`
- `_jazz_state`
- `_jazz_confirmed_tier`
- `_jazz_delete_kind`
- `_jazz_is_deleted`
- `_jazz_metadata`

For history rows, `(row_id, branch_name, batch_id)` comes from the raw-table-local storage key
rather than the payload.

### Flat visible rows

Visible rows store the current winning row body as:

- `_jazz_batch_id`
- `_jazz_updated_at`
- `_jazz_created_by`
- `_jazz_created_at`
- `_jazz_updated_by`
- `_jazz_state`
- `_jazz_confirmed_tier`
- `_jazz_delete_kind`

Then they append:

- `_jazz_branch_frontier`
- `_jazz_worker_batch_id`
- `_jazz_edge_batch_id`
- `_jazz_global_batch_id`

The visible-row raw-table-local key still carries `(branch_name, row_id)`, while the current
visible `batch_id` lives directly in the flat visible row payload. Application columns again
follow after the reserved prefix.

History keeps parents, metadata, and the full delete marker lineage; the visible head keeps only
the fields needed for current reads plus tier/frontier pointers. This keeps ordinary queries fast
without duplicating the full history-row payload.

The common visible-row case stays compact:

- if one visible batch wins the whole row, the payload stores that row directly
- if all durability tiers agree, the tier preview state collapses into that same shared encoding

When the frontier truly conflicts, the visible reducer materializes one merged visible body for the
default head and persists compact provenance alongside it:

- a batch-id pool containing only the rows that actually won at least one visible column
- one packed ordinal vector for the default merged preview when it is synthetic
- packed tier override ordinal vectors only for tiers whose preview differs from the default one
- one reserved opaque merge-artifacts slot for future conflict diagnostics

Lower-tier reads can reconstruct merged previews from that visible-row sidecar without walking the
entire row history.

The sidecar keeps only one provenance pointer per user column:

- it names the latest timestamp-ordered batch that contributed to that column's resolved value
- it does not try to encode every contributing batch for additive strategies such as counters

That reserved merge-artifacts slot is intentionally looser:

- it is engine-owned and versioned as an opaque blob
- it is currently left empty in the released per-column `lww` and `counter` implementation

### Batch bookkeeping tables

Replayable batch lifecycle state is stored in three system raw table instances:

```text
__local_batch_record
__authoritative_batch_settlement
__sealed_batch_submission
```

All three are keyed by:

```text
batch:<batch_id_hex>
```

Their payloads are:

- `__local_batch_record`: one uniform `LocalBatchRecord` row format
- `__authoritative_batch_settlement`: one uniform legacy `BatchSettlement` row format, interpreted
  as `BatchFate`
- `__sealed_batch_submission`: one uniform `SealedBatchSubmission` row format

`BatchFate` is the new model name for replayable whole-batch outcome. Existing persisted
`BatchSettlement` rows remain valid. In that legacy row format, successful cases may contain a
`visible_members` array; new readers must treat that field as deprecated compatibility data, not as
the source of truth for whether a row in the batch is confirmed. Hot read paths should answer
batch-tier questions from `(batch_id -> fate/tier)` only, optionally via an additive sidecar/index.
New writers may leave `visible_members` empty once every active sync peer understands singular
`BatchFate`.

The current local batch record row stores:

- `batch_id`
- `mode`
- `requested_tier`
- `sealed`
- `members` with `(object_id, table_name, branch_name, schema_hash, row_digest)`
- `sealed_submission`
- `latest_fate`

The current sealed submission row stores:

- `batch_id`
- `target_branch_ord`
- `batch_digest`
- `members` with `(object_id, row_digest)`
- `captured_frontier` with `(object_id, branch_ord, batch_id)`

Those stored branch ords resolve through the storage-local `__branch_ord_registry` raw table,
which persists the full `(branch_ord, branch_name)` mapping set atomically as one durable write
rather than as separate `name -> ord` and `ord -> name` tables.

Like the row raw tables above, these system tables keep their format version in the raw table
header, not in every row payload.

## In-Memory Runtime Shapes

### StoredRowBatch

`StoredRowBatch` is the in-memory shape of one history entry. It carries:

- logical row id
- `batch_id`
- branch name
- parent batch ids
- provenance timestamps/actors
- `RowState`
- optional confirmed durability tier
- delete markers
- flat user data bytes
- normalized metadata entries

The important design point is that this same struct is used for:

- direct visible rows
- staged transactional rows
- accepted transactional visible rows

### VisibleRowEntry

`VisibleRowEntry` is the compact current answer for one `(branch, row_id)`. It carries:

- `current_row: StoredRowBatch`
- `branch_frontier`
- optional older or synthetic preview metadata batch ids for `local`, `edge`, and `global`

This is the main hot-path query shape. In durable storage, the common visible-row case now keeps
some fields implicit to save bytes:

- empty `_jazz_parents` encodes as `null`
- empty `_jazz_metadata` encodes as `null`
- `_jazz_branch_frontier` encodes as `null` when it is just `[current_batch_id]`

The reducer has two modes:

- linear append fast path: keep using the appended row or previous row directly
- conflicting frontier merge path: walk to the latest common ancestor, detect changed columns
  relative to that ancestor, and choose the latest changed tip per column

Merge-on-write follows the same rule. When a new direct write lands on a conflicting frontier, the
runtime first materializes the merged preview for that frontier, applies the caller's explicit
column updates on top of it, and writes a new row batch parented by the whole frontier.

### LocalBatchRecord

`LocalBatchRecord` is the replayable writer-side state for one logical batch:

- `batch_id`
- `mode`
- requested durability tier
- `sealed`
- `members: Vec<LocalBatchMember>`
- optional `sealed_submission`
- optional `latest_fate`

Each `LocalBatchMember` carries:

- `object_id`
- logical `table_name`
- `branch_name`
- full `schema_hash`
- `row_digest`

That means reconnect/rejection/retransmit can address the exact history raw table for each member
directly instead of rediscovering batch membership from ambient row-history scans.

For direct and transactional batches, `sealed` becomes `true` only after `commit()` /
`seal_batch()`. Simple one-member direct writes call that seal path immediately, while explicit
direct batches stay writable until the app calls `commit()`.

Runtimes also perform a local compatibility upgrade when opening existing storage: retained direct
batch records that predate explicit direct sealing and have members but no sealed submission are
sealed once by synthesizing the same direct `SealedBatchSubmission` shape. Already-upgraded records
are skipped, so later opens only pay the normal batch-record scan and field checks.

### BatchFate

`BatchFate` is the replayable outcome model for both write modes and the only active sync payload
that acknowledges batch durability or rejection. The current code still names this type and payload
`BatchSettlement`; that name is deprecated in favor of `BatchFate`.

- `Missing`
- `Rejected`
- `DurableDirect`
- `AcceptedTransaction`

Successful fate applies to the whole sealed batch. It does not communicate which rows a receiver
has learned, which rows match a query, or which row-batch waiters exist locally. Those facts are
derived from:

- row delivery: `RowBatchCreated` / `RowBatchNeeded`
- query completion: `QuerySettled.scope`
- local write membership: `LocalBatchRecord.members`
- authority membership: `SealedBatchSubmission.members`

Legacy `BatchSettlement::{DurableDirect, AcceptedTransaction}` storage rows may still contain
`visible_members`. That field is deprecated. Readers may use it only as compatibility
metadata when no better local membership source exists; they must not require a per-row
`visible_members` lookup to decide whether a known row in a successful batch is confirmed at the
fate's tier.

`Rejected` applies to the whole batch, not to one row inside it. A server that rejects any direct or
transactional member persists one `Rejected` settlement for the shared `batch_id`; receivers mark
all locally known rows in that batch rejected and re-run visibility from the remaining history.

### SealedBatchSubmission

`SealedBatchSubmission` is the manifest a sealed batch sends to the authority:

- batch id
- target branch name
- `batch_digest`
- current member set as `SealedBatchMember { object_id, row_digest }`
- captured family-visible frontier, empty for direct batches

### RowBatchKey

`RowBatchKey` is the runtime key for one concrete row batch entry:

- `row_id`
- `branch_name`
- `batch_id`

It is still useful for local waiters and query interest, but persisted write completion is resolved
from the batch's `BatchFate` plus local row-batch knowledge, not from a row-state sync payload.

## Direct Batch Lifecycle

### 1. Batch creation

A direct batch can start in two ways:

- implicitly, through ordinary `insert` / `update` / `delete`
- explicitly, through `beginBatch()` / `begin_direct_batch()`

Implicit writes create a fresh one-member direct batch and seal it immediately.

### 2. Local write

Each write materializes a `StoredRowBatch` directly on the visible branch with:

- `RowState::VisibleDirect`
- one shared `BatchId` for every write in that direct batch

If the same row is written multiple times inside the same direct batch, the runtime overwrites the
existing `(row_id, branch_name, batch_id)` entry instead of creating a second live history row.

### 3. Visible entry update

The row-history reducer updates:

- the flat history row
- the visible row entry
- indices
- sync queues

### 4. Replayable local tracking

`RuntimeCore` upserts a `LocalBatchRecord` for the batch. For direct batches:

- `mode = Direct`
- `sealed = false` until the direct batch is sealed
- `members` is updated in place as rows in that batch are overwritten
- the local runtime can synthesize a `DurableDirect` settlement up to its max local tier once the
  batch is sealed

### 5. Seal

`commit()` on the direct batch handle, or `seal_batch(batch_id)` at the client/runtime layer:

- reads the current member set from the replayable `LocalBatchRecord`
- computes one `batch_digest` over the sorted member set
- persists a `SealedBatchSubmission` with an empty captured frontier
- emits `SyncPayload::SealBatch`

After this point the direct batch is no longer writable. Simple `insert` / `update` / `delete`
calls perform this step immediately before returning their write handle.

### 6. Sync and remote durability

Direct batches flow over sync as:

- `RowBatchCreated` for newly learned entries
- `SealBatch` for the frozen final member set
- `BatchFate::DurableDirect` or `BatchFate::Rejected` for replayable fate

The direct batch is optimistic before authority settlement, but final fate is all-or-nothing. If
the authority rejects any member's insert/update/delete policy check, it rejects the shared
`batch_id`; accepted members from that same batch are not allowed to remain durable or visible as
accepted authority output.

Because the batch record and settlement are durable, a missed live ack no longer strands the write.

## Transactional Batch Lifecycle

### 1. Batch creation

Transactional writes start only through the explicit API:

- `beginTransaction()` / `begin_transaction()`

The batch carries one fixed target branch and one shared `BatchId`.

### 2. Staging writes

Each write materializes a `StoredRowBatch` with:

- `RowState::StagingPending`
- the transactional batch's shared `BatchId`

Ordinary reads ignore staging rows. Same-row rewrites inside the batch overwrite the same stored
entry and keep the frozen pre-batch parent frontier.

### 3. Local batch record

The runtime creates a `LocalBatchRecord` with:

- `mode = Transactional`
- `sealed = false`
- no authoritative settlement yet

### 4. Seal

`commit()` on the transaction handle, or `seal_batch(batch_id)` at the client/runtime layer:

- reads the current member set from the replayable `LocalBatchRecord`
- computes one `batch_digest` over the sorted member set
- captures the family-visible frontier
- persists a `SealedBatchSubmission`
- emits `SyncPayload::SealBatch`

After this point the transactional batch is no longer writable.

### 4a. Explicit rollback

`rollback()` on a TypeScript explicit batch or transaction handle marks only that handle as rolled
back:

- the batch is not sealed
- no `SyncPayload::SealBatch` is emitted
- pending staged rows are not deleted or rewritten
- later writes, reads, `commit()`, or `rollback()` calls on that same handle fail

### 5. Authority decision

The authority validates:

- the exact sealed member set
- the captured frontier
- the target branch
- the row digests and batch digest

The replayable outcome becomes one of:

- `AcceptedTransaction`
- `Rejected`
- `Missing`

Rejection is batch-wide: a single failed member invalidates the whole transaction. Callers that want
independent fate should split the writes into independent batches.

### 6. Accepted publication

If accepted, the same staged `StoredRowBatch` entries become visible with:

- `RowState::VisibleTransactional`
- normal visible-row materialization on the target branch
- one `AcceptedTransaction` fate for the whole batch

Accepted transactional rows do not get a second visible identity. They keep the same
`(row_id, branch_name, batch_id)` identity they had while staged.

## Sync Payloads That Matter For Batches

The sync layer now uses three batch-specific payload families:

- row entry movement: `RowBatchCreated`, `RowBatchNeeded`
- batch sealing: `SealBatch`
- replayable fate: `BatchFate`, `BatchFateNeeded`

That is the important separation to keep in mind:

- row payloads move concrete row batch entries
- batch payloads move replayable whole-batch truth
- query settlement moves read-completeness truth for a query scope

The sync protocol now uses `BatchFate` and `BatchFateNeeded`. Sync format is not a
storage compatibility boundary.

## Public API Surface

### TypeScript

The batch-aware TS surface lives in:

- `JazzClient`
- `SessionClient`
- `Db`
- `DbTransaction`
- `DbDirectBatch`
- `PersistedWrite`

Important APIs:

- `client.beginBatch()`
- `client.beginTransaction()`
- `client.localBatchRecord(batchId)`
- `client.localBatchRecords()`
- `client.acknowledgeRejectedBatch(batchId)`
- `tx.commit()`
- `tx.rollback()`
- `batch.commit()`
- `batch.rollback()`
- `db.beginBatch()`
- `db.beginTransaction()`

The `Db` batch handles bind lazily: the first table operation chooses the runtime client/schema,
and later writes through the same handle must stay on that client-bound schema surface.

Explicit transaction and batch handles also support scoped reads before commit:

- `Transaction.query(...)`
- `DbTransaction.all(...)`
- `DbTransaction.one(...)`
- `DirectBatch.query(...)`
- `DbDirectBatch.all(...)`
- `DbDirectBatch.one(...)`

Open explicit batch writes are not individually waitable:

- `Transaction.create(...)` and `DirectBatch.create(...)` return the row
- `Transaction.update(...)`, `Transaction.delete(...)`, `DirectBatch.update(...)`, and
  `DirectBatch.delete(...)` return `void`
- `Transaction.commit()` and `DirectBatch.commit()` return the waitable batch handle
- `Transaction.rollback()` / `DbTransaction.rollback()` and
  `DirectBatch.rollback()` / `DbDirectBatch.rollback()` return `void` and close the handle without
  sealing the batch

`PersistedWrite` also stays batch-shaped:

- `batchId()` returns the logical batch id
- `wait()` resolves when the requested durability tier is confirmed, or rejects if the batch is rejected
- `localBatchRecord()` reloads replayable local state
- `acknowledgeRejectedBatch()` prunes a retained rejected record

### Rust

The Rust client layer exposes the same model through:

- `JazzClient::begin_direct_batch()`
- `JazzClient::begin_transaction()`
- `JazzClient::local_batch_record()`
- `JazzClient::local_batch_records()`
- `JazzClient::acknowledge_rejected_batch()`
- `JazzClient::seal_batch()`
- `Transaction::commit()`
- `DirectBatch` and `Transaction` CRUD helpers

`SessionClient` mirrors the same explicit batch APIs for backend/session-scoped writes.

## Related Docs

- [Row Histories](row_histories.md) — row entry and visible-entry reducer logic
- [Storage](storage.md) — backends and synchronous storage boundary
- [Sync Manager](sync_manager.md) — row payloads, seals, settlements, and query-scoped delivery
- [App Surface](ts_client.md) — app-facing table/query APIs on top of the batch model
- [Opt-In Transactions, Replayable Reconciliation, and Strict Visibility](../todo/a_mvp/opt_in_transactions_replayable_reconciliation.md) — remaining forward-looking strict-visibility work
