# Sealed History Blocks RFC

Status: design draft

## Problem

Jazz currently keeps history as ordinary row batch entries. That is simple and
semantically direct, but rows with deep edit histories can become expensive to
store, sync, load into memory, and scan for historical reads.

The pathological cases are not rare edge cases:

- text appended in tiny increments, such as an LLM stream
- text edited in short random runs, such as a collaborative markdown document
- live coordinate or presence rows updated at frame rate

In all three cases, the current visible row is small and cheap, but the accepted
history contains thousands of very similar versions. Storing every accepted
version as an independent SQLite row repeats:

- row and batch metadata
- branch and provenance metadata
- unchanged user columns
- large values whose content is mostly shared with nearby versions

The goal is to keep current reads and active collaboration fast while making
cold accepted history much smaller and cheaper to move around.

## Proposed Shape

Use three storage regions for a user table's row state:

```text
current rows
  compact visible projection used by ordinary reads and subscriptions

history_open
  ordinary row history for recent and unsettled work
  large/deep text-like values may point at segment-tree-backed content

history_blocks
  sealed accepted and rejected history ranges, separated by block kind
  includes row history plus the tx metadata needed to decode it
  columnar encoded and lz4-compressed inside ordinary SQLite BLOBs
```

This stays in SQLite userland. We do not introduce a custom VFS, compressed
SQLite pages, or a page-level storage contract. SQLite remains responsible for
transactions, WAL, indexing, locking, and durability.

## Current Rows

Current rows keep their existing job:

- answer ordinary relational queries quickly
- drive subscriptions and visible-row diffs
- preserve branch-aware visible projections
- avoid decoding historical blocks on the read hot path

History block compaction must not make current reads depend on archived history.
If a current row can be read today without walking history, that should remain
true after its older accepted history has been sealed.

## `history_open`

`history_open` is the recent, ordinary history area. It should preserve the
status-quo row-history model closely enough that local writes, active listeners,
and unsettled sync paths stay straightforward.

It contains:

- direct accepted rows that have not yet been sealed
- transactional accepted rows that have not yet been sealed
- pending, staging, rejected, and branch-local rows
- a hot tail of accepted history retained for recent reads and active sync

Large or frequently edited text-like user values in `history_open` may use the
segment tree storage developed in the deep-history spike. That keeps hot/open
history from storing a full copy of a large text value for every tiny edit.

The segment tree is an open-history value representation. It is not the archival
boundary. Once accepted history is sealed into `history_blocks`, the block
encoder is free to store the segment roots, value refs, metadata, and unchanged
columns in a denser columnar form.

Numeric values and compact vectors, such as canvas coordinates, should usually
stay inline in `history_open`. Their open-history repetition is acceptable, and
once sealed into blocks they should compress extremely well as per-column
delta-varint or run-length streams.

## Segment-Tree Values In Open History

Some user values are large enough, and edited often enough, that even the open
history tail should not store a fresh whole value for every version. Text streams
and collaborative documents are the motivating cases.

For those values, `history_open` may store a typed reference to a persisted
segment tree instead of storing the value bytes inline. The segment tree is a
physical value representation with these goals:

- append and short-run replacement should create a small number of new segments
- unchanged text should be shared structurally across nearby row versions
- materializing the current value should be bounded by the number and size of
  live segments, not by the number of historical edits
- sync and cold storage can still treat the row version as a normal Jazz history
  entry whose column value is a versioned reference

Conceptually:

```text
history_open row
  body = segment_root_ref(...)

segment nodes
  leaf: utf8 bytes
  concat: left ref, right ref, byte length
  optional future node kinds: copy/range, balanced internal node
```

The first useful leaf codec is `text_utf8`. Append-ish streams and random
short-run document edits should share this codec. The segment tree should not
try to become a CRDT layer: Jazz history, batch metadata, parent links, branch
state, and merge semantics remain the source of truth. The segment tree only
stores reusable value bytes and structure.

Segment roots are immutable. A new text edit creates a new root that shares
unchanged subtrees with previous roots. Recent `history_open` rows may therefore
point at many roots while the underlying segment storage only contains the new
pieces and concat structure needed for each edit.

The simplest first implementation should also make leaf payload segments
immutable. Append-heavy text may temporarily form a deep concat chain while it is
hot. Background compaction is responsible for keeping this bounded: after old
root history is sealed into `history_blocks`, the current/open head can be
materialized and rebuilt into larger immutable leaves, then written as the new
current root. That intentionally trades a little temporary duplicated sidecar
storage for a much cleaner sync and GC story.

Segment-tree values are opt-in by physical column type or storage policy. They
are appropriate for:

- append-heavy text
- short random text edits
- other large byte/string values with substantial overlap between versions

They are not the default for:

- small scalars
- numeric coordinates
- compact vectors
- enum-like or dictionary-like values
- metadata already represented by interned ids

Those values should stay inline in `history_open` and rely on block columnar
encoding once sealed.

Segment storage is still ordinary SQLite userland. A sketch:

```sql
create table value_segments (
  segment_id blob primary key,
  codec text not null,
  kind text not null,
  byte_len integer not null,
  left_id blob,
  right_id blob,
  payload blob
);
```

The exact schema may use integer local ids, content hashes, or both. The
important property is that segment nodes are immutable and shareable by many
open history rows.

Segment garbage collection is tied to history retention:

- roots referenced by `history_open` must keep their reachable segments alive
- roots referenced by not-yet-archived sync data must stay alive
- once accepted history is sealed into `history_blocks`, the block encoder can
  either reference archived segment ids or inline/re-encode the needed value
  structure
- segments no longer reachable from open rows, active blocks, or sync-retention
  roots may be collected

The first implementation can avoid aggressive GC by keeping all segments until
the sealed-block experiment proves the storage shape. A later implementation
should make segment liveness explicit so long-running documents do not retain
obsolete internal structure forever.

## `history_blocks`

`history_blocks` stores sealed history blocks. Accepted and rejected history are
separate block families inside the same physical table, distinguished by a
`block_kind` value. This keeps accepted point-in-time reads from touching
rejected history while reusing the same payload codec, hash, byte accounting,
and tx-range index machinery.

Each block covers a contiguous accepted-history range. The first implementation
may make blocks per-row, because the motivating deep-history cases are single
rows with many versions. The format should not assume that row history is the
only thing worth sealing: old accepted transaction metadata should be sealed
with the row versions it describes.

A block is the decompression unit. We intentionally do not add a chunk table or
a second level of indirection inside a block: lz4 decompression is fast, and
historical reads can allocate the decompressed block temporarily and drop it
after answering the query.

Sketch:

```sql
create table history_blocks (
  block_id integer primary key,
  block_kind integer not null,
  table_num integer not null,
  row_num integer not null,
  min_node_num integer,
  max_node_num integer,
  min_local_epoch integer,
  max_local_epoch integer,
  min_global_epoch integer,
  max_global_epoch integer,
  row_count integer not null,
  codec text not null,
  format_version integer not null,
  uncompressed_bytes integer not null,
  compressed_bytes integer not null,
  payload_sha256 blob not null,
  payload blob not null
);

create index history_blocks_row_epoch
on history_blocks(block_kind, table_num, row_num, max_global_epoch desc, min_global_epoch);
```

The exact key columns should follow the durable storage format rather than this
sketch literally. The important property is that SQLite can quickly find the
small set of blocks for a `(table, row, point-in-time)` lookup before any block
is decompressed.

## Transaction Metadata Compaction

`jazz_tx` should also be compacted for old sealed history.

Once accepted row history moves out of ordinary history rows, leaving one
ordinary `jazz_tx` row per archived transaction becomes the next metadata floor.
This is especially visible for append and presence workloads where every tiny
row version has a matching tx record. The transaction row is smaller than the
user history row, but it still repeats node, epoch, fate, timestamps, conflict
mode, metadata, write set, and read set shape thousands of times.

The storage split should therefore be:

- recent/open transactions remain as ordinary `jazz_tx` rows
- pending transactions remain ordinary operational state
- accepted transactions whose row history has been sealed move into
  `history_blocks`
- rejected transactions whose rejected rows have aged out of the hot tail move
  into `rejected_history_blocks`

Open `jazz_tx` rows are the operational transaction index. Sealed transaction
metadata is historical data. Compaction must preserve public transaction
identity and replay semantics, but it does not need to preserve one SQLite row
per old transaction.

Inside an accepted block, transaction metadata should be encoded columnarly next
to the row-history streams. A block-level tx section can include:

```text
tx section
  block-local tx ordinals
  node ids: dictionary encoded
  local epochs: delta-varint per node
  global epochs: delta-varint or monotonic run encoding
  kind / conflict mode / outcome: dictionary or RLE
  created_at: delta-varint
  metadata/auth user: dictionary, nullable
  writes: packed tuple stream or references to row-history entries
  reads: packed tuple stream, with implicit previous-local-epoch defaults
  receipts/fate details needed for replay and diagnostics
```

Row entries inside the same block should reference transactions by block-local
ordinal rather than by global `tx_num`. `tx_num` remains an internal open-store
identifier, not a durable archival requirement.

The block manifest should expose just enough tx-range information for lookup
and sync planning without expanding every tx into an SQLite index row. Useful
manifest fields include:

- participating node ids or a compact node-set summary
- min/max local epoch by node, or a side manifest for node-local ranges
- min/max global epoch for accepted point-in-time lookup
- tx count
- first/last public tx id if that can be represented compactly
- content hash for block comparison

Public tx id lookup after compaction needs an indexed path. A compact side index
may be warranted for `(node_id, local_epoch) -> block_id` so
`transaction_info(tx-id)` and sync dependency checks do not scan all blocks. The
index should point to blocks, not duplicate full tx metadata:

```sql
create table history_block_tx_index (
  node_id text not null,
  min_local_epoch integer not null,
  max_local_epoch integer not null,
  block_id integer not null,
  primary key (node_id, max_local_epoch, min_local_epoch, block_id)
) without rowid;
```

The exact index can use interned node nums or compact node-set manifests instead
of text ids. The principle is that SQLite finds the small block set, then the
block decoder answers the exact tx query.

Rejected transaction metadata should follow the same idea, but in the rejected
block family. Rejections often carry diagnostic detail and should not pollute
accepted point-in-time indexes. Recently rejected transactions remain ordinary
rows so error messages, awaiting-dependency repair, and local UI diagnostics
stay simple.

Compaction must not move pending, awaiting, or otherwise unsettled transactions
into sealed blocks. Those rows are live coordination state, not cold history.

## Accepted And Rejected History

Accepted blocks contain only accepted history.

Pending, staging, rejected, and branch-local rows stay in `history_open` until
their lifecycle is resolved. This keeps compaction from having to encode every
visibility state, and it avoids making branch conflict resolution depend on
opaque cold storage.

Rejected records should also be compacted eventually, but into a separate
rejected-history block family selected by `block_kind`. They should not waste
space forever in ordinary open history, and they should not pollute accepted
blocks used for visible history, point-in-time reads, and ordinary sync.

Recently rejected rows should remain in `history_open` for a while because they
are useful for local diagnostics, user-facing error messages, and nearby sync
repair. Cold rejected blocks are for older rejected history that still needs to
exist for audit, replay, or debugging but is not on the common visible-history
path.

Rejected block manifests can be indexed for diagnostics and replay, but they do
not need the same fast accepted point-in-time path as accepted blocks.

## Block Encoding

Inside a block, entries are encoded column-by-column rather than as repeated
whole rows.

A block should contain enough schema and row context to decode entries without
looking at the original `history_open` rows:

```text
header
  block format version
  table identity
  row identity
  schema/storage descriptor id
  row count
  stream directory

streams
  batch ids / epochs: delta or dictionary encoded
  parent references: dictionary or compact vectors
  provenance ids: dictionary and RLE
  branch/tier/fate metadata: dictionary and RLE
  deleted flags: bitset or RLE
  changed-column masks: bitset or RLE
  user columns: codec selected per physical column type
```

For user columns, the default encoding should be previous-value aware:

- unchanged column: one bit or run marker
- small scalar changed value: inline typed value
- repeated value: dictionary reference
- segment-tree value: compact reference to the segment root or archival value id
- coordinate-like numeric value: delta-varint stream
- opaque value: length-prefixed bytes inside the block's lz4 payload

The first implementation can use a simpler binary format if needed, but the
target is columnar enough that unchanged columns and repeated metadata become
near-free.

Prototype note: newly sealed blocks now use a v9 `columnar-json-lz4` payload.
It stores tx, read, history metadata, and user values as parallel arrays and
decodes back to the same logical `Bundle`. This is still a stepping stone rather
than the final binary/delta-varint format, but it removes repeated per-record
JSON object keys and repeated per-row user value keys from the sealed block
body. The v9 payload dictionary-codes repeated string columns such as table ids,
row ids, branch ids, node ids, and user ids. It also retains the v4 behavior that
recognizes text values shaped as JSON `{x, y}` numeric objects and stores those
as numeric `x[]`/`y[]` streams before reconstructing the same text value on
decode. Repeated user column values, including unchanged ordinary scalar or JSON
columns, are dictionary-coded as well; this is the first whole-row block codec
step aimed at unchanged columns rather than only large edited values. Integer
metadata columns such as local epochs, outcomes, read reasons, op codes, and
timestamps are encoded as runs or deltas when that is denser than a raw array;
nullable integer metadata such as global epochs can also use run encoding.
Repeated nullable JSON metadata and repeated integer-vector metadata, such as
rejection details and receipt tiers, can also be run-coded. The decoder still
accepts the earlier `bundle-json-lz4` v1 blocks and v3 / v4 / v5 / v6 / v7 / v8
columnar blocks for compatibility within the spike.

For segment-tree-backed columns, a sealed block should not blindly store one
full materialized value per version. Acceptable first encodings include:

- store the sequence of segment root refs columnarly, relying on repeated refs
  and lz4 compression
- store roots plus the reachable segment nodes needed by the block
- materialize only the per-version deltas that are cheaper than preserving root
  structure

The archival format can choose differently from `history_open` as long as it can
decode to the same logical column values for every accepted row version.

## Compaction

Compaction moves old accepted row history from `history_open` into
`history_blocks`, and old rejected history into `rejected_history_blocks`.

One compaction transaction should:

1. Select a row whose accepted open history exceeds a threshold.
2. Choose an old contiguous accepted range, leaving a hot tail in
   `history_open`.
3. Read the selected ordinary history rows and the accepted `jazz_tx` metadata
   referenced by those rows.
4. Encode one or more sealed blocks containing row-history streams and tx
   metadata streams.
5. Insert the block rows and any block-level tx lookup index entries.
6. Delete or mark compacted the sealed `history_open` rows.
7. Delete or mark compacted `jazz_tx` rows whose accepted history is fully
   represented by sealed blocks and no open/pending state still references them.
8. Commit atomically.

The hot tail is important. It gives recent listeners, reconnects, and near-now
historical reads a simple ordinary-row path. It also gives compaction freedom to
operate in the background without touching the newest writes.

Compaction should be invokable by policy as well as by exact row id. The first
policy shape is deliberately small: accepted/rejected toggles, hot-tail and
minimum-version thresholds, an optional maximum block budget, and an optional
compressed-payload byte budget, and an optional wall-clock budget. The budgets
let an embedder run maintenance in bounded chunks, leaving age and richer
pre-compaction byte-estimate scheduling as higher-level policy decisions.
The policy can also cap rows per sealed block, which lets very deep rows be
split into multiple smaller blocks instead of one large blob.

Compaction only makes SQLite pages reusable. It should not automatically force
the database file to shrink, because checkpoint/truncate/vacuum work can be a
large latency spike. The runtime should expose explicit storage reclamation for
maintenance windows, imports, or tests that want the on-disk file to reflect the
new live footprint immediately.

Open questions:

- whether compacted open rows are deleted or replaced with tombstone stubs
- whether compacted tx rows are deleted or replaced with lookup stubs
- whether block ids are content-addressed, auto-incremented, or both
- whether compaction is triggered by row version count, byte size, age, or all
  three
- how to coordinate tx compaction when one transaction writes multiple rows that
  may seal at different times

## Reads

Current reads use current rows and should not touch blocks.

Historical point reads use a two-tier path:

1. Search `history_open` for a matching recent accepted row.
2. If not found, use the block index to find the block covering the requested
   row and time, decompress it, decode the relevant entry, and drop the
   temporary decoded buffer.

Block lookup should be index-backed and bounded by per-row ranges. For accepted
mainline point-in-time reads, `(table, row_id, epoch)` should identify at most
one relevant block once ranges are non-overlapping.

Branch snapshot reads need care. The first version of blocks should only
accelerate accepted history. Branch-local and pending history remains ordinary.
A branch snapshot can therefore combine:

- accepted base rows from `current`/`history_open`/`history_blocks`
- branch-local overlays from `history_open`

Rejected-history reads use `history_open` for recent rejections and
`rejected_history_blocks` for older ones. They should be explicit diagnostic or
replay paths, not part of ordinary visible history lookup.

If branch snapshots require repeated random access into sealed accepted history,
we may add sparse snapshot/cache tables later. That should be driven by
benchmark data rather than designed up front.

Prototype refinement: accepted compaction keeps the latest main row version at
each live branch base epoch in `history_open`. These sparse branch-base anchors
let ordinary branch reads stay on the current/open-history path instead of
decoding blocks. Older non-anchor history can still be sealed, and block-native
sync/rebuild can still use sealed records when an anchor arrives as a block from
another peer.

Prototype note: `rebuild_current_projection()` now performs the ordinary
open-history replay and then considers the latest accepted sealed row version
per `(table, row, branch)`. Sealed candidates fill gaps left by block-native
import or aggressive compaction, while newer open rows still win. This gives us
a recovery path without making current reads decode blocks.

## Sync

Initially, sync can continue to export logical row history. When a sync scope
needs sealed accepted history, the sender can decode blocks and emit the same
row-batch language peers already understand.

A later optimization can make blocks a wire/storage unit:

- block manifests can be compared by range and hash
- peers can request missing sealed blocks
- receivers can store sealed blocks without expanding every row
- current projection can be materialized from decoded block summaries or from a
  bounded decode pass

Prototype note: the current spike exposes this boundary as
`export_history_blocks(table)` / `import_history_blocks(blocks)`, table/all-table
history delta APIs, and a first equality-query history delta API. The exported
block unit is the exact lz4 block payload plus manifest metadata. Import
validates the manifest, inserts the block idempotently, and rebuilds the local tx
lookup index from exported node/local-epoch ranges without decoding the payload
or recreating ordinary user history rows. It also avoids recreating ordinary
`jazz_tx` rows for cold archived txs; tx helpers resolve those through the block
index and decode the block on demand.

Prototype note: block manifests include a SHA-256 hash of the compressed payload.
Import verifies this hash before inserting the block, so manifest-only import
can stay fast without blindly accepting corrupted payload bytes.
The prototype storage schema also persists this hash in `history_blocks`, so
local manifest listing and block-presence checks do not need to read and hash
every compressed payload.

Prototype note: the runtime exposes both per-table and all-table block
manifest/export helpers. The all-table helpers are intended for maintenance and
sync discovery, where a peer may not know which user tables contain sealed
history.

Prototype note: the runtime now has a block-native table delta path:
`export_table_history_delta(table, remote_manifests)` returns an ordinary bundle
for open/hot history plus the sealed blocks missing from the receiver's
manifest inventory. `export_all_history_delta(remote_manifests)` does the same
across every user table. Receivers import blocks first and then apply the open
bundle. The open bundle contains full row values, so the receiver does not need
an intermediate projection rebuild to preserve omitted fields in the hot tail.
Simple predicate history deltas (`eq`, `contains`, `in`, and `ne`) apply the
same block-native shape by filtering the sealed block set to the query's
visible/repair row ids. Top-created and top-field deltas cover the top-query
bootstrap shape and both have previous-observed repair variants. The top-field
variant uses an options object so adding future page-boundary state does not
create another long positional API.
These APIs return a named `HistoryDelta { bundle, blocks }`. Receivers can apply
any delta through `apply_history_delta(bundle, blocks)`, which imports missing
blocks before applying the hot/open bundle.
Observed-query refresh has the same delta-shaped path:
`export_observed_query_refresh_deltas(remote_manifests)` and
`export_query_read_refresh_deltas(reads, remote_manifests)` return one
`HistoryDelta` per observed read. Simple predicates and top queries can therefore
refresh by sending missing sealed blocks directly. Recursive-reference queries
also have a block-native delta path for the visible recursive row set. Unsupported
query shapes fall back to ordinary bundle refreshes with no blocks until they
get dedicated block planning.
Raw block exports are serializable with manifest metadata, tx ranges, and the
compressed payload bytes. The prototype JSON representation encodes the payload
as hex and is not intended as the final compact wire encoding, but the transfer
artifact must include payload bytes so peers can import blocks without
re-expanding them into row history.

That optimization is deliberately separate from the first storage change. The
first goal is to prove that sealed blocks reduce local storage and historical
load costs without changing Jazz semantics.

## Index Integration

SQLite should index block manifests, not decompressed entries.

Minimum useful indexes:

```sql
-- point-in-time row lookup
(table_name, row_id, max_global_epoch desc, min_global_epoch)

-- compaction and maintenance scans
(table_name, row_id, min_global_epoch)

-- optional sync/range export
(table_name, min_global_epoch, max_global_epoch)
```

The block payload can also carry lightweight summaries, such as:

- first and last batch id
- row count
- min/max epoch
- schema/storage descriptor
- content hash
- uncompressed and compressed byte counts

We should avoid exposing every archived row as an SQLite index entry in the
first design. That would recreate much of the metadata overhead blocks are
meant to remove.

## Semantics

Sealed blocks are a physical storage optimization. They must decode to the same
accepted row history entries that existed before compaction.

Required invariants:

- current visible rows are unchanged by compaction
- accepted historical point reads return the same row as before compaction
- public transaction ids still resolve to equivalent transaction info after
  their `jazz_tx` rows are sealed
- sync export is semantically identical before and after compaction
- replay/rebuild can recover current projection from open history plus blocks
- accepted blocks do not contain pending, staging, rejected, or unresolved
  branch-local entries
- accepted tx metadata blocks do not contain pending, awaiting, or rejected txs
- rejected blocks do not participate in visible accepted-history lookup
- compaction is atomic: a crash leaves either the old open rows or the new block
  visible to the storage layer, not half of each

## Prototype Status

Implemented in the current spike:

- accepted and rejected sealed block families in one `history_blocks` table
- columnar JSON payloads compressed with lz4, currently format v9
- dictionary/run/delta column encodings selected only when smaller than raw JSON
- payload hashes, byte accounting, block manifests, and tx-range indexes
- block import validation for payload hashes, manifest summaries, row identity,
  epoch envelopes, and canonical tx-range indexes
- accepted/rejected compaction by row, table, all tables, and bounded policy
- optional rows-per-block splitting for point-read/storage tradeoff tuning
- explicit storage reclamation separate from compaction
- block-native table, all-table, predicate, top-query, observed-refresh, and
  recursive-reference history deltas
- block import without reopening sealed history rows or recreating cold `jazz_tx`
  rows
- transaction metadata lookup from sealed blocks
- accepted point reads by global epoch and node/local epoch
- runtime decoded-block LRU cache with a fixed cap
- branch-base anchor preservation in `history_open`
- branch export and branch-scoped history-delta guards for sealed main history
- canonical benchmark metrics for Base/Base1/Base2/Base3/Block/Incr
- grouped deep-history benchmark selectors for baseline, Block, and Incr
- narrow batched insert/update/upsert APIs and benchmark switch for grouped
  SQLite commits

Still deliberately partial:

- query-scoped block planning does not cover every future query shape
- the prototype accepted-block compactor currently seals visible non-rejected
  local history; production should tighten this to genuinely settled accepted
  transactions once the runtime has a crisp accepted-vs-optimistic cutoff
- the block payload is JSON inside lz4, not the final compact binary format
- branch reads rely on open branch-base anchors rather than arbitrary sealed
  snapshot reads
- the write-batching APIs are benchmark/prototype hooks, not a scheduler
- segment-tree value storage and sealed history blocks are both present in the
  spike, but a final production policy for when to use each is not settled
- block compaction scheduling is manual/policy-driven, not automatic by age or
  byte pressure

## Benchmark Plan

Extend the deep-history benchmark with a sealed-block experiment:

- write the canonical workload into current rows plus `history_open`
- run accepted and rejected compaction where applicable
- measure compaction time
- measure database size before and after compaction
- measure open `jazz_tx` row count before and after compaction
- measure current read
- measure historical point reads at several depths
- measure `transaction_info(tx-id)` for open and sealed transactions
- measure cold load/rebuild from compacted storage
- measure sync export, first by decoding blocks into existing row-batch format
- measure segment storage bytes separately from ordinary SQLite row bytes for
  text-heavy workloads

The first workloads should remain:

- append-ish stream
- Automerge paper edit trace
- 60 FPS canvas positions

The initial success criterion is not sub-1s everything. The first success
criterion is a clear storage reduction without breaking current reads or making
historical point reads obviously pathological.

## Stretch Goal: Batched SQLite Commit Policy

History blocks reduce stored bytes after compaction, but they do not directly
fix the cost of doing many tiny write transactions. Import, LLM streaming, and
high-frequency presence/canvas updates can all be much faster if the runtime can
group multiple logical Jazz transactions into one SQLite transaction.

This should be an explicit write policy, not a semantic change:

- each logical Jazz transaction keeps its own public tx id, read set, write set,
  outcome, and subscription event
- the SQLite commit boundary may contain many logical Jazz transactions
- readers outside the writer observe the batch atomically at the SQLite level
- in-process listeners can still receive per-logical-transaction events after
  the grouped commit, preserving application semantics while accepting a small
  latency tradeoff
- the policy should be configurable by count, byte estimate, and max wall-clock
  delay
- explicit user transactions should still commit immediately unless the caller
  opts into import/stream batching

Open questions:

- whether to expose this as an import API, a runtime option, or an adaptive
  scheduler mode
- how to bound listener latency for realtime streams
- how to report partial progress if a large grouped import fails validation
- whether compaction should run opportunistically at grouped-commit boundaries

Benchmarks should add a write-batch dimension beside storage compaction so we
can distinguish "fewer durable commits" wins from "fewer stored bytes" wins.

Once the currently measured write, receive, and storage paths are low enough to
make longer runs useful, add a full-system realtime max-speed ingest benchmark:
keep one Jazz transaction/history row per logical update, batch SQLite write and
sidecar commits into low-latency slices such as 10ms, export one native sync
delta per ingest slice, apply the matching native and sidecar deltas on a
receiver, and measure end-to-end time until a live listener observes the latest
update in each slice.

## Non-Goals For The First Version

- custom SQLite VFS
- compressed WAL or SQLite pages
- virtual table integration
- chunked block tables
- storing pending or branch-local history in blocks
- complete query-scoped block-native sync planning for every query operator
- global repacking across many rows

Virtual tables may become useful later as a SQL interface over blocks, but the
first implementation should use explicit Rust storage APIs so the access pattern
and cost model stay obvious.
