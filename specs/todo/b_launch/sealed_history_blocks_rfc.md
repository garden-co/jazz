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
  sealed per-row accepted history ranges
  columnar encoded and lz4-compressed inside ordinary SQLite BLOBs

rejected_history_blocks
  sealed rejected history ranges
  stored separately from accepted blocks
  optimized for audit/debug reads rather than visible history queries
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

`history_blocks` stores sealed accepted history only.

Each block belongs to one logical row and covers a contiguous accepted-history
range for that row. A block is the decompression unit. We intentionally do not
add a chunk table or a second level of indirection inside a block: lz4
decompression is fast, and historical reads can allocate the decompressed block
temporarily and drop it after answering the query.

Sketch:

```sql
create table history_blocks (
  block_id integer primary key,
  table_name text not null,
  row_id blob not null,
  min_batch_id blob not null,
  max_batch_id blob not null,
  min_global_epoch integer,
  max_global_epoch integer,
  row_count integer not null,
  codec text not null,
  format_version integer not null,
  uncompressed_bytes integer not null,
  compressed_bytes integer not null,
  payload blob not null
);

create index history_blocks_row_epoch
on history_blocks(table_name, row_id, max_global_epoch desc, min_global_epoch);
```

The exact key columns should follow the durable storage format rather than this
sketch literally. The important property is that SQLite can quickly find the
small set of blocks for a `(table, row, point-in-time)` lookup before any block
is decompressed.

## Accepted And Rejected History

Accepted blocks contain only accepted history.

Pending, staging, rejected, and branch-local rows stay in `history_open` until
their lifecycle is resolved. This keeps compaction from having to encode every
visibility state, and it avoids making branch conflict resolution depend on
opaque cold storage.

Rejected records should also be compacted eventually, but into a separate
rejected-history block family. They should not waste space forever in ordinary
open history, and they should not pollute accepted blocks used for visible
history, point-in-time reads, and ordinary sync.

Recently rejected rows should remain in `history_open` for a while because they
are useful for local diagnostics, user-facing error messages, and nearby sync
repair. Cold rejected blocks are for older rejected history that still needs to
exist for audit, replay, or debugging but is not on the common visible-history
path.

Sketch:

```sql
create table rejected_history_blocks (
  block_id integer primary key,
  table_name text not null,
  row_id blob not null,
  min_batch_id blob not null,
  max_batch_id blob not null,
  row_count integer not null,
  codec text not null,
  format_version integer not null,
  uncompressed_bytes integer not null,
  compressed_bytes integer not null,
  payload blob not null
);
```

Rejected block manifests can be indexed for diagnostics and replay, but they do
not need the same fast accepted point-in-time path as `history_blocks`.

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
3. Read the selected ordinary history rows.
4. Encode one or more per-row blocks.
5. Insert the block rows.
6. Delete or mark compacted the sealed `history_open` rows.
7. Commit atomically.

The hot tail is important. It gives recent listeners, reconnects, and near-now
historical reads a simple ordinary-row path. It also gives compaction freedom to
operate in the background without touching the newest writes.

Open questions:

- whether compacted open rows are deleted or replaced with tombstone stubs
- whether block ids are content-addressed, auto-incremented, or both
- how large a block should be before sealing a second block for the same row
- whether compaction is triggered by row version count, byte size, age, or all
  three

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
- sync export is semantically identical before and after compaction
- replay/rebuild can recover current projection from open history plus blocks
- accepted blocks do not contain pending, staging, rejected, or unresolved
  branch-local entries
- rejected blocks do not participate in visible accepted-history lookup
- compaction is atomic: a crash leaves either the old open rows or the new block
  visible to the storage layer, not half of each

## Benchmark Plan

Extend the deep-history benchmark with a sealed-block experiment:

- write the canonical workload into current rows plus `history_open`
- run accepted and rejected compaction where applicable
- measure compaction time
- measure database size before and after compaction
- measure current read
- measure historical point reads at several depths
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

## Non-Goals For The First Version

- custom SQLite VFS
- compressed WAL or SQLite pages
- virtual table integration
- chunked block tables
- storing pending or branch-local history in blocks
- block-native sync protocol
- global repacking across many rows

Virtual tables may become useful later as a SQL interface over blocks, but the
first implementation should use explicit Rust storage APIs so the access pattern
and cost model stay obvious.
