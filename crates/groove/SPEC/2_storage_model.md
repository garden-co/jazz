# groove — Specification · 2. Data & storage model

## Overview

groove rests on a small ordered byte store. All domain concepts — schemas,
records, tables, indices, queries, and the tick — are layered above that store
rather than embedded in it. This chapter defines the storage contract that those
layers rely on, the byte encodings used for records and keys, and the layout
rules for tables and indices. Chapters 3–7 build on these guarantees.

Invariant digest:

- `INV-OK-14`: Base-table writes and durable index/view writes MUST be committed through one storage-atomic batch; if the final batch fails after runtime state advances, the Database...
- `INV-STORAGE-1`: `OrderedKvStorage` implementations MUST return scan results in lexicographic key order and `scan_range`/`range` MUST include keys `>= start` and exclude keys `>= end`.
- `INV-STORAGE-2`: `OrderedKvStorage::scan_prefix`/`prefix` MUST return exactly keys beginning with the supplied byte prefix in lexicographic key order, including prefixes whose finite upper bound cannot be computed.
- `INV-STORAGE-4`: `write_many` MUST apply all `Set`/`Delete` operations atomically at the storage-operation level, and a missing column family in the operation list MUST leave earlier valid operations unapplied.
- `INV-STORAGE-5`: `ReopenableStorage::reopen` MUST preserve existing data while adding newly requested column families.
- `INV-STORAGE-6`: Table records MUST be stored as values in the table column family named by `TableSchema::name`, keyed by the encoded primary key derived from the row record.
- `INV-STORAGE-7`: Public insert/update values MUST be interpreted in `TableSchema.columns` declaration order, independent of the `RecordDescriptor` physical encoding order.
- `INV-STORAGE-8`: `RecordDescriptor::fields()` and field indices MUST remain in logical declaration order even though encoded bytes may reorder fixed-width fields before variable-width fields.
- `INV-STORAGE-9`: Fixed-width record scalar payloads and record/array offsets MUST use little-endian encoding inside record values; fixed-width tuple integer members MUST use big-endian order-preserving member encoding.
- `INV-STORAGE-10`: Fixed-width nullable nulls MUST encode as flag `0` plus zero-filled reserved payload width; variable-width nullable nulls MUST encode as only flag `0`.
- `INV-STORAGE-11`: Fixed-width arrays MUST encode as concatenated element encodings without an element count; variable-width arrays MUST encode `count: u32`, offsets for all but the final element, then payloads.
- `INV-STORAGE-12`: `F64` record and ordered-key values MUST NOT be NaN.
- `INV-STORAGE-13`: `EnumSchema` values MUST persist and compare by declaration-order `u8` discriminant; appending variants is compatible, but reordering/removing variants changes stored meaning.
- `INV-STORAGE-14`: Primary-key bytes MUST be order-preserving tagged encodings: integer payloads big-endian, `Bool` as `0|1`, `Uuid` raw bytes, and `String`/`Bytes` escaped with embedded NUL `00 ff` plus terminator `00 00`.
- `INV-STORAGE-15`: Table writes MUST reject rows whose primary-key values do not match the declared `PrimaryKeyColumn.key_type`, and MUST reject table writes for tables with no primary key.
- `INV-STORAGE-16`: Inserts MUST reject an existing primary key, including keys introduced by earlier operations in the same `DatabaseBatch`.
- `INV-STORAGE-18`: Base table writes MUST be staged before the tick and flushed together with durable tick writes only after the tick succeeds.
- `INV-STORAGE-19`: Runtime storage reads during a staged tick MUST observe staged set/delete operations before committed storage, including same-tick durable `Persist` writes.
- `INV-STORAGE-20`: Directly exposed record stores MUST be typed record stores with record-encoded values and order-preserving typed primary keys, while bypassing table batches, primary-key table scans, durable index maintenance, query planning, and IVM ticks. A single trailing variable-width `Bytes` value column MUST encode as exactly the stored bytes.
- `INV-STORAGE-21`: `DatabaseSchema::column_families()` MUST include the `"indices"` column family whenever any table declares an `IndexSchema`, and MUST omit it when no schema index exists.
- `INV-STORAGE-22`: Non-unique durable index logical keys MUST append a `0xff` separator and encoded primary-key bytes; unique index keys MUST omit that suffix.
- `INV-STORAGE-23`: Durable unique indices MUST reject writing a positive delta for an index key already associated with a different record.
- `INV-STORAGE-24`: Persisted index scans MUST decode the persisted index record's `"value"` as primary-key bytes and fetch the current base table record; if the base record is missing for a primary-key table, the index MUST be treated as invalid.
- `INV-STORAGE-25`: Ordered index key encoding via `encode_key_part` MUST preserve logical ordering for supported key values in RocksDB lexicographic order and MUST reject arrays as keys.
- `INV-STORAGE-26`: Windowed record encoding (ch. 2 §2.9) MUST be invisible above the record store: decode∘encode is the identity over record sequences, the storage conformance suite passes identically under windowed and plain representations, and no consumer above the record store can observe which representation is in use.

## Details

Rust names in this chapter (`OrderedKvStorage`, `RecordStore`,
`RocksDbStorage`, …) identify the reference implementation surface. The
normative contract is the behavior specified here.

**Implementation-status note.** The RocksDB reference backend declares its
`lz4` and `zstd` compression features in groove's crate metadata rather than
inheriting them from a consumer such as `jazz-tools`. This is a build-layout
choice, not part of the portable storage contract.

### 2.1 The storage interface: `OrderedKvStorage`

The storage layer supplies exactly the ordered byte map groove needs. It is
partitioned into named column families and exposes a small set of operations
(`OrderedKvStorage` in the reference implementation): point `get`, `set`, and
`delete`; forward range scans over `start..end`; prefix scans in forward and
reverse order; a last-with-prefix helper; and atomic batch writes through
`write_many`.

Higher layers do not treat that byte map as their public storage abstraction.
They work through **record stores**, which are typed storage units described by
a `RecordDescriptor`. Record stores are either groove-**managed** stores
(tables §2.3 and durable indices §2.5, maintained by the tick) or
**directly-exposed** stores (§2.4, declared and maintained by the application).
The backing partitions are still called "column families" in the reference
implementation; at the specification level, higher layers should reason in
terms of record stores.

The only ordering property groove requires from the backing store is
lexicographic byte order. Scans return keys in that order, and `scan_range`
includes keys `>= start` while excluding keys `>= end` (`INV-STORAGE-1`). Batch
writes are atomic: `write_many` applies every operation in the batch or none of
them; if any operation is invalid, no operation partially applies
(`INV-STORAGE-4`).

_Further invariants._ `INV-STORAGE-2` — `scan_prefix` returns exactly the keys
with the given byte prefix, in order, including prefixes with no finite upper
bound. `INV-STORAGE-5` (prov) — `ReopenableStorage::reopen` preserves existing
data while adding newly requested families.

**Implementation-status note.** The shared storage conformance tests exercise
ordering, prefix upper-bound handling, and failed-batch atomicity on the host
memory backend. The wasm-only OPFS adapter compiles against an in-memory B-tree
fixture; coverage of persistence across closing and reopening a real OPFS
namespace remains a browser-harness gap.

### 2.2 Records: logical fields, physical bytes

A **record** is the stored byte representation of a typed tuple. Its schema is
given by a `RecordDescriptor`, but callers see only the tuple's **logical**
field order: declaration order, addressed by name or by index. The physical
layout is private to the encoder. To make records compact and decodable, the
encoder places fixed-width fields first, then variable-width fields described by
an offset table (`INV-STORAGE-8`).

Two value rules protect higher-layer ordering and schema stability. An `F64`
value must never be NaN, whether it appears in a record or in an ordered key
(`INV-STORAGE-12`). An `EnumSchema` variant is persisted and
compared by its declaration-order `u8` discriminant (`INV-STORAGE-13`):
appending variants is forward-compatible, while reordering or removing a
variant changes the stored meaning of existing data and is a breaking change.

The exact byte format for records, nullable values, and arrays is specified in
§2.7.

### 2.3 Tables

A **table** is a managed record store named by `TableSchema::name`. Each row is
stored as an encoded record interpreted by `TableSchema::record_schema`, under
its encoded primary key (`INV-STORAGE-6`). A table must declare a primary key: a
write with no primary key is rejected (`Error::MissingPrimaryKey`), and a key
value whose type does not match the declared `key_type` is also rejected
(`INV-STORAGE-15`). Public insert and update values are provided in
`TableSchema.columns` declaration order (`INV-STORAGE-7`).

Primary keys are encoded separately from record values by an
**order-preserving** scheme. As a result, lexicographic byte order matches
logical key order, including for composite keys. The byte-level scheme and the
set of valid key types are specified in §2.8.

`ForeignKey` and `PrimaryKey.generated` are **reserved metadata** in the schema.
They are carried as schema annotations for validation and planning.

_Further invariants._ `INV-STORAGE-16` — an insert rejects an already-present
primary key, including one introduced by an earlier op in the same batch
(`Error::DuplicatePrimaryKey`).

### 2.4 Directly-exposed record stores

Some application data needs typed persistence without table maintenance. A
**directly-exposed record store** provides that path: the application declares
the store and is responsible for reading and writing it. A
`DirectRecordStoreSchema` defines both the typed key `RecordDescriptor` and the
value `RecordDescriptor`; `Database::direct_record_store` returns a typed handle
with `set`, `get`, `delete`, `range`, `prefix`, and `write_many` operations that
use order-preserving typed primary keys and record-encoded values.

Directly-exposed stores are outside table batches, durable index maintenance,
query planning, and the tick. A write produces no delta and notifies no
subscription, but the store remains a typed record store like any other
(`INV-STORAGE-20`). When the value descriptor contains a single trailing
variable-width `Bytes` column, that column encodes to exactly the stored bytes,
so opaque payloads add no encoding overhead. This makes directly-exposed stores
appropriate for data that does not need incremental maintenance, such as
persistent caches and large binary content. jazz uses them for large-value
content: extents, offsets, and checkpoints (ch. 12).

### 2.5 Durable secondary indices

A durable secondary index is stored separately from the base table rows it
indexes, while each entry remains tied back to a primary-keyed base record.
Schema indices are persisted in the `"indices"` record store under
`durable_index_key_prefix(table, index)`, as records with descriptor
`("key": Bytes, "value": Bytes)`. `DatabaseSchema::column_families()` includes
`"indices"` whenever any table declares an `IndexSchema` (`INV-STORAGE-21`).

Index entries use ordered keys produced by `encode_key_part`, which preserves
logical order and rejects arrays as keys (`INV-STORAGE-25`). An index scan
decodes each entry's `"value"` as primary-key bytes and fetches the
corresponding base record.

_Further invariants._ `INV-STORAGE-22` — a non-unique index key appends a `0xff`
separator + the encoded primary key; a unique index omits that suffix.
`INV-STORAGE-23` — a unique index rejects a positive delta for a key already
bound to a different record. `INV-STORAGE-24` — an index scan resolves the
entry's `"value"` as primary-key bytes and fetches the base record; a missing
base record for a primary-keyed table means the persisted index is invalid.

**Target design (unified arrangement model, ch. 4 §4.6).** Indices are
redefined as a degenerate case of the unified arrangement model: a declared
index IS a durable, pk-ref, implicit-1 arrangement keyed by the declared
columns. `IndexSchema` remains as declaration sugar; the maintenance and probe
paths are the arrangement paths. The `INV-STORAGE-22`/`INV-STORAGE-24` key
encodings become the durable arrangement key encoding. (Terminology: the
spec-preferred term is _arrangement_; "index" remains acceptable user-facing
shorthand for the declared durable pk-ref case.)

**Implementation-status note.** Declared indices currently use the dedicated
`IndexBy`/`Persist` path; they have not yet been folded into the target unified
arrangement abstraction.

### 2.6 Commit ordering

A committed `DatabaseBatch` is the storage boundary at which table writes become
deltas for the tick (ch. 4). Within a single batch, repeated writes to the same
key collapse to one net change per key, so the tick observes each key change at
most once. Base table writes and durable tick writes are staged together and
flushed through one `write_many` call after the tick succeeds. Persisted base
rows and durable schema indices/views therefore share one storage-atomic
boundary (`INV-STORAGE-18`, `INV-STORAGE-19`).

During the tick, reads through the runtime storage handle first observe staged
set/delete operations and then fall through to committed storage. This gives
same-tick read-your-writes behavior for staged base and durable entries. If the
final storage batch fails after in-memory runtime state has advanced, the
`Database` instance is **permanently poisoned**: every subsequent operation
fails, and recovery requires discarding the instance and reopening the database.
Reopening means a fresh open over the same storage, which rebuilds in-memory
state from the durable data. This is a deliberate fail-stop behavior; no partial
rollback is attempted (`INV-OK-14`).

### 2.7 Encoding (normative reference)

This section defines the exact byte encodings referenced by §2.2–2.3.

**Record layout.** Fixed-width fields come first, followed by a `u32` offset
table that gives the _end_ position of every variable-width field except the
last, followed by the variable payloads. For
`[id: u64, active: bool, name: string, blob: bytes]`:

```text
+---------+--------+---------------+------------+------------+
| id: u64 | active | name_end: u32 | name bytes | blob bytes |
+---------+--------+---------------+------------+------------+
```

The first variable value starts immediately after the fixed fields and offset
table. The last variable value ends at the record's end, so its end offset is
implicit. Multi-byte scalar fields and offsets are little-endian, measured from
the record start (`INV-STORAGE-9`). Fixed-width tuple members use concatenated
order-preserving member encodings: integer tuple members are big-endian, `Bool`
is `0|1`, `Uuid` is raw bytes, enum values are their `u8` discriminants, and
nested fixed-width tuple/nullable members recurse (`INV-STORAGE-9`).

**Nullable values** (`INV-STORAGE-10`): a fixed-width null is flag `0` plus a
zero-filled reserved width; a variable-width null is the flag byte alone.

**Arrays** (`INV-STORAGE-11`): fixed-width arrays concatenate elements with no
count; variable-width arrays encode `count: u32`, offsets for all but the last
element, then the payloads.

### 2.8 Primary key encoding (normative reference)

Primary keys use an **order-preserving tagged scheme** separate from record
value encoding (`INV-STORAGE-14`). This is the load-bearing property behind
ordered scans (§2.3): lexicographic byte order matches logical key order. Each
key part is a one-byte type tag followed by a payload: **big-endian for
integers** (the opposite of the little-endian record encoding), `0|1` for
`Bool`, raw bytes for `Uuid`, and NUL-escaped (`00 ff`) + terminated (`00 00`)
for `String`/`Bytes`. A composite key concatenates these encoded parts in
key-column declaration order, so it orders by the first key column, then the
second, and so on. Valid key types are the integer widths, `Bool`, `String`,
`Bytes`, and `Uuid`; `F64`, arrays, and nullable values are not valid key parts.

### 2.9 Windowed record encoding

`INV-STORAGE-8` makes a record's physical layout private to its encoder. This
section lifts that principle from one record to a **sequence of records**: a
record store MAY represent runs of consecutive records as **windows** — one
physical key/value pair holding a bounded number of records in columnar form —
without any layer above the record store knowing.

**Motivation (measured, 2026-07-03).** A single serial text-edit transaction
writes ~10 KV records: ~675 B of physical keys plus ~740 B of values, against
~3 B of new information (the zstd-compressed op log of a 2 000-edit trace is
2.9 B/edit — the compact-history thesis, measured). The bytes are dominated by
re-stating per-record context — repeated key encodings, constant fields,
near-monotone timestamps, parent refs to the preceding record. Windowing
abolishes the repetition instead of compressing it.

**The codec is schema-driven, never semantics-driven.** It lives inside the
record-store implementation — above `OrderedKvStorage` (where keys are opaque)
and below the record-store interface (where both the value
`RecordDescriptor` and the declared `PrimaryKey` structure are known). A
window of N consecutive records (in key order, formed at consolidation time —
flush, compaction, checkpoint — never on the hot write path) becomes one
physical pair: the storage key is the first record's key; the value holds all
N records as **typed columns, key fields included** — record keys cease to be
storage keys at all. Each column independently selects from a small generic
menu per window: constant, delta-varint, dictionary, previous-row-field
reference, verbatim — chosen by measured size, compressor-style. Domain
patterns (same author, chain parents, monotone times) fall into these
encodings because the schema types them and the key design clusters them;
the codec imports no higher-layer semantics.

**No second formats.** The canonical record encoding remains the format.
Runtime code may share, slice, copy, project, or reorder encoded records, but it
must not create a parallel decoded representation and keep that representation
alive as if it were the data model. Decoding is a boundary operation or a
fallback for genuinely computed expressions, not the normal internal
representation for maintained arrangements. The review question is: _is there a
format here that is not the record encoding?_ If yes, it needs a specific reason
and a bounded lifetime.

**Benchmark-status note (July 2026).** The standing canaries are memory
amplification (peak RSS divided by encoded storage bytes) and allocations per
materialized row in the customer cold-start benchmark. After the C-lane
representation work, member 100% cold measured about 6,000 allocations per
row, 7.3s settle, and about 20x memory amplification; member 100% warm had
previously exposed about 46x memory amplification and remains a design-session
target.

**Implementation-status note.** The reference delta representation follows the
same rule: `RecordDelta`
carries `bytes::Bytes` handles to encoded records; pass-through operators clone
handles, not record byte vectors; transform operators build a batch of output
records into `BytesMut`, freeze once, and emit `Bytes` slices for individual
records; consolidation uses in-place sort plus adjacent weight folding; and
join-key construction uses inline small buffers for common keys. This is an
ownership and buffering change only. The record bytes are still the canonical
encoding, and storage-read boundaries wrap owned storage bytes into shared
handles rather than introducing a second payload format.

**Windows are physical, not semantic.** There is no run-sealing judgment: a
window closes when full (bounded to a CPU-cache-sized decode, on the order of
a few hundred records / few KB). A window of mixed traffic degrades gracefully
to dictionary/verbatim encodings — a "bad" window costs bytes, never
correctness. Reads decode the covering window transiently (floor-seek on the
window key, then in-window search over decoded key columns); the ordered
contract of §2.1 is preserved through the accessor.

**Write path.** Hot appends are deltas — via an optional storage capability
("append a small delta to a growing value with delta-cost durability"), which
RocksDB provides as a merge operator, the OPFS B+tree provides as a hot-leaf
upsert plus WAL delta-append, and the memory backend trivially. Backends
without the capability fall back to rewriting the open window value, bounded
by the window size. Which record stores opt in is a per-class attribute
alongside the class compaction profiles (append-forever classes first).

_Further invariant._ `INV-STORAGE-26` — windowed encoding is invisible
above the record store: `decode ∘ encode` is the identity over record
sequences, the storage conformance suite passes identically under windowed and
plain representations, and no consumer above the record store can observe
which representation is in use.

**Implementation-status note.** The reference implementation uses windowed
history consolidation. `history_windows_are_transparent_to_subscription_hydration`,
`post_tick_history_consolidation_preserves_live_subscription_deltas_and_hydration`,
and `history_consolidation_visits_direct_record_stores` verify that the
representation remains invisible to record-store consumers.

### 2.10 Draft — columnar base chunks with a durable row delta

**Draft decision note (evidence reports dated 2026-08-04/05; not accepted or
implemented).** This section records a proposed table representation and the
measurements behind it. It creates no new invariant and does not describe the
current implementation. The proposed representation is **immutable columnar
base chunks + a durable per-row delta + background compaction**. It is
explicitly not a columnar-only design.

For a table selected for this representation, a base chunk covers a bounded
primary-key range and contains a primary-key stream plus one encoded stream per
record field. The chunk directory chooses a generation for each range. The row
delta is a separate durable, primary-keyed store of complete current rows or
tombstones. It is the authoritative current state, not a cache, a best-effort
write buffer, or an optional optimisation.

The proposed interactive write path appends/replaces the affected row's delta
entry under the ordinary storage atomicity and durability boundary. It does not
read, decode, modify, and rewrite a base chunk. Reads reconstruct a logical
table by combining the selected base generation with the delta: a delta row
replaces a base row of the same primary key, a tombstone hides it, and a
delta-only key is included in a range scan. Compaction later reads a stable
base-plus-delta view for a range, writes new immutable chunk generation(s), and
publishes the directory change atomically before reclaiming superseded material.
The exact delta visibility and reclamation rules are intentionally open below;
the preceding paragraph is the proposed representation, not a claim that those
rules already exist.

In particular, inserts whose keys fall into an existing chunk range use the
same durable delta path. Whether their later compaction cost and scheduling
behave like updates is an inference, not a measurement.

#### Evidence and limits

The numbers below are cited from the staged write-shape, RocksDB-update,
OPFS-update, flush-cadence, row-reconstruction, and compression reports. They
were taken on one shared AMD EPYC 4564P development box. Several runs had load
averages around 7–12; the flush report deliberately ran at lower,
non-overlapped load, and the compression run started at lower load. They are
therefore directional crossovers and ratios, not service objectives, device
guarantees, or a substitute for measurements on a target workload. Cache modes
also differ by backend: “cold” is a RocksDB block-cache miss in the native
report and a fresh B-tree cache in the OPFS report, not physical-media-cold
latency.

**Write shape.** For 10,000 individual logical puts, operation count dominates
through roughly `k=64` on both RocksDB and Chromium OPFS. With 0.91 KiB mean
records, the per-batch curve has effectively plateaued by `k=512`: RocksDB was
76.6 ms at 64, 42.9 ms at 512, and 41.7 ms at one 10,000-row commit; OPFS was
567.7 ms, 442.0 ms, and 428.7 ms respectively. With 9.54 KiB mean / 4.80 KiB
median records, byte/page work dominates after 64: RocksDB remained 497–668 ms
and OPFS 2.97–3.19 s while reducing 157 commits to one. The OPFS runs still
issued 447–462 MiB at `k>=64`. A whole chunk is consequently not “as cheap as
one row” once a batch carries multi-KiB records.

**Update RMW and coalescing.** A blind row delta avoids a chunk read and rewrite
on every interactive update. The conservative, at-least-2x single-update
crossovers differ materially by backend:

| record shape          | RocksDB, first material chunk-RMW loss | OPFS, first material chunk-RMW loss |
| --------------------- | -------------------------------------- | ----------------------------------- |
| 64 B, 1 or 8 columns  | warm `k=64`; cold `k=1`                | warm and cold `k=4096`              |
| 1 KiB, 1 or 8 columns | warm and cold `k=1`                    | warm and cold `k=64`                |

For 8-column, 256-byte rows, the first clear RocksDB coalescing wins for one
chunk rewrite over blind row writes were `m=32` distinct rows at `k=64`,
`m=128` at `k=512`, and `m=2048` at `k=4096`; `k=1` never won. The apparent
`k=8, m=4` win had overlapping distributions and is not material. On real
Chromium OPFS, thresholds are later, not earlier: no win through `m=64` at
`k=64`; `m=512` at `k=512`; and `m=2048` was marginal at `k=4096`, with the
first clear win at `m=4096`. Separate, uncoalesced chunk RMW never beat row
writes in either report. This is the direct evidence for the durable delta and
for making compaction a background, coalescing operation rather than an
interactive write strategy.

**CPU reconstruction.** The resident-memory, uncompressed measurement found
that complete-row reconstruction was not materially worse through `k=512` and
was often faster for variable-width records. The clear counterexample was
`k=4096` with 64 columns: columnar/row time was 1.07x for all-fixed, 1.47x for
mixed, and 2.21x for mostly-variable rows (2,012 ns / 1,889 ns; 3,902 ns /
2,653 ns; and 6,813 ns / 3,079 ns). A one-column read remained roughly flat
in chunk length. The adverse result is therefore principally width — touching
64 separate columns for a full row — rather than length alone. This excludes
storage I/O and compression.

**Flush cadence, at the storage boundary only.** The initial-load flush cadence
is a client-layer setting outside this Groove specification; it is already
implemented there. Its storage relevance is that batching durability boundaries
changes both wall time and OPFS write amplification. For a 10,000-row load,
flush-once versus flush-every was 42.0x faster for about-1-KiB rows and 6.6x
for about-10-KiB rows in RocksDB, and 17.7x / 3.3x in OPFS. At every 512 rows,
the run was within 6–16% of flush-once; every 4,096 was within 4%. For 1-KiB
OPFS input, every-write issued 749.4 MB and every-512 55.5 MB. This section
does not choose the client cadence or its crash-loss policy.

**Compression.** Compression is a space trade-off, not evidence that a
compressed chunk belongs on the synchronous read path. In the 64-column,
mixed, `k=64` CPU measurement, per-column LZ4 made a complete row
16.27+/-0.04 us rather than 1.76+/-0.03 us uncompressed, for 1.62x reduction;
per-column Zstd-1 was 128.30+/-0.59 us for 2.12x. Projection requires
per-column framing: at mixed `k=4096`, a projected value took
19.08+/-0.05 us with per-column LZ4 but 1,600.74+/-2.89 us with whole-chunk
framing. Those are CPU-only figures, not backend measurements.

#### Literature backing and boundary

- [PAX — Ailamaki et al., VLDB 2001](https://www.vldb.org/conf/2001/P169.pdf)
  supports the structural choice of grouping attributes within a bounded page:
  it improves cache-line behavior for scans while keeping a row's parts local.
  It does not establish this delta, compaction, or write protocol.
- SAP HANA's delta merge is the closest shipping precedent for the shape
  proposed here: a write-optimised delta in front of a read-optimised column
  store, merged in the background. Notably its L1 delta holds updates in **row**
  format. No production system appears to ship columnar-only for
  transactional workloads.
- The [HTAP survey (arXiv:2404.15670)](https://arxiv.org/abs/2404.15670)
  likewise distinguishes random-write-friendly row layouts from scan-friendly
  column layouts, and describes hybrids rather than a single format.
- Modern-SSD work is supporting context in two directions: it undermines the
  spinning-disk assumptions behind point-lookup-optimised layouts, but it also
  cautions against concluding "read bigger units" — small random reads became
  cheap, and decompression, not I/O, became the bottleneck.

None of these establish the thresholds, the compaction work bound, or the
delta visibility semantics proposed above. Those are contracts this draft is
asserting, to be pinned by measurement.

The work bound in particular is a **custom storage-operator contract**, not a
result carried over from any cited system.

#### Deferred choices

**Merge operators.** The owner decision is to defer them “to keep things simple
and see how fast stuff is without it.” They may later reduce chunk-update cost,
but this draft specifies neither their semantics nor a backend abstraction for
them.

**Read-path compression.** Compression on the synchronous read path is deferred.
The measurement above shows that it changes CPU cost decisively, and any future
compressed projection format must preserve independently readable columns. No
compaction-quality result is used as evidence here: current quality is known
poor and its measurements were deliberately excluded from this design.

### 2.12 Subsumed storage backlog

The former top-level storage notes are now represented by this chapter's
ordered-key/value and record-store contract. Raw table instances, durable
indices, catalogue-like record stores, row-history payloads, visible/current
payloads, and backend-specific persistence are implementation choices under the
same ordered byte-range API. Browser OPFS, SQLite, RocksDB, memory, and future
host-provided backends must be judged by the portable contract here before a
consumer crate advertises them.

The storage-physics notes are performance guidance, not a new semantic model.
Column-family-per-physical-class layouts, per-class compaction/compression, row
encoding improvements, and bounded eviction should be evaluated through the
benchmark/performance chapters while preserving ordered scans, atomic batches,
record descriptors, and reopen/migration diagnostics.

## Open Questions

### Open questions

- 🔶 **Columnar-delta visibility semantics.** Before this draft can become a
  contract, decide the snapshot point at which a durable delta is visible, how
  a reader selects a base generation and its overlay, and when old deltas and
  chunk generations may be reclaimed. This is a correctness and recovery
  design question, not a performance-tuning choice.
- 🔶 **Compaction rule: per backend or one portable rule?** RocksDB's observed
  coalescing points are `m=32`/`128`/`2048` at `k=64`/`512`/`4096`; OPFS is
  later or absent in the matched sweep (`none through 64`, `m=512`, and only a
  marginal `m=2048` / clear `m=4096`). Decide whether the eventual scheduler
  uses backend-specific rules or one conservative portable rule, after target
  workload measurements rather than treating either report as a service goal.
- 🔶 **Inserts into an existing chunk range.** The draft sends them to the row
  delta, but their compaction behavior is inferred from updates and was never
  measured separately. Measure them before using update coalescing thresholds
  to schedule insert-heavy ranges.
- 🔶 **Chunk eligibility and compaction quality.** The `k=64`/`512` evidence
  does not measure wide production schemas, insert-heavy ranges, backlog/space
  limits, or compaction quality. Current compaction quality is known poor; its
  measurements were intentionally not used as supporting evidence. Establish
  receipts for range size, delta depth, bytes, foreground interference, and
  output quality before selecting a broader policy.
- 🔶 **Portable backend contract.** Before exposing storage through WASM/NAPI or
  a server package, pin which guarantees every backend must provide beyond the
  current reference surface: ordered key/value operations, atomic batches,
  prefix/range scans, reopen behavior, snapshot/read-timestamp semantics,
  durability-tier reporting, migration metadata, and raw content-store hooks.
  RocksDB column-family terminology must remain an implementation detail; the
  FFI-facing contract should speak in terms of named record-store partitions and
  ordered byte ranges.
- 🔶 **`reopen` normativity.** Is reopen-preserves-data (`INV-STORAGE-5`, prov)
  required of all conformant backends or only this implementation? Host coverage
  exists for `MemoryStorage`; OPFS currently has only wasm-gated compile coverage
  through its in-memory B-tree fixture, not a runnable browser test that closes
  and reopens a real OPFS namespace.
- 🔶 **Warm-reopen arrangement snapshots.** A proposed warm-reopen optimization
  would persist derived arrangement snapshots in a relaxed-durability storage
  class, stamped with the storage frontier they are consistent through. A clean
  shutdown would write snapshots for shapes or canonical fragments; reopen would
  load a snapshot only when its stamp matches the current frontier and otherwise
  rebuild from base data. Crash safety comes from treating snapshots as derived
  state: a missing or stale snapshot is discarded. The design is deferred for
  now because the flight itemization measured reopen itself at about 49ms, while
  first-serve range enumeration dominated the warm wall; bulk serve enumeration
  and window-decode caching are the load-bearing warm path. Revisit persisted
  arrangement snapshots only if the remaining rebuild cost justifies the added
  storage class, frontier accounting, and eviction interaction.
- 🔶 **Reserved schema metadata enforcement.** `ForeignKey` and
  `PrimaryKey.generated` are reserved for validation and planning; the
  implementation currently carries them but does not enforce them.
- 🔶 **Variable-width tuple members.** Fixed-width tuple members recurse today,
  but a tuple member may not itself be variable-width (`INV-STORAGE-9`, §2.7).
  Allowing variable-width members — by reusing the record encoding (§2.7) _inside_
  a tuple — would let consumers represent structured, variable-length values as a
  native column type instead of a custom encoding. The motivating consumer is
  jazz's large-value op-log, whose ops could then be a true groove column rather
  than a jazz-private byte encoding (jazz ch. 12 open questions).
- 🔶 **Async persistence boundary.** Mobile and host bindings may need
  non-blocking persistence, but the contract still needs atomic batch writes and
  ordered scans. Decide whether async is a wrapper, a second trait, or the only
  portable FFI surface.
- 🔶 **Explicit index declarations.** Storage can maintain many indices, but the
  schema/lowering contract should decide which are declared, migrated, and
  persisted rather than auto-indexing every column forever.
- 🔶 **Index staleness fallback.** Update paths must not silently tolerate stale
  indices when old row content is unavailable; either rebuild, fail loudly, or
  prove correctness under partial history.
- 🔶 **Row common-case encoding.** Compact empty metadata, singleton frontiers,
  enum tags, and visible/current rows without weakening deterministic decoding
  or cross-language fixtures.
- 🔶 **Serverless KV adapters.** Future KV hosts must prove ordered range scans,
  atomic writes, durable reopen, and migration metadata; simple key/value APIs
  without these properties are not equivalent backends.
- 🔶 **Compression policy.** Per-class compression/compaction choices should be
  explicit storage policy knobs with benchmark receipts, not incidental RocksDB
  defaults leaking into the portable contract.
