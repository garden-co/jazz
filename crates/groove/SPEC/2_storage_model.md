# groove — Specification · 2. Data & storage model

## Overview

groove rests on a small ordered byte store. All domain concepts — schemas,
records, tables, indices, queries, and the tick — are layered above that store
rather than embedded in it. This chapter defines the storage contract that those
layers rely on, the byte encodings used for records and keys, and the layout
rules for tables and indices. Chapters 3–7 build on these guarantees.

Invariant digest:

- `INV-OK-14`: Base-table writes and durable index/view writes MUST be committed through one storage-atomic batch; if the final batch fails after runtime state advances, the Database...
- `INV-STORAGE-1`: `OrderedKvStorage::scan(ScanRequest)` MUST return range results in the requested lexicographic direction and include keys `>= start` while excluding keys `>= end`.
- `INV-STORAGE-2`: A prefix `ScanRequest` MUST return exactly keys beginning with the supplied byte prefix in the requested lexicographic direction, including prefixes whose finite upper bound cannot be computed.
- `INV-STORAGE-29`: An explicit ordered scan request's finite item bound MUST cap the complete cursor result in the requested direction; adapters MUST stop reading beyond that bound rather than treating it as a caller-side collection hint.
- `INV-STORAGE-4`: `write_many` MUST apply all `Set`/`Delete` operations atomically at the storage-operation level, and a missing column family in the operation list MUST leave earlier valid operations unapplied.
- `INV-STORAGE-5`: `ReopenableStorage::reopen` MUST preserve existing data while adding newly requested column families.
- `INV-STORAGE-6`: Table records MUST be stored as values in the table column family named by `TableSchema::name`, keyed by the encoded primary key derived from the row record.
- `INV-STORAGE-7`: Public insert/update values MUST be interpreted in `TableSchema.columns` declaration order, independent of the `RecordDescriptor` physical encoding order.
- `INV-STORAGE-8`: `RecordDescriptor::fields()` and field indices MUST remain in logical declaration order even though encoded bytes may reorder fixed-width fields before variable-width fields.
- `INV-STORAGE-9`: Fixed-width record scalar payloads and record/array offsets MUST use little-endian encoding inside record values; fixed-width tuple integer members MUST use big-endian order-preserving member encoding.
- `INV-STORAGE-10`: Fixed-width nullable nulls MUST encode as flag `0` plus zero-filled reserved payload width; variable-width nullable nulls MUST encode as only flag `0`.
- `INV-STORAGE-11`: Fixed-width arrays MUST encode as concatenated element encodings without an element count; variable-width arrays MUST encode `count: u32`, offsets for all but the final element, then payloads.
- `INV-STORAGE-12`: `F64` record and ordered-key values MUST NOT be NaN.
- `INV-STORAGE-13`: A standalone `ScalarEnumSchema` value MUST persist and compare by its declaration-order `u8` discriminant. A distributed embedding such as Jazz MUST qualify that compact discriminant with the authored schema that declares it before treating it as a semantic value; a node-local physical discriminant is only an interned shorthand for that qualified identity.
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
- `INV-STORAGE-26`: Record-store persistence is row-only: each logical stored record has its canonical row key/value entry, and no storage maintenance may replace a run of rows with a second logical representation.
- `INV-STORAGE-27`: A record-valued `ValueType` MUST carry its descriptor inline and accept only canonical child bytes; it MUST NOT appear, directly or recursively, in a durable primary key.
- `INV-STORAGE-28`: Every enum occurrence has an independent persistent registry identity; nested enums and the hidden whole-row enum never share or flatten registry state.

Engine-owned schema catalogues and operator-supplied native schema files are trusted
durable formats: their serialized enum registry identities round-trip exactly. Public
Jazz schema JSON is a separate, deliberately narrower model and cannot carry a Groove
enum registry identity; public conversion creates fresh internal descriptors instead.

### Enum registries and variant records

An `EnumSchema` is an ordered set of `EnumCase`s. A selected `EnumValue` carries a
canonical bounded `u32` case tag and that case's record payload. Tags are dense in
declaration order; appending a case is compatible, while changing an existing case,
its tag, or payload descriptor is not. Scalar enum columns use the same declaration
and registry rules with zero-payload cases, encoded as their compact discriminant.

Groove's standalone registry model deliberately has no distributed-schema
semantics: its discriminant is meaningful only together with the descriptor that
owns it. A host with concurrent schema versions MUST add that qualification at its
boundary. In particular, a naked `u8` is never a globally meaningful user enum
value merely because two schemas both happen to use it. The host may intern a
resolved semantic case into a local dense tag, but equality, hashing, grouping,
ordering, indexing, and projection must preserve the semantic case identity rather
than depend on a local allocation order. This requirement applies to scalar enum
columns now; payload enums need the same identity model before they are exposed
through concurrent Jazz schema versions.

Every enum occurrence owns a persistent registry identity derived from its physical
path. Registries evolve independently: adding a case to one user column cannot alter
another column or the hidden whole-row enum. `TableSchema` persists the registry
snapshots separately from referencing descriptors. It never forms a Cartesian product
of row layouts and nested enum cases, and it has no central flattened registry.

Whole-row storage uses the same bounded-tag machinery through `VariantRecord`; its
registry is hidden inside the table implementation. Jazz normalizes these opaque
physical rows at `VariantProject` before exposing logical rows, so no physical tag or
whole-row enum appears in Jazz's public schema, wire values, lenses, or query API.

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
`delete`; atomic `put_if_absent` and `compare_and_delete` for immutable-object
installation and ABA-safe reclamation; explicit ordered scan requests over a
prefix or half-open range in either direction; a last-with-prefix helper; and
atomic batch writes through
`write_many`. A request may carry a finite item bound. It is a backend contract,
not a caller-side collection hint: the complete cursor yields no more than that
many rows and an adapter stops traversal/hydration at that boundary.

Higher layers do not treat that byte map as their public storage abstraction.
They work through **record stores**, which are typed storage units described by
a `RecordDescriptor`. Record stores are either groove-**managed** stores
(tables §2.3 and durable indices §2.5, maintained by the tick) or
**directly-exposed** stores (§2.4, declared and maintained by the application).
The backing partitions are still called "column families" in the reference
implementation; at the specification level, higher layers should reason in
terms of record stores.

The only ordering property groove requires from the backing store is
lexicographic byte order. A range `ScanRequest` returns keys in that order and
includes keys `>= start` while excluding keys `>= end` (`INV-STORAGE-1`). Batch
writes are atomic: `write_many` applies every operation in the batch or none of
them; if any operation is invalid, no operation partially applies
(`INV-STORAGE-4`). Its completion outcome also distinguishes a failure known to
have left the batch unapplied from a failure that may have followed a durable
commit. Backends must classify an uncertain acknowledgement conservatively as
possibly committed; only a definitely-uncommitted outcome permits callers to
roll back in-process state or retry the same batch.

_Further invariants._ `INV-STORAGE-2` — a prefix scan request returns exactly the keys
with the given byte prefix, in its requested direction, including prefixes with no finite upper
bound. `INV-STORAGE-29` — an explicit scan limit applies across all cursor
batches and stops physical traversal rather than merely truncating a materialized
result. `INV-STORAGE-5` (prov) — `ReopenableStorage::reopen` preserves existing
data while adding newly requested families.

An ordered cursor is **not** a snapshot-isolation primitive. A backend may
observe committed changes between batches; in particular, `MemoryStorage`
reacquires its map for every lazy cursor batch to keep memory proportional to
the active batch rather than the full scan. Code that requires a stable cut
must obtain it at a higher layer rather than infer it from one scan cursor.

A staged transaction overlay applies its logical limit after merging staged
writes with base entries. To avoid under-filling after staged deletes, it may
give its base scan a finite physical budget of the logical limit plus one entry
for each in-range staged key whose final operation is `Delete`; it MUST NOT
clear the bound and let a backend materialize its ordinary unbounded batch.

**Implementation-status note.** The shared storage conformance tests exercise
ordering, prefix upper-bound handling, and failed-batch atomicity on the host
memory backend. The wasm-only IndexedDB adapter compiles against an in-memory B-tree
fixture; coverage of persistence across closing and reopening a real IndexedDB
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
(`INV-STORAGE-12`). A `ScalarEnumSchema` variant is persisted and
compared by its declaration-order `u8` discriminant (`INV-STORAGE-13`):
appending variants is forward-compatible, while reordering or removing a
variant changes the stored meaning of existing data and is a breaking change.

The exact byte format for records, nullable values, and arrays is specified in
§2.7.

**Target design (record-valued values, 2026-08-04).** `ValueType` gains
`Record(Box<RecordDescriptor>)`, whose runtime value is an `OwnedRecord`. The
descriptor is inline in the parent descriptor metadata; groove MUST NOT require
a descriptor registry or encode descriptor bytes beside every child value. The
`Box` makes recursive descriptors finite at the Rust type level. An array of
record values therefore uses the existing array framing around the canonical raw
bytes of each element descriptor; no second outer-record layout is introduced.

Record values are admitted only when their embedded descriptor equals the
declared `ValueType::Record` descriptor and their raw bytes are canonical for
that descriptor: decode every child value, recreate the record, and require
byte equality. Validation alone is insufficient because `OwnedRecord::new`
currently accepts arbitrary raw bytes (`src/records/mod.rs:1577-1582`). The
recreate-and-compare rule is required for byte-based weighted consolidation and
deterministic final tie-breaking.

`Record`, `Array<Record>`, and any recursively containing value type MUST be
rejected as a durable primary-key part. The primary-key codec has no
field-semantic order for raw nested record bytes; this is a rejection rule, not
an invitation to use their byte layout as an ordered key. Arrangement-key
rejection is the graph-validation rule in ch. 3.

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
persistent caches and opaque binary content.

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
same-tick read-your-writes behavior for staged base and durable entries. A
definitely-uncommitted final batch may roll back the just-applied in-memory tick.
If the final storage batch is possibly committed after runtime state advances,
the `Database` instance is **permanently poisoned**: every subsequent operation
fails, and recovery requires discarding the instance and reopening the database.
Reopening means a fresh open over the same storage, which rebuilds in-memory
state from the durable data. This is a deliberate fail-stop behavior; no partial
rollback or retry is attempted for an ambiguous outcome (`INV-OK-14`).

**Open question — ownership of read-your-writes batches.** Atomic
`write_many` is a required ordered-storage property. It is less clear that a
general read-your-writes transaction belongs in `OrderedKvStorage` itself.
Groove currently needs an overlay for public `DatabaseBatch` reads, including a
small number of genuine Jazz ingestion dependencies, while the IVM's same-tick
overlay is being reconsidered as part of interruptible evaluation. Revisit
whether the stable model should be:

- an ordered-storage transaction that also supports reads; or
- a Groove-owned prepared write set/read view that ultimately submits one
  atomic `write_many`.

Do not grow backend transaction lifecycle semantics until this is resolved.

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
`Bytes`, and `Uuid`; `F64`, arrays, record-valued types (including recursively
nested records), and nullable values are not valid key parts.

### 2.9 Canonical row storage

Each record store persists one canonical encoded row value at its encoded primary key. This row representation is authoritative for table rows, durable indices, and directly exposed record stores; scans, point reads, hydration, recovery, and subscriptions observe that same ordered key/value mapping. There is no window, manifest, range index, compaction cursor, or alternate record representation in the Groove storage contract.

A backend MAY use private block/page compression, key-prefix compression, caching, Bloom filters, write batching, or equivalent implementation techniques. Those techniques MUST preserve the ordinary ordered-row semantics of §2.1--2.8 and MUST NOT require an upper layer to decode or reconcile a second logical record format.

**Format-cut decision (2026-08-09).** Existing stores containing the retired `GWIN2` window metadata or values are not supported: Groove neither recognizes, reads, migrates, nor dual-runs that format. This zero-user core branch deliberately takes the incompatible-format cut now, before persisted user stores exist.

_Further invariant._ `INV-STORAGE-26` -- Record-store persistence is row-only: each logical stored record has its canonical row key/value entry, and no storage maintenance may replace a run of rows with a second logical representation. `groove::db::tests::history_rows_remain_plain_across_hydration_post_write_and_reopen` is the planted-sensitive coverage.

### 2.10 Rejected research -- columnar base chunks with a durable row delta

The former hybrid columnar-base proposal is rejected and is not part of Groove's normative trajectory or implementation plan. Its 2026-08-04/05 measurements remain archival benchmark evidence only: batch coalescing can reduce broad rewrite work, but interactive single-row updates and reconstruction introduced a second authoritative representation and compaction/reconciliation boundary. The canonical row baseline above is simpler and is the adopted design. Reconsideration requires a new proposal, fresh evidence, and explicit invariants; no current code or compatibility path reserves this design.

## Open Questions

- 🔶 [#1775](https://github.com/garden-co/jazz/issues/1775) — Current-base selection, compaction handoff, scan exclusion, and compaction-quality receipts.
- 🔶 [#1774](https://github.com/garden-co/jazz/issues/1774) — Portable storage guarantees, reopen normativity, async persistence, serverless backends, row encoding, and compression policy.
- 🔶 [#1776](https://github.com/garden-co/jazz/issues/1776) — Explicit index declarations and stale-index behavior.
