# groove — Specification · 2. Data & storage model

groove rests on a small ordered byte store. All domain concepts — schemas,
records, tables, indices, queries, and the tick — are layered above that store
rather than embedded in it. This chapter defines the storage contract that those
layers rely on, the byte encodings used for records and keys, and the layout
rules for tables and indices. Chapters 3–7 build on these guarantees.

Rust names in this chapter (`OrderedKvStorage`, `RecordStore`,
`RocksDbStorage`, …) identify the reference implementation surface. The
normative contract is the behavior specified here.

The storage contract owner declares its own build requirements. For the RocksDB
reference backend, groove declares the compression features it relies on
(`lz4`, `zstd`) in its own crate metadata rather than inheriting them indirectly
from a consumer such as `jazz-tools`; this keeps standalone groove builds aligned
with the production workspace feature set.

## 2.1 The storage interface: `OrderedKvStorage`

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
data while adding newly requested families. The shared storage conformance tests
exercise order, prefix upper-bound, and failed-batch atomicity on the host
memory backend and compile against the wasm-only OPFS adapter's in-memory B-tree
fixture; real OPFS namespace persistence across a fresh browser open remains a
wasm/browser-harness gap.

## 2.2 Records: logical fields, physical bytes

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

## 2.3 Tables

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

## 2.4 Directly-exposed record stores

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

## 2.5 Durable secondary indices

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

## 2.6 Commit ordering

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

## 2.7 Encoding (normative reference)

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

## 2.8 Primary key encoding (normative reference)

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

## Open questions

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
