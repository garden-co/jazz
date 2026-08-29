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
- `INV-STORAGE-30`: Application table and direct-record-store names MUST have one case-sensitive, collision-free namespace that excludes Groove's engine-owned names; every physical column-family ingress MUST reject embedded NUL and names beyond the portable UTF-8 byte bound before durable mutation.
- `INV-STORAGE-31`: A durable adapter MUST validate its epoch-pinned physical manifest before mutating a pre-existing store; engine files are not interchange, and backend commit/WAL sync—not maintenance flushes or checkpoints—is the durability boundary.
- `INV-STORAGE-32`: An atomic batch acknowledgement MUST distinguish committed, definitely-uncommitted, and possibly-committed outcomes; cancellation after an attempt begins is conservatively possibly committed.
- `INV-STORAGE-33`: A payload `EnumValue` MUST persist its declaration-order `u32` case tag as a minimal little-endian base-128 varint followed immediately by the selected case's canonical record payload; unknown, truncated, overflowing, and non-minimal tags are invalid.
- `INV-STORAGE-35`: The epoch-1 Jazz class-CF layout MUST use its one frozen marker, classifier precedence, class-family names, and length-framed mapped-key grammar; a missing, malformed, old, or future marker in a nonempty class store fails closed before a logical read or write.
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

**Payload-enum wire envelope (epoch 1).** The selected case tag is the minimal
unsigned little-endian base-128 (`LEB128`) representation of its `u32` declaration
index, followed immediately by that case's canonical record payload. Thus tags
`0..=127` have one byte, `128` begins `80 01`, and no length, version, or alternate
tag encoding is present. The decoder rejects an empty or unterminated tag, a fifth
byte whose payload exceeds `0x0f`, a non-minimal multi-byte spelling, an unknown
case tag, or any payload that is not canonical for the selected case. This envelope
is permanent at the Groove storage boundary: adding a new case is compatible, but
renumbering/reordering cases, changing an existing payload descriptor, or adding a
migration/dual-read path is outside this format cut.

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

### Storage epoch manifest

Every durable adapter has one fixed, top-level metadata location outside its
ordinary ordered-KV data plane. Before creating a table, column family, page,
or ordinary key, an opener MUST read the `StorageEpochManifest` there. The
canonical manifest bytes begin with `JSM1` and contain the storage epoch,
adapter ID and format version, the sorted set of required authoritative codec
IDs, and sorted decode-relevant adapter parameters. Groove contributes the
mandatory base IDs `groove.ordered-kv.v1`, `groove.large-value.v1`, and
`groove.ordered-chunk-storage.v1`; every profile and decoded manifest MUST
contain all three before an adapter may mutate. `groove.large-value.v1` covers
the inseparable V1 descriptor/`NodeRef` records and authenticated immutable-node
envelopes; `groove.ordered-chunk-storage.v1` covers that adapter's independent
install-receipt wrapper. The caller composes this closed base with its own
persistent codec profile before opening the adapter. The adapter treats
higher-layer IDs as opaque and never learns their semantics. A Jazz root, for
example, supplies the complete profile in `jazz::storage_codec_profile`, while
a Groove-only root uses exactly the mandatory Groove base. Omission, addition,
duplication, or substitution of a codec ID is invalid for the selected profile
even when the rest of the manifest is canonical. The manifest envelope is the
root boundary rather than an entry in its own registry. Adding an authoritative
opaque byte payload requires a stable ID, a corpus entry, and a new epoch rather
than an adapter-local postcard/`Bytes` exception. Missing, truncated,
noncanonical, corrupt, unknown, or inconsistent manifests fail closed before
any mutation (`INV-STORAGE-31`).

Epoch 1 is the first settled format. Stores written by pre-settlement alpha
builds are unsupported; they are neither guessed nor silently reinterpreted.
Within an epoch, authoritative codec bytes are immutable. An incompatible
change requires a new top-level epoch. A future supported transition is an
explicitly registered adjacent `N -> N+1` copy-on-write migration with a
durable journal and an atomic manifest flip. While that journal is incomplete,
application access is closed; reopen may resume or discard the unpublished
target, but must expose either complete `N` or complete `N+1`, never a mixture.
There is no synthetic migration into epoch 1.

The adapter owns its physical manifest location (for example a RocksDB internal
column family, SQLite metadata table, or IndexedDB root metadata), but it MUST
return a successful open receipt only after validating this common contract.
Memory storage has no durable manifest and is used solely for semantic
conformance. Backend files are not interchange formats.

**Implementation-status note.** RocksDB and SQLite persist and validate this
shared `JSM1` manifest. IndexedDB persists the equivalent structured-clone
epoch-one manifest at its fixed `storage-manifest`/`epoch` location before the
caller receives a page-store handle. It includes the caller-selected closed
codec profile, epoch, adapter/page versions, fixed 16 KiB page size, and
`xxh3-64-le` page checksum identity. The browser physical-open receipt installs
the committed page-v1 fixture by raw IndexedDB transaction, opens it read-only,
writes current data, reopens it, and proves corrupt/unknown manifests fail
without changing pages.

### Default-serialization boundary

Authoritative storage code must not acquire a private Rust serialization
spelling by accident. `dev/gates/default-serialization-persistence.mjs` scans
every production source tree that owns an authoritative storage ingress, egress,
or value carried across one. Such a source cannot import, re-export, or declare
an `extern crate` for a general-purpose serializer. A reviewed exception must
instead use a fully qualified call whose API, occurrence count, and exact local
source anchor are recorded in
`dev/storage/default-serialization-registry.json`; moving the call therefore
requires renewed review rather than silently transferring an allowance to a new
endpoint.

The gate inventories serializer dependencies from `Cargo.lock` as well as
scanning calls. Its current inventory is `postcard`, `serde_json`, and
`ciborium`; known default-codec families such as bincode and MessagePack are
also denied if introduced. Adding another serializer dependency requires an
explicit registry decision before persistence-owning code can use it. Thus
adding a default serializer at the persistence boundary is a reviewable
storage-format change, not an invisible implementation detail.

The registry does not bless a serializer as an engine-owned durable codec. Its
current exceptions are semantic JSON parsing for already-authenticated large
values, temporary in-memory query/fingerprint/measurement helpers, deliberately
malformed test construction, the public JSON edit transformation, and Jazz's
explicit versioned catalogue public-schema JSON envelope. That `CATS` v1
envelope length-delimits the JSON, validates its schema identity, and requires
decode/re-encode byte equality; it is a named canonical contract rather than an
inferred serde layout. New authoritative structured state must instead use the
codec profile's normative record/scalar or order-preserving-key codec, with
permanent IDs, exact fixtures, and fail-closed decode rules.

The only ordering property groove requires from the backing store is unsigned
lexicographic byte order: bytes compare as `0x00 < ... < 0xff`, never as signed
integers, locale text, or a backend-native collation. A range `ScanRequest` returns keys in that order and
includes keys `>= start` while excluding keys `>= end` (`INV-STORAGE-1`). Batch
writes are atomic: `write_many` applies every operation in the batch or none of
them; if any operation is invalid, no operation partially applies
(`INV-STORAGE-4`). Its completion outcome also distinguishes a failure known to
have left the batch unapplied from a failure that may have followed a durable
commit. Backends must classify an uncertain acknowledgement conservatively as
possibly committed; only a definitely-uncommitted outcome permits callers to
roll back in-process state or retry the same batch.

**Commit receipts and cancellation.** A portable atomic-batch submission has
three acknowledgement classes: `Committed`, `Uncommitted(error)`, and
`PossiblyCommitted(error)` (`INV-STORAGE-32`). `Uncommitted` is permitted only
when the adapter proves the error happened before its atomic commit boundary
(for example, complete local validation before beginning a native write).
An adapter that receives a native failure without that proof must report
`PossiblyCommitted`, even if the backend normally makes the failure unlikely.
Dropping/cancelling a submission future after it has begun produces no receipt;
callers must treat that case as possibly committed. Dropping it before the
first poll begins no attempt and is uncommitted. This is an acknowledgement
classification, not a request to make asynchronous storage operations
uncancellable.

Groove's resident-publication lifecycle applies that distinction directly. A
cancelled persistence future may return its publication to `Applied` only while
it is still waiting for its ordered turn and has not started the storage
submission. Once submission starts, cancellation permanently marks the
database unusable and wakes every later publication waiting for its ordered
turn so each observes the terminal order failure rather than hanging. An
explicit `PossiblyCommitted` result does the same before
the host settles its receipt, so holding or dropping that receipt cannot expose
a retry window. A proven `Uncommitted` result is the only result for which an
implementation may retry or roll back; conservatively poisoning instead
remains valid when the higher layer has no complete rollback operation.

This poison is instance-local, not a durable marker. Discarding the poisoned
`Database` and reopening the backend creates a fresh instance that may make
new operations against the durable state it finds. Reopen does not classify the
abandoned submission retroactively and MUST NOT replay it as a retry; it only
restores the state for which storage has a definite durable receipt.

**Worked cancellation/reopen receipt.** Suppose durable state contains row A.
The live database makes row B resident, begins B's atomic submission, and its
host drops that persistence future. The live instance is poisoned: it may not
read, write, retry B, or report B as locally durable. After discarding that
instance, reopening the same backend observes A and whatever byte state the
backend definitively contains; it does not synthesize another attempt for B.
The reopened instance may write a new row C normally. In the controlled
pre-write cancellation receipt, reopening therefore observes A, not B, and
then persists C; a backend that had actually committed B is still safe because
the former instance never retries or rolls back B.

`put_if_absent` and `compare_and_delete` are atomic at the persistence scope
(`INV-STORAGE-28`). A backend either serializes them across every concurrently
open handle (IDB and SQLite), shares one primitive boundary across clones
(memory), or enforces exclusive open (RocksDB). Their comparison is over exact
stored bytes, so deleting and reinstalling the same logical object with a new
installation receipt cannot be mistaken for the earlier installation (ABA).

_Further invariants._ `INV-STORAGE-2` — a prefix scan request returns exactly the keys
with the given byte prefix, in its requested direction, including prefixes with no finite upper
bound. `INV-STORAGE-29` — an explicit scan limit applies across all cursor
batches and stops physical traversal rather than merely truncating a materialized
result. `INV-STORAGE-5` (prov) — `ReopenableStorage::reopen` preserves existing
data while adding newly requested families.

For a finite prefix bound, the exclusive upper key is the exact unsigned-byte
successor: increment the rightmost byte below `0xff` and truncate every later
byte. Thus `[0x12, 0xff]` has upper bound `[0x13]`; an all-`0xff` prefix has no
finite upper bound and must be filtered by its prefix predicate through the end
of the family. Raw storage cursors are not snapshots: concurrent writes may
affect later cursor batches. Repeatable evaluation is a higher-level session
guarantee and is intentionally not imposed on this backend seam.

### Column-family namespace admission

Application table names and directly exposed record-store names share one
case-sensitive namespace (`INV-STORAGE-30`). Each application-selected name
therefore has exactly one owner; a duplicate table name, duplicate direct-store
name, or table/direct-store collision is rejected before Groove initializes a
runtime or writes a layout marker. Application names cannot use the
engine-owned `__groove_*` namespace, the durable-index family `indices`, or
RocksDB's `default` family. These reservations are global: they do not depend
on the selected storage layout or on whether an index happens to be declared.
That keeps an otherwise-valid application schema from becoming unopenable when
it later adds an index or changes a layout.

This does not make all physical families application names. Groove itself may
open its reserved metadata families, and a backend may add its own internal
family. Every physical family name that crosses a backend's open, reopen, or
persisted-catalogue boundary MUST nevertheless be valid before that backend
mutates durable state: it contains no embedded NUL and is at most `u16::MAX`
UTF-8 bytes. The common bound follows the IndexedDB name framing and makes a
valid logical schema portable across the supported backends. It also protects
the RocksDB C-string boundary. Backend discovery and import paths validate the
same physical contract before admitting requested families or replacing live
in-memory state.

### Durable backend physical boundary

The portable ordered-KV contract is logical; a durable adapter additionally has
one fixed, adapter-local manifest owned by the storage epoch. The manifest
names the storage epoch, adapter format version, required codec identities, and
every decode-relevant parameter. A missing, unknown, or internally inconsistent
manifest fails closed **before mutation**. It is not legal to discover a
plausible layout and adopt it, nor to fall back to a current decoder for an
unknown epoch (`INV-STORAGE-31`). The shared epoch manifest specifies the
cross-adapter fields; this section freezes the native adapter descriptors that
it carries.

RocksDB v3 uses the internal `__groove_storage_internal_v3` column family and
the `value-format` key with raw value `raw-v3`. Its Jazz/Groove-owned families
are `__groove_class_history`, `__groove_class_register`,
`__groove_class_global_current`, `__groove_class_ahead_current`,
`__groove_class_changes`, `__groove_class_indices`, and
`__groove_class_meta`; application names cannot collide with them, any
`__groove_*` name, `indices`, or RocksDB's `default`. Keys in every family use
RocksDB's bytewise comparator (unsigned lexicographic bytes). The adapter uses
ordinary put/delete batches only: RocksDB merge operands and their compaction
interpretation are outside the Groove format and MUST NOT encode a logical
delta. The manifest deliberately does **not** freeze SST, block, memtable,
compaction, or WAL file bytes. A successfully WAL-synced write is durable;
close-time memtable flush is performance maintenance, not a second commit.

### Jazz class-CF layout (epoch 1)

Jazz uses the `JazzClassV1` view above the ordered-KV adapter to keep the
logical table name at the Groove boundary while grouping known Jazz logical
families into a small, fixed set of physical families. This is a durable
layout, not a RocksDB-only tuning: the same keys are stored in SQLite's `kv`
table and are the bytes that a future logical export/import must preserve.

The class-layout marker is exactly one entry in
`__groove_class_meta`: key ASCII `groove-storage-layout`, value ASCII
`class-cf-v1`. A fresh store may create precisely that entry. Every other
marker value (including `class-cf-v0`, `class-cf-v2`, a prefix/suffix, or empty
bytes) is invalid. A missing marker is valid only when no classifier-matching
legacy logical family exists and every class family is empty; otherwise open
fails closed before a logical read or write. This deliberately rejects even an
empty legacy logical family, because its existence proves the store was opened
under a different physical layout. Epoch 1 has no legacy-layout migration or
dual read.

The classifier runs in this exact precedence order. `JazzClassV1` maps every
classified logical family; it has no caller-selected subset or alternate
interpretation under the same marker. Unclassified names remain their own
physical family with their unmodified keys. V1 requires the adapter to
enumerate its physical family catalogue before marker creation. An adapter that
cannot provide that catalogue fails closed: it cannot prove that an apparently
empty class store is not a legacy logical-family store.

| classifier, in order                                                                                        | physical family                 |
| ----------------------------------------------------------------------------------------------------------- | ------------------------------- |
| starts `jazz_`, ends `_history`                                                                             | `__groove_class_history`        |
| starts `jazz_`, ends `_register`, but not `_register_global_current` or `_register_ahead_current`           | `__groove_class_register`       |
| starts `jazz_`, ends `_global_current` or `_register_global_current`, and does not contain `_ahead_current` | `__groove_class_global_current` |
| starts `jazz_`, ends `_ahead_current` or `_register_ahead_current`                                          | `__groove_class_ahead_current`  |
| exactly `jazz_global_changes`                                                                               | `__groove_class_changes`        |
| exactly `indices`                                                                                           | `__groove_class_indices`        |
| starts `jazz_`                                                                                              | `__groove_class_meta`           |

For a mapped logical family `L` and its logical key `K`, the physical key is
exactly `u32be(len(utf8(L))) | utf8(L) | K`. `len` is the UTF-8 byte length,
not Unicode scalar or UTF-16 length, and `L` is admitted by the portable
column-family-name rules before this framing. There is no tag, separator,
escaping, checksum, or second table prefix. Prefix/range scans frame their
logical boundary the same way and strip exactly `4 + len(utf8(L))` bytes after
the physical adapter returns a key. Thus two logical families cannot alias even
when one name is a prefix of the other. The exact marker, classifier, order,
and key grammar are part of epoch 1 (`INV-STORAGE-35`); changing any requires
a new storage epoch rather than a fallback decoder. The layout validates a
logical family against the portable no-NUL/`u16::MAX` UTF-8 bound before it
calculates the framing length on every mapped operation.

SQLite v1 freezes header `application_id = 0x4a415a5a` (`JAZZ`),
`user_version = 1`, the `meta`, `column_families`, and `kv` DDL, and the Jazz
metadata blobs `format = jazz-groove-ordered-kv`, big-endian
`format_version = 1`, and `ddl_id = jazz-groove-ordered-kv-ddl-v1`. The tables
are `STRICT`; `kv` is `WITHOUT ROWID` with primary key `(cf, k)`. SQLite page
and WAL bytes are not part of the format. A successful SQLite transaction
commit is the durability boundary; WAL checkpointing is maintenance and never
authorizes a different visible state.

Backend stores are never file-level interchange. A separately versioned,
canonical logical export/import transfers global identities and authoritative
history, then rebuilds derived state on the receiving backend. It cannot bypass
epoch validation or make an unknown physical manifest decodable.

`MemoryStorage::export_snapshot` is a narrower restart/test harness boundary:
it captures the complete ordered-KV map rather than a logical interchange and
therefore has no compatibility decoder for alpha bytes. Its sole V1 spelling is
`"GMS1" | version:u16be | family_count:u32be | family*`; a family is
`name_len:u16be | UTF-8 name | entry_count:u32be | entry*`, and an entry is
`key_len:u32be | key | value_len:u32be | value`. Family names and entries use
their unsigned B-tree order. The decoder consumes exactly one snapshot, rejects
invalid UTF-8, truncation, trailing bytes, duplicate or alternate ordering, and
re-encodes the semantic map byte-identically before replacing resident state.
Physical-name validation still occurs before replacement. This explicit codec
is not serde, postcard, or a durable-adapter manifest; a future incompatible
snapshot requires a new magic/version and an explicit epoch decision
(`INV-STORAGE-35`).

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
memory backend. `jazz-tools` also runs a real-browser IndexedDB physical-open
receipt against the production page store; it does not substitute
`MemoryPageStore` for this boundary.

### IndexedDB page-store physical format

IndexedDB is one durable adapter, not a second logical Groove layout. Its
database name; `pages`, `metadata`, and `storage-manifest` object-store names;
and the `current` and `epoch` keys are fixed within storage epoch 1. Before a
caller receives a handle, `storage-manifest`/`epoch` must be the exact
structured-clone manifest: epoch `1`, adapter `jazz-idb-tree`, adapter format
`1`, the mandatory Groove base plus the complete caller-composed Jazz codec
profile, page size `16384`, page checksum `xxh3-64-le`, page magic
`IDBTREE\\0`, and page format `1`. Missing,
unknown, extra, or inconsistent fields fail closed before a mutation. Epoch-one
starts at the settlement baseline: a browser schema-generation-2 database receives the new store
but no manifest and is unsupported rather than adopted. `current` is a
structured-clone record with magic `jazz-idb-tree`, format version `1`, fixed
16 KiB page size, generation, nullable root page id, and next page id. Missing,
malformed, or unknown magic/version metadata fails closed before a mutation; a
new incompatible layout uses a new epoch instead of guessing at these bytes.
The browser's IndexedDB schema generation is separately `3`: that number only
selects the object-store layout containing `storage-manifest`, and is not a
Jazz adapter, metadata, or page-codec version.
Browser page ids are JavaScript safe integers, so root, child, and next ids are
bounded to `0..=2^53-1`; exhaustion fails instead of rounding an identity. The
stored page size is validated before page write or decode.

The IndexedDB B-tree page body is adapter-private but durable. A page is exactly
`"IDBTREE\\0" | version:u8(1) | xxh3_64(payload):u64le | payload`. The first
payload byte is a fixed page tag: leaf `0`, internal `1`, or overflow `2`. All
collection and byte lengths are `u32le`; page ids and logical overflow lengths
are `u64le`. The logical overflow length stays `u64` in memory until a host
materializes bytes, so native and wasm32 accept the same canonical page. No
`usize`, serde/postcard layout, omitted option field, or trailing payload byte
is durable.

Leaf entries are strictly key-ordered. Internal keys are strictly ordered and
their explicit child count is exactly one larger than their key count. Overflow
next tags are exactly `0` (none) or `1 | page_id:u64le`. Unknown tags, malformed
lengths, checksum failures, and noncanonical trailing bytes fail closed. Exact
Rust/TypeScript fixtures include a logical overflow length above `u32` to pin
this cross-architecture contract.

Every logical operation keeps one page-identity ownership set across its
structural root-to-leaf walk and every overflow edge it follows. Repeated page
ids are cycle or shared-subgraph corruption—including two leaf values naming
the same overflow head—not deduplication.

Tree writes are copy-on-write: the changed leaf and every changed ancestor get
fresh page ids, then one IndexedDB transaction writes the new immutable closure
and replaces `current` after checking the observed generation. A crash before
publication leaves at most unreachable new pages; a published root never names
a torn or missing child. Reclamation is a separate reachability operation and
may delete only pages proven unreachable from the published root, never pages
merely replaced by an in-flight write. Reopening observes either the old root
and complete closure or the new root and complete closure.
Before persistence, one logical write—including every operation in a
`write_many` call—is also locally atomic. If page construction or validation
fails, the tree restores its prior root and allocation frontier and discards
every newly staged page; a later successful flush cannot publish those orphans
or an earlier operation from the failed batch.

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

A record decoder MUST consume exactly the descriptor-defined record span: a
truncated fixed field, offset table, or variable payload; an out-of-range or
non-monotonic offset; a trailing byte in a fixed-only record; an invalid scalar;
or a non-canonical nested record is invalid, not an alternate representation.

`F64` record values use their IEEE-754 bits little-endian; positive and negative
infinity are valid, while every NaN bit pattern is invalid on encode, decode, and
structural validation before a caller-supplied raw `VariantRecord` can enter durable
storage. Ordered-index `F64` uses the separately specified order transform in §2.8.

**Nullable values** (`INV-STORAGE-10`): a fixed-width null is flag `0` plus a
zero-filled reserved width; a variable-width null is the flag byte alone.

A present value has flag `1`; no other flag is valid. The reserved bytes of a
fixed-width null MUST be zero, so there is exactly one byte representation of
null for each declared type.

**Arrays** (`INV-STORAGE-11`): fixed-width arrays concatenate elements with no
count; variable-width arrays encode `count: u32` little-endian, offsets for all
but the last element, then payloads. Array offsets are little-endian absolute
positions from the beginning of that array payload. Zero elements encode as the
four-byte zero count with no payload; an empty fixed-width array encodes empty.

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

The epoch-1 tags are frozen: `U8=00`, `U16=01`, `U32=02`, `U64=03`,
`I64=0d`, `I32=0e`, `Bool=05`, `String=06`, `Bytes=07`, `Uuid=0a`, and
fixed-width `Tuple=0b`. Signed integer payloads flip their sign bit before
big-endian emission. A direct record-store key may use only the supported
declared key types and fixed tuples thereof; a tuple payload recursively uses
the same tagged encoding for each member in declaration order. Every key decoder is type-directed
and MUST consume the entire key: unknown/wrong tags, truncated payloads,
`Bool` payloads other than `00|01`, malformed NUL escapes, invalid UTF-8
strings, and trailing bytes are rejected. These primary-key bytes are also the
suffix bytes of a non-unique durable index (`INV-STORAGE-22`).

**Ordered index parts.** Durable index logical keys use the same tags and
payloads, extended only for indexable values: `F64=04` uses the IEEE bits with
the positive-sign flip / negative-bit inversion transform; `Nullable(None)=08`
and `Nullable(Some(x))=09` followed by the recursively encoded `x`; and a
tuple is `0b` followed by its parts. `String` and `Bytes` use `00 ff` for an
embedded NUL and `00 00` terminator. Arrays, records, payload enums, and large
values are not index key parts. Non-unique keys append exactly `ff` followed by
the complete typed primary-key bytes; unique keys append nothing. Index decoders
MUST reject malformed or trailing logical-key bytes rather than accepting a
prefix as a key. Positive and negative infinity are valid ordered `F64` values;
every NaN bit pattern is invalid on both encode and decode (`INV-STORAGE-12`).

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
