# Transaction record v2 (`jazz.tx-record.v2`)

Compatibility write-up for the alpha.60 transaction record change (#4012,
#3768 and the tx metadata diet), required by the AGENTS.md "Durable encodings"
rule. Target release: `2.0.0-alpha.60`, the coordinated breaking release that
also carries linear history (#3281).

This PR regenerates its own fixtures, with Anselm's OK, in a separate commit
("Regenerate fixtures for tx record format (no version bump)"): the native
storage corpus (logical pack, SQLite image and RocksDB archive, with their
digests), the browser IndexedDB corpus, and the inline byte pins listed below.
That commit bumps no version. The single storage version bump for alpha.60 is
done by track F. Until then, refusal of roots written before this change rests
on the `groove.dictionary.v1` entry in the node profile (see "Upgrade path"),
not on a `jazz.node-storage` bump.

## Old form (linear history, #3281)

`jazz_transactions`, keyed `(time, node_id)`, held every fact about a
transaction in one record:

- identity and status: time, node_id, kind, n_total_writes, fate, global_time,
  durability;
- `made_by` as a full `RowAuthor` record (71 bytes for a short test author);
- the four exclusive read-evidence slots and `permission_subject`, kept for the
  transaction's whole life although they matter only while it is pending;
- `contribution_merge` provenance, which grows with the transaction's branch
  writes;
- a merge-strategy string marking a view-scoped write count;
- three rejection columns, one of them a fixed-width nullable tuple that cost
  25 bytes on every transaction that was not rejected.

Every fate, seq or durability read loaded all of it. History rows stored
`base_pending` as a fixed-width nullable tuple: 25 bytes even when null, which
is every settled write.

## New form

SPEC 2 §2.8 has the normative layout. In short:

| table                       | key               | holds                                                                                                                                                                                                                        | lifetime                                          |
| --------------------------- | ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------- |
| `jazz_transactions`         | `(time, node_id)` | time, node_id, kind, flags `U8`, n_total_writes, `made_by` interned (4-byte code), fate, global_time, durability, user_metadata, the nullable kind-specific rejection enum, the permission subject when it is not the author | forever                                           |
| `jazz_tx_provenance`        | `(time, node_id)` | the contribution provenance record                                                                                                                                                                                           | forever, only when flag bit 1 is set              |
| `jazz_pending_transactions` | `(time, node_id)` | the four read-evidence slots                                                                                                                                                                                                 | until the batch that records a seq or a rejection |
| `jazz_history_horizon`      | `id: U8 = 0`      | the seq at or below which history was truncated                                                                                                                                                                              | forever; this release requires 0                  |

`base_pending` in history becomes a nullable `{time: U64, node: Uuid}` record.

`made_by` (in `jazz_transactions` and `jazz_rejected_transactions`) and the
`created_by`/`updated_by` of history, global-current and ahead-current rows
are Groove interned columns (`groove.dictionary.v1`, Groove SPEC 2 "Interned
columns and dictionaries"): each stores a 4-byte code into the node-local
`authors` dictionary, a Groove-owned family keyed by the big-endian code and
valued by the canonical `RowAuthor` bytes, append-only and loaded into memory
on open. Every history image stores its `updated_by` again (5 bytes with
`created_by`'s code); the earlier implicit form, null when it equalled the
transaction's `made_by` and filled in from the transaction record on read, is
gone.

Exclusive read evidence moves to `jazz.exclusive-read-evidence.v2` (#3768):
the predicate-read slot gains each aggregate read's recorded result
(`jazz.aggregate-read-result.v1`), and aggregate reads no longer record the
rows they consumed. The other three slots keep their v1 bytes.

Byte pins: `node::codec::transaction_record_tests`,
`aggregate_read_result::tests`, `serializable_read_evidence::tests` and
`jazz-protocol` `wire::tests::predicate_read_aggregate_result_wire_bytes_are_pinned`.

## Wire

The transaction record change is storage-only. A `Transaction` on the wire
still carries its full author, provenance, permission hint and read evidence,
and a replicated row carries its full `$createdBy`/`$updatedBy`: Groove
decodes interned columns on every read, and a descriptor with an interned
column refuses to encode (`InternedTypeNotEncodable`), so codes cannot reach
the wire. Author codes never leave the node.

`PredicateRead` gains a trailing `aggregate_result` (#3768). Old and new peers
cannot decode each other's units, so mixed peers must be refused at Hello by
the alpha.60 wire version bump. If an old-form unit did reach a new authority,
its aggregate read would have no result and be rejected as `MalformedCommit`,
never silently accepted.

## Upgrade path

None. alpha.60 is a coordinated breaking release, so a store written by an
earlier build is refused by the storage codec profile before any of these
decoders runs, as for earlier layout changes: the layout is covered by the
node root's `jazz.node-storage.v2` marker (#4071, v2 since the union row cases of #4023), and interned authors add
`groove.dictionary.v1` to the node profile, so a node root without it is
refused with `UnsupportedStorageCodecs { missing: ["groove.dictionary.v1"] }`. A store from this build that a
later release truncates is refused by this build with
`TruncatedHistoryUnsupported` instead of being read as if its history were
complete.

## Downgrade to alpha.59

Opening a store written by this build with alpha.59 is refused before any
mutation, but with an untyped error. alpha.59 predates the typed
`UnsupportedStorageCodecs` refusal and admits an existing root only when its
manifest equals the expected one exactly:

- Native (RocksDB and SQLite): the adapters call `admit_existing` on the stored
  manifest before they read or write anything else
  (`v2.0.0-alpha.59:crates/jazz-storage-rocksdb/src/lib.rs:628`,
  `v2.0.0-alpha.59:crates/jazz-storage-sqlite/src/lib.rs:443`).
  `StorageEpochManifest::admit_existing`
  (`v2.0.0-alpha.59:crates/groove/src/storage/manifest.rs:220-231`) requires
  `found == self`, so a manifest naming `groove.dictionary.v1`,
  `groove.durable-index.v2` or `jazz.node-storage.v2` fails with
  `Invalid("storage manifest is inconsistent with this adapter")`.
- Browser (IndexedDB): `assertStorageManifest` compares `requiredCodecIds`
  element by element
  (`v2.0.0-alpha.59:packages/jazz-tools/src/runtime/indexeddb-page-store.ts:959-984`)
  and throws "Missing or invalid IndexedDB storage epoch manifest" before the
  store claims its runtime owner.

alpha.59 never decodes a tx-record v2 record or an author code, so it cannot
misread one. The same was already true of main's roots before this change.
No test runs an alpha.59 reader; the current Groove test
`selected_codec_profile_rejects_omitted_extra_and_unknown_ids_at_admission`
pins only the exact-match rule. A user who downgrades gets that opaque error
and must restore an alpha.59 store or start from an empty one.

## Authors dictionary residency

The `authors` dictionary is append-only and has no GC. An entry is never
removed, so it keeps:

- authors whose rows were deleted;
- authors that only appear on rejected transactions;
- codes reserved by batches that were dropped before they applied.

Open loads the whole dictionary into memory
(`DictionaryRegistry::load`, `crates/groove/src/dictionary.rs:177`, called
from `crates/groove/src/db/facade.rs:392`): a `Vec` of values plus a hash map
from value to code, about 150 to 200 bytes per author. Memory and open time
grow with the number of distinct authors the node has ever seen, not with the
live data. That is acceptable for alpha.60. Lazy or paged residency and a GC
or compaction story are follow-up work.

## Decisions a reader should know

- **Settled transactions keep no read evidence, but keep their permission
  subject.** Replayed and relayed units carry the permission subject, and
  peers admit and deduplicate them by it, so it is not pending-only. It costs
  a flag bit when it is the author. Duplicate detection treats a stored copy
  without read evidence as matching a redelivery that carries it.
- **Author codes are node-local, append-only and Groove's.** Jazz only marks
  its author columns interned; Groove allocates codes while encoding a write
  and stages each new entry into the same storage write as the first record
  that uses it, so no durable record can name a code without an entry. A
  batch that is dropped before it applies keeps its code reserved in memory,
  so no code is handed to two authors. Open loads and validates the whole
  dictionary (dense codes, canonical values) and refuses a corrupt one.
- **Queries see full authors.** `$createdBy` and `$updatedBy` can be
  filtered, sorted and projected because Groove decodes codes before rows
  reach IVM sources, deltas and reads. An interned column cannot be a key,
  index or foreign-key column; Jazz indexes none of its author columns.
- **The horizon is reserved, not used.** Truncation itself is alpha.61. New
  side tables can be added later without a codec change, and truncation can
  keep the winning transactions' records, so the horizon record and its
  refusal are all this release needs.
- **An exclusive aggregate read after a write in the same transaction is
  refused** with `UnsupportedSerializableRead` (#3768). The authority re-runs the
  aggregate over committed state, which does not include the transaction's
  own writes, so such a result cannot be compared soundly. Anselm accepted
  this restriction for alpha.60 (2026-10-13).

## Measured size

`crates/jazz/tests/storage_per_row_bytes.rs` (1000 todos, each inserted and
then updated in its own transaction, so 2001 transactions; one author; raw key
plus value bytes on RocksDB). Core and the writer client store the same
per-transaction bytes:

| per transaction                          | before (#3281, `tx-metadata` probe) |   after |
| ---------------------------------------- | ----------------------------------: | ------: |
| `jazz_transactions` record (key + value) |                                 287 |      88 |
| `by_global_time` index entry             |                                  30 |      30 |
| `jazz_tx_touched_rows` record            |                                  97 |      97 |
| **transaction metadata**                 |                             **414** | **215** |

The run measured 83 bytes for the record; keeping the permission subject
added its 5-byte null slot, which the pinned record bytes confirm.

The `authors` dictionary adds one entry per distinct author (75 bytes for
the test author: a 4-byte key and the 71-byte author), not per transaction;
the earlier `jazz_authors` table and its value index cost about 180 bytes.
History images are 20 bytes smaller from the variable-width `base_pending`.

Interning the row author columns, same workload (bytes per row on RocksDB):

| per row                                  | before (implicit history `updated_by`) |    after |
| ---------------------------------------- | -------------------------------------: | -------: |
| history image value (Core, writer)       |                                    251 |      179 |
| `jazz_physical_2_history` (Core, writer) |                                    668 |      524 |
| global-current value                     |                                    291 |      149 |
| `jazz_physical_2_global_current`         |                                    354 |      212 |
| **Core total**                           |                               **1546** | **1259** |
| **reader total**                         |                                **985** |  **771** |

Each author column drops from the 71-byte author to a 4-byte code; a history
image's `updated_by` costs 4 bytes instead of the 1-byte null it had when it
was the transaction's own author.

The rest of the way to about 120 bytes per transaction is the touched-row
list, which settled transactions now delete; see
`touched-rows-release-storage-compatibility.md`. That changes no layout.
