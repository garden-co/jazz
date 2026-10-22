# Row value tightening

Compatibility write-up for three alpha.60 storage changes that shrink every
stored row, required by the AGENTS.md "Durable encodings" rule. Target
release: `2.0.0-alpha.60`, the coordinated breaking release that also carries
linear history and the transaction record v2.

This PR regenerates its own fixtures, with Anselm's OK, in a separate commit
("Regenerate fixtures for row value tightening (no version bump)"): the native
storage corpus (logical pack, SQLite image and RocksDB archive, with their
digests) and the browser IndexedDB corpus. The inline transaction-record byte
pins in `crates/jazz/layers/node/src/node/codec.rs` move with the code
change. No wire or package version changes; the single storage version bump
for alpha.60 is done by track F. The key-free layout does get its own manifest
marker, `groove.key-free-rows.v1` (see "Format marker" below), because it
changes how every stored row is read.

## 1. No primary-key copies in stored row values (Groove)

**Old form.** A Groove table row was stored under its encoded primary key, and
the value was the full physical record: every column, primary-key columns
included. Each history image repeated `row_uuid`, `branch_key`, `seq`,
`tx_time` and `tx_node_id` (46 bytes), each current row `row_uuid` and
`branch_key` (22 bytes), each transaction record `time` and `node_id`
(16 bytes), and each touched-rows record its key.

**New form.** The stored value is the variant tag plus the physical record
with the primary-key fields left out (Groove SPEC 2, `stored_record_layout`).
Every read path rebuilds the logical record from the decoded key: point reads,
primary-key and index scans, table deltas, and the IVM table, index, lookup
and recursion sources. A stored row whose key does not decode as exactly the
table's primary key is refused with `InvalidDirectRecordStoreKey`
(`IvmRuntimeError::InvalidStoredRowKey` inside IVM) rather than given made-up
key columns. Tables without a primary key are unchanged. Interned columns keep
their codes in stored rows and in the rebuilt record alike; the rebuild only
copies field bytes and encodes the key parts (`records::KeyFreeLayout`).

This applies to every Groove table with a primary key, so it covers every
Jazz node-root table: history, global and ahead current, rejected versions,
transactions, provenance, pending transactions, touched rows, catalogue and
the rest. The server account registry and catalogue-entry roots use raw
ordered KV, not Groove tables, and are unaffected.

## 2. One nullable `base` enum in history

**Old form.** History rows ended with `base_seq` (nullable `U64`, 9 bytes even
when null, because a null fixed-width field reserves its width) and
`base_pending` (a separate nullable `{time, node}` record, 1 byte when null).

**New form.** One nullable `base` field of enum `jazz_write_base` with cases
`seq {seq: U64}`, `pending {pending: (U64, Uuid)}` and
`seq_and_pending {seq, pending}`. Costs: 1 byte when empty, 10 for a settled
seq, 26 for a pending predecessor, 34 for both. A sentinel encoding (seq 0 for
none) was rejected because `Some(0)` is a value that can reach storage and must
still be refused when its fate is decided. The enum uses an engine-owned
system registry identity (`SystemVariantRegistry::write_base`) so the physical
history table and every logical history projection share one shape across
schema renames.

## 3. u16 authored column ids

**Old form.** `authored_columns` was a nullable `Array<U64>` of physical column
ids: 8 bytes per authored column.

**New form.** A nullable `Array<U16>`: 2 bytes per column. A node allocates at
most 65534 physical columns; allocating the next one fails with
`InvalidCatalogueUpdate("more than 65534 physical columns per node are not
supported yet ...")`. `u16::MAX` stays the deletion marker. SPEC 10 states the
limit.

## Wire

All three changes are storage-only. Versions on the wire carry their own
encoding of base and authored columns, which is unchanged, and a stored value
never leaves the node. The wire fixtures (`wire_message_frames_v6.json`,
`large_json_wire_v1.json`) pass unchanged.

## Format marker

Jazz node roots compose Groove's `groove.key-free-rows.v1` into their storage
codec profile (`JAZZ_NODE_STORAGE_CODECS`), next to `groove.dictionary.v1` and
`groove.durable-index.v2`. A node root whose manifest lacks it stores full
records, and opening it with this build fails at manifest admission with
`UnsupportedStorageCodecs { missing: ["groove.key-free-rows.v1"] }` before any
row is decoded (`node_root_without_key_free_rows_is_refused` in
`crates/jazz/tests/storage_format_refusal.rs`; the browser root's required
codec list in `indexeddb-page-store.ts` carries it too). The node manifest
receipt moves accordingly (`root_profiles_have_pinned_manifest_bytes`). Server
catalogue and account roots use raw ordered KV and do not carry it.

## Upgrade path

None. alpha.60 is a coordinated breaking release: a node root written by an
earlier release is refused by the storage codec profile before any of these
decoders runs (it lacks `groove.dictionary.v1`, see
`tx-record-v2-storage-compatibility.md`, and `groove.key-free-rows.v1`). Roots
written by unreleased main builds with full stored records lack
`groove.key-free-rows.v1` and are refused the same way.

## Downgrade to alpha.59

As for the transaction record v2: alpha.59 refuses a root written by this
build at manifest admission, before decoding any row; it does not know
`groove.key-free-rows.v1`.

## Measured size

`crates/jazz/tests/storage_per_row_bytes.rs` (1000 todos, each inserted and
then updated in its own transaction; RocksDB raw key plus value bytes), main
`9ed3a362` against this branch:

| store                   | before | after | change |
| ----------------------- | -----: | ----: | -----: |
| Core (history complete) |    955 |   752 | -21.3% |
| Writer client           |    955 |   752 | -21.3% |
| Reader client           |    603 |   499 | -17.2% |

Average value bytes per entry (keys are unchanged):

| table                            | before | after | main savings                                                  |
| -------------------------------- | -----: | ----: | ------------------------------------------------------------- |
| `jazz_physical_2_history`        |    179 |   109 | key copies (46), u16 column ids, 1-byte empty `base`, offsets |
| `jazz_physical_2_global_current` |    149 |   117 | key copies (22), u16 column ids                               |
| `jazz_transactions`              |     49 |    33 | key copies (16)                                               |
