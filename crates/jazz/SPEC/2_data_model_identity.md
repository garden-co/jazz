# jazz — Specification · 2. Data model & identity

## Overview

This chapter defines the logical shape of jazz data: the schema model, the
identities that name durable objects, the layout of rows and row versions, and
the lowering from application schema to groove storage. It is limited to
_identity and shape_. Transaction semantics (ch. 3), history and merging
(ch. 4), reads (ch. 5), authorization (ch. 7), sync (ch. 8), schema evolution
(ch. 10), branches (ch. 11), and large-value op-logs (ch. 12) all build on the
names defined here, but their behavior is specified in those chapters.

Invariant digest:

- `INV-CLASS-1`: Column-class shipping principle: upstream-decided mutable state and node-local derived state MUST NOT be shipped as replicated row payload.
- `INV-DATA-1`: Stable wire identity fields MUST use the UUID newtypes (NodeUuid, RowUuid, SchemaVersionId, MigrationLensId, BranchId, AuthorId) in wire byte order; node-local alias t...
- `INV-DATA-2`: NodeAlias and SchemaVersionAlias MUST be node-local storage aliases allocated in jazznodes and jazzschemaversions; all egress from stored rows MUST resolve aliases bac...
- `INV-DATA-3`: AuthorId::SYSTEM MUST equal the UUIDv5 derivation Uuid::newv5(&Uuid::NAMESPACEOID, b"jazz:system-author").
- `INV-DATA-4`: TxTime MUST encode physical milliseconds in the high 48 bits and a logical counter in the low 16 bits; construction MUST reject values outside those packed ranges.
- `INV-DATA-5`: A TxId MUST identify a transaction as (time: TxTime, node: NodeUuid); stored transaction rows MUST use primary key (time, nodeid) where nodeid is the local alias for t...
- `INV-DATA-6`: SchemaVersionId MUST be UUIDv5 over JazzSchema::canonicalbytes() in namespace SCHEMAVERSIONNAMESPACE.
- `INV-DATA-7`: Canonical schema identity MUST change when a column's MergeStrategy changes.
- `INV-DATA-8`: Canonical schema identity MUST distinguish plain Bytes, LargeValueKind::Text, and LargeValueKind::Blob.
- `INV-DATA-9`: A declared MergeStrategy::Counter MUST be accepted only on non-nullable integer columns of type U8, U16, U32, or U64.
- `INV-DATA-10`: A declared MergeStrategy::Counter MUST NOT be used with a large-value column.
- `INV-DATA-11`: A merge strategy declaration MUST name an existing user column of the containing TableSchema.
- `INV-DATA-12`: A table read or write policy, when present, MUST name the table it is attached to and MUST validate against the complete JazzSchema.
- `INV-DATA-13`: ColumnSchema::text and ColumnSchema::blob MUST lower to nullable groove Bytes user cells in history storage.
- `INV-DATA-14`: jazz{table}history MUST have primary key (rowuuid, txtime, txnodeid), include schemaversion, parents, nullable user{col} cells, and have bytx(txtime, txnodeid, rowuuid).
- `INV-DATA-15`: jazz{table}register MUST have primary key (rowuuid, txtime, txnodeid), include schemaversion, parents, and non-null deletion, and have bytx(txtime, txnodeid, rowuuid).
- `INV-DATA-16`: The wire row descriptor for replicated row payloads MUST include only rowuuid, parents, nullable deletion, and nullable user{col} cells; receiver-local currentness and...
- `INV-DATA-17`: A stored row version MUST belong to exactly one layer: content versions in jazz{table}history with user cells, deletion-register versions in jazz{table}register with d...
- `INV-DATA-18`: Per-layer global-current tables MUST be keyed by rowuuid; content global-current MUST carry all user columns and index only references plus explicitly indexed columns.
- `INV-DATA-19`: jazzglobalchanges MUST be keyed by (tablename, rowuuid, layer, globalseq) and MUST expose index byglobalseq(globalseq, tablename, rowuuid, layer).
- `INV-DATA-20`: JazzSchema::lowertogroove() MUST include the fixed metadata tables, transaction/rejection tables, per-application-table rejected/history/register/global-current tables...

## Details

### 2.1 Column classes (the principle that drives sync)

Sync behavior is determined by what kind of state a stored column represents.
Every stored column belongs to exactly one of three classes, and that class
_mechanically_ determines how the value is shipped and who may write it. The
sync protocol (ch. 8) derives behavior from these classes rather than
special-casing individual columns:

- **replicated-immutable** — `row_uuid`, `tx_id`, `parents`, the user columns,
  `made_by`, read sets, snapshots. Written once by the author, never mutated.
- **upstream-decided mutable state** — `fate`, `global_seq`, `rejection_reason`
  (ch. 3). Written only by the fate authority, distributed as fate messages.
- **node-local derived state** — observed durability, local currency (computed by
  groove `arg_max_by`), and the core-written global-current rows / change
  streams. Recomputed or rewritten from accepted state on each node.

The load-bearing consequence is that only replicated-immutable columns are ever
shipped as row payload (`INV-CLASS-1`). Fate is shipped as fate messages, and
node-local derived state is never shipped.

These three classes cover stored _columns_. A `text`/`blob` column cell is an
ordinary replicated-immutable column (§2.3). Its large _content bytes_ are not a
fourth column class; they are an out-of-band **auxiliary content payload**,
carried on a separate content channel and stored in the raw `jazz_content`
store. Chapter 12 owns that content channel and defines the term "auxiliary
content payload".

### 2.2 Identity types

Cross-node identity is stable because every durable name is a wire-stable UUID
newtype (`ids.rs`): `NodeUuid`, `RowUuid`, `SchemaVersionId`,
`MigrationLensId`, `BranchId`, `AuthorId`, and
`TxId { time: TxTime, node: NodeUuid }`. Global ordering uses `GlobalSeq`
(ch. 3–4). A transaction id combines a packed hybrid logical clock (`TxTime`,
physical milliseconds plus a logical counter) with the writing node; the
transaction is identified and tie-broken by both values (`INV-DATA-5`). The
well-known `AuthorId::SYSTEM` is a fixed, content-derived id that passes all
policies (ch. 7, `INV-DATA-3`).

Storage may use compact local aliases without changing the wire identity model.
Each node interns `NodeUuid` and `SchemaVersionId` to local `u64` aliases
(`NodeAlias`, `SchemaVersionAlias`). The boundary is strict: aliases are
node-local, never appear on the wire, and every value leaving stored rows for
the wire resolves its alias back to the corresponding `NodeUuid` or
`SchemaVersionId` (`INV-DATA-1`, `INV-DATA-2`). Aliases are rebuilt on recovery.
The exact `TxTime` bit-packing and the `SYSTEM` literal are in §2.7.

### 2.3 Application schema

An application schema declares the logical tables, columns, references, access
policies, indexes, and merge behavior that jazz stores. In the reference model,
the schema is a `JazzSchema { tables: Vec<TableSchema> }`; each table carries
`name`, `columns`, `references`, `read_policy`, `write_policy`,
`indexed_columns`, and `merge_strategies`. User columns lower into storage under
a `user_` prefix. A missing nullable user cell means the row version did not set
that column.

Large-value columns are declared as `text` or `blob`
(`ColumnSchema::text`, `ColumnSchema::blob`). At this layer, they lower to
nullable groove `Bytes` cells; ch. 12 owns the op-log mechanics for their large
content bytes. The default merge strategy is column last-writer-wins by HLC
(`MergeStrategy::Lww`). The one implemented non-LWW strategy is a counter
(`MergeStrategy::Counter`), and it is constrained: it is accepted only on a
non-nullable integer column (`U8`/`U16`/`U32`/`U64`) and never on a large-value
column (`INV-DATA-9`, `INV-DATA-10`).

_Further invariants._ `INV-DATA-11` — a merge-strategy declaration names an
existing user column. `INV-DATA-12` — a table policy validates against the whole
schema. `INV-DATA-13` — `text`/`blob` columns lower to nullable groove `Bytes`
cells.

### 2.4 Schema identity is content-addressed

Schema identity is derived from schema content so independently observed copies
of the same storage shape name the same version, while any storage-shape change
names a different version. A `SchemaVersionId` is
`Uuid::new_v5(SCHEMA_VERSION_NAMESPACE, JazzSchema::canonical_bytes())`
(`INV-DATA-6`), domain-tagged `"jazz-schema-v0"`. The canonical bytes cover
sorted tables, names, columns in declared order, types, large-value kind, merge
strategy, and references. They deliberately do **not** include read/write
policies: policies are runtime/catalogue metadata attached to a storage schema
version, so publishing permissions for the same tables can refresh authorization
without creating a second physical storage partition. Changing any storage-shape
input yields a new `SchemaVersionId`. This content-addressing is what lets
multiple storage schema versions coexist (ch. 10).

_Further invariants._ `INV-DATA-7`, `INV-DATA-8` — `SchemaVersionId` changes when
a column's merge strategy changes, and when a column switches among `Bytes` /
`Text` / `Blob`.

### 2.5 Rows, versions, and layers

Rows have stable identity across history. A `RowUuid` names the logical row and
is shared by every historical version of that row. A **row version** is
identified by the row, the writing transaction, and the layer; versions form a
DAG through `parents` (ch. 4 specifies domination and merging). A stored version
belongs to exactly one layer (`INV-DATA-17`): _content_ versions live in
`jazz_{table}_history` and carry the user cells; _deletion-register_ versions
live in `jazz_{table}_register` and carry a non-null `_deletion` and no user
cells.

The replicated wire payload for a version (`VersionRecord`) is exactly the
replicated-immutable fields (§2.1): `row_uuid`, `parents`, a nullable
`_deletion`, and nullable `user_{col}` cells. Receiver-local currency and
authority-state columns are excluded (`INV-DATA-16`). Mixed-version _sync_ is
owned by ch. 8 / ch. 10.

### 2.6 Storage lowering

Storage lowering gives the logical schema a fixed groove representation
(`JazzSchema::lower_to_groove()`, `INV-DATA-20`). The lowered schema consists of
node/schema/catalogue/partition **metadata**, **transaction/audit** tables,
per-application-table **layer tables**, the append-only
**`jazz_global_changes`** change stream, and the raw **`jazz_content`** store
for large-value bytes (ch. 12). For each application table, the layers are
`jazz_{table}_history` for content versions and `jazz_{table}_register` for
deletion events, plus per-layer `…_global_current` winner tables. Those winner
tables are node-local derived state (§2.1): they are maintained from accepted
fates and never shipped. The exact table set, primary keys, and indexes are the
reference in §2.7.

### 2.7 Reference: identity encoding & storage lowering

This section is the normative identity and storage-format detail referenced by
§2.2 and §2.6. It is intended for implementers and for debugging
identity/storage formats.

**Identity encoding.** `TxTime` packs physical milliseconds in the high 48 bits
and a logical counter in the low 16; construction rejects values outside those
ranges (`INV-DATA-4`). `AuthorId::SYSTEM` is
`Uuid::new_v5(&NAMESPACE_OID, b"jazz:system-author")` (`= 93c209ee-…-c0bbcf6a`).
Node-local aliases live in `jazz_nodes` / `jazz_schema_versions` and are rebuilt
from those tables on recovery.

**Lowered tables.** `lower_to_groove()` produces:

- _metadata_ — `jazz_nodes`, `jazz_schema_versions`, `jazz_catalogue`,
  `jazz_catalogue_pointer`, `jazz_partitions`, and the branch-scaffolding
  `jazz_branches` / `jazz_branch_partitions` (behavior in ch. 11);
- _transaction/audit_ — `jazz_transactions` keyed `(time, node_id)`,
  `jazz_rejected_transactions`;
- _per application table_ — `jazz_{table}_history` and `jazz_{table}_register`,
  each PK `(row_uuid, tx_time, tx_node_id)` with index `by_tx(tx_time,
tx_node_id, row_uuid)`; history adds `schema_version` + `parents` + nullable
  `user_{col}`, register adds a non-null `_deletion` (`INV-DATA-14`,
  `INV-DATA-15`). Per-layer `…_global_current` winner tables are keyed by
  `row_uuid`; the content table carries all user columns and indexes only
  references plus explicitly indexed columns (`INV-DATA-18`);
- _change stream_ — the append-only `jazz_global_changes`, keyed
  `(table_name, row_uuid, layer, global_seq)` with index
  `by_global_seq(global_seq, table_name, row_uuid, layer)` (`INV-DATA-19`);
- _content_ — the raw ordered KV store `jazz_content` (ch. 12).

## Open Questions

### Open questions

- 🔶 **`jazz_nodes.uuid` uniqueness.** The README states the interned node UUID
  is unique, but `schema.rs::nodes_table` declares only the `id` primary key with
  no uniqueness constraint. Decide whether UUID uniqueness is a normative
  invariant with storage-level enforcement, or the README prose is stale.
- 🔶 **Mixed-version row descriptors.** Mixed-version sync is owned by ch. 8 /
  ch. 10; the implementation currently requires sender and receiver row
  descriptors to match exactly.
