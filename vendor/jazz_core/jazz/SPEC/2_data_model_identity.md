# jazz — Specification · 2. Data model & identity

This chapter defines the logical shape of jazz data: the schema model, the
identities that name durable objects, the layout of rows and row versions, and
the lowering from application schema to groove storage. It is limited to
_identity and shape_. Transaction semantics (ch. 3), history and merging
(ch. 4), reads (ch. 5), authorization (ch. 7), sync (ch. 8), schema evolution
(ch. 10), branches (ch. 11), and large-value op-logs (ch. 12) all build on the
names defined here, but their behavior is specified in those chapters.

## 2.1 Column classes (the principle that drives sync)

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

## 2.2 Identity types

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

## 2.3 Application schema

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

## 2.4 Schema identity is content-addressed

Schema identity is derived from schema content so independently observed copies
of the same schema name the same version, while any semantic shape change names
a different version. A `SchemaVersionId` is
`Uuid::new_v5(SCHEMA_VERSION_NAMESPACE, JazzSchema::canonical_bytes())`
(`INV-DATA-6`), domain-tagged `"jazz-schema-v0"`. The canonical bytes cover
sorted tables, names, columns in declared order, types, large-value kind, merge
strategy, references, and read/write policy. Changing any of those inputs yields
a new `SchemaVersionId`. This content-addressing is what lets multiple schema
versions coexist (ch. 10).

_Further invariants._ `INV-DATA-7`, `INV-DATA-8` — `SchemaVersionId` changes when
a column's merge strategy changes, and when a column switches among `Bytes` /
`Text` / `Blob`.

## 2.5 Rows, versions, and layers

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

## 2.6 Storage lowering

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

## 2.7 Reference: identity encoding & storage lowering

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

## Open questions

- 🔶 **`jazz_nodes.uuid` uniqueness.** The README states the interned node UUID
  is unique, but `schema.rs::nodes_table` declares only the `id` primary key with
  no uniqueness constraint. Decide whether UUID uniqueness is a normative
  invariant with storage-level enforcement, or the README prose is stale.
- 🔶 **Mixed-version row descriptors.** Mixed-version sync is owned by ch. 8 /
  ch. 10; the implementation currently requires sender and receiver row
  descriptors to match exactly.
