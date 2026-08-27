# jazz — Specification · 2. Data model & identity

## Overview

This chapter defines the logical shape of jazz data: the schema model, the
identities that name durable objects, the layout of rows and row versions, and
the lowering from application schema to groove storage. It is limited to
_identity and shape_. Transaction semantics (ch. 3), history and merging
(ch. 4), reads (ch. 5), authorization (ch. 7), sync (ch. 8), schema evolution
(ch. 10), and branch views (ch. 11) all build on the
names defined here, but their behavior is specified in those chapters.

Invariant digest:

- `INV-CLASS-1`: Column-class shipping principle: upstream-decided mutable state and node-local derived state MUST NOT be shipped as replicated row payload.
- `INV-DATA-1`: Stable UUID wire identity fields MUST use the UUID newtypes (`NodeUuid`, `RowUuid`, `SchemaVersionId`, `MigrationLensId`) as exactly 16 canonical bytes, whose lexicographic byte order is their ordering; `AuthorSubject` MUST use its canonical `[iss,sub]` JSON string; node-local alias and intern types MUST NOT be part of wire identity.
- `INV-DATA-2`: `NodeAlias` and `SchemaVersionAlias` MUST be node-local storage aliases allocated in `jazz_nodes` and `jazz_schema_versions`; all egress from stored rows MUST resolve aliases back to `NodeUuid` and `SchemaVersionId`.
- `INV-DATA-3`: `AuthorSubject::SYSTEM` MUST have the exact portable value `["urn:jazz:system","system"]`, and no authenticated user may claim the reserved system issuer.
- `INV-DATA-4`: `TxTime` MUST encode physical milliseconds in the high 46 bits and a logical counter in the low 18 bits. Its unsigned packed order is its canonical ordering. Its internal allocator MUST advance the physical component on logical exhaustion and return a typed overflow only after exhausting the final packed value.
- `INV-DATA-5`: A `TxId` MUST identify a transaction as `(time: TxTime, node: NodeUuid)`; stored transaction rows MUST use primary key `(time, node_id)` where `node_id` is the local alias for the wire `NodeUuid`.
- `INV-DATA-6`: `SchemaVersionId` MUST be UUIDv5 over `JazzSchema::canonical_bytes()` in namespace `SCHEMA_VERSION_NAMESPACE`.
- `INV-DATA-7`: Canonical schema identity MUST change when a column's `MergeStrategy` changes.
- `INV-DATA-9`: A declared `MergeStrategy::Counter` MUST be accepted only on non-nullable integer columns. Public `Integer` and `BigInt` columns lower to `I32` and `I64`; internal schemas may use `U8`, `U16`, `U32`, `U64`, `I32`, or `I64`.
- `INV-DATA-11`: A merge strategy declaration MUST name an existing user column of the containing `TableSchema`.
- `INV-DATA-12`: A table read or write policy, when present, MUST name the table it is attached to and MUST validate against the complete `JazzSchema`.
- `INV-DATA-14`: History storage MUST preserve each content version's row identity, transaction identity, schema identity, parent set, and user cells.
- `INV-DATA-15`: Deletion-register storage MUST preserve each deletion version's row identity, transaction identity, schema identity, parent set, and deletion event.
- `INV-DATA-16`: The wire row descriptor for replicated row payloads MUST include only `row_uuid`, `parents`, nullable `_deletion`, and nullable `user_{col}` cells; receiver-local currentness and authority-state columns MUST be excluded.
- `INV-DATA-17`: A stored row version MUST belong to exactly one physical layer: content with user cells or deletion-register state with `_deletion` and no user cells.
- `INV-DATA-18`: Derived global-current storage MUST identify the per-layer winner by row and preserve the content fields needed for global current reads.
- `INV-DATA-19`: The global change stream MUST retain enough table, row, layer, and sequence information to reconstruct global as-of reads.
- `INV-DATA-20`: Schema lowering MUST provide storage for metadata, transaction outcomes, row-version layers, globally accepted current state, and change history.
- `INV-DATA-21`: Deletion/register history MUST be one schema-independent immutable relation shared by every stable `PhysicalTableId`; its identity MUST include `(physical_table_id, branch_key, row_uuid, tx_time, tx_node_id)` so a row UUID never collides across logical tables or branch-key branch-local rows.
- `INV-DATA-22`: A per-lineage derived current row MUST carry the independently selected content winner and deletion winner/event, an explicit visibility bit, and projected content cells. It is node-local derived state, never replicated payload.

## Details

### 2.1 Column classes (the principle that drives sync)

Sync behavior is determined by what kind of state a stored column represents.
Every stored column belongs to exactly one of three classes, and that class
_mechanically_ determines how the value is shipped and who may write it. The
sync protocol (ch. 8) derives behavior from these classes rather than
special-casing individual columns:

- **replicated-immutable** — `row_uuid`, `tx_id`, `parents`, the user columns,
  `made_by`, read sets, snapshots. Written once by the author, never mutated.
- **upstream-decided mutable state** — `fate`, `global_time`, `rejection_reason`
  (ch. 3). Written only by the fate authority, distributed as fate messages.
- **node-local derived state** — observed durability, local currency (computed by
  groove `arg_max_by`), and the core-written global-current rows / change
  streams. Recomputed or rewritten from accepted state on each node.

The load-bearing consequence is that only replicated-immutable columns are ever
shipped as row payload (`INV-CLASS-1`). Fate is shipped as fate messages, and
node-local derived state is never shipped.

### 2.2 Identity types

Cross-node identity is stable because durable object names are wire-stable UUID
newtypes (`ids.rs`): `NodeUuid`, `RowUuid`, `SchemaVersionId`,
`MigrationLensId`, and
`TxId { time: TxTime, node: NodeUuid }`. Global settlement ordering uses the
distinct packed HLC newtype `GlobalTime` (ch. 3–4). A transaction id combines a
packed hybrid logical clock (`TxTime`,
physical milliseconds plus a logical counter) with the writing node; the
transaction is identified and tie-broken by both values (`INV-DATA-5`). The
An `AuthorSubject` is instead the exact canonical JSON string `[iss,sub]`; its
in-memory intern is never durable or portable. The well-known
`AuthorSubject::SYSTEM` string passes all policies (ch. 7, `INV-DATA-3`).

Storage may use compact local aliases without changing the wire identity model.
Each node interns `NodeUuid` and `SchemaVersionId` to local `u64` aliases
(`NodeAlias`, `SchemaVersionAlias`). The boundary is strict: aliases are
node-local, never appear on the wire, and every value leaving stored rows for
the wire resolves its alias back to the corresponding `NodeUuid` or
`SchemaVersionId` (`INV-DATA-1`, `INV-DATA-2`). Aliases are rebuilt on recovery.
The exact `TxTime` bit-packing and the `SYSTEM` literal are in §2.7. Alias
mappings are durable prerequisites: the mapping is atomically persisted before
any dependent row bytes, retained while any durable reference can reach it, and
is never guessed, reassigned, hashed, compared semantically, included in public
provenance, or sent across a node boundary. Missing, malformed, or colliding
mappings fail closed before decode or mutation. Different replicas may assign
different local aliases to the same global identity.

### 2.3 Application schema

An application schema declares the logical tables, columns, references, access
policies, indexes, and merge behavior that jazz stores. In the reference model,
the schema is a `JazzSchema { tables: Vec<TableSchema> }`; each table carries
`name`, `columns`, `references`, `read_policy`, `write_policy`,
`indexed_columns`, and `merge_strategies`. User columns lower into storage under
a `user_` prefix. A missing nullable user cell means the row version did not set
that column.

The default merge strategy is column last-writer-wins by HLC
(`MergeStrategy::Lww`). A counter declaration is accepted only on a non-nullable
integer column (`INV-DATA-9`). Public `Integer` and `BigInt` columns lower to
`I32` and `I64`; lower-level runtime schemas also support the unsigned fixed-width
integer representations.

**Implementation status.** The reference implementation currently provides
`MergeStrategy::Counter` as its non-LWW built-in strategy. Public-schema
conversion validates this constraint before installing the runtime schema.

_Further invariants._ `INV-DATA-11` — a merge-strategy declaration names an
existing user column. `INV-DATA-12` — a table policy validates against the whole
schema.

### 2.3.1 Ordinary-value baseline

`string` and `bytes` are ordinary column values with ordinary Jazz row history.
The current core has no specialized Text/Blob large-value type, edit API,
materialized value handle, content store, extent/chunk protocol traffic, or
large-value query source. Sync transports only ordinary commit, schema, query,
and subscription data. A future large-value design is tracked in
[#1757](https://github.com/garden-co/jazz/issues/1757); it must be introduced as
new semantics rather than inferred from this baseline.

### 2.4 Schema identity is content-addressed

Schema identity is derived from schema content so independently observed copies
of the same storage shape name the same version, while any storage-shape change
names a different version. A `SchemaVersionId` is
`Uuid::new_v5(SCHEMA_VERSION_NAMESPACE, JazzSchema::canonical_bytes())`
(`INV-DATA-6`), domain-tagged `"jazz-schema-v0"`. The canonical bytes cover
sorted tables, names, columns in declared order, types, merge
strategy, and references. They deliberately do **not** include read/write
policies: policies are runtime/catalogue metadata attached to a storage schema
version, so publishing permissions for the same tables can refresh authorization
without creating a second physical storage partition. Changing any storage-shape
input yields a new `SchemaVersionId`. This content-addressing is what lets
multiple storage schema versions coexist (ch. 10).

_Further invariants._ `INV-DATA-7` — `SchemaVersionId` changes when a column's
merge strategy changes.

### 2.5 Rows, versions, and layers

Rows have stable identity across history. A `RowUuid` names the logical row and
is shared by every historical version of that row. A **row version** is
identified by the row, the writing transaction, and the layer; versions form a
DAG through `parents` (ch. 4 specifies domination and merging). A stored version
belongs to exactly one layer (`INV-DATA-17`). Content versions live in the
resolved `PhysicalTableId`'s history table and carry user cells under their
`SchemaVersionAlias` descriptor. Deletion-register versions live in the one
schema-independent deletion history relation and carry a non-null `_deletion`
with no user cells. The record names the stable `PhysicalTableId` of its content
lineage, rather than a logical table name or schema version. `PhysicalTableId`
is allocated once when a table lineage is created and retained through
compatible schema changes, including table renames; a dropped lineage is never
silently reused. Its full branch-local row identity also contains the canonical
branch key declared by ch. 11, so content and deletion events for one
application object in different tuples remain independent. This permits exactly
one sparse immutable deletion history across the database without cross-table or
cross-branch-key row-UUID collisions (`INV-DATA-21`).

The replicated wire payload for a version (`VersionRecord`) is exactly the
replicated-immutable fields (§2.1): `row_uuid`, `parents`, a nullable
`_deletion`, and nullable `user_{col}` cells. Receiver-local currency and
authority-state columns are excluded (`INV-DATA-16`). Mixed-version _sync_ is
owned by ch. 8 / ch. 10.

**Implementation status.** The reference codec currently requires sender and
receiver row descriptors to match exactly. Compatibility across differing
descriptors is not yet a settled contract; see the open question below.

### 2.6 Storage lowering

Storage lowering gives catalogue and system state a fixed Groove representation
(`JazzSchema::lower_to_groove()`, `INV-DATA-20`). Durable schema-version mappings
then add one set of application layer tables per `PhysicalTableId`, with Groove
schema variants selected by `SchemaVersionAlias`. The fixed schema includes
node/schema/catalogue metadata, transaction/audit tables, the append-only
`jazz_global_changes` stream, and maintained-query state. The exact table set,
primary keys, and indexes are the
reference in §2.7.

### 2.7 Reference implementation: identity encoding & storage lowering

This section records the current reference implementation's identity and storage
layout. It is useful for implementers and debugging, but exact table names,
primary keys, and indexes are not the portable data-model contract. The layout
is covered by `schema::storage_lowering_declares_system_columns_by_shape`.

**Identity encoding.** Every UUID identity is exactly its 16 canonical bytes;
lexicographic byte order is the physical and semantic order. `TxTime` and the
authority-assigned `GlobalTime` use the same packed HLC representation: physical milliseconds in the high 46 bits
and a logical counter in the low 18. It can represent Unix milliseconds through
approximately year 4200 and 262,144 ordered positions per millisecond. On
logical exhaustion it advances physical time by one millisecond; only the final
packed position returns a typed clock-overflow (`INV-DATA-4`). Their unsigned
packed order is the canonical storage order. `TxTime` remains
an opaque ordering/version field: public row provenance exposes only physical
Unix milliseconds. UUID object identities retain their newtype encodings;
`AuthorSubject::SYSTEM` is the canonical JSON string
`["urn:jazz:system","system"]`, and authenticated author subjects are exact
canonical `[iss,sub]` JSON strings. Node-local aliases live in `jazz_nodes` /
`jazz_schema_versions` and are rebuilt from those tables on recovery.

### 2.7.1 Settled history layout and canonical receipts

The authoritative identity of one immutable row version is exactly
`(PhysicalTableId, BranchKey, RowUuid, Layer, TxId)`. `Layer` is either content
or deletion; it is part of the identity even though a deletion is physically
stored in the shared deletion table. `PhysicalTableId` is a local storage
coordinate only, while `BranchKey`, `RowUuid`, and `TxId` retain their canonical
portable meanings. A parent is only a `TxId` within that same enclosing
table/branch/row/layer coordinate. Parent lists are strictly sorted and unique;
cross-coordinate parents are rejected rather than inferred or re-routed.

`BranchKey` bytes use the frozen engine-owned codec in SPEC 11: a versioned,
length-delimited, strictly increasing `(column name, canonical typed value
bytes)` sequence, with no duplicate or empty names and no trailing or alternate
bytes accepted on decode. The typed value envelope and Groove payload must each
round-trip canonically. This is the only settled branch-key representation used
in history primary keys, shared deletion keys, current/index prefixes, reopen,
and rebuild; legacy serde/postcard shapes are never guessed.

Accepted transactions, immutable version rows, and their atomically persisted
fate/durability are authority state. Current winners, visibility rows, global
change/index rows, and materialized views are derived accelerators: they are
discardable and must rebuild from accepted immutable history. Rejected foreign
versions never enter that history; an origin's retry payload belongs to a
separate local retry store. Tests pin the canonical branch bytes and rejection
of unordered, duplicate, and trailing forms in
`protocol::tests::branch_key_canonical_bytes_are_exact_and_reject_noncanonical_forms`;
branch-view, deletion, concurrent-winner, and recovery suites are the
accepted/reopen/rebuild receipts for the same coordinate.

**Lowered tables.** `lower_to_groove()` produces:

- _metadata_ — `jazz_nodes`, `jazz_schema_versions` (including durable physical
  and branch mappings), `jazz_catalogue`, and
  `jazz_catalogue_pointer`;
- _transaction/audit_ — `jazz_transactions` keyed `(time, node_id)`,
  `jazz_rejected_transactions`;
- _per physical lineage_ — `jazz_physical_{id}_history`, keyed by
  `(row_uuid, tx_time, tx_node_id)`, plus a per-lineage combined derived current
  row per exact branch key. The database has exactly one
  `jazz_deletion_history`, keyed by `(physical_table_id, branch_key,
row_uuid, tx_time, tx_node_id)` and with a seek/index prefix
  `(physical_table_id, branch_key, row_uuid)`. The
  deletion record's `schema_version` is retained provenance, not a storage
  partition. Content rows use `schema_version` as a Groove descriptor
  discriminator, while deletion history uses a stable system descriptor
  (`INV-DATA-14`, `INV-DATA-15`, `INV-DATA-18`, `INV-DATA-21`);
- _change stream_ — the append-only `jazz_global_changes`, keyed
  `(physical_table_id, row_uuid, layer, global_time)` with physical-table and
  global-time indexes (`INV-DATA-19`);

### 2.14 Subsumed table-first row-history notes

The former top-level row-history and row-history-engine notes are now part of
this chapter's model instead of a separate alpha archive. A logical row is
identified by `RowUuid`; application columns and engine-managed metadata are
stored in one flat row/version record; and current reads are served from compact
visible/current state rather than by scanning history. Retained history remains
the source for replay, sync, deletion/restore, and historical reads, while the
visible/current surfaces are derived indexes over that history.

The old alpha wording around reserved `_jazz_*` fields maps to the typed identity
and column-class model here: stable row identity, transaction identity, authorship
or `made_by`, parents, deletion state, durability/fate observations, branch key or
schema context, and implementation metadata are engine-owned facts, not
application columns. Catalogue rows stay on the schema/lens catalogue lane
(ch. 10); they are not ordinary user rows even though they reuse the same storage
and sync machinery.

## Open Questions

- 🔶 [#1758](https://github.com/garden-co/jazz/issues/1758) — Canonical authorship and node identity.
- 🔶 [#1777](https://github.com/garden-co/jazz/issues/1777) — Mixed-version descriptors and visible-row encoding.
