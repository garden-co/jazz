# jazz — Specification · 10. Schema evolution: lenses & migrations

## Overview

Multiple schema versions coexist in one database, and migration lenses translate
between them without rewriting history. This is one of jazz's most novel
properties. This chapter defines the catalogue, per-version storage,
authored-partition writes, and lens-projected reads. It builds on schema
identity (ch. 2), history winner selection (ch. 4), and the catalogue sync lane
(ch. 8).

Invariant digest:

- `INV-LENS-1`: A published SchemaVersion MUST have schema.id == schema.schema.versionid().
- `INV-LENS-2`: A published MigrationLens MUST have lens.id == lens.contentid() and both lens.source and lens.target MUST be known SchemaVersionIds; contentid() MUST hash the canonica...
- `INV-LENS-3`: Catalogue mutation messages MUST be accepted only from catalogue admin identity and MUST reject non-admin authors.
- `INV-LENS-4`: Every stored content/register history row MUST carry a schema-version alias, and every wire VersionRecord MUST expose the full SchemaVersionId.
- `INV-LENS-5`: Unknown-schema commit units MUST park without ingesting a transaction and MUST drain when the corresponding SchemaVersion catalogue value arrives.
- `INV-LENS-6`: Unknown-schema shape registrations MUST park and MUST register only after the named schema-version catalogue value arrives.
- `INV-LENS-7`: CurrentWriteSchema updates MUST be monotone by revision; stale revisions MUST leave currentwriteschema unchanged.
- `INV-LENS-8`: Durable catalogue schemas, lenses, current-write pointer, and per-version partitions MUST survive node restart.
- `INV-LENS-9`: A current-write-schema pointer flip or commit arrival for a known authored schema MUST create/reopen its per-version history and register storage tables before writes/read scans use them.
- `INV-LENS-10`: New local writes MUST store versions under currentwriteschema.schema, using the base table only when it equals the node's base schema and a partition table otherwise.
- `INV-LENS-11`: An incoming commit unit MUST be stored in the partition named by its authored schema, even when that schema is not the current write schema.
- `INV-LENS-12`: Natural lens reads MUST fan out across registered per-version tables and project rows into the requested schema after schema-agnostic winner selection.
- `INV-LENS-13`: Natural forward/reverse lens projection MUST implement RenameColumn, CopyColumn, AddColumn, and DropColumn.backwardsdefault deterministically, and MUST reject Transfor...
- `INV-LENS-14`: For every non-rejected natural lens delta sequence, translating then applying MUST equal applying then translating for all known schema materializations.
- `INV-LENS-15`: ShapeId MUST include the authored SchemaVersionId; identical canonical query bytes against different schema versions MUST produce different shape ids.
- `INV-LENS-16`: A commit unit MUST NOT be rejected or rewritten solely because its authored schema differs from the current write schema.
- `INV-LENS-17`: TransformColumn MUST be accepted only when its transform key is registered as bijective and canonical-equality-preserving; the current registry is identity/no-op only.
- `INV-LENS-18`: Large-value columns MAY be renamed by a lens but MUST NOT be content-transformed.
- `INV-LENS-19`: Policy evaluation under lenses MUST translate data into the pinned permission evaluation schema and MUST NOT translate policy bundles.
- `INV-LENS-20`: Per-version tables MUST NOT be automatically garbage-collected; background durable migration may compact current winners but MUST NOT delete historical tables automati...

## Details

### 10.1 The model

Schema evolution is modeled as immutable catalogue data plus explicit
translations between versions. A `SchemaVersion` names one content-addressed
schema snapshot. A `MigrationLens` names a pure, deterministic translation
between two `SchemaVersionId`s, so old and new materializations can coexist while
presenting a coherent view to readers and writers.

Each lens is bidirectional: it defines behavior in **both** directions, forward
for old→new and backward for new→old. A direction may instead be declared as
`RejectSourceDelta`, which refuses that explicit translation rather than
producing a translated value. It does not reject a commit merely because its
authored schema differs from the write pointer (§10.4). Publishing a schema or a
lens **never rewrites existing history**. Old rows remain in the version where
they were written, and translation happens at read time (§10.5).

Identity is content-addressed. `SchemaVersionId = JazzSchema::version_id()`
(ch. 2), and `MigrationLensId = lens.content_id()`. The lens id hashes a
canonical byte encoding of the source id, target id, declared table lenses,
ordered lens ops, and recursively tagged default values. The embedded
`MigrationLens.id` field is excluded from that encoding, and catalogue ingest
rejects a mismatched id (`INV-LENS-1`, `INV-LENS-2`).

### 10.2 The catalogue

Schema evolution is coordinated through the catalogue, which serializes
publication and write-pointer changes under administrative authority. Catalogue
mutations travel as admin-gated `SyncMessage::{PublishSchema, PublishLens,
SetCurrentWriteSchema}` messages with `CatalogueAck` replies; a non-admin author
is rejected (`INV-LENS-3`). `AuthorId::SYSTEM` is the catalogue admin.

`CurrentWriteSchema` is the single moving write pointer. Updates are monotone by
`revision`, and a stale revision is acknowledged with `applied: false` without
changing the pointer (`INV-LENS-7`).

A commit unit or shape registration that names an unknown schema version cannot
be interpreted yet, so it **parks** as a catalogue orphan. The orphan drains when
that `SchemaVersion` arrives (`INV-LENS-5`, `INV-LENS-6`, ch. 8).

### 10.3 Per-version storage

Physical storage preserves the version under which data was stored. Every stored
content/register row carries a `schema_version` ref, represented locally as a
node-local `SchemaVersionAlias` resolving to the wire `SchemaVersionId`, and the
row stays in the physical table for that version (`INV-LENS-4`, ch. 2).

The base schema uses the base table. Non-base versions live in suffixed tables
(`jazz_{table}_{schemaHash}_history` / `_register`), tracked in
`jazz_partitions`. A current-write-pointer flip to a schema with new tables, or
commit arrival for a known authored schema whose partition is absent, creates or
reopens those partition tables before any write or read scan uses them
(`INV-LENS-9`).

> **Implementation note (current):** Pointer-flip and commit-arrival
> provisioning both refresh the live Groove database layout while retaining
> in-memory parked commit units and shape registrations so a catalogue drain can
> continue after the refresh.

_Further invariants._ `INV-LENS-8` — durable catalogue schemas, lenses, the
current-write pointer, and per-version partitions survive node restart
(recovered in a catalogue stage before the groove database is constructed).

### 10.4 Writes: authored partitions

New local writes are authored under `current_write_schema.schema` and stored in
that schema's partition: the base table when it equals the node's base schema,
and a partition table otherwise (`INV-LENS-10`).

An incoming commit unit is stored in the partition named by its authored schema,
regardless of the current-write pointer (`INV-LENS-11`). If that known schema's
partition is absent, commit arrival provisions it before ingest. An unknown
schema still parks until its catalogue value arrives; draining that parked commit
then provisions the authored partition if necessary (`INV-LENS-5`,
`INV-LENS-9`). No lens inversion or forward translation is required to accept
such a write, and a pointer difference alone is never a rejection condition
(`INV-LENS-16`).

A current-write-pointer flip is a core-ordered, monotone catalogue write
(§10.2) and never invalidates in-flight work. The pointer selects the schema for
new local authoring; it does not redirect commits another client already
authored under a different valid schema.

### 10.5 Reads: fan-out, then project

Reads begin from storage reality, then project into the requested schema. A read
against schema S unions the visible-current rows from every registered
per-version table for the logical table, selects content/deletion winners by the
**schema-agnostic `(tx_time, node)` ordering first**, and only then translates
the winning cells into S (`INV-LENS-12`, ch. 4).

Natural lens projection implements `RenameColumn`, `CopyColumn`, `AddColumn`,
and `DropColumn.backwards_default` deterministically in both directions
(`INV-LENS-13`). The shape's `ShapeId` carries the authored `SchemaVersionId`,
so the same AST against two versions is two shapes (`INV-LENS-15`, ch. 6).

Merge strategies (ch. 4) consume candidate values **after** translation into the
reading schema. Because translation is deterministic, merge determinism is
preserved; counter deltas translate as values like any other column.

When multiple registered lens paths connect two schema versions, lens path
selection is deterministic over the schema-version graph. The chosen path is the
shortest path by lens count. Ties are broken by a stable ordering of candidate
endpoints and lens content ids; publication or storage iteration order must not
affect the chosen path. Schema updates are rare, so this is specified as a
clarity-first graph walk rather than a hot-path optimization.

RLS policy evaluation under lenses uses the permission-evaluation schema pinned
by the node/admin policy bundle. Row data is translated into that schema before
predicates are checked. The policy bundle itself is not lens-translated: column
renames, additions, and drops are applied to the data projection, and the pinned
policy AST is evaluated unchanged against that projection (`INV-LENS-19`).

The correctness contract (the oracle): for every non-rejected natural lens delta
sequence, **translate-then-apply equals apply-then-translate** across all known
schema materializations (`INV-LENS-14`).

**Worked example.** A row is first written under schema `v1`, landing in the
`v1` table with `schema_version = v1`. An admin flips the current-write pointer
to `v2`, which creates the `v2` partition tables (`INV-LENS-9`). New local `v2`
writes land in `v2`; an older client's later `v1`-authored commit remains in
`v1`, even though the pointer is `v2` (`INV-LENS-11`). Conversely, if a `v2`
client's commit arrives before the pointer flips and the `v2` partition does not
exist yet, arrival provisions that partition and stores the commit there. A read
against `v2` unions the `v1` table and the `v2` partition, picks the winner by
`(tx_time, node)` first, then projects the winning cells into `v2`
(`INV-LENS-12`). Writes are single-partition by authored schema; reads are
multi-partition, spanning all partitions.

### 10.6 The lens op surface

The lens operation surface is deliberately small and resolved before it reaches
the core. The supported operations are `LensOp::{RenameTable, RenameColumn,
CopyColumn, AddColumn, DropColumn, TransformColumn, RejectSourceDelta}`.

Natural projection accepts `TransformColumn` only when its transform key is
present in the built-in registry and declares bijective,
canonical-equality-preserving semantics (`INV-LENS-17`). The initial registry is
intentionally identity/no-op only (`jazz.identity` / `identity`), so
`TransformColumn` is currently a schema-documentation escape hatch rather than a
value-changing migration. Enum-by-variant-name and pinned-float transforms are
future append-only registry entries.

Large-value text/blob columns may be renamed, but `TransformColumn` over their
content is rejected at lens publication (`INV-LENS-18`). **The core only ever
receives resolved lenses**: a draft lens, such as an ambiguous diff where a
drop+add might be a rename, is a product/tooling concept, and the validation tool
refuses unresolved drafts upstream.

### 10.9 Subsumed schema-file and schema-subset notes

The former schema manager and schema file notes are folded into the catalogue
model here. Developer-authored `schema.ts`, `permissions.ts`, and migration/lens
modules are source material for immutable `SchemaVersion` and `MigrationLens`
catalogue entries. CLI validation, dev-server loading, runtime open, and server
conversion must share one executable-schema gate so a schema accepted by one
path is not later rejected by another.

Column defaults are schema metadata but execute at the write origin: omitted
defaulted fields become explicit cells in the committed payload before policy
dry-runs and before sealing. Literal defaults are in scope; dynamic defaults such
as `now()` require a deterministic authority/origin rule before they can be
accepted. Merge-strategy-only changes still change schema identity because they
change future merge behavior.

## Open Questions

### Open questions

- 🔶 **Binding-facing lens facade.** TS/WASM/NAPI should expose published
  schemas, migration lenses, current-write-schema movement, and catalogue acks
  as stable facade operations rather than leaking partition-table details. The
  ABI should use opaque `SchemaVersionId`/`MigrationLensId` bytes plus structured
  validation errors and deterministic golden fixtures for natural lens behavior.
- 🔶 **Catalogue admin set.** `AuthorId::SYSTEM` is the catalogue admin; the
  implementation has no broader admin set.
- 🔶 **Policy pin movement validation.** Schema versions/lenses may be published
  ahead of the permission-evaluation pin moving, but policy stays on the pinned
  schema until the admin moves the pin — at which point the new current schema
  must have a valid bundle, and a lens that drops a column referenced by the
  active bundle is rejected at publish (same family as the
  missing-backwards-default check).
- 🔶 **No auto-GC.** Per-version tables must never be auto-garbage-collected;
  background durable migration may compact current winners but never delete
  historical tables (`INV-LENS-20`). Not implemented.
- 🔶 **`RenameTable` payload.** `RenameTable`'s payload is ignored in favor of
  `TableLens` source/target during evaluation. Decide whether the op should be
  removed or the redundant payload should be validated.
- 🔶 **Catalogue as a separate lane.** The design distributes the catalogue on a
  lane beside read/write sync; the protocol has the message variants but no
  separate-lane enforcement (ch. 8).
- 🔶 **Schema-projected source nodes.** Projected historical/current reads still
  materialize version-partition rows inside the source resolver. Decide the
  first-class lowered source-node surface for schema/lens projections so
  projected sources compose with the normal query graph instead of staying as an
  inline resolver path.
- 🔶 **Shared core-supported schema gate.** Keep CLI publish, dev server schema
  load, native/WASM runtime open, and server conversion on one validator until
  the public schema vocabulary and executable core support converge.
- 🔶 **Dynamic defaults.** Literal defaults are write-origin expansion; dynamic
  defaults need a deterministic time/source rule and policy ordering before they
  can enter the executable subset.
- 🔶 **Column metadata.** Arbitrary column metadata can help generated UIs, but
  must be versioned, preserved through lenses, and kept separate from executable
  policy/planner semantics unless explicitly promoted.
- 🔶 **Lens hardening.** Preserve hidden newer fields under old-client writes,
  make lens-path selection ambiguity-aware, allow corrected or asymmetric
  migrations where safe, and define type-changing migrations.
- 🔶 **Schema version GC.** The current contract forbids automatic deletion of
  version partitions. If explicit GC is ever added, it needs completeness,
  branch/history, lens, and audit constraints.
