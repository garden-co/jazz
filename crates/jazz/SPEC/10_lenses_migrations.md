# jazz — Specification · 10. Schema evolution: lenses & migrations

Multiple schema versions coexist in one database, and migration lenses translate
between them without rewriting history. This is one of jazz's most novel
properties. This chapter defines the catalogue, per-version storage,
copy-on-write-into-current writes, and lens-projected reads. It builds on schema
identity (ch. 2), history winner selection (ch. 4), and the catalogue sync lane
(ch. 8).

## 10.1 The model

Schema evolution is modeled as immutable catalogue data plus explicit
translations between versions. A `SchemaVersion` names one content-addressed
schema snapshot. A `MigrationLens` names a pure, deterministic translation
between two `SchemaVersionId`s, so old and new materializations can coexist while
presenting a coherent view to readers and writers.

Each lens is bidirectional: it defines behavior in **both** directions, forward
for old→new and backward for new→old. A direction may instead be declared as
`RejectSourceDelta`, which refuses that translation as a normal transaction
rejection (§10.4), rather than producing a translated value. Publishing a schema
or a lens **never rewrites existing history**. Old rows remain in the version
where they were written, and translation happens either at read time or as
copy-on-write at ingest (§10.4–10.5).

Identity is content-addressed. `SchemaVersionId = JazzSchema::version_id()`
(ch. 2), and `MigrationLensId = lens.content_id()`. The lens id hashes a
canonical byte encoding of the source id, target id, declared table lenses,
ordered lens ops, and recursively tagged default values. The embedded
`MigrationLens.id` field is excluded from that encoding, and catalogue ingest
rejects a mismatched id (`INV-LENS-1`, `INV-LENS-2`).

## 10.2 The catalogue

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

## 10.3 Per-version storage

Physical storage preserves the version under which data was stored. Every stored
content/register row carries a `schema_version` ref, represented locally as a
node-local `SchemaVersionAlias` resolving to the wire `SchemaVersionId`, and the
row stays in the physical table for that version (`INV-LENS-4`, ch. 2).

The base schema uses the base table. Non-base versions live in suffixed tables
(`jazz_{table}_{schemaHash}_history` / `_register`), tracked in
`jazz_partitions`. When the current-write pointer flips to a schema with new
tables, those partition tables are created or reopened before any write or read
scan uses them (`INV-LENS-9`).

_Further invariants._ `INV-LENS-8` — durable catalogue schemas, lenses, the
current-write pointer, and per-version partitions survive node restart
(recovered in a catalogue stage before the groove database is constructed).

## 10.4 Writes: copy-on-write into current

Writes converge on the schema selected by the current write pointer. New local
writes are stored under `current_write_schema.schema`: the base table when that
schema equals the node's base schema, and a partition table otherwise
(`INV-LENS-10`).

Incoming work authored against an older schema is not appended to the old
partition. When a forward lens path exists, the commit unit is
**forward-translated into the current schema partition at ingest**
(`INV-LENS-11`). If the selected lens path declares `RejectSourceDelta`, the
old-schema delta is rejected as a normal `Fate::Rejected(reason)`, not as a
protocol error (`INV-LENS-16`).

The transaction records its author's schema version as **audit metadata with no
semantic role**. A current-write-pointer flip is a core-ordered, monotone
catalogue write (§10.2), and it **never invalidates in-flight work**: a
transaction admitted under the previous pointer translates forward at ingest
like any other old-schema write.

## 10.5 Reads: fan-out, then project

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
to `v2`, which creates the `v2` partition tables (`INV-LENS-9`). From then on,
_new_ writes land in the `v2` partition, including an old client's
`v1`-authored commit, which is forward-translated into the `v2` partition at
ingest if a forward lens path exists (`INV-LENS-11`). The original `v1` row is
**not** moved: old partitions stop receiving new rows once the pointer moves but
keep their existing historical rows. That is exactly why a read fans out: a read
against `v2` unions the `v1` table and the `v2` partition, picks the winner by
`(tx_time, node)` first, then projects the winning cells into `v2`
(`INV-LENS-12`). Writes are single-partition, using the current partition; reads
are multi-partition, spanning all partitions.

## 10.6 The lens op surface

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

## Open questions

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
