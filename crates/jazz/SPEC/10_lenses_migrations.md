# jazz — Specification · 10. Schema evolution: lenses & migrations

## Overview

Multiple schema versions coexist in one database, and migration lenses translate
between them without rewriting history. This is one of jazz's most novel
properties. This chapter defines the catalogue, shared physical storage,
copy-on-write-into-current writes, and lens-projected reads. It builds on schema
identity (ch. 2), history winner selection (ch. 4), and the catalogue sync lane
(ch. 8).

Invariant digest:

- `INV-LENS-1`: A published `SchemaVersion` MUST have `schema.id == schema.schema.version_id()`; every non-genesis schema MUST be admitted in one catalogue operation with its lineage-defining lens before it is known or writeable.
- `INV-LENS-2`: A published `MigrationLens` MUST have `lens.id == lens.content_id()` and both `lens.source` and `lens.target` MUST be known `SchemaVersionId`s; `content_id()` MUST hash the canonical lens payload and exclude the embedded id field.
- `INV-LENS-3`: Catalogue mutation messages MUST be accepted only from catalogue admin identity and MUST reject non-admin authors.
- `INV-LENS-4`: Every stored content/register history row MUST carry a schema-version alias, and every wire `VersionRecord` MUST expose the full `SchemaVersionId`.
- `INV-LENS-5`: Unknown-schema commit units MUST park without ingesting a transaction and MUST drain when the corresponding `SchemaVersion` catalogue value arrives.
- `INV-LENS-6`: Unknown-schema shape registrations MUST park and MUST register only after the named schema-version catalogue value arrives.
- `INV-LENS-7`: `CurrentWriteSchema` updates MUST be monotone by `revision`; stale revisions MUST leave `current_write_schema` unchanged.
- `INV-LENS-8`: Durable catalogue schemas, lenses, current-write pointer, schema-version aliases, and physical mappings MUST survive node restart.
- `INV-LENS-9`: Publishing a non-genesis schema and its lineage-defining lens MUST atomically persist the schema, lens, alias, complete physical mapping, and explicit new/dropped-table declarations, then register every physical table and schema variant before acknowledging it or draining parked work.
- `INV-LENS-10`: New local writes MUST retain `current_write_schema.schema` as their schema discriminator and resolve storage through that schema's durable physical mapping.
- `INV-LENS-11`: Old-schema commit units with a forward lens path to the current write schema MUST be copied forward into the current schema variant at ingest.
- `INV-LENS-12`: Natural lens reads MUST select winners from the shared physical lineage before projecting rows into the requested schema.
- `INV-LENS-13`: Natural lens projection MUST apply supported operations deterministically in both directions and MUST reject unsupported transformations.
- `INV-LENS-14`: For every non-rejected natural lens delta sequence, translating then applying MUST equal applying then translating for all known schema materializations.
- `INV-LENS-15`: `ShapeId` MUST include the authored `SchemaVersionId`; identical canonical query bytes against different schema versions MUST produce different shape ids.
- `INV-LENS-16`: `RejectSourceDelta` on an old-to-current forward lens path MUST reject the source delta with the declared reason as a normal transaction rejection, not a protocol error.
- `INV-LENS-17`: TransformColumn MUST be accepted only when its transform key is registered as bijective and canonical-equality-preserving.
- `INV-LENS-18`: Large-value columns MAY be renamed by a lens but MUST NOT be content-transformed.
- `INV-LENS-19`: Policy evaluation under lenses MUST translate data into the pinned permission evaluation schema and MUST NOT translate policy bundles.
- `INV-LENS-20`: Published physical lineages and authored schema variants MUST NOT be automatically garbage-collected.

## Details

### 10.1 The model

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

### 10.2 The catalogue

Schema evolution is coordinated through the catalogue, which serializes
publication and write-pointer changes under administrative authority. Catalogue
mutations travel as admin-gated
`SyncMessage::{PublishSchemaWithLens, PublishLens, SetCurrentWriteSchema}`
messages with `CatalogueAck` replies; a non-admin author is rejected
(`INV-LENS-3`). `AuthorId::SYSTEM` is the catalogue admin.

The schema supplied when a database is created is its **genesis schema**. Its
local physical mapping is allocated during creation/reopen and it is the only
schema that has no lineage-defining parent lens. Every other schema enters the
catalogue through one `PublishSchemaWithLens` bundle. The bundled lens MUST
target the bundled schema and source an already-admitted schema. The bundle
also carries exhaustive, explicit new-table and dropped-table declarations;
those declarations and the lens table endpoints MUST partition the source and
target table sets without duplicates or omissions. A standalone unknown
`PublishSchema` is invalid, and a later standalone `PublishLens` may add a
cross-lens but cannot redefine a schema's physical mapping.

`CurrentWriteSchema` is the single moving write pointer. Updates are monotone by
`revision`, and a stale revision is acknowledged with `applied: false` without
changing the pointer (`INV-LENS-7`).

A commit unit or shape registration that names an unknown schema version cannot
be interpreted yet, so it **parks** as a catalogue orphan. The orphan drains
only after the complete schema-and-lineage bundle is durable and its Groove
variants are registered (`INV-LENS-5`, `INV-LENS-6`, ch. 8). There is no
partially-known or provisionally writeable schema state.

### 10.3 Shared physical storage

Physical storage preserves the version under which data was stored. Every stored
content/register row carries a `schema_version` ref, represented locally as a
node-local `SchemaVersionAlias` resolving to the wire `SchemaVersionId`.
Compatible schema versions share the `PhysicalTableId` established by their
published lenses while retaining distinct Groove descriptor variants
(`INV-LENS-4`, ch. 2).

Publishing a non-genesis schema first derives its complete physical mapping
from the bundled lineage lens: compatible unchanged/renamed tables and columns
reuse source physical ids; added tables, added/copied columns, and incompatible
column epochs receive fresh ids; dropped entities simply have no target logical
mapping. Schema, lens, local alias, mapping, and explicit new/dropped-table
declarations are committed in one storage batch. Jazz then registers all
physical tables, indexes, row-layout variants, and projection cases before
acknowledging the bundle or draining work parked on that schema
(`INV-LENS-9`). No row can therefore be written into storage whose identity is
later reconciled or discarded. The legacy logical `(table, schema-version)` registry
`jazz_partitions` no longer exists; durable `jazz_schema_versions` mappings are
the complete reopen input.

_Further invariants._ `INV-LENS-8` — durable catalogue schemas, lenses, the
current-write pointer, aliases, and physical mappings survive node restart and
are recovered before the full Groove database is constructed.

### 10.4 Writes: copy-on-write into current

Writes converge on the schema selected by the current write pointer. New local
writes carry that schema's local alias as their Groove discriminator and resolve
their table and columns through its durable physical mapping (`INV-LENS-10`).

Incoming work authored against an older schema is not stored under the old
variant. When a forward lens path exists, the commit unit is
**forward-translated into the current schema variant at ingest**
(`INV-LENS-11`). If the selected lens path declares `RejectSourceDelta`, the
old-schema delta is rejected as a normal `Fate::Rejected(reason)`, not as a
protocol error (`INV-LENS-16`).

The transaction records its author's schema version as **audit metadata with no
semantic role**. A current-write-pointer flip is a core-ordered, monotone
catalogue write (§10.2), and it **never invalidates in-flight work**: a
transaction admitted under the previous pointer translates forward at ingest
like any other old-schema write.

### 10.5 Reads: select, then project

Reads begin from storage reality, then project into the requested schema. A read
against schema S resolves its `PhysicalTableId`, selects content/deletion
winners from that shared lineage by the **schema-agnostic `(tx_time, node)`
ordering first**, and only then translates the winning cells into S
(`INV-LENS-12`, ch. 4).

Natural lens projection applies supported operations deterministically in both
directions and rejects unsupported transformations (`INV-LENS-13`). The shape's
`ShapeId` carries the authored `SchemaVersionId`,
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

**Worked example.** A row is first written under schema `v1`, landing in a
physical history lineage with `schema_version = v1`. Atomically publishing `v2`
with its `v1 ↔ v2` lineage lens maps `v2` to that lineage and registers its
descriptor variant. After the
write pointer moves, new writes use the `v2` discriminator, including an old
client's `v1`-authored commit after forward translation (`INV-LENS-11`). The
original row is not rewritten. A `v2` read scans the same physical lineage,
selects the winner first, and projects its authored variant into `v2`
(`INV-LENS-12`).

### 10.6 The lens op surface

The lens operation surface is deliberately small and resolved before it reaches
the core.

**Implementation status.** The current core supports `LensOp::{RenameTable,
RenameColumn, CopyColumn, AddColumn, DropColumn, TransformColumn,
RejectSourceDelta}`. Natural projection accepts `TransformColumn` only for a
registered transform that declares bijective, canonical-equality-preserving
semantics (`INV-LENS-17`). The current registry contains only the identity/no-op
keys `jazz.identity` and `identity`; `registered_transform_column_identity_is_accepted_and_projected`
and `transform_column_rejects_unregistered_transform_at_publish` cover that
surface. Additional transform keys are status work, not an invariant.

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
- 🔶 **Explicit schema-version GC.** `INV-LENS-20` forbids automatic deletion of
  published physical lineages or authored variants. If explicit GC is ever added, what completeness,
  branch/history, lens, and audit evidence must authorize it?
- 🔶 **Multiple-parent schema lineage.** Initial admission has exactly one
  lineage-defining parent. Later cross-lenses may add translation paths but may
  not change physical placement. If a future schema must inherit physical
  identities from multiple independently evolved parents, define an atomic
  multi-parent lineage proof rather than making arrival order authoritative.
- 🔶 **`RenameTable` payload.** `RenameTable`'s payload is ignored in favor of
  `TableLens` source/target during evaluation. Decide whether the op should be
  removed or the redundant payload should be validated.
- 🔶 **Catalogue as a separate lane.** The design distributes the catalogue on a
  lane beside read/write sync; the protocol has the message variants but no
  separate-lane enforcement (ch. 8).
- 🔶 **Schema-projected source nodes.** Some projected historical/current reads
  still materialize rows inside the source resolver. Decide the
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
