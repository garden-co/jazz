# jazz — Specification · 10. Schema evolution: lenses & migrations

## Overview

Multiple schema versions coexist in one database, and migration lenses translate
between them without rewriting history. This is one of jazz's most novel
properties. This chapter defines the catalogue, shared physical storage,
authored-schema writes, and lens-projected reads. It builds on schema
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
- `INV-LENS-8`: Durable catalogue schemas, lenses, current-write pointer, schema-version aliases, and physical mappings MUST survive node restart; installing an authority snapshot MUST preserve the node-local storage identity of an already-open schema so pre-snapshot local writes remain addressable.
- `INV-LENS-9`: Publishing a non-genesis schema and its lineage-defining lens MUST durably stage the complete ordered bundle, keep it invisible while every physical table and schema variant is registered, then durably activate it before acknowledging or draining parked work; reopen MUST resume staged activation idempotently.
- `INV-LENS-10`: New local writes MUST retain `current_write_schema.schema` as their schema discriminator and resolve storage through that schema's durable physical mapping.
- `INV-LENS-11`: Incoming commit units MUST retain their authored schema discriminator and resolve storage through that schema's durable physical mapping, even when the current write pointer names another schema.
- `INV-LENS-12`: Natural lens reads MUST select winners from the shared physical lineage before projecting rows into the requested schema.
- `INV-LENS-13`: Natural lens projection MUST apply supported operations deterministically in both directions and MUST reject unsupported transformations.
- `INV-LENS-14`: For every non-rejected natural lens delta sequence, translating then applying MUST equal applying then translating for all known schema materializations.
- `INV-LENS-15`: `ShapeId` MUST include the authored `SchemaVersionId`; identical canonical query bytes against different schema versions MUST produce different shape ids.
- `INV-LENS-16`: A commit unit MUST NOT be rejected or rewritten solely because its authored schema differs from the current write schema.
- `INV-LENS-17`: TransformColumn MUST be accepted only when its transform key is registered as bijective and canonical-equality-preserving.
- `INV-LENS-19`: Policy evaluation under lenses MUST translate data into the pinned permission evaluation schema and MUST NOT translate policy bundles.
- `INV-LENS-20`: Published physical lineages and authored schema variants MUST NOT be automatically garbage-collected.
- `INV-LENS-21`: A compatible table rename MUST retain its `PhysicalTableId`; deletion history and combined-current state therefore continue under that id without copying, rewriting, or rescanning unrelated lineages.
- `INV-LENS-23`: The `jazz_catalogue` bootstrap kernel uses only the fixed
  numeric record kinds `0..=7` described in §10.2. Unknown kinds fail closed
  during discovery and reopen. This freezes the kernel discriminator only; it
  does not yet freeze descriptor identity or the remaining catalogue payload
  encodings.
- `INV-LENS-22`: A content version's explicit authored-column presence MUST be stored only as a nullable, strictly increasing array of nonzero local `PhysicalColumnId`s; the exact authored schema/table mapping converts it to or from logical wire names, and malformed or unmapped ids MUST fail before any derived current row is persisted.

## Details

### 10.1 The model

Schema evolution is modeled as immutable catalogue data plus explicit
translations between versions. A `SchemaVersion` names one content-addressed
schema snapshot. A `MigrationLens` names a pure, deterministic translation
between two `SchemaVersionId`s, so old and new materializations can coexist while
presenting a coherent view to readers and writers. Incoming commits retain the
schema in which they were authored; translation happens when a read requests a
different schema, not while history is ingested.

Each lens is bidirectional: it defines behavior in **both** directions, forward
for old→new and backward for new→old. A direction may instead be declared as
`RejectSourceDelta`, which refuses that explicit projection rather than
producing a translated value. It does not reject a commit merely because its
authored schema differs from the write pointer (§10.4). Publishing a schema or a
lens **never rewrites existing history**. Rows remain in the version where they
were authored, and translation happens at read time (§10.5).

Identity is content-addressed. `SchemaVersionId = JazzSchema::version_id()`
(ch. 2), and `MigrationLensId = lens.content_id()`. The lens id hashes a
canonical byte encoding of the source id, target id, declared table lenses,
ordered lens ops, and recursively tagged default values. The embedded
`MigrationLens.id` field is excluded from that encoding, and catalogue ingest
rejects a mismatched id (`INV-LENS-1`, `INV-LENS-2`).

### 10.2 The catalogue

#### Epoch-pinned catalogue kernel

The first bytes needed to open a database cannot themselves depend on an
application descriptor. Jazz therefore freezes one deliberately tiny
catalogue kernel per storage epoch. It contains only the typed records needed
to discover immutable schema descriptors, migration lenses, local alias
mappings, staged/active lineage receipts, and the current-write pointer. Its
record kind is a permanent unsigned numeric discriminator, not a user-visible
string and not an extensible Rust enum: `genesis = 0`, `schema = 1`, `lens =
2`, `schema_lineage_staged = 3`, `schema_lineage_pending = 4`,
`schema_lineage_active = 5`, `write_pointer_pending = 6`, and
`bootstrap_ready = 7`. An unknown kernel kind, malformed field, duplicate
identity, or incomplete receipt is corruption and fails closed before decode,
activation, or mutation. Adding a kernel case requires a new storage epoch.

That exception is intentionally narrow. The catalogue does **not** become a
second place to hard-code Jazz internals: all other Jazz system tables live in
a reserved system namespace and are described, activated, and recovered by the
same descriptor machinery as application tables. This epoch slice freezes only
the closed `jazz_catalogue.kind` discriminator and its primary-key role. The
schema-descriptor identity model, physical table/column/enum identities,
storage-local mappings, and canonical payload codecs remain separate storage
settlement work; this section makes no claim about their final encoding or
whether declaration order and names participate in descriptor identity.

Schema evolution is coordinated through the catalogue, which serializes
publication and write-pointer changes under administrative authority. Catalogue
mutations travel as admin-gated
`SyncMessage::{PublishSchemaWithLens, PublishLens, SetCurrentWriteSchema}`
messages with `CatalogueAck` replies; a non-admin author is rejected
(`INV-LENS-3`). `AuthorSubject::SYSTEM` is the catalogue admin.

Exactly one database-wide catalogue sequencer assigns a dense monotone
`CatalogueSeq`. An arbitrary core or replica never assigns catalogue sequence;
edges forward authenticated, prevalidated requests to that sequencer. Catalogue
sequence is an administrative ordering domain, not a Jazz data transaction and
not branch-branch-local row causality. A receiver parks an envelope whose earlier catalogue
sequence or active source schema is missing. The same sequence with different
canonical content is fatal catalogue corruption, not first-arrival wins.
Validation happens before consuming sequence. If a sequenced operation must be
abandoned after assignment, the sequencer emits a replicated tombstone for that
slot so Active/tombstoned catalogue order remains dense.

The database lineage records one durable **genesis schema** at creation. Genesis
is not whichever schema a replica happens to supply at open: a joining replica
must install the durable genesis plus the ordered active catalogue chain before
accepting pointers or data. Genesis's local physical mapping is allocated during
database bootstrap and it is the only schema that has no lineage-defining
parent lens. Every other schema enters the
catalogue through one `PublishSchemaWithLens` bundle. The bundled lens MUST
target the bundled schema and source an already-admitted schema. The bundle
also carries exhaustive, explicit new-table and dropped-table declarations;
those declarations and the lens table endpoints MUST partition the source and
target table sets without duplicates or omissions. A standalone unknown
`PublishSchema` is invalid, and a later standalone `PublishLens` may add a
cross-lens but cannot redefine a schema's physical mapping.

The same ordered catalogue chain is part of a downstream view's reproducible
input closure (ch. 8 §8.4.1). A receiver must not treat a subscription's read
schema as an unproven local label: it parks canonical facts until it has the
active authored schema and every ordered lens needed to project to that read
schema. Catalogue sequence establishes the only permitted projection order;
receiving a later schema or a terminal cache does not authorize skipping a
missing predecessor.

Opening an existing database with a caller-supplied schema that disagrees with
its durable genesis is a hard bootstrap error. A joiner with no local lineage
installs the authority's genesis record, then replays the dense Active/tombstone
catalogue chain, then applies pointers and data; it never manufactures genesis
from its preferred client schema.

The pre-sequence request has its own content-addressed identity, distinct from both
`SchemaVersionId` and `MigrationLensId`. Its canonical digest covers catalogue
schema, lens, and the sorted exhaustive new/dropped table declarations, but not
the later assigned `CatalogueSeq`. The sequencer wraps that request in a
committed sequence envelope. An exact envelope replay is idempotent; an exact
request retry is deduplicated before assigning a second slot. A new
`PublishSchemaWithLens` request always rejects a target already reserved by an
earlier request. A separate `PublishLens` operation is the only way to add an
agreeing cross-lens. Reusing a request id, sequence, or target for different
canonical content fails before id allocation, registration, or durable mutation.

`CurrentWriteSchema` is the single moving write pointer. Updates are monotone by
`revision`, and a stale revision is acknowledged with `applied: false` without
changing the pointer (`INV-LENS-7`).

A commit unit or shape registration that names an unknown schema version cannot
be interpreted yet, so it **parks** as a catalogue orphan. The orphan drains
only after the complete schema-and-lineage bundle is durable and its Groove
variants are registered (`INV-LENS-5`, `INV-LENS-6`, ch. 8). There is no
partially-known or provisionally writeable schema state.

Current-pointer messages and child schema bundles whose dependencies are not
Active park durably across reopen. They retry after each activation, in
catalogue order; a transient missing dependency is not a terminal rejection and
never exposes the pointer early.

### 10.3 Shared physical storage

Physical storage preserves the version under which data was stored. Every stored
content/register row carries a `schema_version` ref, represented locally as a
node-local `SchemaVersionAlias` resolving to the wire `SchemaVersionId`.
Compatible schema versions share the `PhysicalTableId` established by their
published lenses while retaining distinct Groove descriptor variants
(`INV-LENS-4`, ch. 2).

Each physical lineage has one stable Groove field catalogue derived from its
`PhysicalColumnId`s. A content row carries its local `SchemaVersionAlias` as
its descriptor discriminator; the surrounding physical table and that
alias select the row's descriptor. The alias, its schema mapping, and the
descriptor registry are durable local storage state and are recovered before
any payload is decoded. They never appear in a public value or on the wire.
An alias or mapping remains retained while any retained history, current row,
branch-local row, snapshot, or rejected payload can name it.

Every content-history row also has nullable `authored_columns` metadata. When
present, its sole durable spelling is a Groove `Array<U64>` containing strictly
increasing, nonzero local `PhysicalColumnId`s. It is not JSON-in-bytes, a
logical-name payload, or an alternate serialized collection. The same typed
field is copied into derived ahead/global content-current carriers only after
resolving every id through the row's exact authored schema and logical table;
zero, noncanonical order, type mismatch, or an absent mapping fails closed
before the derived write. `None` is the deliberately conservative
legacy/lens-payload fallback: every present cell is treated as authored.

`VersionRecord` remains portable and carries logical authored column names.
Local authoring and incoming wire ingest map those names to the receiving
node's physical ids; exporting a stored row maps the ids back through its
stored schema/table mapping. A compatible `RenameColumn` therefore retains one
physical id while `v1.title` and `v2.name` remain their respective authored
wire names. This epoch intentionally does not accept the former JSON-in-bytes
storage spelling.

Contribution-merge provenance is likewise logical on the transaction/wire
surface, but its durable `jazz_transactions` coordinate is a standard Groove
record with nonzero `physical_table_id: U64` and a permanent component enum:
`column` has tag 0 and the one-field record `{ physical_column_id: U64 }`,
`operation` has tag 1 and `{ physical_column_id: U64, identity: Bytes }`, and
`register` has tag 2 and an empty record. The field order and enum tags are
durable. The storage boundary resolves a logical `(table, column)` to those
local ids when writing and resolves the ids back to the active logical
spellings when reading. Compatible table and column lens renames retain the
same ids; recovery consults retained mappings when the active spelling has
changed.

An operation identity is strategy-owned rather than opaque provenance:
`Counter` uses exactly an empty `identity`, while `GSet` uses exactly the
canonical one-field Groove record `{ element: <the declared array element
type> }`. The receiving node validates the table/column ownership, content
layer, merge strategy, enum tag, payload shape, and canonical identity bytes
at remote admission and again while reopening durable state, before any
derived mutation or resident state. Zero, unknown, ambiguous, malformed,
trailing, or noncanonical contribution payloads fail closed. This is local
storage identity only: API and wire records never expose physical ids or a
private postcard contribution encoding.

Jazz registers a schema variant and every projection needed for its logical
views before activating a catalogue bundle or accepting a row under that
alias. Variant registration is append-only: extending a lineage with a new
descriptor or projection case neither changes the identity or output
descriptor of an existing lowered plan nor resets its active subscriptions.
Each projection case either emits the declared logical row or deliberately
`Ignore`s a variant that cannot supply the projection; an unregistered variant
is a configuration error. This is the only boundary at which opaque physical
variant rows become logical Jazz rows (ch. 14).

Physical secondary indexes are append-only for a lineage. A schema may use an
index only when it declares the corresponding logical index, but dropping that
declaration does not remove the physical index. Adding an index for an
existing physical column registers and backfills it across retained variants
before the schema bundle becomes Active. A variant missing an indexed field
contributes no entry; later compatible variants extend the same physical index
rather than creating a schema-version partition.

Publishing a non-genesis schema first derives its complete physical mapping
from the bundled lineage lens: compatible unchanged/renamed tables and columns
reuse source physical ids; added tables, added/copied columns, and incompatible
column epochs receive fresh ids; dropped entities simply have no target logical
mapping. Schema, lens, local alias, mapping, declarations, and catalogue
sequence are first committed durably with state `Staged`. Staged definitions
are invisible to schema APIs, write-pointer updates, writes, shapes, and commit
admission. Jazz then idempotently builds or rebuilds all physical tables,
indexes, row-layout variants, and projection cases. Only after that succeeds
does a second durable catalogue transaction mark the bundle `Active`. Reopen
resumes activation of staged bundles deterministically before exposing the
node. Only `Active` causes acknowledgement or parked-work drainage
(`INV-LENS-9`). IDs allocated by a staged bundle are never reused, including
after failed activation; this makes retry/reopen registration safe without
rollback exposure. The legacy logical `(table, schema-version)` registry
`jazz_partitions` no longer exists; durable `jazz_schema_versions` mappings are
the complete reopen input.

Once activation begins, any registration or Active-marker failure puts the node
in a fail-stop catalogue state: it must not continue serving against the
temporarily installed in-memory schema. Reopen resumes the durable Staged
bundle idempotently and either reaches Active or fails closed again.

Admission validates the entire logical bundle before staging: related source/target
table endpoints are unique and exhaustive with the explicit new/dropped sets;
the ordered ops reproduce each target descriptor exactly; `RenameTable`
payloads agree with their enclosing endpoints; no rename/copy/add collision is
ambiguous; and physical epochs are reused only when representation and merge
semantics are compatible. A publication has no schema-layer byte cap: generic
transport fragmentation carries it atomically across bounded physical frames.
Structural declaration-count, name-length, and operation-count limits are
checked before Groove registration. The initial named limits are
`MAX_SCHEMA_LINEAGE_DECLARATIONS = 4096`,
`MAX_SCHEMA_LINEAGE_NAME_BYTES = 1024`, and
`MAX_SCHEMA_LINEAGE_OPS = 16384`; changing them is a protocol compatibility
decision, not an unreviewed implementation tweak.
Catalogue admin authority comes from the authenticated transport/session
context; the serialized `author` field is provenance and cannot let a forged
client self-declare `SYSTEM` authority.

_Further invariants._ `INV-LENS-8` — durable catalogue schemas, lenses, the
current-write pointer, aliases, and physical mappings survive node restart and
are recovered before the full Groove database is constructed. Aliases and
physical ids are node-local: when a client has already opened and written under
a schema before receiving an authority snapshot, snapshot planning preserves
that schema's local storage identity and reconciles its ancestors and descendants
around the local anchor. Pending local rows therefore remain addressable.

### 10.4 Writes: authored schema variants

New local writes carry the schema selected by `current_write_schema` as their
Groove discriminator and resolve their table and columns through that schema's
durable physical mapping (`INV-LENS-10`).

Incoming work retains the schema under which it was authored, regardless of the
current write pointer (`INV-LENS-11`). Before ingest, Jazz verifies that every
known authored schema has a local alias, physical mapping, and registered Groove
descriptor variant. A schema-and-lineage bundle normally installs those facts
atomically before the schema becomes Active; the ingest preflight also closes
the provisional catalogue compatibility path. An unknown schema still parks
until its complete lineage arrives (`INV-LENS-5`, `INV-LENS-9`).

Compatible schema versions may share one `PhysicalTableId`, but the stored
schema discriminator remains the authored variant. Ingest does not require a
lens projection, and a pointer mismatch alone is never a rejection or rewrite
condition (`INV-LENS-16`). A `RejectSourceDelta` declaration affects an explicit
projection through that lens, not admission of history authored on either side.

The same physical id is also the deletion-history routing key. A compatible
`RenameTable` preserves it, so old and new logical names resolve to the same
sparse deletion events and combined current row. Adding or incompatibly
replacing a table allocates a new id and cannot observe old deletion events even
if a caller reuses a `RowUuid`; dropped ids remain retained for history and are
never reassigned (`INV-LENS-21`).

A current-write-pointer flip is a core-ordered, monotone catalogue write
(§10.2) and **never invalidates in-flight work**. The pointer selects the schema
for new local authoring; it does not redirect commits another client already
authored under a different Active schema.

### 10.5 Reads: select, then project

Reads begin from storage reality, then project into the requested schema. A read
against schema S resolves its `PhysicalTableId`, selects the combined current
row (or independently selected historical winners at a fixed cut) from that
shared lineage by the **schema-agnostic `(tx_time, node)` ordering first**, and
only then translates the winning cells into S (`INV-LENS-12`, ch. 4).

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
descriptor variant. After the write pointer moves, new local writes use the
`v2` discriminator. An older client's later `v1`-authored commit still uses the
`v1` descriptor variant in that shared lineage (`INV-LENS-11`); neither it nor
the original row is rewritten. A `v2` read scans the physical lineage, selects
the winner first in the authored lineage, and projects the winning authored
variant into `v2`, including when the winner comes from a settled subscription
cache (`INV-LENS-12`). Projection never lets two differently authored versions
compete as `v2` rows, and a receiver may not use an unrelated local `v2` row to
fill a missing `v1` winner witness.

Concretely, suppose `v1.users { id, name, email }` becomes
`v2.people { id, name, email_address }` through an ordered `RenameTable(users,
people)` plus `RenameColumn(email, email_address)` lens. Alice's immutable
`v1` version for `users/u1` remains a `v1`-encoded `VersionRecord`. For Bob's
`v2.people` subscription, the authority sends that authored record with its
`users`, `u1`, `v1`, transaction, branch key, and source identity unchanged, plus
the ordered catalogue/lens closure and any safe membership witness. Bob decodes
the bytes as `v1.users`, applies the two lens operations, then feeds the logical
`v2.people { id: u1, name, email_address }` fact into the local IVM. Bob's
terminal may render a selected `{ name, email_address }` app row, but that row
is not sent as a replacement for Alice's authored version; dropping it and
rerunning the local IVM produces the same result (ch. 8 §8.4.1).

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

**The core only ever receives resolved lenses**: a draft lens, such as an ambiguous diff where a
drop+add might be a rename, is a product/tooling concept, and the validation tool
refuses unresolved drafts upstream.

Database-local `PhysicalTableId` and `PhysicalColumnId` values never cross
API/wire boundaries; local resolution happens only at the storage boundary.

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

- 🔶 [#1779](https://github.com/garden-co/jazz/issues/1779) — Lens/catalogue lifecycle, validation, administration, and schema projection.

### Detailed issue context

- **Binding-facing lens facade.** TS/WASM/NAPI should expose published
  schemas, migration lenses, current-write-schema movement, and catalogue acks
  as stable facade operations rather than leaking branch-key storage details. The
  ABI should use opaque `SchemaVersionId`/`MigrationLensId` bytes plus structured
  validation errors and deterministic golden fixtures for natural lens behavior.
- **Catalogue admin set.** `AuthorSubject::SYSTEM` is the catalogue admin; the
  implementation has no broader admin set.
- **Policy pin movement validation.** Schema versions/lenses may be published
  ahead of the permission-evaluation pin moving, but policy stays on the pinned
  schema until the admin moves the pin — at which point the new current schema
  must have a valid bundle, and a lens that drops a column referenced by the
  active bundle is rejected at publish (same family as the
  missing-backwards-default check).
- **Explicit schema-version GC.** `INV-LENS-20` forbids automatic deletion of
  published physical lineages or authored variants. If explicit GC is ever added, what completeness,
  branch-key/history, lens, and audit evidence must authorize it?
- **Multiple-parent schema lineage.** Initial admission has exactly one
  lineage-defining parent. Later cross-lenses may add translation paths but may
  not change physical placement. If a future schema must inherit physical
  identities from multiple independently evolved parents, define an atomic
  multi-parent lineage proof rather than making arrival order authoritative.
- **Catalogue sequence unification.** Schema-lineage activation requires an
  authoritative database-wide monotone catalogue sequence. Current-write
  pointers and later ordered catalogue mutations should enter the same sequence;
  the remaining work is migrating the pointer API from its legacy independent
  revision field to the common committed envelope.
- **`RenameTable` payload.** `RenameTable`'s payload is ignored in favor of
  `TableLens` source/target during evaluation. Decide whether the op should be
  removed or the redundant payload should be validated.
- **Catalogue as a separate lane.** The design distributes the catalogue on a
  lane beside read/write sync; the protocol has the message variants but no
  separate-lane enforcement (ch. 8).
- **Schema-projected source nodes.** Some projected historical/current reads
  still materialize rows inside the source resolver. Decide the
  first-class lowered source-node surface for schema/lens projections so
  projected sources compose with the normal query graph instead of staying as an
  inline resolver path.
- **Shared core-supported schema gate.** Keep CLI publish, dev server schema
  load, native/WASM runtime open, and server conversion on one validator until
  the public schema vocabulary and executable core support converge.
- **Dynamic defaults.** Literal defaults are write-origin expansion; dynamic
  defaults need a deterministic time/source rule and policy ordering before they
  can enter the executable subset.
- **Column metadata.** Arbitrary column metadata can help generated UIs, but
  must be versioned, preserved through lenses, and kept separate from executable
  policy/planner semantics unless explicitly promoted.
- **Lens hardening.** Preserve hidden newer fields under old-client writes,
  make lens-path selection ambiguity-aware, allow corrected or asymmetric
  migrations where safe, and define type-changing migrations.
- **Authored-column presence through lenses.** `INV-HIST-8` distinguishes an
  explicitly authored unchanged cell from materialized inherited context, while
  `INV-LENS-11`/`INV-LENS-14` require old-schema deltas to translate through
  lens paths. Define how authored presence propagates through `RenameColumn`,
  `CopyColumn`, `AddColumn`, `DropColumn`, and `TransformColumn`. Until that
  contract exists, a lens-translated version deliberately marks presence
  unavailable and conservatively treats every present translated payload cell
  as authored; this is not a claim of per-column LWW fidelity across a lens.
