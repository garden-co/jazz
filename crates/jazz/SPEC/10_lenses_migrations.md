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
- `INV-LENS-2`: A published `MigrationLens` MUST have `lens.id == lens.content_id()` and both `lens.source` and `lens.target` MUST be known `SchemaVersionId`s; `content_id()` MUST hash the canonical lens payload, and the durable/wire form MUST reconstruct rather than store that derived id.
- `INV-LENS-3`: Catalogue mutation messages MUST be accepted only from catalogue admin identity and MUST reject non-admin authors.
- `INV-LENS-4`: Every stored content/register history row MUST carry a schema-version alias, and every wire `VersionRecord` MUST expose the full `SchemaVersionId`.
- `INV-LENS-5`: Unknown-schema commit units MUST park without ingesting a transaction and MUST drain when the corresponding `SchemaVersion` catalogue value arrives.
- `INV-LENS-6`: Unknown-schema shape registrations MUST park and MUST register only after the named schema-version catalogue value arrives.
- `INV-LENS-7`: `CurrentWriteSchema` updates MUST be monotone by `revision`; stale revisions MUST leave `current_write_schema` unchanged.
- `INV-LENS-8`: Durable catalogue schemas, lenses, current-write pointer, schema-version aliases, and physical mappings MUST survive node restart; installing an authority snapshot MUST preserve the node-local storage identity of an already-open schema so pre-snapshot local writes remain addressable.
- `INV-LENS-9`: Publishing a non-genesis schema and its lineage-defining lens MUST durably stage the complete ordered bundle, keep it invisible while every physical table and schema variant is registered, then durably activate it before acknowledging or draining parked work; one activation batch MUST replace its durable pending obligation with the schema, lens, physical mapping, and active receipt; reopen MUST resume staged activation idempotently.
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
- `INV-LENS-23`: A store MUST be reopened only by the storage format it was written in (`_jazz_format`); another format, an unstamped or damaged file, or a pre-SQLite store MUST be refused with a typed error, the file left unchanged.
- `INV-LENS-25`: The deploy catalogue MUST be the store's typed catalogue tables (§10.2), written in the same transaction as the admission and activation they describe. A change that fails leaves no rows; nothing is recovered from blobs or replayed on open; every permissions revision is kept.
- `INV-LENS-22`: A content version's explicit authored-column presence MUST be stored only as a nullable, strictly increasing array of nonzero local `PhysicalColumnId`s; the exact authored schema/table mapping converts it to or from logical wire names, and malformed or unmapped ids MUST fail before any derived current row is persisted.
- `INV-LENS-24`: A stored table or column name MUST never be reused for another logical column; only a compatible re-addition keeps its predecessor's identity (§10.7).

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
ordered lens ops, and recursively tagged default values. The durable catalogue
and postcard wire form contain that bounded canonical byte blob, not a separate
`MigrationLens.id` field; decoding reconstructs the id from the payload, making
a mismatched durable lens identity unrepresentable (`INV-LENS-1`, `INV-LENS-2`).

### 10.2 The catalogue

#### Catalogue tables (SQLite engine)

A store's catalogue is SQLite rows in the store file, beside the rows they
describe (`crates/jazz/layers/db/src/catalogue.rs` and `names.rs`). There is
no catalogue blob, snapshot or recovery log, and nothing is replayed on open:
what an open needs is read back from its own rows.

| Table                  | Key                     | Holds                                                                                                                                                                                      |
| ---------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `_jazz_versions`       | `n`                     | each admitted schema version, in admission order, and on a replica the names digest its authority confirmed (`confirmed`, §10.7)                                                           |
| `_jazz_names`          | `(n, tbl, col)`         | the stored table and column each version's names resolve to                                                                                                                                |
| `_jazz_stored_columns` | `(tbl, col)`            | each stored column's value type and backwards default                                                                                                                                      |
| `_jazz_lens_cells`     | `(tbl, col, since)`     | stored columns a lens fills in rows written by versions that do not name them                                                                                                              |
| `_jazz_schemas`        | public schema hash      | each structural schema (policies cleared) as canonical JSON, the version `n` it was admitted as (`NULL` until admitted) and `published_at`                                                 |
| `_jazz_lenses`         | `(source, target)` hash | each published migration's forward transform as canonical JSON, with `published_at`                                                                                                        |
| `_jazz_permissions`    | `revision`              | every permissions revision: the write schema it selects, its per-table policies (defaults omitted) as canonical JSON, `published_at`, and once activated `activated_seq` and `read_digest` |

Definitions are immutable and stored whole as canonical JSON text (object
keys sorted, no whitespace), so SQL can read them with `json_each`; decisions
about them (admission, publication time, activation) are real columns. The
current permissions revision is the highest; its parent is the one before it.
Every revision is kept.

`Db::apply_catalogue(CatalogueChange { put, admit, activate })` writes new
definition rows, admits versions with their lenses and makes a write schema
current at a revision in one savepoint: a change that fails leaves neither
rows nor in-memory state behind. A server deploy computes that change from
the stored rows (`prepare_catalogue_change`) and applies it in one
transaction, so a deploy is never half written. Admitted versions are
immutable: a deploy that gives an admitted version a new incoming lens is
refused and must publish a new convergent target instead. Lenses are needed
only at admission, because the cells they produce are persisted in
`_jazz_lens_cells`.

On open, a store makes the head revision's write schema (with its policies)
current; with no revision yet it opens under the lowest admitted version, and
a store with no catalogue at all under the schema it was given. The server's
HTTP catalogue index (schema hashes, lenses, permission bundle ids) is
rebuilt from these rows; a permission bundle id is a version 5 UUID over the
bundle's canonical JSON, chained to its parent revision.

The storage format is `jazz::format::STORAGE_FORMAT`; a layout change bumps
it. The pinned corpora `sqlite-storage-v1` and `server-data-dir-v1` read the
current layout back.

Schema evolution is coordinated through the catalogue, which serializes
publication and active schema changes under administrative authority. Catalogue
mutations travel as admin-gated
`SyncMessage::{PublishSchemaWithLens, PublishLens}`
messages with `CatalogueAck` replies; a non-admin author is rejected
(`INV-LENS-3`). `AuthorSubject::SYSTEM` is the catalogue admin.

Exactly one database-wide catalogue sequencer assigns a dense monotone
`CatalogueSeq`. Core owns this sequencer; clients and local relays never assign catalogue
sequence. Authenticated publication requests are evaluated at Core. Catalogue
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

Strict node recovery rejects a caller-supplied schema absent from the durable
catalogue. A partial client replica may first discover an admitted durable schema
and recover under that schema so its authenticated upstream connection can start.
The requested application schema remains unadmitted: reads fail closed and view
registration cannot author its lineage until an authority catalogue supplies it.
An offline or unpublished target therefore cannot silently read using the old
schema. Closing during this interval preserves the old catalogue, local rows,
and pending writes. Recovery does not replace the durable genesis or manufacture
physical mappings from the requested schema. This also applies to persistent
backend replicas opened with complete-history attribution; strict serving-node
opens retain their existing recovery contract. A remote read begun during this
interval waits for the authenticated upstream's first full catalogue snapshot,
then rejects if the requested schema is still absent. The serialized read's
existing coverage deadline also bounds this admission wait. Local reads, retained
prepared-query reads, point reads, subscriptions, and mutation admission do not
fall back to the recovered schema. Cancellation releases the read's wait;
shutdown, upstream failure, and disconnect resolve it with an error. Replacement
connections own a new admission attempt, so late activity from an older
connection cannot resolve that wait. Discovery uses a requested-schema point
lookup, and only a miss needs the genesis-kind prefix and genesis-schema lookup;
ordinary recovery still validates the entire durable catalogue once.

A host-authenticated application session, or relay with one explicitly admitted
session scope, receives the same full application catalogue snapshot before its
first query that ordinary view delivery already sends. This is application-wide
schema/lineage/physical-identity metadata, including policy expressions present
in schema payloads; it does not carry application rows or grant row access.
A generic relay with no admitted session scope gains no eager catalogue delivery.
Authentication, application routing, and source catalogue readiness precede this
announcement. An owner itself awaiting requested-schema admission must also wait
for a validated snapshot from its current upstream before announcing its
recovered catalogue to a downstream foreground; otherwise
it could incorrectly reject that foreground's same requested schema as absent.
A validated snapshot missing the requested schema is still forwarded so the
foreground can reject explicitly. This failed initial admission does not wait
for a future publication on an idle connection; after publication the caller
reconnects or reopens to make a new admission attempt. Transient send backpressure retains the
unannounced snapshot for retry. This eager application-session delivery occurs
only before its first accepted snapshot on a physical connection. Resuming a
cursor clears only its catalogue-announcement marker, so unchanged metadata is
still delivered once and subscription resume state remains intact. Later changes continue
to accompany ordinary view delivery. Idle session ticks do not clone or hash the
full catalogue. Trusted authority/backend links retain continuous propagation.

A joiner with no local lineage
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

An incoming parked commit unit is link-delivery state, not a decoded
transaction/history row. If the receiver restarts before its schema lineage is
active, reconnect re-delivers the unchanged canonical commit unit; it parks
again until activation and then ingests/projects exactly once. Restart never
uses the caller schema to decode the unavailable unit, and a retransmit with
the same `tx_id` but different canonical content fails as a conflicting commit
unit.

Current-pointer messages and child schema bundles whose dependencies are not
Active park durably across reopen. They retry after each activation, in
catalogue order; a transient missing dependency is not a terminal rejection and
never exposes the pointer early.

Activation is a single durable state transition. Its batch removes the pending
lineage envelope while recording the immutable staged payload, schema, lens,
physical mapping, and active sequence receipt. The retained staged payload is
the canonical witness for the active receipt; it is not a live retry queue. A
crash before that batch leaves only retryable pending/staged evidence and no
active receipt, while a crash after it leaves only the complete active bundle.
For example, if an A-authored unit parks
until the A→B publication arrives, the successful activation drains and
projects that unit once. A reconnect that retransmits the same unit after reopen
must observe the already-active lineage and retain one history/current result,
never a second projection or a stale pending activation.

### 10.3 Shared physical storage

This section and its physical-identity manifests describe the retired Groove
engine. On the SQLite engine, versions share stored tables and columns
through `_jazz_names`, and identities are name-based (§10.7).

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

A valid subscription whose registered shape awaits catalogue activation stays
pending; absence of the active schema is not a permanent query rejection. The
serving connection retains the usage-site handle and its admitted immutable
session snapshot, then resumes ordinary subscription admission when the shape
becomes available. Unsubscribe or connection closure retires that pending
request. Pending requests are bounded by the existing per-peer registration
count and aggregate registration-byte budget; exceeding these bounds is a
protocol error, not silent eviction. Malformed or unsupported shapes still fail
explicitly. For example, Bob's query opened against a schema draft returns no
rows before its lineage lens is activated, then resolves through the same
subscription after activation without requiring Bob to retry it.

`jazz_schema_versions.physical_mapping` is one typed, exact-consumed **v1**
binary payload, not JSON or a Rust-layout serialization. Its v1 payload-enum
introduction ordinal is always a little-endian `u32`, including values below
256; no alternate physical-mapping version or narrow-ordinal compatibility
decoder exists. It canonically orders
logical table names, column names, registry column ids, nested occurrence paths,
and variant fields; malformed UTF-8, duplicate/out-of-order keys, unknown format
versions, truncation, and trailing bytes fail closed at catalogue recovery.
Each immutable publication carries an authority-allocated
`PhysicalIdentityManifest`. `GlobalPhysicalTableId`,
`GlobalPhysicalColumnId`, and `GlobalPhysicalEnumVariantId` are permanent UUID
identities. The authority constructs a publication from the active source
publication plus the proposed schema and lens while replaying every active,
staged, and parked durable manifest as issuance history: mapped entities retain
UUIDs and genuinely new tables, column epochs, and recursive enum variants
receive fresh UUIDs absent from that entire history. There is no separate
allocation protocol or retirement ledger: retained historical schema mappings
and parked/staged publication payloads are the canonical reservation set.
Names, documentation,
structural paths, authored positions, and source order are lookup coordinates,
never identity; same-shaped siblings therefore remain distinct. Canonical
manifest bytes participate in the publication content id and travel in lineage
publications, while the authority snapshot carries the genesis manifest.

An identity is never returned to an allocation pool. A dropped table or column,
an incompatible replacement epoch, and every enum UUID recursively beneath it
are permanently retired: a later new table, column, or enum occurrence MUST
receive a UUID absent from every preceding durable or reserved manifest, not
merely from its immediate source manifest or the currently mapped target paths.
Only the exact compatible mapped coordinate may retain its UUID (including the
shared prefix of its recursive enum occurrences).

A pure rename of a physically compatible column retains its UUID. Any
incompatible epoch (changed value representation, merge strategy, or
non-additive structural shape) MUST receive a fresh `GlobalPhysicalColumnId`;
activation and recovery reject a publication that either changes the former or
reuses the latter. A pending publication whose source has not arrived is still
validated for its exact content id, manifest coverage, UUID uniqueness, and
structural bounds; when the source does arrive, its source-to-target identity
evolution is validated before the pending receipt can stage or activate.

The stored `PhysicalTableId` and `PhysicalColumnId` `u64` values are only
node-local compression aliases below the semantic layer. Nodes may allocate
different aliases or receive publications in different network orders and must
still converge on the same global UUID manifest. Before activation and before
dependent rows can be decoded, Jazz rejects nil UUIDs, duplicate UUIDs, missing
or extra descriptor coverage, and any mapped identity that changes across an
evolution. Durable enum registry vectors bind local tags to permanent enum
variant UUIDs; authored ordinals select entries in a schema but do not identify
them. A binding may retain introducing schema/position solely as ordering and
diagnostic provenance so an established append-only local tag prefix survives
reopen; UUID alone defines equality, descriptor spelling, recursive parent path,
collision detection, and cross-node identity. There is intentionally no legacy JSON reader or in-place migration for
this pre-freeze format.

This freeze covers authority allocation, publication/snapshot transport,
durable physical-mapping encoding, and activation validation. The remaining
#2037 server-catalogue work is operational ownership and rollout of this same
publication contract; it may not introduce a second allocator or a node-local
semantic identity. Lineage retention and compaction preserve the UUID manifest
of every retained publication.

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
are recovered before the full Groove database is constructed. Aliases are
node-local, while physical UUID identities are global and immutable. When a
client has already opened and written under a schema before receiving an
authority snapshot, snapshot planning preserves its local aliases, verifies and
installs the authority UUID manifest, and reconciles ancestors and descendants
around the local anchor. Pending local rows therefore remain addressable while
cross-node semantic identity converges.

**Union row cases.** Besides one Groove case per schema version (tag = the
schema's local alias), every physical history, global current, ahead current
and ahead shadow table has a **union case** per schema version `G` whose
lineage has columns `G` does not map: tag = `alias(G) | 1 << 31`, fields =
`G`'s own case followed by those columns ("extras"), in physical column id
order. The extras of `G` are the columns of the same physical table mapped by
any schema version ranked no later than `G` in the catalogue: a lineage's
target ranks at its catalogue position and the authority genesis before them
all. Every node replicates that order from its Core, so every node that knows
`G` derives the same extras, fixed once `G` is published; old union cases
never change. Local aliases cannot order them: a client allocates its own
schema's alias first and learns older schemas later. A schema version only
the node knows (not yet published) has no union case. Rejected-version tables
have no union case. A union row reads exactly like a `G` row to code that only
knows `G`.

Core stores each post-image in the union case of the schema version ranked
last among those mapping the table (ch. 4 §4.6, "Across schema versions").
That need not be the write's own schema, nor name the table as the write
does, so a post-image replaces the write's pending record by its physical
row (table, branch, row), not by its name or schema. An extra is
**derived** when the lens path from `G` carries it on from one of `G`'s own
columns (a rename chain or a bijective transform); it stays null, and readers
derive it from `G`'s cell as for any `G` row. Every other extra is
**stored**: a column `G` dropped, a sibling's column, and a copy
(`CopyColumn`) target, which is a column of its own (ch. 4 §4.6) whose
writes a lens from `G` would otherwise overwrite with the source's value.
The lens path is taken through schema versions ranked no later than `G`
only, so the stored set is fixed with `G`'s union case: a later schema with
several predecessors cannot open a shorter route that turns a stored extra
into a derived one (or the reverse) under existing union rows. Reading a
union row as schema `R`, by any read path: a column of `R` that is a stored
extra reads that field (null reads as absent); any other column takes the
`G` → `R` projection.

On the wire a Core image keeps `G`'s cells and descriptor; its stored extras
travel as `UnionExtraCells`, one entry per schema version defining them, each
a record of that schema's authored cells (`_app_<column>`, nullable). The
entries travel inside the row's `JVRR` v3 blob, flagged by its descriptor
head (ch. 16); a row without entries leaves the flag clear and carries
nothing extra. A receiver validates each cell against its schema and
stores the image unchanged, in `G`'s union case: its entries must be exactly
`G`'s stored extras there, as the shared catalogue order guarantees. The image
keeps its identity, so what the receiver serves or compares later is Core's
record. Commit units carry no union cells.

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
clarity-first graph walk rather than a hot-path optimization. Each step can traverse its
lens forward or backward: B-to-C translation through `A → B` and `A → C` applies
the reverse A-to-B transformation followed by the forward A-to-C transformation.

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

A relay that rebuilds a current-row input from an admitted authority witness
must keep the witness's authored schema alias separate from the logical read
schema. Before publishing a `VersionRecord`, it reloads the immutable history
record at the exact physical table, branch, row, transaction, and layer. A table
rename does not permit first-match lookup across branches: one transaction may
write the same row UUID in two branches. These are runtime identity rules;
they do not introduce a new durable or wire representation.

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

### 10.7 Stored identities (SQLite engine)

On the SQLite engine the catalogue is the stored names (`_jazz_names`,
`crates/jazz/layers/db/src/names.rs`) and it syncs as `KnownCatalogue` and
`Catalogue` (ch. 8). There is no second store of identities: a table's
identity, and each column's, is a name-based UUID (RFC 9562 version 5) of
the **authority namespace** and the stored table (and stored column) name.
`Db::table_identity` and `Db::column_identity` return them under a handle's
schema version.

- The authority namespace is 16 random bytes an authority with no upstream
  mints once and keeps in `_jazz_meta`. Every `Catalogue` carries it. A
  replica keeps the first one it mirrors and refuses an upstream that sends
  another (another app's authority): its remote reads fail with why and its
  local reads go on.
- A version's identities are the authority's only once the authority has
  confirmed that version's names. `Catalogue.confirmed` lists each version
  the sender's authority admitted with the digest of its names, and a
  replica keeps the list in `_jazz_versions.confirmed`. A version stays
  confirmed while its names keep that digest. Until then, and on a store
  that has mirrored no catalogue, a lookup fails with `NotObserved`. A
  replica never mints an identity, and neither does a store marked
  authority that has an upstream.
- A rename lens keeps the stored name, so it keeps the identity.
- Each stored column's value type is kept in `_jazz_stored_columns` when a
  version storing it is first admitted. A column never changes type under
  its stored column; that is refused at admission and at open.
- A column added again without a lens, under a name an earlier version
  stored in its table, continues the stored column that the last such
  version kept it in, and keeps that column's identity, only if its type is
  the recorded one. Otherwise it starts a new stored column, which gives it
  a new identity. A column a lens adds always starts a new stored column,
  because the lens says the source lacks it.
- A new stored column's name is derived, never minted: a column `name`
  that version `v` introduces (a column of a new table, one a lens adds or
  copies, one whose type changed, one added without a lens that continues
  no stored column) is stored as `name~` and the first six hex digits of
  BLAKE3 over the length-prefixed parts `"column"`, the 16 bytes of `v`'s
  compatibility id, and `name`. A table a lens adds is stored as `name~`
  and the same hash over `"table"`, `v`'s compatibility id and `name`. The
  compatibility id is the version id of `v` with its columns sorted by
  name and without merge strategies or indexes: versions that differ only
  in those are compatible (deployment connects them with an empty lens),
  share it, and so name what they introduce alike, whichever a store
  admits first. A column or table a version
  keeps from its source (untouched or renamed) keeps the source's stored
  one.
- The app's lenses make the names agree. An app ships its schema versions
  and the lenses between them (`DbConfig::with_migrations`; the JSON a
  deployment publishes, which `jazz-tools validate` and `deploy` write to
  `migrations/index.ts` beside `schema.ts`, and which TypeScript apps pass
  to `defineApp`). A store opening, or viewing, a version it has not
  admitted first admits the versions on a lens path to it, starting at one
  it admitted or at the app's first version, as the authority admitted
  them. Of several lenses into a version, every store, the authority
  included, takes the one from the smallest source version id. A version
  no lens leads into is reached by an empty lens from the smallest
  compatible version one does (or the store admitted). Deployment may
  anchor its empty lens on another member of the compatible group; the
  names agree either way, since compatible versions share a compatibility
  id and an empty lens keeps every name. Along those lenses every store derives the same names, whatever
  else it admitted and in whatever order, so a replica stores a version's
  columns where its authority does without asking, and the authority's
  `Catalogue.confirmed` digest only checks it.
- A version admitted without a lens (a store's first version, or one
  opened without the app's lenses into it) is named against the store's
  current version: a table or column of a name an admitted version stored
  continues it (a column only with its type), and a new table keeps its
  own name where no admitted version stores one. Those names depend on
  what the store admitted before. A replica never renames what it stores:
  if its upstream's catalogue names a version it admitted otherwise, the
  replica refuses that catalogue ("not supported yet"); its remote reads
  say why and its local reads go on.
- Two lineages of one table that share a name collide only if their hashes
  do (one in 2^24 per pair). A store that admits both refuses the second
  with a typed `Schema` error; a store that only admits one cannot tell,
  and its upstream's catalogue then refuses it as above.

This is how the SQLite engine meets `INV-LENS-24`: an identity is never
reused for another column epoch, and only a compatible re-addition keeps its
predecessor's.

### 10.8 Enum tags (SQLite engine)

An enum column (`s.enum(...)`, or an array of one) stores each variant as an
INTEGER **tag**, kept per stored column in `_jazz_enum_tags (tbl, col, tag,
name)` (`crates/jazz/layers/db/src/values.rs`, `EnumTags`). Tags are an
interning optimisation private to each store. Values are variant names
everywhere above SQLite: in writes, commits, row images on the wire, the
catalogue, filters, policies and reads. Only a store's own cells hold tags,
and no other store ever sees one.

- **Every store tags its own variants.** A store tags each variant a version
  declares when it admits the version, before any column is added, so an
  added enum column's DDL default is a tag too. A new name takes the
  column's next tag, one past its highest. A removed variant keeps a tag,
  and every name a version ever declared for the stored column still reads
  back. A name a row carries that no admitted version declares (a removed
  variant, say) is tagged when it is stored. A store's memory of its tags
  follows its file: a savepoint that rolls back puts back the tags it
  interned or renumbered.
- **The current schema's order is tag order.** When a store makes a version
  current (activation, or opening under a version newer than any it
  admitted), it renumbers each of that version's enum columns so its
  declared variants are tags `0..n` in declaration order and every other
  name follows in its previous tag order, rewriting the column's cells in
  every row version in the same transaction. Appending a variant changes
  no tag. A column added with a default keeps SQLite's `DEFAULT` tag; when
  a renumbering gives that tag to another variant, the store records the
  default by name (`_jazz_enum_defaults`), and a row inserted by a version
  that does not name the column stores that name's tag.
- **On the wire.** Row images carry enum cells as variant names (a JSON
  array of names for an array column); the receiving store stores each as
  its own tag. Two stores may tag a variant differently.
- **Copies and renames.** A rename keeps the stored column, so it keeps the
  tags. A copy lens's column is a stored column with tags of its own: it
  starts with its source's tags, removed variants included, so its cells
  are its source's and it sorts like its source; either may add variants
  later.
- **Order.** `ORDER BY`, `MIN`, `MAX` and range comparisons on an enum
  column follow the declaration order of the query's schema version. For
  the current version that is tag order, which an index on the column
  serves. Another version (an older client during a rollout) reads each
  cell as its rank in its own declaration order, a `CASE` no index serves,
  with names it does not declare after its own. A range compared with a
  name the column never held is refused as a query error. A `GROUP BY` on
  an enum column identifies each group by its variant name, so a
  renumbering keeps group identity.
- **Comparisons.** A variant name compared with an enum column for
  equality (`=`, `IN`, array containment, a policy literal or session
  value) is bound as its tag. Anything else, such as text containment or a comparison with another
  enum column's cells, reads the cell as its name.
- **Type.** `_jazz_stored_columns` records an enum column as an enum
  (whatever its variants), so a stored column never holds both tags and
  another type's values: a column changed between enum and another type is
  handled like any type change (10.7), as a new stored column or a refusal.
  Writes are checked against the writing version's declared variants.
  Enum columns are never large-capable (ch. 19).

Raw SQL (an inspector, a debugging session) reads names by joining
`_jazz_enum_tags` on the stored table, stored column and cell.

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

### Active schema and local schema

The authority's **active schema** selects one structural schema and one explicit
permission bundle at a single monotone revision. Policy-only updates advance
that revision without changing structural schema identity or creating migration
lenses. Admitting structural schemas or lenses does not activate them. Activation
requires an admitted migration path, traversing each lens in either direction.
For example, `A → B` and `A → C` permit switching from B to C through A.
An explicit empty permission bundle is a valid deny-all selection. The node's local API schema remains independent, so
older clients retain their schema view across authority activations.

The server catalogue retains the existing permissions-bundle/head encodings and
HTTP endpoints as compatibility boundaries. Internally they describe the active
schema. Runtime activation persists its selection and authorization source in
one batch before installing the new authorization view. Structural catalogue
entries do not need to be rewritten with permissions on policy updates.

Opening a legacy store without an active-schema record recovers its selected
schema and grants from the old write pointer and schema payload, then persists
an active-schema record at revision zero. The old independent write counter is
not an authority revision. Server startup installs the durable administrative
selection before serving traffic. Reopening the upgraded store preserves its
active revision; snapshots have no legacy revision exception. Standalone schema
publications cannot replace grants, and standalone write pointers cannot change
the active selection. Interoperation with old running nodes is not supported.
The existing snapshot wire layout is retained: the active authorization source
is projected into its schema payload at serialization and extracted on receipt.

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
