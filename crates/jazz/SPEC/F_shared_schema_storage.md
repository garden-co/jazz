# jazz — Design exploration · Shared storage across schema versions

## Overview

> **Status:** Early, non-normative proposal. If accepted, its decisions should
> be folded into the normative schema-evolution, data-model, and groove-lowering
> chapters.

Jazz currently gives each schema version its own physical storage tables. This
keeps every stored row tied to the descriptor under which it was written, but it
also makes a logical-table read span every schema-version partition that may
contain data. Physical tables and their indexes accumulate as the schema
evolves, even when a migration only renames a table or column.

This proposal replaces that layout with one shared physical storage lineage for
each logical table. Schema versions remain immutable application-facing views,
but they no longer define separate physical tables. Table and column renames
change the logical-to-physical mapping; adding a column extends the shared
physical representation; dropping a column removes it from newer logical views
without immediately removing its stored values. Writes authored against any
supported schema are translated into this shared representation, while reads
resolve the relevant current or historical row once and project it into the
requested schema.

“Shared physical storage” does not mean collapsing all Jazz storage into one
literal table. Content history, deletion/restore history, and derived current
state remain separate physical structures. The intended change is that
those structures are shared by all versions of a logical table instead of being
duplicated once per schema version.

## Motivation

The primary motivation is to make read cost independent of the number of schema
versions that have existed. A shared lineage removes schema-partition fanout and
allows compatible schema versions to reuse the same physical indexes. It also
reduces table proliferation and avoids rebuilding equivalent storage structures
for migrations that do not materially change a table.

The importance of reusing indexes must not be understated: currently we keep separate
indexes for each table version, and they are not combined into a useful logical cross-version index.
Once a table has schema partitions, Jazz disables the ordinary prepared-query fast path.

Keeping retired columns in the shared representation can also prevent avoidable
data loss: newer schemas can hide those columns, while older supported schemas
can continue to read or update them according to the migration and authorization
rules. The same separation between logical schemas and physical storage could
later support type changes and data transformations without requiring another
physical table for every schema version.

**TODO: how do we solve this?**
The shared representation will grow as columns are introduced. Physical
cleanup, schema support cutoffs, indexing of defaults, and the exact semantics
of writes that omit hidden columns are deliberately left for the fuller design.

## Details

### Row translation

Consider:

```
v1: { title }
v2: { title, body = "" }

v2 writes body = "important"
an offline v1 client later updates title
```

Today, forward translation inserts the `body` default into the old client’s version, replacing `"important"` with `""`.

To preserve `body`, when applying lenses to writes we must rename columns but **NOT** apply lens default/backwards default values (keeping existing values for dropped columns and marking new columns as unset
unless they're explicitly written to).

The same issue appears after a drop:

```
v1: { title, legacy }
v2: { title }  // legacy dropped
```

After a v2 title update, a v1 reader should see the old legacy value, not the backwards default (which is the current semantics). The backwards default should only be used when projecting columns that miss a real value.

### Row storage

Groove records are schema-driven typed tuples whose physical layout can reorder fixed- and variable-width fields ([storage model (line 86)](../../groove/SPEC/2_storage_model.md#L86)). Existing history tables encode user fields according to that version’s `TableSchema` ([schema.rs (line 905)](../src/schema.rs#L905)).

Shared storage will use **schema-versioned tuple decoding**. Every encoded history
and current row begins with a stable envelope that can be decoded without knowing
the payload shape. The current Groove envelope is a fixed-width, little-endian
`u64` discriminator; Jazz writes its node-local `SchemaVersionAlias` there. The
`PhysicalTableId` comes from the
storage key/table context. Together they identify the logical table mapping and
the Groove tuple descriptor needed to decode the payload:

1. resolve `SchemaVersionAlias` to `SchemaVersionId`;
2. load that schema version's mapping for the surrounding `PhysicalTableId`;
3. walk the schema's ordered columns, resolving each to its `PhysicalColumnId`,
   and construct the typed payload descriptor.

Rows written under different schema-version aliases can therefore coexist in one
physical table even when their typed tuple representations differ. A rename can produce
an equivalent descriptor when the physical ids and types remain unchanged, but
it is still safe for rows to retain their own schema-version aliases.

The authored `SchemaVersionId` remains the write's wire-level provenance, its
local `SchemaVersionAlias` selects the stored payload descriptor, and the
requested `SchemaVersionId` selects the logical projection returned by a read.
The schema catalogue and the local `jazz_schema_versions` rows containing each
alias and its physical mapping must be durable and recovered before payload
decoding. A schema version or mapping cannot be removed while any retained
history, current row, branch, or snapshot uses its alias.

#### Groove versioned rows

Groove should support heterogeneous rows through **schema versions**, without depending
on Jazz schema ids, lenses, or physical mappings. A versioned table declares one stable
field catalogue (field name plus type), and each schema version selects an ordered subset
of those fields. Each row begins with a stable header containing a fixed-width `u64`
version, from which Groove derives the matching `RecordDescriptor` before decoding the
remaining record. Registries are supplied when a database is opened or rebuilt; a
registered field or schema-version layout cannot change while retained rows use it.

Writes name the version explicitly, and an update may replace
a row with another variant. A field absent from a variant remains distinguishable
from a present nullable field.

Indexes are declared against stable field names/ids across the table's variants:

- every variant containing an indexed field must give it the same type;
- a variant missing any field of an index produces no entry for that index;
- updates remove entries using the old row's discriminator and descriptor, then
  add entries using the new row's discriminator and descriptor;
- index rebuilds and query evaluation select each row's descriptor before
  reading indexed or projected fields.

Physical indexes are append-only for the lifetime of a `PhysicalTableId`. The
query planner only uses indexes declared by the requested Jazz schema, but an
index remains available after a later schema stops declaring it. When a schema
first declares an index for an existing physical column, Jazz registers that
physical index on the live Groove table and Groove backfills retained rows
before publication continues. This does not rebuild the database or disturb
existing subscriptions. Variants missing the indexed field contribute no entry;
future variants append their projection or `Ignore` case before their first row
is stored.

For each `PhysicalTableId`, Jazz derives the variant registry from its local schema
catalogue and physical mappings. Variant fields use stable physical names derived
from `PhysicalColumnId`, rather than application-facing names, so compatible
schema versions expose the same field and reuse the same Groove index. Logical
renames remain entirely in Jazz's projection layer. This abstraction could also
model other heterogeneous stores in the future, such as object-table subtypes or
inherited table rows, by assigning their concrete row shapes local `u64` discriminators.

Groove can implement this incrementally: first discriminator-aware record
encoding and primary-key reads/scans, then variant-aware IVM ingress and query
field resolution, and finally variant-aware index maintenance and rebuilding.

For field resolution, Groove uses a generic `VariantProject` source-boundary
node that maps different row versions to one fixed output descriptor. Jazz
builds its cases from lenses and physical mappings; Groove is not aware of those
concepts. The node's projection target and output descriptor are immutable, so
every downstream IVM node and active subscription continues to use the
descriptor with which it was compiled.

The set of supported input versions is not part of the node's graph identity.
`VariantProject` consults an append-only runtime registry keyed by table,
projection target, and source discriminator. Registering a previously unsupported
source case extends the node's input domain without changing its `NodeId`, graph
topology, output descriptor, or existing cases. Jazz must register a variant's
descriptor and every required projection case before storing the first row with
that discriminator. Consequently case registration never needs to backfill rows
that were already present, and active subscriptions can consume subsequent rows
as ordinary incremental deltas without graph rebuilding or reset.

Maintained query evaluation projects current rows into the subscription's
logical schema, but sync bundles must preserve the immutable payload and schema
under which each winning version was authored. For now, when a maintained
witness's table or descriptor no longer matches its authored schema, Jazz uses
its stable version identity `(row, layer, tx_time, tx_node)` to perform one exact
lookup in the shared physical history table before serializing the bundle. This
is an O(1) fallback independent of schema-version count and is not used for
same-layout witnesses. If this point-read cost becomes material, Groove can
later expose an opaque original-row witness through `VariantProject`.

Each registered case is either a projection or `Ignore`. `Ignore` intentionally
emits no row for that discriminator, while an unregistered discriminator remains
a configuration error. Durable indexes use private fixed-output projection
families: variants containing every indexed field project those fields plus the
primary key, and variants missing any indexed field register `Ignore`.

#### Avoiding data loss

Jazz currently selects a winning content version across all writes. If that version omits a retired column,
the schema's default value is returned to the user. That leads to frequent data loss for columns dropped across
schema versions.

The key idea in this proposal is to **distinguish unset fields from lens default values**: a row's field is unset in storage unless it's explicitly set by an insert or update operation. Lens default values are only applied when reading rows with unset fields from storage. When decoding a row written under an older schema, physical columns introduced only by later schemas are considered unset. Unset fields are also semantically different from fields explicitly set to `null` by the application.

For every content write, we need to:

- translate that write to the current schema (preserving dropped columns)
- load the write’s declared causal parent (also translated to the current schema)
  - if there are multiple parents, synthetize a merge row for them
- determine each column's value for the new row version, with the following precedence:
  - explicit value on new version
  - explicit value from the parent version
  - otherwise keep unset

**Note:** An inherited value must not become an “explicit setter”; otherwise it may incorrectly compete with real writes during LWW conflict resolution.

When resolving conflicts between row versions:

- set values always take precedence over unset values when using LWW
- for counters, a missing field is equivalent to 0

#### Storage keys

Schema versions currently partition application data in the KV keyspace, even
though `SchemaVersionId` is not a column of the history or current-row primary
keys. Non-base schema partitions include the schema-version UUID in their
logical Groove table names, for example
`jazz_<table>_<schema-version>_history` and
`jazz_<table>_<schema-version>_global_current`. Groove's class-CF layout embeds
that logical table name in the physical KV key prefix. Durable secondary-index
keys likewise begin with the logical table name and index name. Before the
branch-overlay cutover, branch history and register table names also contained
both the branch id and schema version.

Shared storage removes the schema-version dimension from all application-data
row and index keys, including history, global current, ahead current, secondary
indexes and branch overlays. Keys should be scoped by stable physical identity instead.

### Sync

The current proposal does not modify the sync protocol in any way.

A commit's `VersionRecord` already carries:

- the authored `SchemaVersionId`;
- the authored logical table name;
- parents and provenance;
- optional cells, where absence is distinguishable from explicit null.

That is sufficient for a receiver to intern the authored schema as a local
`SchemaVersionAlias`, resolve logical names to local physical IDs, and use the
alias as the Groove schema discriminator. Schema aliases, physical IDs, and the
storage envelope remain entirely local.

When receiving rows written in a schema different from the current one, missing columns are preserved as unset
(instead of using the schema default values).

Ideally, fields should not be sent to a consumer unless they're required by their schema, since doing so could expose unwanted information and potentially be a security hazard (see related open question). We currently do send them: if a v2 version contains a later-retired column and a v3 client subscribes to that row, sending the raw v2 version also sends the retired column, even though v3 does not expose it.

Downstream clients do not NEED to receive retired columns for sync and conflict resolution to work properly.
Changing this to avoid leaking unnecessary data should be easy, as the server already takes care of applying the necessary lenses to convert data to the version required by the client.

### Table and column identity

Names are application-facing aliases, not physical identities. Referring to
tables and columns by name alone fails for:

- dropping `x` and later adding a semantically unrelated `x`;
- table or column name reuse after a rename;
- `CopyColumn`, where source and target must subsequently diverge;
- column type changes;
- two schema branches independently adding a same-named table or column.

The catalogue therefore maintains a resolved physical-identity mapping for every
schema version:

- `PhysicalTableId` identifies one shared table-storage lineage;
- `PhysicalColumnId` identifies one physical column within that lineage;
- each logical table and column name in a `SchemaVersionId` maps to one of those
  physical ids.

These are opaque, database-local `u64` ids, analogous to `SchemaVersionAlias`.
They are durable and stable for the lifetime of a persisted mapping, but do not
need to match across nodes. During one process lifetime each database allocates
them monotonically.
On restart it derives the next ids as one greater than the maximum ids in live
persisted mappings. An id removed from all mappings may therefore be reused after
a restart; this is safe because publishing a lens first discards all storage and
metadata belonging to the replaced provisional identity. Tuple decoding, query
planning, projection, and index lookup resolve application names through the
relevant schema's identity mapping.

Identity follows these rules:

- An unchanged table or `RenameTable` preserves its `PhysicalTableId`.
- A newly added table receives a fresh `PhysicalTableId`. Dropping and later
  adding a table with the same name also creates a fresh id.
- An unchanged column or `RenameColumn` preserves its `PhysicalColumnId`.
- `AddColumn` creates a fresh `PhysicalColumnId`.
- `CopyColumn` preserves the source id and creates a fresh target id. The initial
  target value may be copied by the lens, but later writes to either column are
  independent.
- `DropColumn` retires the column from the target schema view without deleting or
  reassigning its physical id. A later same-named `AddColumn` receives a fresh id;
  it does not revive the retired column.
- A change that is not representation- and semantics-preserving creates a new
  physical column epoch, represented by a fresh `PhysicalColumnId`. This includes
  incompatible type or large-value-kind changes and merge-strategy changes. A
  future transform lens may relate the two epochs, but they do not share storage
  or indexes merely because their logical names match.
- Independently introduced same-named entities receive different physical ids.
  Name and shape equality alone never merges identities.

#### Publishing lenses

A new schema may be published and written to before the lens mapping it to an existing schema is published,
so data will end up in a new physical table. If that happens, identity resolution is not a metadata-only relabel.

This is mostly a dev-only concern: it's expected that devs experiment with schema shapes before writing migrations, but in prod new schemas will usually be published alongside their migrations. This means data preservation is not an essential requirement.

We can handle this scenario in the following way:

- `PublishSchema` allocates a database-local provisional mapping.
- A mapping becomes authoritative when it is first used as a lens source or is
  first reconciled as a lens target. Jazz derives this state from the durable
  lens catalogue; it does not need a separate persisted flag.
- The first lens targeting a provisional schema reconciles its mapping,
  preserving identities across renames and unchanged entities. That result is
  then immutable.
- A later lens targeting that schema independently derives the mapping implied
  by its source mapping and lens operations. Jazz accepts the lens only if the
  result exactly matches the authoritative target mapping; otherwise
  publication is rejected before either the lens or mapping is persisted.
- When reconciliation replaces a provisional table identity, Jazz discards all
  storage scoped to the target/new physical table before installing the
  reconciled mapping.
- This discard includes history, derived current state, indexes, branches, and
  other table-scoped side storage.

This deliberately accepts data loss in the uncommon dev workflow where a schema
receives writes before its lens is published. In the future we may replace the
discard with migration from one physical table to another.

### Indexing

Indexes are reused across schema versions, so in cases where indexed columns are not modified by a migration,
existing indexes "just work". Similarly, renamed physical IDs can share equality indexes, but transformed types may not.

Jazz does not currently support transforming columns through lenses, so this is a problem we can defer solving.

For dropped indexed columns, writes coming from previous schemas but translated to the new schema can still modify the "dropped" index (as they preserve the dropped columns from the previous schema).

**TODO: Handle adding a new indexed column to tables with existing data**

### Branches

Branch identity remains a separate key/prefix dimension. We just need to make sure it continues to work properly by removing the schema-partitioning from branches as well.

## Open Questions

### What happens with a server's active subscriptions when the current schema changes?

Active subscriptions continue to use the schema they were created with for query semantics and output projection. Authorization is evaluated independently against the server’s current permission-evaluation schema, with row data projected into that schema before evaluating policies. If the permission-evaluation schema changes, all active maintained subscriptions should probably be invalidated and recompiled under the new policy before further results are served.

This is what Jazz currently does for policy-only changes on the existing authorization schema. Moving the permission head to a different schema is not currently handled. We need to ensure this scenario works properly and add test coverage for it.

### Allowing old clients to update retired fields is a security decision

Which policy authorizes old-schema writes to retired columns?

Consider dropping `owner_id`, `is_admin`, or sensitive PII.

If an old client can continue changing that retired field:

- which policy authorizes it—the authored schema policy, current policy, pinned policy, or all of them?
- can the value affect an old pinned RLS policy?
- could a later schema accidentally re-expose attacker-written retired data?

### Column cleanup conflicts with indefinite offline compatibility

Safe physical cleanup requires proving that:

- no accepted writer may use the retired schema;
- no branch base or retained historical query needs the values;
- no policy pin references them;
- no pending/retry/offline transaction can reintroduce them;
- all relevant replicas have crossed a cutoff watermark;
- old-schema reads are no longer promised.

Jazz currently deliberately forbids automatic schema-partition GC for these reasons ([lens spec (line 218)](./10_lenses_migrations.md#L218)).

## Implementation Plan

Overall approach: start preserving today’s copy-forward/default behavior, including its known data-loss limitation.
Jazz is still alpha, so this storage cutover does not migrate, recognize, or
remain compatible with databases written by the former per-schema history layout.

1. Add physical identity metadata without changing storage behavior.
   **Status: complete (2026-08-03).** The mappings are durable shadow metadata;
   application data still uses the existing schema-partitioned storage path.
   - Introduce database-local `u64` `PhysicalTableId` and `PhysicalColumnId`
     values.
   - Persist each schema version’s logical-to-physical mapping alongside its
     database-local alias in `jazz_schema_versions`.
   - Recover the next table/column ids from live persisted mappings during the
     catalogue-open stage. Fully discarded provisional ids may be reused after
     restart.
   - Include column identity now because reusable storage and indexes cannot
     safely be defined using logical names.

2. Add schema-versioned Groove storage and IVM support.
   **Status: complete (2026-08-07).** Groove's stable field catalogue,
   per-version ordered layouts, common batch insert/update/get/scan paths, mixed-version
   replacement, reopen coverage, window preservation, and Jazz
   `SchemaVersionAlias` binding are implemented. Groove's descriptor-correct
   heterogeneous IVM deltas and live-extensible, fixed-output variant projection
   are also implemented. Variant-aware durable indexes use explicit `Ignore`
   cases, enforce uniqueness across variants, survive reopen, and extend without
   rebuilding active subscriptions.
   - Add generic schema-versioned tables to Groove, using a per-table `u64`
     version-to-descriptor registry.
   - Make IVM table deltas retain their source discriminator and descriptor.
     Cross-version updates retract the old payload through its old descriptor
     and insert the new payload through its new descriptor.
   - Add fixed-output `VariantProject` nodes backed by an append-only runtime
     case registry. Registering a new input case must not rebuild the graph or
     reset active subscriptions.
   - Require Jazz to register the descriptor and projection cases for a schema
     version before accepting rows authored in that version.
   - Build durable indexes on fixed variant projections. A variant missing any
     indexed field registers `Ignore` and emits no index entry; an unregistered
     case is an error, and uniqueness spans all projected variants.
   - Make primary-key reads/scans, index rebuilding, and query field resolution
     select the row descriptor through its schema version.
   - Use `SchemaVersionAlias` as Jazz's discriminator and derive physical field
     descriptors from `PhysicalTableId` plus durable schema mapping metadata.
   - Initially use existing lens projection/default behavior when normalizing
     schema versions.
   - Test adding a variant while a subscription is active, mixed-version scans,
     point reads, indexes, and restart recovery before changing table placement.

3. Connect Jazz's catalogue to Groove's live variant registry.
   **Status: complete (2026-08-07).** Jazz now lowers stable history field
   catalogues from `PhysicalColumnId`, restores every alias/layout and logical
   projection on open, and extends live Groove tables, layouts, and projection
   cases before enabling writes in a newly published schema. Publishing a lens
   no longer rebuilds Groove merely to add a variant; an active history
   subscription retains its output descriptor and runtime identity while
   receiving writes stored under the new alias.
   - Build one stable Groove field catalogue per `PhysicalTableId`, naming user
     fields from `PhysicalColumnId` rather than logical column names.
   - Register every `SchemaVersionAlias` and compile its projection cases from
     the durable physical mapping and lens paths.
   - On lens publication, append the new variant and cases to the running Groove
     database before enabling writes; do not rebuild Groove merely to teach it
     the new variant.
   - Prove that an active Jazz subscription retains its output descriptor and
     receives new-version writes without rehydration.

4. Share immutable history first.
   **Status: complete (2026-08-07).** Each `PhysicalTableId` now owns one
   schema-versioned content-history table and one deletion-register table;
   ordinary per-schema history/register tables and names have been removed.
   Writes derive both tables from the row's stored alias, raw reads recover the
   originating logical table from that alias and the physical lineage, and each
   logical lineage exposes exactly one source per immutable layer regardless of
   schema-version count. Content uses `VariantProject` because its payload
   descriptor varies; deletion rows share one stable system-only descriptor and
   need no variant projection. Mixed content, deletes, and restores survive
   restart and table renames. Publishing a lens discards both immutable layers
   of a replaced provisional physical table.
   - At this stage global/ahead-current and branch-overlay tables remained
     partitioned, giving the immutable-history cutover a contained vertical slice.
   - Current projection chooses content and deletion winners independently
     across the remaining schema partitions before applying the winning deletion
     state, so a restore in one schema can reveal content authored in another.

5. Share current state and indexes.
   **Status: complete (2026-08-07).** Each `PhysicalTableId` now owns shared
   global-current and ahead-current content and deletion-register tables. Jazz
   registers their version layouts and logical `VariantProject` cases alongside
   immutable history, and all ordinary current reads, maintained subscriptions,
   primary-key probes, and secondary-index probes use the physical lineage
   directly. Content and deletion winners remain independent; a winning delete
   hides content regardless of their relative timestamps, while a winning
   restore reveals it. Cross-schema maintained witnesses use the bounded exact
   history lookup described above to preserve authored sync payloads.

   **Index lifecycle decision (2026-08-07):** physical indexes are append-only.
   Groove supports registering and backfilling a new durable index on a live
   versioned table; Jazz names it from `PhysicalColumnId`, while each logical
   schema's planner only selects indexes declared by that schema. Integration
   coverage writes a row before its physical index exists, proves live backfill,
   then adds another schema variant and verifies that the same query still reads
   exactly one physical index entry and one current row.
   - Move global-current, ahead-current, and durable index prefixes to physical identities.
   - Replace the current union-and-`arg_max` path in
     [query_eval.rs](../src/node/query_eval.rs) with one source plus logical projection.
   - Re-enable the ordinary prepared-query path.
   - Add a storage-read receipt proving read cost stays constant as schema-version count increases.

6. Convert the remaining schema-keyed storage.
   **Status: in progress.** Rejected-version storage, global-change identity,
   and branch overlays are complete; the remaining key audit is still pending.
   - Branch overlays: each `(PhysicalTableId, BranchId)` owns one versioned
     content-history table and one stable deletion-register table.
     `jazz_branch_partitions` stores only those two identities. Writes retain
     their authored `SchemaVersionAlias`; reads and merge-back project winners
     into their requested schema, including across table and column renames.
     The mapping and mixed-version rows survive restart. Discarding a
     provisional physical lineage clears its branch rows and partition metadata
     before that local ID can be reused.
   - Rejected versions: `jazz_rejected_transactions` remains global transaction
     metadata. Each `PhysicalTableId` now owns one schema-versioned rejected
     payload archive; logical-name payload tables are no longer lowered. Rows
     retain their authored `SchemaVersionAlias`, and recovery derives their
     authored logical table and descriptor from the alias plus physical lineage.
     If lens publication discards a provisional lineage, Jazz discards every
     retained retry payload touching that lineage as an atomic whole: its global
     retry header and archived versions in all lineages. The ordinary rejected
     transaction audit record remains.
   - Global changes: `jazz_global_changes` now keys and indexes rows by
     `PhysicalTableId`, so historical cuts and whole-table conflict detection
     span table renames without duplicating events or scanning schema variants.
     Discarding a provisional lineage also removes its change rows before that
     local physical ID can be reused.
   - Audit large-value checkpoints and any remaining table/column-name-derived
     keys.
   - Remove `jazz_partitions` only after recovery no longer depends on it.

7. Implement unset/data-preservation semantics later.

   This becomes a semantic change on top of a storage model already capable of selecting layouts and physical columns, rather than being entangled with eliminating partition fanout.
