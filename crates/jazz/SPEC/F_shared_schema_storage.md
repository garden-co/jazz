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
the payload shape. At minimum, the envelope contains its format version and the
node-local `SchemaVersionAlias`. The `PhysicalTableId` comes from the
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
keys likewise begin with the logical table name and index name. Branch history
and register table names contain both the branch id and schema version.

Shared storage removes the schema-version dimension from all application-data
row and index keys, inclusing history, global current, ahead current, secondary
indexes and branch overlays. Keys should be scoped by stable physical identity instead.

### Sync

The current proposal does not modify the sync protocol in any way.

A commit's `VersionRecord` already carries:

- the authored `SchemaVersionId`;
- the authored logical table name;
- parents and provenance;
- optional cells, where absence is distinguishable from explicit null.

That is sufficient for a receiver to resolve logical names to physical IDs and encode the row using its own local physical layout. `PhysicalLayoutId` and the storage envelope should remain entirely local.

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
- Publishing lenses may reconcile provisional mappings, preserving identities across renames and unchanged entities.
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

### Conflicting lens paths may assign different physical identities

A schema may be reachable through multiple lens paths. Those paths could disagree
about whether a target table or column preserves an existing physical id or
introduces a new one. The physical mapping cannot depend on whichever path a node
happens to traverse: replicas must resolve a `SchemaVersionId` to the same
physical identities.

This proposal defers the graph-consistency rule. Possible solutions include:

- reject publication unless every path produces the same identity mapping;
- assign each schema one authoritative resolved mapping and validate later lenses
  against it;
- add an explicit identity-merge operation backed by a physical lineage
  migration.

Until this is resolved, the design assumes that all lens paths published for a
storage-resolved schema agree on its physical table and column identities.

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

2. Add schema-versioned record envelopes.
   - Teach Groove/Jazz storage tables to hold rows encoded under multiple schema
     versions.
   - Decode the stable envelope first, then use its `SchemaVersionAlias` plus the
     surrounding `PhysicalTableId` to derive the payload descriptor from durable
     schema and mapping metadata.
   - Initially use existing lens projection/default behavior when normalizing
     schema versions.
   - Test mixed-schema-version scans, point reads, indexes, and restart recovery
     before changing table placement.

3. Share immutable history first.
   - Key content-history and deletion-register structures by `PhysicalTableId`.
   - Replace `version_storage_sources()` fanout with one physical source per lineage.
   - Keep global/ahead-current tables partitioned temporarily, giving us a contained vertical slice.

4. Share current state and indexes.
   - Move global-current, ahead-current, and durable index prefixes to physical identities.
   - Replace the current union-and-`arg_max` path in [query_eval.rs](/Users/nicolasr/Desktop/Jazz/jazz2/crates/jazz/src/node/query_eval.rs:1256) with one source plus logical projection.
   - Re-enable the ordinary prepared-query path.
   - Add a storage-read receipt proving read cost stays constant as schema-version count increases.

5. Convert the remaining schema-keyed storage.
   - Branch overlays: retain branch identity, remove schema identity.
   - Audit global-change keys, rejected-version storage, large-value checkpoints, and any table/column-name-derived keys.
   - Remove `jazz_partitions` only after recovery no longer depends on it.

6. Implement unset/data-preservation semantics later.

   This becomes a semantic change on top of a storage model already capable of selecting layouts and physical columns, rather than being entangled with eliminating partition fanout.
