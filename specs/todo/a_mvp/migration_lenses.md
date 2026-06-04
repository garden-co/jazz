# Migration Lenses — TODO (MVP)

Instead of enforcing a single active schema for the database that is mutated
with DDL-like queries, Jazz allows for multiple schema versions to coexist and
to be mutually compatible via `migration lenses`.

This is achieved by having one set of backing SQLite tables per logical Jazz
table x schema version. Clients can read or write from any schema version and
will be able to access any data that is reachable via a chain of compatible
migration lenses.

The authoring process involves creating new schema versions via admin access
and then creating migration lenses between pairs of versions. The admin also
defines a `stable schema version`, which is the schema version to which all
data should eventually converge to.

Data is read from all schema-version tables that contain rows, but it's always
written to the current-schema table known by that node.

The data migration process is performed independently and in the background for
each node. It can be avoided in cases where the schema migration does not introduce
any changes in a given table (in which case the table is just renamed), or when
the changes are `naturally mappable` via SQL DDL (column rename / column addition
with default value / column deletion with backwards default value).

In cases where data is read or written before being migrated, the lenses are
applied at that point.

## Implementation Model

### Backing Layouts

For each logical table, SQLite contains multiple backing physical tables:

```text
{logical_table}_{schema_hash}_current
{logical_table}_{schema_hash}_historyHot
{logical_table}_{schema_hash}_historyColdBlocks
```

When running migrations, we only modify current and history hot tables.
Each table has one of the following states:

- `active`: may own current rows and be read by queries. All new writes are routed
  to this table. Only one physical table can be active for a given logical table
- `migrating`: may own current rows, is being drained into another physical table,
  and remains readable
- `closed`: fully migrated, ready for deletion (could be replaced with actually
  deleting the table)

### Lens Lowering

A migration lens is an _SQL-lowerable_ transform between two schema versions for
one logical table.

The current lenses supports only operations naturally mappable to SQL DDL:

- read/write column & table aliases for renames
- new columns with defaults for added columns
- backwards defaults for deleted columns
  - TODO do we actually delete the columns, or do we keep them to serve
    old clients?
- added tables are ignored by old clients
- dropped tables are preserved to continue serving old clients
  - TODO is this what we want? Or should we just drop the table and return
    no rows for old clients?

### Read Path

A read request references a client schema version. The lowered query:

1. asks catalogue/layout metadata for `active` or `migrating` physical tables for the
   logical tables
2. filters out layouts known to be empty for the query's branch/snapshot scope
3. lowers each remaining table through a lens path into the request schema
4. unions the translated rows into one row stream per logical table
5. applies policy filters, predicates, ordering, pagination, and subscription
   diffing in request-schema semantics

When the `active` layout is the only non-empty relevant layout and the request
schema, the planner should use a fast path that reads only that one backing table.

### Write Path

A write request references a client schema version. For each logical row
being written, the write transaction must:

1. translate the prior visible row into the writer's schema if needed (see "Read path")
2. apply the mutation in writer-schema semantics
3. resolve the row's `active` physical target table
4. translate the resulting row into the `active` physical target table
5. append history and update current projection in the chosen target table

This whole process happens inside the same SQLite transaction.

TODO delete/migrate old current and history rows as part of the SQLite tx as well?
TODO servers that receive writes from old schemas (server should migrate delta to
stable schema)

### Background Migration

Changing the `stable schema version` starts a durable background migration.

The migration process runs in small SQLite transactions:

1. select a bounded batch of logical rows still physically current in the source
   layout
2. translate each row through the selected lens path into the target schema
3. delete the source physical rows

Migration is a physical storage operation. It must not create user-visible Jazz
transactions, change logical timestamps, trigger ordinary mutation callbacks, or
alter sync-visible history semantics.

The operation must be idempotent.

### Row Home And Deduplication

The runtime needs a canonical row-home invariant:

- at most one physical layout owns the current projection for a logical
  `(table, row_id, branch/snapshot scope)` at a time
- migration may move that ownership
- writes may move that ownership
- reads may see old and new layouts concurrently, but must collapse duplicate
  logical rows deterministically

### Branches And Historical Reads

Migration of current data does not remove the need to answer historical
or branch-pinned reads.

If a branch snapshot or conflict candidate refers to a version that exists only
in an older layout, that older history must remain readable until the snapshot
can be served from migrated history or is no longer reachable.

### Sync Boundary

Sync messages remain logical. They should not depend on the sender's local
physical layout names.

A receiver applies incoming logical rows/history into its local chosen target
layout using its catalogue state, stable-schema affinity, and available lenses.
Two peers may therefore hold the same logical row in different physical layouts
while still converging semantically.

TODO: what happens if a column's merge strategy changes? Should it be allowed?
