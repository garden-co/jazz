# Schema Manager — Status Quo

The Schema Manager lets one Jazz runtime understand more than one schema version at a time.

That matters because local-first systems are rarely perfectly coordinated:

- one client may already be running a newer schema
- another client may reconnect with an older build
- a server may need to answer queries for both at once

The Schema Manager handles that by combining:

- schema hashes
- lens paths between schema versions
- branch naming conventions
- catalogue-backed discovery of schemas and lenses
- copy-on-write updates into the current schema branch

## The Friendly Mental Model

Think of the runtime as keeping several table images alive at once.

Each image corresponds to:

- an environment such as `dev` or `prod`
- a concrete schema hash
- a user branch such as `main`

The composed branch name is what ties those parts together:

```text
{env}-{schemaHash8}-{userBranch}
```

So a query for "todos on main" is really asking for the correct schema-versioned branch view of that table.

## Schema Hashes

Every schema version gets a deterministic hash.

That gives the runtime a stable name for:

- branch routing
- catalogue storage
- lens source/target lookup
- server-mode query execution for multiple client versions

The exact hash algorithm is an implementation detail. The important architectural point is that schema identity is content-based rather than deployment-order-based.

## Lenses

Lenses describe how data moves between schema versions.

They are used in two directions:

- read path: older stored rows can be interpreted through a newer schema
- write path: updates to older rows are written back into the current schema branch

That lets the runtime preserve a simple external story:

- application code queries the schema it knows about
- the engine takes responsibility for translating older stored data when a valid lens path exists

## Schema Context

The Schema Manager maintains a runtime schema context that answers questions like:

- what is the current schema?
- which other schemas are reachable through live lens paths?
- which composed branches should this query target?
- which schemas are known but not yet fully activated?

This context is what the Query Manager consumes when it compiles a query or materializes a row.

## Catalogue Entries

Schemas and lenses replicate through the dedicated `catalogue` lane.

That means:

- user table rows use row histories + visible entries
- schema metadata uses catalogue entries
- both still reuse the same `row_format` machinery underneath

This separation keeps schema discovery explicit and prevents system metadata from pretending to be user table data.

Permissions ride alongside that catalogue state instead of being baked into the
structural schema snapshot. The runtime keeps:

- structural schemas for branch/lens resolution
- immutable permissions bundles keyed by object id
- a current permissions head that selects the active bundle for a schema hash

When the Schema Manager can resolve both the structural schema and the current
permissions bundle, it merges them into an authorization schema and installs
that into `QueryManager`.

In an edge/core deployment, catalogue authority is core-only. Schemas,
permissions, and migrations are published to the core server. Edge servers learn
those catalogue entries through server-to-server sync and install them locally
once they arrive. Edge admin catalogue publish endpoints reject writes instead
of proxying them upstream, so publication tools have one authoritative target.

That same catalogue lane carries permission changes. When the core receives a
new permissions head, connected edges receive the bundle/head pair through sync;
active edge subscriptions are re-filtered once the authorization schema changes.

## Client Mode and Server Mode

### Client mode

A client usually has one current schema baked into the app bundle. The Schema
Manager starts from that schema and keeps any reachable older schemas available
for reads.

If the client only boots with structural schema, it starts in
`PermissiveLocal`. If its runtime schema envelope or rehydrated catalogue state
includes a loaded permissions bundle, the manager switches the query layer into
`Enforcing`.

### Server mode

A server may learn schemas gradually from connected clients through catalogue
sync. It can then answer queries for several client schema hashes at once
without restarting or rebuilding the runtime from scratch.

Dynamic servers boot in fail-closed mode even before they have learned the
current permissions head. Once they receive a permissions head and its bundle,
they keep enforcing with that authorization schema. An empty loaded bundle is
still distinct from "no bundle loaded" and still means explicit grants only.

Core and edge servers use the same dynamic schema machinery, but with different
catalogue authority:

- core servers accept admin catalogue publishes and replicate catalogue state to
  connected edges
- edge servers receive catalogue state from core and wait when a requested
  schema or permission head has not arrived yet

This means an edge that sees row data before it has the matching schema or
authorization bundle does not guess. It holds the affected query/write work
until catalogue state catches up, then retries through the normal
SchemaManager/QueryManager process path.

The JS/native runtime schema wire payload now carries that loaded-bundle bit so
an empty loaded bundle stays distinguishable from a structural-schema-only boot.

## Copy-on-Write Updates

If a client updates a row that was originally stored on an older schema branch, the write path is intentionally simple:

1. load the row through the current schema view
2. apply the user's update in the current schema
3. write a new row batch entry on the current schema branch

The old stored row history remains intact. The new visible row is written as a fresh flat visible
record on the current schema branch.

## Why This Fits the Table-First Engine

The Schema Manager does not bolt versioning on top of unrelated storage. It works directly with the same pieces the rest of the runtime already uses:

- branch-aware visible rows
- row histories
- raw tables
- catalogue entries

That is why schema evolution can be described as "which table image should we read and how should we transform it?" rather than as a completely separate subsystem.

## Key Files

| File                                                | Purpose                                   |
| --------------------------------------------------- | ----------------------------------------- |
| `crates/jazz-tools/src/schema_manager/manager.rs`   | SchemaManager orchestration               |
| `crates/jazz-tools/src/schema_manager/context.rs`   | Live schema context and branch resolution |
| `crates/jazz-tools/src/schema_manager/lens.rs`      | Lens definitions and transforms           |
| `crates/jazz-tools/src/schema_manager/auto_lens.rs` | Auto-generated migration/lens helpers     |
| `crates/jazz-tools/src/catalogue.rs`                | Catalogue entry model                     |
| `crates/jazz-tools/src/query_manager/manager.rs`    | Query execution with schema context       |
