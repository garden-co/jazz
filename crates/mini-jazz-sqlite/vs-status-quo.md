# SQLite core vs status quo

This document captures comparisons between the current Jazz implementation and
the SQLite-backed design explored in [SPEC.md](./SPEC.md).

## History and edit metadata

> Current Jazz stores each logical row as row-batch history plus a separate
> visible-row projection. A concrete history entry is identified by
> `(row_id, branch_name, batch_id)` and contains reserved `_jazz_*` engine
> fields, row state, parent batch ids, durability/tier metadata, delete markers,
> provenance/edit metadata, and user columns. Current reads usually do not scan
> history; they load a compact `VisibleRowEntry` for `(branch_name, row_id)`.
> History and visible entries are stored in schema-qualified raw table instances,
> and exact routing uses system locator tables such as
> `__visible_row_table_locator` and `__history_row_batch_table_locator`.
> Storage also maintains Jazz-owned secondary indices separately from the
> visible/history payloads.

The SQLite design converts inserts, updates, and deletes into append-only
history tables for every high-level Jazz table. Main-branch current state is a
derived projection for fast ordinary reads, not a separate source of truth.

What is a separate persisted current-state area of the raw storage arenas today
becomes a close-to-covering derived index.

Current Jazz also has mechanisms for actual history truncation / hard deletion
when data must be physically removed rather than merely hidden by a delete
marker. The SQLite-core spec currently treats ordinary deletes and restores as
append-only history operations. Physical truncation, hard delete, retention
policy, and privacy/erasure semantics need a separate migration/product
decision rather than being assumed from ordinary delete behavior.

## Distributed transactions and branching

> Current Jazz has two write lifecycles. Direct writes are treated as
> one-member batches that become optimistically visible immediately, then later
> receive authoritative batch fate. Transactional writes stage
> `StoredRowBatch` entries as `StagingPending`, are sealed explicitly, and only
> become visible if the authority accepts the whole batch. Branches are carried
> through composed branch names and branch-local visible/history keys rather
> than through a global MVCC snapshot model. Conflict handling happens per row by
> recomputing the visible entry from row history/frontiers, using schema-declared
> merge strategies such as `lww` and `counter`. Batch fate is durable and
> batch-scoped; successful fate applies to the whole sealed batch, and rejection
> removes staged/conflicted rows from ordinary visibility.

The SQLite design collapses batches and transactions into one sealed
transaction concept. Transaction and branch start points are expressed with
compact dotted version vectors, and query lowering filters history through those
vectors.

## Sync and reconciliation

> Current Jazz sync is query-scoped. A client registers a desired query
> subscription; the upstream runtime records it, compiles a server-side query
> graph with the client's schema/session context, settles it against visible
> rows, sends the needed row batch entries, sends batch fate, and finally emits
> `QuerySettled` for the requested durability tier. The graph stays alive after
> the initial fill. Later local or remote row changes dirty relevant graph nodes;
> settling the graph computes which rows entered, changed, or left scope, and
> sync sends only affected rows/fate. Reconnect treats subscriptions as desired
> state: forwarded subscriptions are replayed upstream and scope is rebuilt.
> Transport code does not evaluate policies itself; the Query Manager owns
> filtering, ordering, policy checks, and sync-scope computation.

The SQLite design keeps query-scoped sync, but the upstream tier reruns lowered
SQL to capture all rows needed for the lower tier to reproduce the result
locally. Reconnect remains desired-state based.

## Multi-schema and migration lenses

> Current Jazz identifies each structural schema by content hash. User row
> history and visible rows are stored in schema-qualified raw table instances,
> and runtime branch names are composed from environment, schema hash, and user
> branch. The Schema Manager keeps known schemas, live schema sets, and lens
> paths. Queries are issued against the client's current schema view; when older
> stored rows are reachable, lenses translate table/column/value shapes on read.
> Writes to older rows are intentionally copy-on-write into the current schema
> branch. Schemas, lenses, and permission bundles live in catalogue state rather
> than ordinary user history. Servers may learn schemas dynamically from clients
> and enforce only once the matching permissions head/bundle is available.

The SQLite design stores each schema version as its own physical SQLite table.
Migration lenses lower translation work into reads, and writes through a lens
create versions in the writer's current schema.

## Deferred compatibility and migration notes

Migration from current Jazz data and Jazz 1 concepts is intentionally deferred
until the new core semantics are stable. The SQLite-backed design should avoid
gratuitously changing product-facing APIs and public row ids, but detailed
compatibility mapping is out of scope for the main spec.

Status-quo concepts that will eventually need a migration story include:

- grouped-write ids and row-batch history becoming sealed transactions
- `_jazz_*` physical fields becoming new lowering-specific system columns
- schema hashes, permission heads, and lens migration metadata
- composed branch names and prefixes becoming explicit branch/source context
- sync protocol compatibility windows

| Current term                    | New design direction                                        |
| ------------------------------- | ----------------------------------------------------------- |
| `DurabilityTier`                | delivery target and authority/edge/global observation tier  |
| `QuerySettled`                  | query settled signal                                        |
| `Db`                            | product API facade over query/write/sync plans              |
| `Session`                       | user/auth context plus separate policy and attribution mode |
| `VisibleRowEntry`               | current projection row                                      |
| `branch_name` / composed prefix | branch/source/schema context                                |
| `row_format`                    | physical row codec                                          |
| `_jazz_*`                       | physical system fields; new lowering currently prefers `j_` |
