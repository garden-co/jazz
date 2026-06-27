# Architecture Specs

Jazz is a local-first relational database built around a table-first storage engine.

The easiest way to picture the runtime is:

- application code talks about tables, rows, filters, and subscriptions
- the engine stores those rows in raw tables
- every stored row is one flat `row_format` record with reserved `_jazz_*` columns plus the application columns
- current reads come from compact visible entries
- history, replay, sync, and durability all speak the same row-batch language

These docs are being migrated with the alpha graft. The authoritative engine
now lives in the first-class Rust crates under `crates/`. The
[`legacy-alpha-status-quo/`](legacy-alpha-status-quo/) directory is a historical
archive of deleted or legacy `jazz-tools` alpha internals. Treat those notes as
migration context only, not active architecture.

## Reading Order

### 1. Historical Alpha Table-First Notes

**[Row Histories](legacy-alpha-status-quo/row_histories.md)** — Historical alpha notes on logical rows, row batch entries, visible entries, and reserved `_jazz_*` columns.

**[Batches](legacy-alpha-status-quo/batches.md)** — Historical alpha notes on direct/transactional batches, `BatchId`, storage keys, replayable settlements, seal flow, and old Rust/TS batch APIs.

**[Storage](legacy-alpha-status-quo/storage.md)** — Historical alpha notes on the old synchronous storage boundary, raw tables, indices, catalogue rows, row-history persistence, and durable backends.

### 2. Historical Alpha Querying Notes

**[Query Manager](legacy-alpha-status-quo/query_manager.md)** — Legacy alpha reactive query graphs over relational state. Use this for public query/API vocabulary and migration context; core/Groove should own new execution semantics.

**[Subgraph Sharing](legacy-alpha-status-quo/subgraph_sharing.md)** — Historical alpha notes on nested array subqueries and old graph-engine subgraph templates.

### 3. Schema and Metadata

**[Schema Manager](legacy-alpha-status-quo/schema_manager.md)** — Historical alpha notes on multi-version schema handling, schema hashes, lenses, live schema sets, copy-on-write updates, and the catalogue lane.

**[Schema Files](legacy-alpha-status-quo/schema_files.md)** — Historical alpha notes on the developer-facing schema workflow: `schema.ts`, `permissions.ts`, migrations, and CLI commands.

### 4. Sync and Runtime Orchestration

**[Sync Manager](legacy-alpha-status-quo/sync_manager.md)** — Legacy alpha query-scoped sync and row-batch replication. New network sync should stay on core wire frames.

**[Query/Sync Integration](legacy-alpha-status-quo/query_sync_integration.md)** — Historical alpha notes on how query subscriptions became sync scopes, how initial snapshots were replayed, and how live row changes flowed back into subscription updates.

**[Batched Tick Orchestration](legacy-alpha-status-quo/batched_tick_orchestration.md)** — Historical notes for the deleted legacy alpha runtime scheduler.

### 5. Transport and Adapters

**[HTTP Transport](legacy-alpha-status-quo/http_transport.md)** — Historical alpha notes on app-scoped HTTP/admin routes and the old WebSocket transport. The old alpha websocket transport has been deleted.

**[Browser Adapters](legacy-alpha-status-quo/browser_adapters.md)** — Historical alpha notes on the old browser split between an in-memory main-thread runtime and a persistent OPFS-backed worker runtime.

**[Life of a Subscription](legacy-alpha-status-quo/life_of_a_subscription.md)** — Historical alpha walkthrough for old `db.all(...)` and `db.subscribeAll(...)` subscription plumbing.

### 6. App-Facing Surface

**[App Surface](legacy-alpha-status-quo/ts_client.md)** — Historical alpha TypeScript API notes for `schema.ts`, typed `app` handles, `createDb(...)`, and reactive query APIs.

## Architecture Sketch

```text
Typed App + Db APIs
  -> Query builders, inserts, updates, subscriptions
  -> core Db / Node
     -> Groove incremental queries
     -> core wire frames
     -> core storage
  -> jazz-tools facade/scaffolding still being removed
     -> public schema/query types
     -> admin catalogue routes
     -> old QueryManager/SyncManager/storage surfaces being hollowed or ported
```

If you want one sentence to hold onto while reading the rest:

> Jazz stores application data as flat relational rows, and the engine-managed columns needed for history, branching, visibility, sync, and durability live in that same row format instead of in a separate conceptual universe.

## TODO Specs

Design work that has not landed yet lives in [`specs/todo/`](todo/). Useful entry points:

- **[Commit Author as Principal + Created-By Permissions](todo/a_mvp/commit_author_principal_created_by_permissions.md)** — provenance-driven creator semantics and permission hooks
- **[Opt-In Transactions with Replayable Reconciliation](todo/a_mvp/opt_in_transactions_replayable_reconciliation.md)** — remaining strict-visibility and reconciliation design work on top of the now-landed batch model
- **[Globally Consistent Transactions](todo/b_launch/globally_consistent_transactions.md)** — minimally viable authority-backed transactional correctness
- **[Protocol and Storage Version Tags](todo/b_launch/protocol_and_storage_version_tags.md)** — explicit format/versioning strategy for on-disk state and wire payloads
- **[Sharding Design Sketch](todo/b_launch/sharding_design_sketch.md)** — future distribution work
