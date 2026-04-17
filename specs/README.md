# Architecture Specs

Jazz is a local-first relational database built around a table-first storage engine.

The easiest way to picture the runtime is:

- application code talks about tables, rows, filters, and subscriptions
- the engine stores those rows in raw tables
- every stored row is one flat `row_format` record with reserved `_jazz_*` columns plus the application columns
- current reads come from compact visible entries
- history, replay, sync, and durability all speak the same row-version language

These docs describe the system as it works today.

## Reading Order

### 1. Table-First Foundation

**[Row Histories](status-quo/row_histories.md)** — The core mental model. Explains logical rows, row versions, visible entries, reserved `_jazz_*` columns, and why ordinary reads are "visible-region first".

**[Storage](status-quo/storage.md)** — The synchronous storage boundary beneath the runtime. Covers raw tables, indices, row locators, catalogue rows, row-history persistence, and the current durable backends.

### 2. Querying Current State

**[Query Manager](status-quo/query_manager.md)** — Reactive query graphs over current relational state. Covers index-first planning, materialization, subscriptions, policies, and branch/schema-aware execution.

**[Subgraph Sharing](status-quo/subgraph_sharing.md)** — Deeper dive on nested array subqueries and how the current graph engine reuses compiled subgraph templates.

### 3. Schema and Metadata

**[Schema Manager](status-quo/schema_manager.md)** — Multi-version schema handling. Covers schema hashes, lenses, live schema sets, copy-on-write updates, and the catalogue lane.

**[Schema Files](status-quo/schema_files.md)** — The developer-facing schema workflow: `schema.ts`, `permissions.ts`, migrations, and CLI commands.

### 4. Sync and Runtime Orchestration

**[Sync Manager](status-quo/sync_manager.md)** — Query-scoped sync, role-aware writes, row-version replication, and delivery/settled signals across worker, edge, and global tiers.

**[Query/Sync Integration](status-quo/query_sync_integration.md)** — How query subscriptions become sync scopes, how initial snapshots are replayed, and how live row changes flow back into subscription updates.

**[Batched Tick Orchestration](status-quo/batched_tick_orchestration.md)** — How `RuntimeCore` coordinates local work, queued sync traffic, and storage flushing without making local writes feel asynchronous.

### 5. Transport and Adapters

**[HTTP Transport](status-quo/http_transport.md)** — The concrete `/sync` + `/events` protocol used by clients and servers.

**[Browser Adapters](status-quo/browser_adapters.md)** — How browser apps are split between an in-memory main-thread runtime and a persistent worker runtime backed by OPFS.

**[Life of a Subscription](status-quo/life_of_a_subscription.md)** — A walkthrough of what actually happens when a browser app calls `db.all(...)` or `db.subscribeAll(...)`.

### 6. App-Facing Surface

**[App Surface](status-quo/ts_client.md)** — The TypeScript view of the system: `schema.ts`, typed `app` handles, `createDb(...)`, and reactive query APIs.

## Architecture Sketch

```text
Typed App + Db APIs
  -> Query builders, inserts, updates, subscriptions
  -> RuntimeCore<Storage, Scheduler, SyncSender>
     -> SchemaManager
        -> QueryManager
     -> SyncManager
     -> MonotonicClock
     -> Storage
        -> raw tables and indices
        -> row locators and metadata
        -> visible entries + row histories
        -> catalogue entries
```

If you want one sentence to hold onto while reading the rest:

> Jazz stores application data as flat relational rows, and the engine-managed columns needed for history, branching, visibility, sync, and durability live in that same row format instead of in a separate conceptual universe.

## TODO Specs

Design work that has not landed yet lives in [`specs/todo/`](todo/). Useful entry points:

- **[Polymorphic Contract Tables and References](todo/a_mvp/polymorphic_contract_tables_and_references.md)** — nominal contract/variant tables with contract refs and coordinated variant writes
- **[Opt-In Transactions with Replayable Reconciliation](todo/a_mvp/opt_in_transactions_replayable_reconciliation.md)** — transaction-shaped write semantics on top of the row-history foundation
- **[Protocol and Storage Version Tags](todo/b_launch/protocol_and_storage_version_tags.md)** — explicit format/versioning strategy for on-disk state and wire payloads
- **[Sharding Design Sketch](todo/b_launch/sharding_design_sketch.md)** — future distribution work
- **[Built-in File Storage](todo/a_mvp/built_in_file_storage.md)** — first-class file/blob storage using the same relational substrate
- **[Weak Tests](todo/a_mvp/weak_tests.md)** — remaining test-hardening follow-up
