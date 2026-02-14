# Architecture Specs

Jazz2 is a local-first distributed relational database. These specs document the architecture as implemented, with code references throughout.

Read the status-quo specs in the order below — each builds on the ones before it.

## Reading Order

### 1. Data Model

**[Object Manager](status-quo/object_manager.md)** — The foundational data layer. Objects are versioned using a git-like DAG (branches, commits). Every piece of data in the system is an object identified by its BLAKE3 content hash.

### 2. Storage

**[Storage](status-quo/storage.md)** — How objects and indices are persisted. The `Storage` trait provides synchronous access to objects and indices. `MemoryStorage` for tests and browser main thread, `SurrealKvStorage` for native durability, and `OpfsBTreeStorage` for WASM + OPFS durability. Also covers platform bindings (groove-napi, groove-wasm) and deployment topology.

### 3. Query Engine

**[Query Manager](status-quo/query_manager.md)** — Reactive SQL query graphs. Queries compile into a node pipeline: `IndexScanNode → Materialize → Filter → Sort → Limit → Output`. Mutations propagate through the graph incrementally. Subscriptions deliver deltas to callers.

**[Subgraph Sharing](status-quo/subgraph_sharing.md)** — How array subqueries (JOINs that return nested arrays) work internally. Each outer row gets its own subgraph instance compiled from a shared template. Documents the current recompile-per-binding approach and its performance characteristics.

### 4. Schema Evolution

**[Schema Manager](status-quo/schema_manager.md)** — Wraps the Query Manager with schema versioning. Schemas are content-addressed (BLAKE3 hash). Bidirectional lenses transform data between schema versions. Composed branch names (`{env}-{hash8}-{userBranch}`) isolate schemas from each other.

**[Schema Files](status-quo/schema_files.md)** — The developer-facing layer: SQL dialect for defining schemas, TypeScript DSL (`col.string()`, `col.ref()`), CLI build pipeline, schema diffing, and auto-lens generation.

### 5. Sync Protocol

**[Sync Manager](status-quo/sync_manager.md)** — Multi-client sync over a message-based protocol. Clients have roles (`User | Admin | Peer`) that determine write permissions. Downward sync is query-scoped — clients only receive data matching their subscriptions. Covers `PersistenceAck` and `QuerySettled` for durability guarantees.

**[Query/Sync Integration](status-quo/query_sync_integration.md)** — The bridge between queries and sync. When a client subscribes to a query, the Sync Manager tracks which objects contribute to it (`contributing_object_ids`). Inbound objects from sync are routed to the Query Manager, which re-evaluates affected queries and sends deltas back through sync.

### 6. Execution Model

**[Batched Tick Orchestration](status-quo/batched_tick_orchestration.md)** — How all the above pieces execute together. `RuntimeCore<S: Storage, Sch: Scheduler, Sy: SyncSender>` is the main entry point. `immediate_tick()` processes mutations synchronously; `batched_tick()` handles sync I/O. The scheduler trait abstracts platform-specific debouncing (native threads, WASM microtasks, test immediate).

### 7. Transport & Client

**[HTTP Transport](status-quo/http_transport.md)** — Wire protocol between clients and servers. Binary streaming over SSE (length-prefixed frames: `[4-byte u32 BE length][JSON]`). Single unified `/sync` POST endpoint. Auth via JWT (users), admin secret header, or backend secret.

**[TypeScript Client Codegen](status-quo/ts_client_codegen.md)** — Generates a typed TypeScript client (`schema/app.ts`) from schema definitions. Produces type-safe query builders with `.where()`, `.include()`, `.orderBy()`. Runtime: `createDb()` → async init, sync mutations, delta-aware subscriptions. Worker bridge manages WASM + OPFS in the browser.

### 8. Testing Philosophy

**[Making Tests More E2E](status-quo/making_tests_more_e2e.md)** — Why RuntimeCore is the primary correctness layer (not unit tests on internals). 20+ RuntimeCore tests use realistic 3-tier setups. Browser E2E tests exercise the full stack through Chromium + WASM + Worker + OPFS.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────┐
│  TypeScript Client (codegen'd app.ts)                   │
│  ├── QueryBuilder    (.where, .include, .orderBy)       │
│  ├── Db              (all, one, insert, subscribeAll)   │
│  └── Worker Bridge   (main thread ↔ dedicated worker)   │
├─────────────────────────────────────────────────────────┤
│  HTTP Transport (binary SSE + /sync POST)               │
├─────────────────────────────────────────────────────────┤
│  RuntimeCore<Storage, Scheduler, SyncSender>            │
│  ├── SchemaManager   (versioning, lenses, catalogue)    │
│  │   └── QueryManager (reactive query graphs)           │
│  ├── SyncManager     (roles, scoped sync, ack/settled)  │
│  ├── ObjectManager   (DAG: branches, commits)           │
│  └── Storage         (SurrealKV native / opfs-btree WASM+OPFS) │
└─────────────────────────────────────────────────────────┘
```

## TODO Specs

Remaining work items and future designs live in [`specs/todo/`](todo/). Notable:

- **[Sharding Design Sketch](todo/sharding_design_sketch.md)** — Future architecture for distributing data across storage shards (nothing implemented)
- **[Storage](todo/storage.md)** — Multi-tab leader election and browser E2E verification
- **[TypeScript Client Codegen](todo/ts_client_codegen.md)** — Example app relations demo, React/Vue bindings
