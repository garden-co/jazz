# Query Manager — Status Quo

This is where SQL lives. The [Object Manager](object_manager.md) stores versioned objects; the Query Manager interprets those objects as table rows and provides SQL queries over them.

The key property is **reactivity**: queries aren't one-shot. When you subscribe to "all todos where done=false", the query stays live. As data changes — local inserts, sync updates from other clients — the query graph re-evaluates incrementally and emits deltas (added/removed/updated rows). This is what makes local-first UIs work: the UI subscribes once and stays current automatically.

Queries compile into a pipeline of nodes (`IndexScan → Materialize → Filter → Sort → Output`) that process changes incrementally. The [Sync Manager](sync_manager.md) uses query results to determine which objects to send to clients — see [Query/Sync Integration](query_sync_integration.md). The [Schema Manager](schema_manager.md) wraps this layer to add schema versioning and cross-version queries.

## Architecture Layers

```
┌─────────────────────────────────────────────────────────┐
│                    Application                          │
│              (Value for input/output)                   │
├─────────────────────────────────────────────────────────┤
│                   QueryManager                          │
│  ┌─────────────┐  ┌──────────┐  ┌───────────────────┐  │
│  │ Row Codec   │  │ Indices  │  │ Query Graph       │  │
│  │ encode at   │  │ (source  │  │ (binary rows      │  │
│  │ boundary    │  │ nodes)   │  │  throughout)      │  │
│  └─────────────┘  └──────────┘  └───────────────────┘  │
├─────────────────────────────────────────────────────────┤
│                   SyncManager                           │
├─────────────────────────────────────────────────────────┤
│                  ObjectManager                          │
│            (+ global object subscription)               │
└─────────────────────────────────────────────────────────┘
```

> `crates/groove/src/query_manager/manager.rs:226-266`

## Core Design Decisions

| Decision               | Details                                                                       | Why                                                                                   |
| ---------------------- | ----------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| Row = Object           | Each row is a separate Jazz object; ObjectId = primary key                    | Rows inherit versioning, sync, and conflict resolution from the object layer for free |
| Row format             | Fixed fields first, then variable offsets, nullable 1-byte prefix             | Comparison without deserialization — sort and filter operate on bytes                 |
| Binary throughout      | `Value` only at API boundary; internally `&[u8]` with `RowDescriptor`         | Avoids allocation and type dispatch in the hot path                                   |
| Index-first            | No table scans; every query starts with an index (`_id` for unfiltered)       | Predictable performance; the `_id` index doubles as the row manifest                  |
| Auto-index all columns | Every column gets a single-column index (zero-config)                         | Local-first databases are small enough that index overhead is negligible              |
| All indices persisted  | Via `Storage` trait (MemoryStorage, FjallStorage, OpfsBTreeStorage)           | Cold start loads indices, not all row data — fast startup                             |
| No index rebuild       | Incremental maintenance only; missing index = error                           | Indices are always consistent with data; no expensive rebuild path                    |
| TupleDelta throughout  | Unified delta type for progressive materialization                            | Each node transforms deltas without knowing what's upstream or downstream             |
| Branch-aware           | Indices keyed by `(table, column, branch)`; queries specify target branch(es) | Schema versions use different branches — queries must address the right one           |

## The `_id` Index as Row Manifest

The `_id` index for each table is the authoritative list of row ObjectIds. There's no separate "table of contents" — the index IS the manifest. This means a table with no rows has no `_id` index entries, and discovering all rows in a table is just an `_id` index scan.

**Durability** of a new row requires persisting BOTH the row object AND all index updates (`_id` + column indices). These happen atomically within a single `Storage` call sequence.

**Cold start**: Load `_id` index → discover ObjectIds → load column indices → lazy-load row objects on demand via query. This means startup time is proportional to index size, not total data size.

> `crates/groove/src/query_manager/manager.rs:534-549` (row_is_indexed checks \_id)

## Index Storage

Indices are abstracted behind the `Storage` trait — QueryManager never deals with pages or B-tree internals directly.

- `Storage::index_insert()`, `index_remove()`, `index_lookup()`, `index_range()`, `index_scan_all()`
- `MemoryStorage`: HashMap<IndexKey, BTreeMap<encoded_value, HashSet<ObjectId>>>
- `FjallStorage`: native durable storage for server/CLI/client
- `OpfsBTreeStorage`: durable OPFS storage for browser workers

> `crates/groove/src/storage/mod.rs:67-195` (trait), `215-310` (MemoryStorage impl)

`ScanCondition` uses `Value` directly: `All`, `Eq(Value)`, `Range { min: Bound<Value>, max: Bound<Value> }`.

> `crates/groove/src/query_manager/index/mod.rs`

## Query Graph Architecture

### Tuple Model

```
TupleElement: Id(ObjectId) | Row { id, content, commit_id }
Tuple: Vec<TupleElement>  — Hash/Eq based on IDs only
TupleDelta: { added, removed, updated }
```

> `crates/groove/src/query_manager/types.rs`

### Node Traits

| Trait           | Purpose                  | Implementations                                                                                                   |
| --------------- | ------------------------ | ----------------------------------------------------------------------------------------------------------------- |
| `SourceNode`    | Read from external state | IndexScanNode                                                                                                     |
| `TransformNode` | Merge tuple sets         | UnionNode                                                                                                         |
| `RowNode`       | Process TupleDelta       | MaterializeNode, FilterNode, SortNode, LimitOffsetNode, JoinNode, ArraySubqueryNode, OutputNode, PolicyFilterNode |

> `crates/groove/src/query_manager/graph_nodes/mod.rs:36-83`

### Graph Pipeline

Single-table: `IndexScan → [Union] → Materialize → [Filter] → [Sort] → [LimitOffset] → Output`

Join: `IndexScan(left) → Materialize → JoinNode ← index lookup(right) → Materialize → [Filter] → Output`

All nodes receive explicit branch names in constructor — no implicit "main" default.

> `crates/groove/src/query_manager/graph.rs:229-661` (compile_with_schema_context)

### settle() Method

`settle(storage, row_loader)` processes the graph in topological order — source nodes first, output last. Each node transforms its input delta and passes results downstream. Row objects are loaded on demand via the `row_loader` callback (lazy, not eagerly cached). This means a query that filters on an indexed column never loads rows that don't match.

> `crates/groove/src/query_manager/graph.rs:1177-1400+`

## Query API

Builder pattern with chaining:

| Method                                | Purpose                                |
| ------------------------------------- | -------------------------------------- |
| `.branch()` / `.branches()`           | Target branch(es)                      |
| `.filter_eq/ne/lt/le/gt/ge/between()` | Conditions                             |
| `.order_by()` / `.order_by_desc()`    | Sorting                                |
| `.limit()` / `.offset()`              | Pagination                             |
| `.select()`                           | Column projection                      |
| `.alias()`                            | Table aliasing                         |
| `.join()` / `.on()`                   | Equi-joins                             |
| `.with_array()`                       | Correlated array subqueries (nestable) |
| `.include_deleted()`                  | Include soft-deleted rows              |
| `.build()`                            | Produce `Query` struct                 |

> `crates/groove/src/query_manager/query.rs:400-650`

## Deletion Semantics

| Type        | Content   | Metadata       | `_id_deleted` | Undeletable | Authoritative     |
| ----------- | --------- | -------------- | ------------- | ----------- | ----------------- |
| Soft Delete | Preserved | `delete: soft` | Added         | Yes         | No                |
| Hard Delete | Empty     | `delete: hard` | Removed       | No          | Yes (always wins) |

- `_id` index: live rows only
- `_id_deleted` index: soft-deleted rows with preserved content
- `include_deleted()` queries both

> `crates/groove/src/query_manager/manager.rs:534-598` (row_is_indexed, row_is_deleted, is_hard_deleted)

## Policy Evaluation (ReBAC)

Policies evaluated via PolicyGraphs for complex clauses (EXISTS, INHERITS). Session propagates through multi-tier sync.

> `crates/groove/src/query_manager/policy.rs`, `policy_graph.rs`, `graph_nodes/policy_filter.rs`

## Dynamic Schema Context

QueryManager supports dynamic schema activation without recreation — preserves active subscriptions and indices.

- `set_current_schema()` initializes once
- `add_live_schema()` / `register_lens()` mark subscriptions for recompile
- Branch names: `{env}-{hash8}-{userBranch}` (e.g., `dev-a1b2c3d4-main`)
- Queries without explicit `.branch()` auto-expand from schema context
- Pending row buffer: rows on unknown branches buffered until schema activates

> `crates/groove/src/query_manager/manager.rs:276-427`

## Explicit Context Execution

Two modes:

- **Implicit** (`execute(query)`) — uses manager's schema context
- **Explicit** (`execute_with_explicit_context(query, schema, context)`) — for servers

> `crates/groove/src/query_manager/manager.rs`

## Server Subscriptions

`ServerQuerySubscription` tracks: query, graph, session, resolved branches, last_scope (for change detection), needs_recompile flag.

> `crates/groove/src/query_manager/manager.rs:188-204`

## File Structure

```
crates/groove/src/query_manager/
├── mod.rs, manager.rs, manager_tests.rs
├── types.rs          # Value, ColumnType, RowDescriptor, Tuple, TupleDelta, Schema, SchemaHash
├── encoding.rs       # encode/decode at boundary
├── query.rs          # Query, QueryBuilder, Condition, JoinSpec, ArraySubquerySpec
├── graph.rs          # QueryGraph: compile, settle, topo_sort
├── session.rs, policy.rs, policy_graph.rs, rebac_tests.rs
├── index/mod.rs      # ScanCondition enum
└── graph_nodes/      # index_scan, union, materialize, filter, sort, limit_offset,
                      # output, alias, project, join, subgraph, array_subquery,
                      # policy_filter, exists_output
```

## Error Types

Key variants: `TableNotFound`, `ColumnCountMismatch`, `EncodingError`, `ObjectNotFound`, `QueryCompilationError`, `IndexError`, `RowNotDeleted`, `RowAlreadyDeleted`, `RowHardDeleted`, `PolicyDenied`, `UnknownSchema`.

> `crates/groove/src/query_manager/manager.rs:26-50`
