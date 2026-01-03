# Incremental Query System

## Overview

The incremental query system replaces the current "re-evaluate everything on change" reactive query mechanism with a computation graph that propagates deltas efficiently. When a row changes, only affected parts of the query are re-evaluated, and subscribers receive fine-grained change notifications.

## Goals

1. **Incremental evaluation** - Only recompute what's affected by a change
2. **Delta propagation** - Changes flow as "row added/removed/updated", not full result sets
3. **Early cutoff** - If a node's output doesn't change, stop propagation
4. **Lazy initialization** - Graphs populate on first access, not at creation
5. **Reusable fragments** - Future: share common subgraphs across queries

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      IncrementalQuery                            │
│   (user-facing handle with subscribe/get methods)               │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                       GraphRegistry                              │
│   - Manages all active QueryGraphs                               │
│   - Routes row changes to relevant graphs                        │
│   - Owns shared RowCache                                         │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                        QueryGraph                                │
│   - DAG of QueryNodes in topological order                       │
│   - Propagates deltas through nodes                              │
│   - Tracks initialization state                                  │
└─────────────────────────────────────────────────────────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        ▼                       ▼                       ▼
┌───────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  TableScan    │     │     Filter      │     │     Output      │
│ (source node) │     │ (predicate)     │     │ (terminal)      │
└───────────────┘     └─────────────────┘     └─────────────────┘
```

## Core Types

### RowDelta

Represents a change to a single row:

```rust
/// Reference to prior row state via commit graph
pub struct PriorState {
    /// Frontier commit IDs before the write (empty if row was just created)
    pub prior_tips: Vec<CommitId>,
}

/// A change to a single row
pub enum RowDelta {
    /// Row was inserted
    Added(Row),

    /// Row was deleted
    Removed {
        id: ObjectId,
        prior: PriorState,
    },

    /// Row was updated
    Updated {
        id: ObjectId,
        new: Row,
        prior: PriorState,
    },
}
```

**Key insight**: The `prior` field contains commit tips, not the actual old row data. Old values can be looked up on-demand from the commit graph when needed (e.g., for filter evaluation). This avoids copying row data for every update.

### DeltaBatch

A collection of row changes:

```rust
pub struct DeltaBatch {
    deltas: Vec<RowDelta>,
}

impl DeltaBatch {
    pub fn added(row: Row) -> Self;
    pub fn updated(id: ObjectId, new: Row, prior_tips: Vec<CommitId>) -> Self;
    pub fn removed(id: ObjectId, prior_tips: Vec<CommitId>) -> Self;
    pub fn is_empty(&self) -> bool;
    pub fn compact(&mut self);  // Remove redundant changes (add+remove = nothing)
}
```

### Predicate

Filter conditions for query evaluation:

```rust
pub enum Predicate {
    True,
    False,
    Eq { column: String, value: Value },
    Ne { column: String, value: Value },
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
}

impl Predicate {
    pub fn matches(&self, row: &Row, schema: &TableSchema) -> bool;
    pub fn and(self, other: Predicate) -> Predicate;
    pub fn or(self, other: Predicate) -> Predicate;
}
```

### RowCache

Shared cache of row data, accessible to all graphs:

```rust
pub struct RowCache {
    /// table -> row_id -> cached Row (None = confirmed deleted)
    rows: HashMap<String, HashMap<ObjectId, Option<Row>>>,
}

impl RowCache {
    pub fn get(&self, table: &str, id: ObjectId) -> Option<Option<&Row>>;
    pub fn insert(&mut self, table: &str, row: Row);
    pub fn mark_deleted(&mut self, table: &str, id: ObjectId);
    pub fn invalidate(&mut self, table: &str, id: ObjectId);
}
```

### QueryNode

Nodes in the computation graph:

```rust
pub struct NodeId(u32);

pub enum QueryNode {
    /// Source: all row IDs in a table
    TableScan {
        table: String,
        cached_ids: HashSet<ObjectId>,
    },

    /// Source: row IDs from reverse index lookup (future)
    IndexLookup {
        table: String,
        index_key: IndexKey,
        target_id: ObjectId,
        cached_ids: HashSet<ObjectId>,
    },

    /// Transform: filter rows by predicate
    Filter {
        table: String,
        input: NodeId,
        predicate: Predicate,
        cached_ids: HashSet<ObjectId>,
    },

    /// Terminal: marks the output of the graph
    Output {
        table: String,
        input: NodeId,
    },
}
```

Each node that filters/transforms maintains a `cached_ids` set representing which row IDs are currently in its output. This enables:
- Fast output collection (just look up cached IDs in RowCache)
- Efficient delta evaluation (check if ID was/is in set)

### QueryGraph

The computation graph itself:

```rust
pub enum GraphState {
    Uninitialized,  // Will load on first access
    Initializing,   // Currently loading
    Ready,          // Ready for incremental updates
}

pub struct QueryGraph {
    id: GraphId,
    state: GraphState,
    table: String,
    schema: TableSchema,
    nodes: Vec<QueryNode>,  // Topological order
    node_indices: HashMap<NodeId, usize>,
    output_node: NodeId,
}

impl QueryGraph {
    /// Get current output, initializing lazily if needed
    pub fn get_output(&mut self, cache: &mut RowCache, db: &DatabaseState) -> Vec<Row>;

    /// Process a row change, return output delta
    pub fn process_change(
        &mut self,
        delta: RowDelta,
        cache: &mut RowCache,
        db: &DatabaseState,
    ) -> DeltaBatch;
}
```

### QueryGraphBuilder

Programmatic API for constructing graphs:

```rust
pub struct QueryGraphBuilder {
    table: String,
    schema: TableSchema,
    nodes: Vec<QueryNode>,
    next_id: u32,
}

impl QueryGraphBuilder {
    pub fn new(table: impl Into<String>, schema: TableSchema) -> Self;
    pub fn table_scan(&mut self) -> NodeId;
    pub fn index_lookup(&mut self, column: &str, target_id: ObjectId) -> NodeId;
    pub fn filter(&mut self, input: NodeId, predicate: Predicate) -> NodeId;
    pub fn output(self, input: NodeId, graph_id: GraphId) -> QueryGraph;
}
```

**Example usage:**

```rust
let schema = db.get_table("users").unwrap();
let mut builder = QueryGraphBuilder::new("users", schema);

// SELECT * FROM users WHERE active = true
let scan = builder.table_scan();
let filter = builder.filter(scan, Predicate::eq("active", Value::Bool(true)));
let graph = builder.output(filter, GraphId(1));
```

### GraphRegistry

Manages all active query graphs:

```rust
pub struct GraphRegistry {
    queries: RwLock<HashMap<GraphId, RegisteredQuery>>,
    table_index: RwLock<HashMap<String, Vec<GraphId>>>,
    cache: RwLock<RowCache>,
    next_graph_id: RwLock<u64>,
}

impl GraphRegistry {
    pub fn register(&self, graph: QueryGraph) -> GraphId;
    pub fn unregister(&self, id: GraphId);
    pub fn subscribe(&self, graph_id: GraphId, callback: OutputCallback) -> Option<ListenerId>;
    pub fn unsubscribe(&self, graph_id: GraphId, listener_id: ListenerId) -> bool;
    pub fn get_output(&self, graph_id: GraphId, db: &DatabaseState) -> Option<Vec<Row>>;
    pub fn notify_row_change(&self, table: &str, delta: RowDelta, db: &DatabaseState);
}
```

### IncrementalQuery

User-facing handle:

```rust
pub struct IncrementalQuery {
    graph_id: GraphId,
    registry: Arc<GraphRegistry>,
    db_state: Arc<DatabaseState>,
}

impl IncrementalQuery {
    /// Get current results (lazy init on first call)
    pub fn get(&self) -> Vec<Row>;

    /// Subscribe to deltas (Added/Removed/Updated)
    pub fn subscribe(&self, callback: impl Fn(&DeltaBatch)) -> ListenerId;

    /// Subscribe to full row set (convenience, less efficient)
    pub fn subscribe_rows(&self, callback: impl Fn(Vec<Row>)) -> ListenerId;

    /// Unsubscribe
    pub fn unsubscribe(&self, id: ListenerId) -> bool;
}

impl Drop for IncrementalQuery {
    fn drop(&mut self) {
        // Automatically unregisters the graph
    }
}
```

## Delta Propagation

When a row changes:

1. **Database captures prior state**: Before writing, capture `frontier(row_id, "main")` as `prior_tips`
2. **Database writes**: Perform the actual insert/update/delete
3. **Database notifies registry**: `registry.notify_row_change(table, delta, db)`
4. **Registry fans out**: For each graph subscribed to this table:
   - Update RowCache with new value
   - Call `graph.process_change(delta, cache, db)`
   - If output delta is non-empty, notify subscribers

### Node Evaluation

Each node processes input deltas and produces output deltas:

**TableScan**: Passes through, updating its `cached_ids` set

**Filter**:
```
For each delta:
  Added(row):
    if predicate.matches(row):
      cached_ids.insert(row.id)
      emit Added(row)

  Removed { id, prior }:
    if cached_ids.remove(id):
      emit Removed { id, prior }

  Updated { id, new, prior }:
    was_match = cached_ids.contains(id)
    is_match = predicate.matches(new)

    match (was_match, is_match):
      (false, true)  -> cached_ids.insert(id); emit Added(new)
      (true, false)  -> cached_ids.remove(id); emit Removed { id, prior }
      (true, true)   -> emit Updated { id, new, prior }
      (false, false) -> (no output - early cutoff)
```

**Output**: Passes through unchanged

### Early Cutoff

If a node produces an empty `DeltaBatch`, propagation stops for that path. This happens when:
- A filter receives an update to a row that doesn't match (and didn't match before)
- A filter receives a removal for a row that wasn't in its set

## Database Integration

### DatabaseState Changes

```rust
pub struct DatabaseState {
    // ... existing fields ...
    graph_registry: Arc<GraphRegistry>,
}
```

### New Method

```rust
impl Database {
    pub fn incremental_query(&self, sql: &str) -> Result<IncrementalQuery, DatabaseError>;
}
```

### Mutation Hooks

Insert, update, and delete capture prior tips and notify the registry:

```rust
// In insert():
let row = Row::new(row_id, values);
self.state.graph_registry.notify_row_change(
    table,
    RowDelta::Added(row),
    self.state.as_ref(),
);

// In update():
let prior_tips = self.state.node.frontier(id, "main").unwrap_or_default();
// ... perform update ...
self.state.graph_registry.notify_row_change(
    table,
    RowDelta::Updated { id, new: new_row, prior: PriorState::new(prior_tips) },
    self.state.as_ref(),
);

// In delete():
let prior_tips = self.state.node.frontier(id, "main").unwrap_or_default();
// ... perform delete ...
self.state.graph_registry.notify_row_change(
    table,
    RowDelta::Removed { id, prior: PriorState::new(prior_tips) },
    self.state.as_ref(),
);
```

## Module Structure

```
groove/src/sql/query_graph/
├── mod.rs          - Public API exports
├── delta.rs        - RowDelta, PriorState, DeltaBatch
├── predicate.rs    - Predicate enum and matching
├── cache.rs        - RowCache
├── node.rs         - NodeId, QueryNode, evaluation logic
├── graph.rs        - QueryGraph, GraphState, GraphId
├── builder.rs      - QueryGraphBuilder
└── registry.rs     - GraphRegistry, RegisteredQuery
```

## Future Work

### Batched Propagation

For server sync scenarios where many changes arrive at once:

```rust
impl Database {
    /// Start a batch - changes are queued, not propagated
    pub fn begin_batch(&self) -> BatchHandle;

    /// Flush batch - propagate all changes, fire callbacks once
    pub fn flush_batch(&self, handle: BatchHandle);
}
```

This allows coalescing multiple changes before propagation, reducing callback overhead during bulk sync.

### Shared Subgraphs

Multiple queries with common prefixes (e.g., same table scan, same initial filter) could share those nodes:

```
Query A: SELECT * FROM users WHERE active = true AND role = 'admin'
Query B: SELECT * FROM users WHERE active = true AND age > 30

Shared: TableScan(users) -> Filter(active = true)
           ↓                        ↓
    Filter(role='admin')    Filter(age > 30)
           ↓                        ↓
       Output A                 Output B
```

### JOIN Support

Extend with JoinNode that combines two row sets:

```rust
QueryNode::Join {
    left: NodeId,
    right: NodeId,
    left_table: String,
    right_table: String,
    condition: JoinCondition,
    cached_pairs: HashSet<(ObjectId, ObjectId)>,
}
```

### Index-Aware Sources

Use RefIndex for efficient starting points:

```rust
// SELECT * FROM posts WHERE author = ?
// Instead of: TableScan -> Filter
// Use: IndexLookup(posts.author, user_id) -> Output
```

### ReBAC Integration

Permission constraints merged at graph construction time:

```rust
// User query
let user_pred = Predicate::eq("project", project_id);

// ReBAC constraint
let rebac_pred = Predicate::or(vec![
    Predicate::eq("owner", current_user),
    Predicate::in("project", user_projects),
]);

// Combined - planner decides optimal order
let combined = user_pred.and(rebac_pred);
```

The planner should recognize which predicates are more selective and order filters accordingly.

## Implementation Phases

### Phase 1: Core Types
- `delta.rs` - RowDelta, PriorState, DeltaBatch
- `predicate.rs` - Predicate with matches()
- `cache.rs` - RowCache
- `node.rs` - QueryNode with evaluate()
- `graph.rs` - QueryGraph with process_change()
- `builder.rs` - QueryGraphBuilder

### Phase 2: Integration
- `registry.rs` - GraphRegistry
- Database modifications (incremental_query, mutation hooks)
- IncrementalQuery handle

### Phase 3: Testing
- Unit tests for each node type
- Delta propagation tests
- Early cutoff verification
- Lazy initialization tests
- Integration tests with Database
