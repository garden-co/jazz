# New Jazz Implementation Plan

## Phase 1: Core Data Structures

### Objects with Commit Graph

**Approach**: Start with naive implementation using explicit IDs and uncompressed storage everywhere. Build comprehensive test suite first, then optimize later.

- [x] Implement Object with commit graph structure
- [x] Implement Commit with content, parents, metadata
- [x] Implement Branch with frontier tracking
- [x] Content hashing for CommitID (BLAKE3)
- [x] UUIDv7 generation for Object IDs
- [x] Tests for commit graph operations (add commit, find frontier, etc.)
- [x] Tests for branching (create branch, list branches)
- [x] Tests for merging scenarios (sequential, concurrent tips)
- [x] LCA (Lowest Common Ancestor) computation
- [x] MergeStrategy trait with LastWriterWins implementation
- [x] Branch merging (merge_branches)

**Future optimizations** (after tests are solid):
- [ ] Delta encoding for commits
- [ ] Relative pointers instead of explicit hashes
- [ ] Compression
- [ ] FastCDC for content-defined chunking (currently using fixed-size chunking)

### Storage Abstraction
- [x] ContentRef enum (Inline ≤1KB, Chunked >1KB)
- [x] ChunkHash type (BLAKE3)
- [x] ContentStore trait (async get/put/has chunk)
- [x] CommitStore trait (async commit operations, list_commits walks full history)
- [x] Environment trait (combines ContentStore + CommitStore + Send + Sync + Debug)
- [x] MemoryEnvironment (full in-memory implementation for testing)
- [x] MemoryContentStore (legacy content-only store for backwards compatibility)
- [x] Sync read/write methods (write_sync, read_sync) - require explicit branch
- [x] Async read/write methods (write, read) - require explicit branch
- [x] Streaming read/write methods (write_stream, read_stream) - require explicit branch

### Listeners (Local Subscriptions)
- [x] Synchronous callback system (replaced futures-signals)
- [x] ObjectListenerRegistry for managing subscriptions by (object_id, branch)
- [x] ObjectCallback type: `Box<dyn Fn(Arc<ObjectState>) + Send + Sync>`
- [x] ObjectState with previous tips and current tips
- [x] ListenerId for subscription management
- [x] subscribe() returns ListenerId, calls callback immediately with current state
- [x] unsubscribe() removes listener by ID
- [x] Helper functions: compute_change_ranges, ByteDiff, DiffRange
- [x] Callbacks fire synchronously during write operations (same call stack)

### LocalNode Architecture
- [x] LocalNode owns Arc<dyn Environment> for storage
- [x] LocalNode.in_memory() convenience constructor with MemoryEnvironment
- [x] All read/write methods on LocalNode use storage internally
- [x] ObjectListenerRegistry for synchronous callback management
- [x] write_sync/write/write_stream automatically notify listeners
- [x] No main branch convenience methods - all operations require explicit branch name

**Current module structure**:
- `commit.rs` - CommitId, Commit with ContentRef
- `branch.rs` - Branch with LCA, frontier tracking
- `merge.rs` - MergeStrategy trait, LastWriterWins
- `object.rs` - Object with branches, sync/async/streaming read/write, ContentStream
- `node.rs` - LocalNode (owns Environment), generate_object_id(), read/write/subscribe APIs
- `storage.rs` - ContentRef, ChunkHash, ContentStore, CommitStore, Environment, MemoryEnvironment
- `listener.rs` - ObjectListenerRegistry, ObjectCallback, ObjectState, ListenerId, ByteDiff
- `sql/` - SQL layer module (see below)

### Persistence Backends
- [ ] Implement RocksDB backend
- [ ] Implement SQLite fallback
- [ ] Implement IndexedDB for browser
- [ ] Tests for persistence roundtrip

## Phase 1.5: SQL Layer

### Step 1: Basic Storage (Complete)
- [x] `ColumnType` and `ColumnDef` types
- [x] `TableSchema` type with serialization
- [x] Row binary encoding/decoding (length-prefix header + column values)
- [x] Nullable column handling (1-byte presence flag in content)
- [x] SQL parser: CREATE TABLE, INSERT, UPDATE, SELECT
- [x] `create_table` - store schema as Object
- [x] `insert` - create row Object with encoded data
- [x] `get` - fetch and decode row by ID
- [x] `update` - create new commit on row Object
- [x] `delete` - tombstone commit on row Object
- [x] `execute()` method for SQL strings

### Step 2: References and Queries (Complete)
- [x] `Ref` column type with target schema validation
- [x] Index object creation per (source_table, source_column) Ref column
- [x] Synchronous index maintenance on insert/update/delete
- [x] `select` with scan-based where clause (`=` only)
- [x] `find_referencing` using index lookup
- [x] `reactive_query()` for callback-based reactive queries
- [x] ReactiveQuery with subscribe()/unsubscribe() for synchronous callbacks
- [x] ReactiveQueryRegistry for tracking active queries per table
- [x] Callbacks fire synchronously on insert/update/delete
- [x] JOIN support in SQL parser and executor
- [x] Qualified column references (table.column) in projections and WHERE
- [x] Table-qualified star projection (table.*)

### Step 3: ObjectId Type System (Complete)
- [x] `ObjectId` newtype wrapping u128
- [x] Crockford Base32 encoding (26 chars, case-insensitive, I/L→1, O→0)
- [x] `Display` and `Debug` traits showing Base32 format
- [x] `FromStr` for parsing Base32 strings
- [x] ObjectId used throughout public API (LocalNode, ObjectKey, Row, etc.)
- [x] SQL parser accepts ObjectIds as plain string literals
- [x] Value coercion in database executor (String→Ref for Ref columns)
- [x] WASM bindings accept Base32 strings for ObjectIds

### Step 4: Incremental Query System (In Progress)

See `specs/incremental-queries.md` for full design.

**Phase 1: Core Types**
- [ ] `RowDelta`, `PriorState`, `DeltaBatch` - change representation
- [ ] `Predicate` - filter conditions with `matches()`
- [ ] `RowCache` - shared row data cache
- [ ] `QueryNode` - graph nodes (TableScan, Filter, Output)
- [ ] `QueryGraph` - DAG with lazy init and delta propagation
- [ ] `QueryGraphBuilder` - programmatic construction API

**Phase 2: Integration**
- [ ] `GraphRegistry` - manages active graphs, routes changes
- [ ] `IncrementalQuery` - user-facing handle
- [ ] Database integration (incremental_query method, mutation hooks)

**Phase 3: Recursive Queries**
- [x] `RecursiveFilter` node type for self-referential policies
- [x] `AccessReason` enum (Base, Inherited, Both) for removal cascading
- [x] Fixpoint iteration for transitive closure during initialization
- [x] Children index for efficient downward propagation
- [x] Delta propagation with cascade to children
- [x] Integration with policy system (detect self-referential INHERITS, build RecursiveFilter)

**Phase 4: Future Extensions**
- [ ] Batched propagation for server sync (begin_batch/flush_batch)
- [ ] Shared subgraphs across queries
- [ ] Index-aware source nodes (IndexLookup)
- [ ] ReBAC constraint merging at graph construction

### SQL Module Structure
```
sql/
├── mod.rs          - Re-exports and module organization
├── types.rs        - ObjectId, SchemaId, IndexKey, QueryState
├── schema.rs       - ColumnType, ColumnDef, TableSchema
├── row.rs          - Value, Row, encode_row, decode_row
├── parser.rs       - SQL parser (CREATE, INSERT, UPDATE, SELECT)
├── index.rs        - RefIndex for reverse lookups
├── table_rows.rs   - TableRows for row membership tracking
├── database/
│   ├── mod.rs      - Database, ReactiveQuery, coercion logic
│   └── tests.rs    - Database unit tests
└── query_graph/    - Incremental query system
    ├── mod.rs      - Public API exports
    ├── delta.rs    - RowDelta, PriorState, DeltaBatch
    ├── predicate.rs - Predicate enum and matching
    ├── cache.rs    - RowCache
    ├── node.rs     - NodeId, QueryNode, evaluation
    ├── graph.rs    - QueryGraph, GraphState, GraphId
    ├── builder.rs  - QueryGraphBuilder
    └── registry.rs - GraphRegistry
```

### WASM Bindings (groove-wasm)
- [x] WasmDatabase wrapper with execute() method
- [x] WasmQueryHandle for reactive query subscriptions
- [x] JavaScript callback integration
- [x] Panic hook for better error messages
- [x] update_row() accepts Base32 ObjectId strings

### TypeScript Schema Package (@jazz/schema)

SQL-first schema codegen: SQL schema files are the source of truth, and TypeScript types are generated from them.

**Why SQL-first?**
- ReBAC policies are expressed in SQL, so SQL is already the schema language
- SQL is universally known by developers and LLMs
- Single source of truth (no Zod ↔ SQL ↔ TypeScript synchronization)

**Implemented Features**
- [x] `generateFromSql(sqlPath, options)` - Parse SQL and generate TypeScript
- [x] Parse CREATE TABLE statements with column types
- [x] Map SQL types to TypeScript (STRING→string, I64→bigint, REFERENCES→ObjectId, etc.)
- [x] Generate base row interfaces (e.g., `User`, `Folder`, `Note`)
- [x] Generate insert types with optional nullable fields and reference unions
- [x] Infer reverse relationships from forward refs (e.g., `User.Folders` from `Folder.owner`)
- [x] Generate Depth types for controlling eager loading depth
- [x] Generate conditional Loaded types with generic depth parameter

**Usage**
```bash
# From CLI
npx tsx -e "import { generateFromSql } from '@jazz/schema'; generateFromSql('schema.sql')"

# Or in package.json
"generate": "npx tsx -e \"import { generateFromSql } from '@jazz/schema'; generateFromSql('src/schema.sql', { output: 'src' })\""
```

**Example Output (from schema.sql)**
```typescript
export type FolderLoaded<D extends FolderDepth = {}> = {
  id: ObjectId;
  name: string;
  owner: 'owner' extends keyof D
    ? D['owner'] extends true
      ? User
      : D['owner'] extends object
        ? UserLoaded<D['owner'] & UserDepth>
        : ObjectId
    : ObjectId;
}
  & ('Notes' extends keyof D
    ? D['Notes'] extends true
      ? { Notes: Note[] }
      : D['Notes'] extends object
        ? { Notes: NoteLoaded<D['Notes'] & NoteDepth>[] }
        : {}
    : {});
```

## Phase 2: Syncing
- [ ] Design sync protocol for commit graph reconciliation
- [ ] Implement client-side sync
- [ ] Implement server-side sync
- [ ] Tests for sync scenarios (sequential commits, concurrent commits, reconnection)

## Phase 3: Permissions & Identity

See `specs/rebac-policies.md` for full design.

### ReBAC Policy System

**Phase 3.1: Core Types and Parser**
- [x] `PolicyAction`, `Policy`, `PolicyExpr` AST types
- [x] SQL parser for CREATE POLICY
- [x] Store policies in Database (via `create_policy()`)
- [x] Policy serialization/deserialization

**Phase 3.2: Evaluation Engine**
- [x] `PolicyEvaluator` with cycle detection and depth limit
- [x] Basic expression evaluation (comparisons, AND/OR/NOT)
- [x] INHERITS evaluation with recursive lookup
- [x] Integration with SELECT queries (`select_all_as`, `select_where_as`)
- [x] Default allow with warning for missing policies

**Phase 3.3: Write Policies**
- [x] INSERT policy evaluation (CHECK on @new)
- [x] UPDATE policy evaluation (WHERE on existing, CHECK on @old/@new)
- [x] DELETE policy evaluation (WHERE, fallback to UPDATE)
- [x] Database integration (`insert_as`, `update_as`, `delete_as` methods)

**Phase 3.4: Query Integration**
- [x] Combine policy predicates with user query predicates
- [x] Integrate with incremental query graph builder (`incremental_query_as` method)
- [x] `policy_expr_to_predicate` converts simple policies to Predicate for efficient filtering
- [x] INHERITS flattened to JOINs for true incremental evaluation
- [x] Optimize predicate ordering by selectivity
- [x] Self-referential recursive INHERITS (e.g., folder→parent_folder→...→root) via RecursiveFilter
- [x] Nested INHERITS chains of arbitrary depth (e.g., doc→folder→workspace→org→owner) via chained JOINs
  - Supports 2+, 3+, 4+ hop chains with correct delta propagation from any table in the chain
  - Each Join node maintains a reverse_index for efficient delta routing from downstream tables
  - OR policies at intermediate chain levels correctly evaluate both paths
    (e.g., `owner_id = @viewer OR INHERITS SELECT FROM workspace_id` on folders - access granted
    if folder.owner_id matches OR workspace.owner_id matches)

**Phase 3.5: Testing and Debugging**
- [ ] EXPLAIN POLICY command
- [ ] `check_policy()` programmatic API

### Identity (Future)
- [ ] Server-authenticated accounts
- [ ] @viewer binding from authentication context

## Phase 4: Advanced Features
- [ ] Additional merge strategies (beyond LastWriterWins)
- [ ] Migration branches
- [ ] Opt-in E2EE for sensitive data
- [ ] Index optimization (beyond brute-force)

---

## Test Coverage

Current test count: **253 tests** passing across all modules (120 unit + 133 integration)

- Unit tests in `sql/row.rs`, `sql/types.rs`, `sql/database/tests.rs`
- Integration tests in `tests/` directory:
  - `branch.rs` - Branch operations and LCA
  - `commit.rs` - Commit ID determinism
  - `listener.rs` - ObjectListenerRegistry
  - `node.rs` - LocalNode operations
  - `object.rs` - Object read/write/streaming
  - `sql_database.rs` - Full SQL layer tests
  - `sql_parser.rs` - SQL parsing tests
  - `sql_row.rs` - Row encoding/decoding
  - `sql_schema.rs` - Schema serialization
  - `storage.rs` - Storage abstractions

---

## Open Design Questions

- [ ] Revisit whether primary key `id` (ObjectId/UUIDv7) column should be implicit or explicit in CREATE TABLE syntax. Currently implicit - may want to require explicit declaration for clarity.

---

## Clarifications

**Reactive SQL**: Queries are still reactive (subscription as default, one-time load as special case). The data you subscribe to is determined by an SQL query - the query defines *what* to watch, and changes to matching data trigger updates.

**ObjectId Format**: ObjectIds are displayed and parsed as 26-character Crockford Base32 strings (e.g., `0000000000000034NBSM938NKR`). This format is case-insensitive and substitutes commonly confused characters (I/L→1, O→0).

**Value Coercion**: The SQL parser produces `Value::String` for all string literals. The database executor coerces strings to `Value::Ref(ObjectId)` when the target column type is `Ref`. This avoids ambiguity since strings like "ALICE" are valid Base32 but should remain as strings for String columns.
