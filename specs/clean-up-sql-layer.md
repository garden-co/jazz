# SQL Layer Clean-up: Lessons Learned

This document captures the difficulties encountered while building the SQL → Incremental Query Graph system. The goal is to inform a future redesign that addresses these pain points with a cleaner architecture.

## Current Architecture Overview

The SQL layer transforms SQL queries into incremental computation graphs:

```
SQL String → Parser → AST → Database.build_*_graph() → QueryGraph → Node evaluation
```

Key components:
- **Parser**: Converts SQL strings to AST (`Select`, `Join`, `Condition`, etc.)
- **Database**: Builds query graphs from AST, handles schema lookups
- **QueryGraph**: DAG of nodes that process deltas incrementally
- **QueryNode**: Individual computation nodes (Join, Filter, ArrayAggregate, etc.)
- **JoinGraphBuilder**: Helper to construct multi-join graphs

## Difficulties Encountered

### 1. Forward vs Reverse JOIN Asymmetry

**Problem**: JOINs have fundamentally different semantics depending on which table has the foreign key:

- **Forward JOIN** (Issues → Projects via `issue.project`): 1:1 relationship, lookup by FK value
- **Reverse JOIN** (Issues ← IssueLabels via `issuelabel.issue`): 1:N relationship, find all referencing rows

**Manifestations**:
- Different code paths for evaluating deltas (`lookup_row` vs `find_referencing`)
- Different caching strategies (single joined row vs sets of IDs)
- Different output semantics (add columns vs filter existence)

**Current Fix**: Detect join direction by analyzing ON clause, use `ChainJoinInfo::Forward` vs `ChainJoinInfo::Reverse` enum, and implement separate evaluation logic.

**Pain Point**: This detection logic is scattered across `find_join_column`, `find_chain_join_info`, and `eval_join_input_delta`. The `join_column` field encodes direction via string format (`"table@existing.column"` for reverse).

### 2. Reverse JOINs for Filtering vs Including

**Problem**: Reverse JOINs serve two different purposes:

1. **Filtering (EXISTS)**: "Find Issues where some IssueLabel has label = X"
2. **Including (ARRAY)**: "Include all IssueLabels for each Issue"

These have conflicting requirements:
- Filtering should NOT add columns to output (would break encoding)
- Including DOES add columns (via ArrayAggregate)

**What Went Wrong**: Initially, reverse JOINs added their columns to the `JoinedRow`. This caused:
- Decoder expected N columns but got N+M (encoding mismatch)
- Filter predicates referenced columns that weren't at expected indices
- `RangeError: Invalid typed array length` when decoder read garbage

**Current Fix**:
- Reverse JOINs for filtering don't add columns, just filter which rows pass
- Filter predicates on reverse-joined tables are passed TO the JOIN (not as separate Filter node)
- ArrayAggregate later re-fetches and builds arrays independently

**Pain Point**: The distinction between "reverse JOIN for filtering" and "reverse JOIN for array output" is implicit and handled by completely separate code paths.

### 3. Table Alias Handling

**Problem**: SQL allows aliases (`FROM Issues i`), and ON clauses use aliases (`i.project = Projects.id`), but the graph builder expects table names.

**What Went Wrong**:
1. `find_join_column` compared ON clause references against table names, missing alias matches. Queries with aliases silently failed (callback never invoked, loading stuck).
2. `build_multi_join_predicate` only checked table names, not aliases. WHERE clause `i.priority = 'low'` failed with "Unknown table i".

**Current Fix**:
- Pass aliases through and check both table name AND alias in all comparisons.
- Added `build_multi_join_predicate_with_aliases` for predicate building.

**Pain Point**: Alias handling is ad-hoc, added to each function that needs it. No centralized "table reference resolution" layer. We now have TWO predicate building functions (`build_multi_join_predicate` and `build_multi_join_predicate_with_aliases`).

### 4. Multi-JOIN Chain Complexity

**Problem**: Chain JOINs (A → B → C) require tracking:
- Which tables are already joined (`input_tables`)
- Which columns are available (combined schema)
- How to route deltas (does this delta affect this join?)

**What Went Wrong**:
- Initial implementation only supported single JOINs
- Chain JOINs required special `qualified_column` format (`table.column`)
- Delta routing logic became complex (is this delta for this node or downstream?)
- `contained_tables` tracking for knowing what data a delta represents

**Current Fix**: `JoinGraphBuilder` tracks `all_right_tables` and builds `combined_schema`. Join nodes have `input_tables` list. Delta routing checks membership.

**Pain Point**: The "combined schema" grows with each join but reverse joins shouldn't add columns. This inconsistency required special handling.

### 5. Filter Predicate Column Indexing

**Problem**: Filter predicates reference columns by name, but evaluation uses column indices.

**What Went Wrong**:
- Multi-table joins have qualified column names (`Table.column`)
- The combined schema has indices based on join order
- Reverse joins changed what columns exist, breaking index calculations
- "index out of bounds: len is 10 but index is 11" when filter tried to access reverse-joined column

**Current Fix**: For reverse JOINs, extract filter conditions and pass them to the JOIN itself, not to a separate Filter node. The JOIN applies the filter during `find_referencing`.

**Pain Point**: Column indexing is implicit throughout. No explicit "this predicate applies to this table's columns" mapping.

### 6. Binary Encoding/Decoding Brittleness

**Problem**: The TypeScript decoder must exactly match the Rust encoder's format.

**What Went Wrong**:
- Forward ref includes expected `[primary columns, joined columns]`
- Adding reverse join columns changed the format unexpectedly
- Decoder read ObjectId bytes where it expected length prefix → garbage values
- Error messages unhelpful ("Invalid typed array length: 1162555696" = "ESSI" in ASCII)

**Current Fix**: Strict separation - reverse JOINs don't add columns, ArrayAggregate adds arrays at known positions.

**Pain Point**: No schema versioning or self-describing format. Encoder/decoder must be kept in sync manually.

### 7. Nullable Column Handling

**Problem**: Nullable columns need special encoding (presence byte).

**What Went Wrong** (commit 8995a33):
- Nullable refs weren't using the nullable encoding format
- Decoder expected presence byte but got ObjectId bytes directly

**Current Fix**: `Value::NullableSome` and `Value::NullableNone` variants make nullability self-describing.

**Pain Point**: Required changing the value representation throughout the codebase.

### 8. Projection After Reverse JOINs

**Problem**: When the SQL `FROM` table is different from the graph's "left" table (due to reverse JOIN direction swap), the output should project back to the original table.

**What Went Wrong** (commit f0f3bf9):
- Query `SELECT Issues.* FROM Issues JOIN IssueAssignees` swapped tables internally
- Output contained IssueAssignees columns instead of Issues columns

**Current Fix**: `builder.set_projection()` to specify which table's columns should be in the output.

**Pain Point**: The table swapping is an implementation detail that leaks into multiple places.

## Patterns That Emerged

### String-Encoded Metadata

Several places encode metadata in strings:
- `join_column = "target@existing.column"` for reverse joins
- Qualified column names `"Table.column"` for multi-table queries

This is error-prone and requires parsing at runtime.

### Implicit State in Node Evaluation

Node evaluation depends on:
- What tables are "contained" in the delta
- Whether delta came from "input" or "join table"
- What columns exist at what indices

This state is reconstructed from context rather than explicitly passed.

### Separate Code Paths for Similar Operations

Forward and reverse JOINs have almost entirely separate implementations despite similar structure. This leads to bugs being fixed in one path but not the other.

### 9. Implicit `id` Column Not in Schema

**Problem**: Every table has an implicit `id` column (the ObjectId primary key), but it's NOT included in the schema's `columns` list. Code that looks up columns must handle `id` as a special case everywhere.

**What Went Wrong**:
1. `schema.column("id")` returns `None` - predicate building in `build_multi_join_predicate` failed with "Column id not found in table Issues"
2. After fixing predicate building, predicates used qualified names like `Issues.id` but the predicate matcher only checked `column == "id"` (unqualified)
3. The Filter node showed `predicate: Issues.id = '...'` with `cached: 0 rows` even though 50 joined rows existed

**Current Fix**:
- Predicate building checks `if col_name == "id"` before looking up column metadata, treats it as `ColumnType::String`
- Predicate matching checks `column == "id" || column.ends_with(".id")` to handle both qualified and unqualified references

**Pain Point**: The `id` special case is scattered across multiple files:
- `database/mod.rs`: `build_multi_join_predicate`, `build_multi_join_predicate_with_aliases`, `extract_table_conditions`
- `query_graph/predicate.rs`: `Predicate::matches()` for both `Eq` and `Ne` variants

**Future Redesign Should**: Consider adding `id` explicitly to the schema columns list during table creation to eliminate all special cases.

### 10. WASM Type Boundaries

**Problem**: WASM bindings expose functions like `update_row(table, id, column, value: &str)` where `value` is always a string. This fails silently for non-string types.

**What Went Wrong**:
1. TypeScript client passed `bigint` (for timestamp columns) directly to `update_row`
2. `passStringToWasm0` crashed with "memory access out of bounds" trying to convert bigint to string
3. After converting to string, Rust returned error "TypeMismatch { expected: I64, got: String(...) }"

**Current Fix**: Added separate `update_row_i64(table, id, column, value: i64)` method. TypeScript client checks `typeof value` and calls the appropriate method.

**Pain Point**: The WASM boundary requires knowing column types at call time. Each type needs its own method, leading to API proliferation.

**Future Redesign Should**: Consider a unified `update_row` that accepts JSON-encoded value with type tag, or use `JsValue` with runtime type checking in Rust.

### 11. No Filter Pushdown / Query Optimization

**Problem**: The graph builder constructs nodes in a fixed order (JOINs first, then Filter), regardless of which would be more efficient.

**Example**: A query filtering Issues by `priority = 'urgent'` AND junction table conditions:

```
├── [ 0] Join: Issues + Projects         → 50 joined rows
├── [ 1] Join: + IssueAssignees           → 13 joined rows
├── [ 2] Join: + IssueLabels              → 5 joined rows
├── [ 3] Filter: Issues.priority = 'urgent' → 1 row
├── [ 4] ArrayAggregate: IssueLabels
├── [ 5] ArrayAggregate: IssueAssignees
└── [ 6] Output
```

The Filter on `Issues.priority` happens AFTER all the JOINs, so we're joining 50 rows only to filter down to 1 at the end. It would be far more efficient to filter Issues FIRST:

```
├── [ 0] Filter: Issues.priority = 'urgent' → ~1 row (early!)
├── [ 1] Join: Issues + Projects           → 1 joined row
├── [ 2] Join: + IssueAssignees            → 1 joined row
├── [ 3] Join: + IssueLabels               → 1 joined row
├── [ 4] ArrayAggregate: IssueLabels
├── [ 5] ArrayAggregate: IssueAssignees
└── [ 6] Output
```

**Current State**: No query optimization. Nodes are created in the order they appear in the SQL/builder calls.

**Pain Point**: For large datasets, this creates significant unnecessary work. A query that should touch 1 row instead processes 50+ through multiple JOIN nodes.

**Future Redesign Should**:
- Implement predicate pushdown (move filters before JOINs when possible)
- Analyze which predicates can be pushed to which tables
- Consider filter selectivity when ordering operations
- Potentially reorder JOINs based on estimated cardinality

## Ideas for Redesign

1. **Explicit Join Direction Type**: Instead of string encoding, have `JoinNode::Forward { ... }` and `JoinNode::Reverse { ... }` variants.

2. **Schema-Aware Nodes**: Each node should explicitly declare its input and output schemas, not derive them from table lookups.

3. **Centralized Table Resolution**: A single layer that resolves table names, aliases, and qualified references.

4. **Typed Column References**: Instead of string column names, use `ColumnRef { table_idx, column_idx }` that's validated at graph construction time.

5. **Self-Describing Encoding**: Include schema info in the encoded data so decoder can validate format.

6. **Separate EXISTS from ARRAY**: Make "filter by existence" and "include as array" explicitly different operations, not implicit in join direction.

7. **Graph Construction DSL**: Instead of ad-hoc builder methods, a declarative specification of the query structure that gets compiled to the graph.

8. **Query Optimizer / Predicate Pushdown**: Add an optimization pass between AST parsing and graph construction that reorders operations for efficiency. Push filters on single tables before JOINs, estimate cardinalities, choose optimal JOIN order.

## GCO-1068: Unified Row Buffer Format Migration

**Goal**: Replace the legacy `Row`/`Value` types with a unified buffer format (`OwnedRow`/`RowRef`/`RowValue`) for zero-copy reads and efficient WASM transfer.

### Completed

1. **Buffer Format Types** (`groove/src/sql/row_buffer.rs`):
   - `RowDescriptor`: Schema metadata with pre-computed offsets
   - `OwnedRow`: Owned buffer with `Arc<RowDescriptor>`
   - `RowRef<'a>`: Zero-copy borrowed view
   - `RowValue<'a>`: Borrowed enum for type-safe access
   - `RowBuilder`: Fluent API for buffer construction
   - Conversion methods: `from_legacy_row()`, `to_legacy_row_with_schema()`

2. **Buffer-Compatible Predicate Matching** (`predicate.rs`):
   - `Predicate::matches_buffer()` for filtering with buffer rows
   - `buffer_value_equals_pred()` helper for type comparisons

3. **Buffer Delta Types** (`delta.rs`):
   - `BufferRowDelta`, `BufferDeltaBatch` for buffer-based deltas
   - `BufferJoinedRow` for JOIN operations with buffer types

4. **Node Migrations**:
   - `LimitOffset` node: `all_rows: BTreeMap<ObjectId, OwnedRow>`
   - `RecursiveFilter` node: `all_rows: HashMap<ObjectId, OwnedRow>`
   - Both convert at boundaries (incoming `Row` → buffer, buffer → output `Row`)

### Remaining Work

1. **Join Node**: Uses `cached_rows: HashMap<ObjectId, JoinedRow>`. Migration requires:
   - Use `BufferJoinedRow` for internal storage
   - Handle complex table offset tracking
   - Update `eval_join_input_delta` and related helpers

2. **ArrayAggregate Node**: Uses `cached_arrays: HashMap<ObjectId, Vec<Row>>` and `outer_rows: HashMap<ObjectId, Row>`. Migration requires:
   - Nested row storage for `Value::Array` construction
   - May need `Value::Array` format redesign

3. **Database Module**: 177 occurrences of `Row`/`Value` usage:
   - Query building
   - Result collection
   - May need gradual migration with adapter layer

4. **Remove Legacy Types**: After full migration:
   - Remove `Row`/`Value` from `row.rs`
   - Update all consumers to use buffer types

### Architecture Notes

The buffer format uses a fixed-then-variable layout:
- Fixed-size columns (Bool, I32, I64, F64, Ref) have direct byte offsets
- Variable-size columns (String, Bytes) use varint length prefixes
- Nullable types have presence byte before value

This enables:
- O(1) access for fixed-size columns
- Zero-copy string/bytes reads via borrowed slices
- Efficient WASM transfer (single memcpy)

## Related Commits

- `982e89a` - Fix table alias handling in WHERE clause for multi-JOIN queries
- `70821ef` - Add multi-JOIN support for filtered queries with reverse relations
- `fd0f81a` - Add nested JOIN support for ARRAY subqueries
- `9617965` - Fix table alias handling in reverse JOIN queries
- `f0f3bf9` - Add projection support for reverse JOIN queries
- `7e0f984` - Add relation filters and reverse JOIN support (WIP)
- `8995a33` - Fix binary encoding mismatch for nullable refs
- `ec1b25b` - Replace nullable_mask with self-describing Value variants
- `e7314c2` - Support arbitrary-length join chains with correct delta propagation
- `987c362` - Migrate LimitOffset and RecursiveFilter nodes to buffer format (GCO-1068)
