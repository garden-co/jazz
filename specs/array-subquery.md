# ARRAY(subquery) Support

## Overview

Add support for `ARRAY(SELECT ...)` in projections, enabling queries like:

```sql
SELECT f.*, ARRAY(SELECT n FROM notes n WHERE n.folder_id = f.id) as notes
FROM folders f
```

This returns rows with nested arrays of related rows, enabling ORM-style eager loading of one-to-many relationships.

## Syntax

Following PostgreSQL syntax - the subquery must return a single column:

```sql
-- Single scalar column
ARRAY(SELECT title FROM notes WHERE folder_id = f.id)
-- Returns: Value::Array([Value::String("A"), Value::String("B")])

-- Table alias as composite type (returns full rows)
ARRAY(SELECT n FROM notes n WHERE n.folder_id = f.id)
-- Returns: Value::Array([Value::Row(...), Value::Row(...)])

-- Aliased in projection
SELECT f.id, f.name, 
       ARRAY(SELECT n FROM notes n WHERE n.folder_id = f.id) as notes
FROM folders f
```

The key insight: in PostgreSQL, a bare table alias (like `n`) in a SELECT list returns the entire row as a composite type. This is the idiomatic way to get an array of rows.

### Correlated References

The subquery can reference columns from the outer query:

```sql
-- Reference outer row's id column
ARRAY(SELECT n FROM notes n WHERE n.folder_id = f.id)

-- Reference any outer column
ARRAY(SELECT c FROM comments c WHERE c.author_id = p.author_id)
```

## Value Types

Add two new value types:

```rust
pub enum Value {
    // ... existing variants ...
    
    /// A row value (ordered collection of values with implicit schema).
    /// Used for ROW(...) expressions and full-row subquery results.
    Row(Vec<Value>),
    
    /// An array of values (homogeneous or heterogeneous).
    /// Used for ARRAY(...) expressions.
    Array(Vec<Value>),
}
```

### JSON Serialization

```rust
// Value::Row serializes as JSON object (needs schema for keys)
// For now, serialize as array until we have schema context
Value::Row(values) => json!(values)

// Value::Array serializes as JSON array  
Value::Array(values) => json!(values)
```

### Example Output

```json
{
  "id": "01ABC...",
  "name": "Work",
  "notes": [
    {"id": "01DEF...", "title": "Meeting notes", "content": "..."},
    {"id": "01GHI...", "title": "TODO list", "content": "..."}
  ]
}
```

## Parser Changes

### AST Extensions

```rust
/// A SELECT expression that can appear in projections.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectExpr {
    pub projection: Projection,
    pub from: FromClause,
    pub where_clause: Vec<Condition>,
}

/// Expression in a projection (column, literal, or computed).
#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionExpr {
    /// Simple column reference: `name` or `t.name`
    Column(QualifiedColumn),
    
    /// Array subquery: `ARRAY(SELECT ...)`
    ArraySubquery {
        subquery: Box<SelectExpr>,
        alias: Option<String>,
    },
}

/// Extended projection supporting expressions.
#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    /// SELECT *
    All,
    /// SELECT table.*
    TableAll(String),
    /// SELECT expr1, expr2, ... (columns or expressions)
    Expressions(Vec<ProjectionExpr>),
}
```

### Parsing

```
projection ::= '*' 
             | table '.' '*'
             | projection_expr (',' projection_expr)*

projection_expr ::= qualified_column
                  | 'ARRAY' '(' select_expr ')' ['AS' identifier]

select_expr ::= 'SELECT' projection 'FROM' from_clause [where_clause]
```

## Query Execution

### Non-Incremental (Direct) Execution

For each outer row:
1. Evaluate the subquery with outer row bindings in scope
2. Collect subquery results into `Value::Array`
3. Include in output row at the appropriate position

```rust
fn execute_array_subquery(
    &self,
    subquery: &SelectExpr,
    outer_row: &Row,
    outer_schema: &TableSchema,
) -> Value {
    // Build bindings from outer row
    let bindings = self.build_outer_bindings(outer_row, outer_schema);
    
    // Execute subquery with bindings
    let inner_rows = self.execute_select_with_bindings(&subquery, &bindings);
    
    // Convert to Value::Array
    let array_values: Vec<Value> = inner_rows
        .into_iter()
        .map(|row| Value::Row(row.values))
        .collect();
    
    Value::Array(array_values)
}
```

### Incremental Execution

Add a new query graph node: `ArrayAggregate`

```rust
QueryNode::ArrayAggregate {
    /// The outer table (source of correlation)
    outer_table: String,
    /// Column in outer table used for correlation
    outer_column: String,
    /// The inner table being aggregated
    inner_table: String,
    /// Column in inner table that references outer
    inner_ref_column: String,
    /// Schema of inner table (for building Row values)
    inner_schema: TableSchema,
    /// Cached arrays: outer_id → Vec<Row>
    cached_arrays: HashMap<ObjectId, Vec<Row>>,
    /// Reverse index: inner_id → outer_id (for propagating inner changes)
    inner_to_outer: HashMap<ObjectId, ObjectId>,
}
```

#### Delta Propagation

**When inner table row is added:**
1. Look up which outer row it references (via `inner_ref_column`)
2. Add to `cached_arrays[outer_id]`
3. Emit `Updated` delta for outer row (entire array replaced)

**When inner table row is removed:**
1. Look up outer_id via `inner_to_outer`
2. Remove from `cached_arrays[outer_id]`
3. Emit `Updated` delta for outer row

**When inner table row is updated:**
1. Check if ref changed (moved to different outer row)
2. If same outer: update in place, emit `Updated` for outer
3. If different outer: remove from old, add to new, emit `Updated` for both

**When outer table row is added:**
1. Initialize empty array in `cached_arrays`
2. Scan inner table for matching rows (lazy or eager)
3. Emit `Added` with populated array

**When outer table row is removed:**
1. Remove from `cached_arrays`
2. Clean up `inner_to_outer` entries
3. Emit `Removed`

### Array-Level Deltas (Future)

For granular UI updates, we could extend `RowDelta` to include array operations:

```rust
enum ArrayDelta {
    /// Element added at index
    Insert { index: usize, value: Value },
    /// Element removed at index  
    Remove { index: usize },
    /// Element updated at index
    Update { index: usize, new_value: Value },
}

enum RowDelta {
    // ... existing ...
    
    /// A nested array within the row changed
    ArrayChanged {
        id: ObjectId,
        column_index: usize,
        changes: Vec<ArrayDelta>,
    },
}
```

This is deferred to a later phase - initially, any change to nested rows results in a full row `Updated` delta.

## TypeScript Codegen

### Schema Representation

```typescript
// Generated type
interface Folder {
  id: string;
  name: string;
  notes: Note[];  // From ARRAY subquery
}

interface Note {
  id: string;
  title: string;
  folder_id: string;
}
```

### Query Builder API (Future)

```typescript
// Prisma-style include
const folders = await db.folders.findMany({
  include: {
    notes: true,  // Generates ARRAY(SELECT * FROM notes WHERE folder_id = folders.id)
  }
});

// With filtering
const folders = await db.folders.findMany({
  include: {
    notes: {
      where: { archived: false },
      orderBy: { created_at: 'desc' },
    }
  }
});
```

## Implementation Phases

### Phase 1: Value Types & Parser
- [x] Design spec (this document)
- [x] Add `Value::Row` and `Value::Array` variants
- [x] Add serialization (to_bytes, from_bytes) for new types
- [x] Parse `ARRAY(SELECT ...)` in projections
- [x] Parse correlated column references

### Phase 2: Direct Execution
- [x] Execute ARRAY subqueries in `execute_select`
- [x] Handle correlated references (outer row bindings)
- [x] Return proper `Value::Array` in results
- [x] Add integration tests

### Phase 3: Incremental Support
- [x] Add `ArrayAggregate` node type
- [x] Implement delta propagation from inner table
- [x] Implement delta propagation from outer table
- [x] Wire into query graph builder
- [x] Add incremental tests

### Phase 4: TypeScript Integration
- [ ] Extend codegen to detect ARRAY columns
- [ ] Generate nested types
- [ ] Generate query builder helpers

## Limitations & Future Work

1. **No ORDER BY in subquery** - Results are unordered (could add later)
2. **No LIMIT in subquery** - All matching rows included (could add later)  
3. **Single correlation column** - Only `WHERE inner.ref = outer.id` patterns
4. **No nested ARRAY** - Can't do `ARRAY(SELECT ..., ARRAY(...) FROM ...)`
5. **Full array replacement** - No granular array deltas initially

## Alternatives Considered

### JSON_AGG (Postgres-style)
```sql
SELECT f.*, JSON_AGG(n.*) as notes
FROM folders f
LEFT JOIN notes n ON n.folder_id = f.id
GROUP BY f.id
```
Rejected because:
- Requires GROUP BY which we don't support
- LEFT JOIN semantics are complex
- JSON_AGG is less explicit than ARRAY()

### Multiple Queries (Prisma-style)
Issue separate queries and stitch in application code.
Deferred because:
- Groove is embedded, so no network round trips anyway
- Single query is simpler for the user
- Incremental updates are easier with single query model

### Lateral Join
```sql
SELECT f.*, notes.*
FROM folders f,
LATERAL (SELECT * FROM notes WHERE folder_id = f.id) as notes
```
Could add later, but ARRAY() is more explicit about the nesting intent.
