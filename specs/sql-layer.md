# SQL Layer Design

## Overview

The SQL layer provides a relational interface on top of Jazz's distributed commit graph. Each table is a schema definition object, and each row is a separate Object with its own commit graph. This enables fine-grained sync and per-row conflict resolution.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    SQL Interface                     │
│         (CREATE TABLE, INSERT, SELECT, etc.)         │
├─────────────────────────────────────────────────────┤
│                   Schema Objects                     │
│            (table definitions, columns)              │
├─────────────────────────────────────────────────────┤
│                    Row Objects                       │
│         (one Object per row, commit graph)           │
├─────────────────────────────────────────────────────┤
│                  Index Objects                       │
│     (one per reference column, reverse lookups)      │
├─────────────────────────────────────────────────────┤
│                   Environment                        │
│        (ContentStore + CommitStore + ...)            │
└─────────────────────────────────────────────────────┘
```

## Schema Objects

A table schema is itself an Object with a commit graph, allowing schema evolution tracking.

```rust
struct TableSchema {
    name: String,
    columns: Vec<ColumnDef>,
}

struct ColumnDef {
    name: String,
    ty: ColumnType,
    nullable: bool,
}

enum ColumnType {
    Bool,             // 1 byte
    I64,              // 8 bytes, little-endian
    F64,              // 8 bytes, IEEE 754 little-endian
    String,           // varint length + UTF-8 bytes
    Bytes,            // varint length + raw bytes
    Ref(String),      // 16 bytes (u128 object ID), references a row in named table
}
```

Each schema gets a `SchemaId` (the Object ID of the schema object). Rows reference their schema by this ID.

## Row Objects

Each row is a separate Object with:
- Object ID = UUIDv7 (serves as primary key)
- Content = compact binary row data
- Commits track row history

### Row Binary Format

The format is type-less (schema provides types) with a length-prefix header for O(1) column access.

**Structure:**
```
[length-prefix header][column values in schema order]
```

**Length-prefix header:**
- One varint per variable-size column (String, Bytes)
- Encodes the byte length of that column's content (including presence flag if nullable)
- Fixed-size columns have no header entry

**Column values:**
- Written in schema order
- Fixed-size: just the value bytes
- Variable-size: just the data bytes (length is in header)
- Nullable columns: prepend 1-byte presence flag (0x00 = null, 0x01 = present)

### Size Reference

| Type   | Fixed Size | Notes |
|--------|------------|-------|
| Bool   | 1 byte     | 0x00 = false, 0x01 = true |
| I64    | 8 bytes    | Little-endian |
| F64    | 8 bytes    | IEEE 754 little-endian |
| Ref    | 16 bytes   | u128 object ID, little-endian |
| String | variable   | varint len in header, UTF-8 data in body |
| Bytes  | variable   | varint len in header, raw data in body |

### Example

Schema: `(id: Ref NOT NULL, count: I64 NULL, name: String NOT NULL, bio: String NULL)`

Variable columns: `name`, `bio` (2 header entries)
Fixed columns: `id` (16), `count` (1 + 8 = 9 with null flag)

Row data for `id=abc..., count=42, name="Alice", bio=NULL`:
```
[5]           <- name content length (5 bytes for "Alice")
[1]           <- bio content length (1 byte for null flag only)
[id: 16 bytes]
[0x01][42 as i64: 8 bytes]   <- count: present + value
[Alice: 5 bytes]              <- name: just data (not nullable)
[0x00]                        <- bio: null flag only
```

Row data for `id=abc..., count=NULL, name="Bob", bio="Hello"`:
```
[3]           <- name content length
[6]           <- bio content length (1 + 5)
[id: 16 bytes]
[0x00][padding: 8 bytes]      <- count: null (value bytes still present for fixed offset)
[Bob: 3 bytes]
[0x01][Hello: 5 bytes]        <- bio: present + value
```

**Note on nullable fixed-size columns:** Even when null, fixed-size columns keep their value bytes (can be zeroed) to maintain fixed offsets. Only the presence flag indicates null.

### Benefits of This Format

1. **O(1) column access**: Header gives exact offsets for variable columns
2. **Efficient diffing**: Can compare column-by-column for merge strategies
3. **Compact**: No type tags, no field names, minimal overhead
4. **Streamable**: Header first allows skipping columns without reading full row

## Index Objects

Indexes enable efficient reverse lookups for reference columns.

**Granularity:** One index object per `(source_table, source_column)` pair.

**Structure:**
```rust
// Index object content
struct RefIndex {
    // Maps target_id -> Vec<source_row_id>
    entries: BTreeMap<ObjectId, Vec<ObjectId>>,
}
```

**Maintenance:** Synchronously updated on row write:
1. Row write extracts Ref column values
2. For each Ref column with an index, update the index object
3. Both row commit and index commit happen together

**Example:**
- Table `posts` has column `author: Ref(users)`
- Index object: `posts.author` index
- When inserting `post_123` with `author = user_456`:
  - Index adds `user_456 -> [post_123]`
- Query "all posts by user_456" reads index directly

## Rust Types

```rust
/// Runtime value representation
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Ref(ObjectId),
}

/// A row with its object ID
pub struct Row {
    pub id: ObjectId,
    pub schema_id: SchemaId,
    pub values: Vec<Value>,  // In schema column order
}

/// Schema identifier (object ID of schema object)
pub type SchemaId = ObjectId;

/// Object identifier (UUIDv7)
pub type ObjectId = u128;
```

## API Design

### Table Operations

```rust
impl LocalNode {
    /// Create a new table, returns schema ID
    pub async fn create_table(&self, schema: TableSchema) -> Result<SchemaId>;

    /// Get table schema by name
    pub async fn get_table(&self, name: &str) -> Result<Option<TableSchema>>;

    /// List all tables
    pub async fn list_tables(&self) -> Result<Vec<TableSchema>>;
}
```

### Row Operations

```rust
impl LocalNode {
    /// Insert a new row, returns row ID (generated UUIDv7)
    pub async fn insert(&self, table: &str, values: Vec<Value>) -> Result<ObjectId>;

    /// Get row by ID
    pub async fn get(&self, table: &str, id: ObjectId) -> Result<Option<Row>>;

    /// Update row by ID
    pub async fn update(&self, table: &str, id: ObjectId, values: Vec<Value>) -> Result<()>;

    /// Delete row by ID
    pub async fn delete(&self, table: &str, id: ObjectId) -> Result<()>;
}
```

### Query Operations (Step 2)

```rust
impl LocalNode {
    /// Simple select with optional where clause
    /// WHERE clause supports only `column = value` for now
    pub async fn select(
        &self,
        table: &str,
        where_clause: Option<(&str, Value)>,
    ) -> Result<Vec<Row>>;

    /// Reverse lookup via index: find all rows referencing target_id
    pub async fn find_referencing(
        &self,
        source_table: &str,
        source_column: &str,
        target_id: ObjectId,
    ) -> Result<Vec<Row>>;

    /// Subscribe to query results with synchronous callback
    pub fn reactive_query(
        &self,
        sql: &str,
    ) -> Result<ReactiveQuery>;
}

/// A reactive query with synchronous callback support.
/// Callbacks fire synchronously during the same call stack as mutations.
pub struct ReactiveQuery {
    // ...
}

impl ReactiveQuery {
    /// Subscribe with a callback that fires immediately and on every change.
    pub fn subscribe(&self, callback: impl Fn(Arc<Vec<Row>>)) -> ListenerId;

    /// Unsubscribe a callback.
    pub fn unsubscribe(&self, id: ListenerId) -> bool;

    /// Get current rows.
    pub fn get(&self) -> QueryState;
}
```

## Implementation Phases

### Step 1: Basic Storage
- [ ] `ColumnType` and `ColumnDef` types
- [ ] `TableSchema` type with serialization
- [ ] Row binary encoding/decoding
- [ ] SQL parser: CREATE TABLE, INSERT, UPDATE, SELECT
- [ ] `create_table` - store schema as Object
- [ ] `insert` - create row Object with encoded data
- [ ] `get` - fetch and decode row by ID
- [ ] `update` - create new commit on row Object
- [ ] `delete` - tombstone commit on row Object
- [ ] `execute()` method for SQL strings

### Step 2: References and Queries
- [ ] `Ref` column type with target schema validation
- [ ] Index object creation per Ref column
- [ ] Synchronous index maintenance on insert/update/delete
- [ ] `select` with scan-based where clause (`=` only)
- [ ] `find_referencing` using index lookup
- [ ] `subscribe_select` for reactive queries
- [ ] `execute_reactive()` for reactive SELECT

## SQL Parser

A minimal SQL subset parser for common operations.

### Supported Statements

**CREATE TABLE:**
```sql
CREATE TABLE users (
    name STRING NOT NULL,
    email STRING,
    age I64,
    active BOOL NOT NULL
);

CREATE TABLE posts (
    author REFERENCES users NOT NULL,
    title STRING NOT NULL,
    body STRING,
    published BOOL
);

CREATE TABLE comments (
    post REFERENCES posts NOT NULL,
    author REFERENCES users NOT NULL,
    content STRING NOT NULL
);
```

Note: Every table implicitly has an `id` column (the Object ID / UUIDv7 primary key). You don't declare it.

**INSERT:**
```sql
INSERT INTO users (name, email, age, active)
VALUES ('Alice', 'alice@example.com', 30, true);

-- With explicit NULL
INSERT INTO posts (author, title, body, published)
VALUES (x'0192...', 'Hello World', NULL, false);
```

**UPDATE:**
```sql
UPDATE users SET email = 'new@example.com', age = 31
WHERE id = x'0192...';
```

**SELECT:**
```sql
-- Select all rows
SELECT * FROM users;

-- Select specific columns
SELECT name, email FROM users;

-- Match by ID
SELECT * FROM users WHERE id = x'0192...';

-- Filter with =
SELECT * FROM users WHERE active = true;
SELECT * FROM posts WHERE published = false;

-- Join via reference (implicit join through REFERENCES column)
SELECT * FROM posts WHERE author = x'0192...';

-- Combined: join + filter
SELECT * FROM posts WHERE author = x'0192...' AND published = true;

-- Multi-table join: find all comments by a user on published posts
SELECT comments.*
FROM comments
JOIN posts ON comments.post = posts.id
WHERE comments.author = x'0192...' AND posts.published = true;

-- Reverse lookup: find all posts by a specific author
SELECT * FROM posts WHERE author = x'0192...';

-- Chained joins: get user info for all commenters on a post
SELECT users.*
FROM comments
JOIN users ON comments.author = users.id
WHERE comments.post = x'0192...';
```

### Grammar

```
statement     = create_table | insert | update | select

create_table  = "CREATE" "TABLE" identifier "(" column_defs ")"
column_defs   = column_def ("," column_def)*
column_def    = identifier type nullable?
type          = "BOOL" | "I64" | "F64" | "STRING" | "BYTES"
              | "REFERENCES" identifier
nullable      = "NOT" "NULL" | "NULL"

insert        = "INSERT" "INTO" identifier "(" columns ")" "VALUES" "(" values ")"
columns       = identifier ("," identifier)*
values        = value ("," value)*

update        = "UPDATE" identifier "SET" assignments where_clause?
assignments   = assignment ("," assignment)*
assignment    = identifier "=" value

select        = "SELECT" projection "FROM" from_clause where_clause?
projection    = "*" | qualified_columns
qualified_columns = qualified_column ("," qualified_column)*
qualified_column  = (identifier ".")? identifier   -- table.column or just column

from_clause   = identifier join_clause*
join_clause   = "JOIN" identifier "ON" condition

where_clause  = "WHERE" conditions
conditions    = condition ("AND" condition)*
condition     = qualified_column "=" value

value         = string_lit | number_lit | bool_lit | "NULL" | uuid_lit
string_lit    = "'" [^']* "'"
number_lit    = "-"? [0-9]+ ("." [0-9]+)?
bool_lit      = "true" | "false"
uuid_lit      = "x'" hex_chars "'"
identifier    = [a-zA-Z_][a-zA-Z0-9_]*
```

### Parser Implementation

```rust
pub enum Statement {
    CreateTable(CreateTable),
    Insert(Insert),
    Update(Update),
    Select(Select),
}

pub struct CreateTable {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

pub struct Insert {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Value>,
}

pub struct Update {
    pub table: String,
    pub assignments: Vec<(String, Value)>,
    pub where_clause: Vec<Condition>,
}

pub struct Select {
    pub projection: Projection,
    pub from: FromClause,
    pub where_clause: Vec<Condition>,
}

pub enum Projection {
    All,                              // *
    Qualified(Vec<QualifiedColumn>),  // table.column or column
    TableAll(String),                 // table.*
}

pub struct QualifiedColumn {
    pub table: Option<String>,
    pub column: String,
}

pub struct FromClause {
    pub table: String,
    pub joins: Vec<Join>,
}

pub struct Join {
    pub table: String,
    pub on: Condition,
}

pub struct Condition {
    pub left: QualifiedColumn,
    pub right: Value,
}

/// Parse a SQL string into a statement
pub fn parse(sql: &str) -> Result<Statement, ParseError>;
```

### Execution

```rust
impl LocalNode {
    /// Execute a SQL statement
    pub async fn execute(&self, sql: &str) -> Result<ExecuteResult>;

    /// Execute and subscribe to reactive results (for SELECT)
    pub fn execute_reactive(&self, sql: &str) -> Result<ObjectSignal<Vec<Row>>>;
}

pub enum ExecuteResult {
    /// CREATE TABLE - returns schema ID
    Created(SchemaId),
    /// INSERT - returns new row ID
    Inserted(ObjectId),
    /// UPDATE - returns number of rows affected
    Updated(usize),
    /// SELECT - returns matching rows
    Selected(Vec<Row>),
}
```

### Query Execution Strategy

1. **Match by ID** (`WHERE id = x'...'`): Direct object lookup, O(1)
2. **Filter with =** (`WHERE column = value`): Table scan with filter
3. **Join via reference**: Use reverse index for `REFERENCES` columns
4. **Combined**: Apply in order - use indexes where available, then scan/filter

## Open Questions

1. **Tombstones vs hard delete**: How to represent deleted rows? Tombstone commit with special marker?

2. **Schema changes**: Future work - how to handle adding/removing/modifying columns?

3. **Composite indexes**: Future work - indexes on multiple columns?
