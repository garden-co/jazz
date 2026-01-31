# SQL-Based Schema & Lens Definition Format

## Overview

This spec describes the SQL dialect for defining schemas and lenses, file conventions, and the `jazz build` CLI command.

## SQL Dialect

### Schema DDL (CREATE TABLE)

```sql
CREATE TABLE todos (
    title TEXT NOT NULL,
    completed BOOLEAN NOT NULL
);

CREATE TABLE users (
    name TEXT NOT NULL,
    email TEXT,
    age INTEGER
);
```

**Supported types:** `TEXT`, `INTEGER`, `BIGINT`, `BOOLEAN`, `TIMESTAMP`, `UUID`

**Constraints:** `NOT NULL` (column is non-nullable). Omitting means nullable.

### Lens DDL (ALTER TABLE)

```sql
ALTER TABLE users ADD COLUMN age INTEGER DEFAULT 0;
ALTER TABLE users DROP COLUMN deprecated_field;
ALTER TABLE users RENAME COLUMN email TO email_address;
CREATE TABLE new_table (id TEXT NOT NULL);
DROP TABLE old_table;
```

## File Convention

```
schema/
├── current.sql              # Editable source of truth
├── schema_a1b2c3d4e5f6.sql   # Frozen v1 (12-char hex hash)
├── schema_f7e8d9c0b1a2.sql   # Frozen v2
├── lens_a1b2c3d4e5f6_f7e8d9c0b1a2_fwd.sql  # v1 → v2
└── lens_a1b2c3d4e5f6_f7e8d9c0b1a2_bwd.sql  # v2 → v1
```

Hash: First 12 hex chars (6 bytes) of BLAKE3 hash via `SchemaHash::short()`.

## CLI: `jazz build`

```bash
jazz build [--schema-dir ./schema]
```

1. Parse `schema/current.sql` → Schema
2. Compute `SchemaHash` → `new_hash`
3. Compare to latest `schema_*.sql` version
4. If changed: generate new frozen schema + forward/backward lens files

## API

### SQL Parser (`groove::schema_manager::sql`)

```rust
pub fn parse_schema(sql: &str) -> Result<Schema, SqlParseError>;
pub fn parse_lens(sql: &str) -> Result<LensTransform, SqlParseError>;
pub fn schema_to_sql(schema: &Schema) -> String;
pub fn lens_to_sql(transform: &LensTransform) -> String;
```

### File Convention (`groove::schema_manager::files`)

```rust
pub struct SchemaDirectory { ... }

impl SchemaDirectory {
    pub fn new(path: impl AsRef<Path>) -> Self;
    pub fn current_schema(&self) -> Result<Schema, FileError>;
    pub fn schema_versions(&self) -> Result<Vec<SchemaHash>, FileError>;
    pub fn schema(&self, hash: SchemaHash) -> Result<Schema, FileError>;
    pub fn lens(&self, from: SchemaHash, to: SchemaHash, dir: Direction) -> Result<LensTransform, FileError>;
    pub fn write_schema(&self, schema: &Schema, hash: SchemaHash) -> Result<PathBuf, FileError>;
    pub fn write_lens_pair(&self, from: SchemaHash, to: SchemaHash, fwd: &LensTransform) -> Result<(PathBuf, PathBuf), FileError>;
}
```

### Schema Diff (`groove::schema_manager::diff`)

```rust
pub fn diff_schemas(old: &Schema, new: &Schema) -> DiffResult;

pub struct DiffResult {
    pub transform: LensTransform,
    pub ambiguities: Vec<Ambiguity>,
}

pub enum Ambiguity {
    PossibleRename { table, old_col, new_col },
    TypeChange { table, column, old_type, new_type },
}
```

## Implementation

| File | Purpose |
|------|---------|
| `crates/groove/src/schema_manager/sql.rs` | SQL parsing/generation |
| `crates/groove/src/schema_manager/files.rs` | File convention API |
| `crates/groove/src/schema_manager/diff.rs` | Schema diffing |
| `crates/jazz-cli/src/commands/build.rs` | CLI build command |
| `examples/todo-server/schema/current.sql` | Example schema file |
