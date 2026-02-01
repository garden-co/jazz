# Schema Files: SQL & TypeScript Definition Format

## Overview

This spec describes the SQL dialect for defining schemas and migrations, file conventions, and the `jazz build` CLI command.

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

### Migration DDL (ALTER TABLE)

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
├── current.sql                                           # Editable source of truth
├── schema_v1_455a1f10a158.sql                            # v1 with hash
├── schema_v2_add_description_357c464c4c43.sql            # Optional description before hash
├── schema_v3_abc123def456.sql                            # v3
├── migration_v1_v2_fwd_455a1f10a158_357c464c4c43.sql     # v1 → v2 forward
├── migration_v1_v2_bwd_455a1f10a158_357c464c4c43.sql     # v1 → v2 backward
└── ...
```

**Naming rules:**
- Schema: `schema_vN_{description}_{hash}.sql` where description is optional
- Migration: `migration_vA_vB_{fwd|bwd}_{hashA}_{hashB}.sql` (direction before hashes for readable truncation)
- Hash: First 12 hex chars (6 bytes) of BLAKE3 hash via `SchemaHash::short()`
- Hash is always the last component before `.sql` or `_fwd`/`_bwd`

**Version rules:**
- Versions must be sequential: v1, v2, v3, ...
- Versions must start at v1
- Gaps in versions cause build errors

**Hash validation:**
- Frozen schema files are immutable
- On each build, the content hash is verified against the filename hash
- If they don't match, build fails with "Frozen schemas must not be edited"

## CLI: `jazz build`

```bash
jazz build [--schema-dir ./schema] [--ts]
```

### Algorithm

1. Load all `schema_v*_*.sql` files
2. For each file:
   a. Parse version number, optional description, hash from filename
   b. Compute actual hash from content
   c. If filename hash ≠ computed hash → Error (frozen schemas must not be modified)
3. Validate sequential versions (v1, v2, v3...)

4. Compute hash of `current.sql`
5. Find if any schema file has this hash in its filename
   - If match exists: "Schema unchanged (matches vN)"
   - If no match: Create `schema_vN_{hash}.sql` (N = max_version + 1)

6. For each adjacent version pair (v1→v2, v2→v3...):
   - If `--ts`: generate TypeScript migration stub if missing
   - Else: generate SQL migration files if missing

7. Report results

### Examples

```bash
# First build creates v1
jazz build --schema-dir ./schema
# → Creates schema_v1_{hash}.sql

# Edit current.sql, then build again
jazz build --schema-dir ./schema
# → Creates schema_v2_{hash}.sql
# → Creates migration_v1_v2_{h1}_{h2}_fwd.sql
# → Creates migration_v1_v2_{h1}_{h2}_bwd.sql

# TypeScript mode (for jazz-ts users)
jazz build --ts --schema-dir ./schema
# → Creates TypeScript migration stub for user review
```

## TypeScript DSL

The `jazz-ts` package provides a TypeScript DSL for defining schemas, which is then compiled to SQL.

### Schema Definition

```typescript
// schema/current.ts
import { table, col } from "jazz-ts"

table("todos", {
  title: col.string(),
  completed: col.boolean(),
  description: col.string().optional(),
})
```

Uses side-effect collection — no export needed.

### Migration Definition

When the Rust CLI generates a migration stub in TypeScript mode, it creates:

```typescript
// schema/migration_v1_v2_455a1f10a158_357c464c4c43.ts
import { migrate, col } from "jazz-ts"

migrate("todos", {
  description: col.add().string({ default: "" }),
})
```

When the diff is ambiguous (e.g., possible rename vs add+drop), the operation is marked with `// TODO: Review this auto-generated operation`.

### TypeScript CLI: `jazz-ts build`

```bash
# Build TypeScript schemas and migrations
node packages/jazz-ts/bin/jazz-ts.js build --jazz-bin ./target/debug/jazz

# Or with pnpm
pnpm --filter jazz-ts exec jazz-ts build --jazz-bin ../../target/debug/jazz
```

The `jazz-ts build` command:
1. Compiles `current.ts` → `current.sql`
2. Compiles `migration_v*_v*_*_*.ts` → `*_fwd.sql` + `*_bwd.sql`
3. Runs `jazz build --ts` to validate and create new schema versions

### DSL Reference

**Column types (for `table()`):**
- `col.string()` → `TEXT NOT NULL`
- `col.string().optional()` → `TEXT`
- `col.boolean()` → `BOOLEAN NOT NULL`
- `col.int()` → `INTEGER NOT NULL`
- `col.float()` → `REAL NOT NULL`

**Migration operations (for `migrate()`):**
- `col.add().string({ default: "" })` → Forward: add column with default; Backward: drop column
- `col.drop().string({ backwardsDefault: "" })` → Forward: drop column; Backward: add column with default
- `col.rename("oldName")` → Rename column from old name

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
pub struct SchemaFileInfo {
    pub version: u32,
    pub description: Option<String>,
    pub hash: String,
}

pub struct MigrationFileInfo {
    pub from_version: u32,
    pub to_version: u32,
    pub from_hash: String,
    pub to_hash: String,
    pub direction: Option<Direction>,
}

pub struct SchemaDirectory { ... }

impl SchemaDirectory {
    pub fn new(path: impl AsRef<Path>) -> Self;
    pub fn current_schema(&self) -> Result<Schema, FileError>;
    pub fn schema_versions(&self) -> Result<Vec<SchemaFileInfo>, FileError>;
    pub fn schema_by_version(&self, version: u32) -> Result<Schema, FileError>;
    pub fn migration(...) -> Result<LensTransform, FileError>;
    pub fn write_schema(&self, schema: &Schema, version: u32, description: Option<&str>, hash: &str) -> Result<PathBuf, FileError>;
    pub fn write_migration_sql_pair(...) -> Result<(PathBuf, PathBuf), FileError>;
    pub fn write_migration_ts_stub(...) -> Result<PathBuf, FileError>;
}

pub fn parse_versioned_schema_filename(name: &str) -> Option<SchemaFileInfo>;
pub fn parse_migration_filename(name: &str) -> Option<MigrationFileInfo>;
pub fn schema_filename(info: &SchemaFileInfo) -> String;
pub fn migration_sql_filename(...) -> String;
pub fn migration_ts_filename(...) -> String;
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

## Git Branch Scenario

The versioned naming with hashes handles concurrent development:

```
main:     schema_v1_aaa.sql, schema_v2_bbb.sql
feature:  schema_v1_aaa.sql, schema_v2_ccc.sql  (different v2!)

# Merge creates conflict in filenames, user must resolve:
# - Keep one v2, renumber the other to v3
# - Or: use description to disambiguate: schema_v2_feature_ccc.sql
```

The hash in the filename ensures that two schemas with the same version number but different content will be detected as a conflict during git merge.

## Error Cases

1. **Gap in versions**: `schema_v1_*.sql`, `schema_v3_*.sql` (missing v2) → Error
2. **Invalid naming**: `schema_foo.sql` (no version/hash) → Ignored
3. **Modified frozen schema**: Content hash ≠ filename hash → Error: "Frozen schemas must not be edited"
4. **Duplicate version with same hash**: OK (same schema)
5. **Duplicate version with different hash**: Git merge conflict (needs manual resolution)

## Implementation

| File | Purpose |
|------|---------|
| `crates/groove/src/schema_manager/sql.rs` | SQL parsing/generation |
| `crates/groove/src/schema_manager/files.rs` | File convention API |
| `crates/groove/src/schema_manager/diff.rs` | Schema diffing |
| `crates/jazz-cli/src/commands/build.rs` | CLI build command |
| `packages/jazz-ts/src/cli.ts` | TypeScript CLI |
| `packages/jazz-ts/src/sql-gen.ts` | TypeScript → SQL generation |
| `examples/todo-server-rs/schema/current.sql` | Example SQL schema |
| `examples/todo-server-ts/schema/current.ts` | Example TypeScript schema |
