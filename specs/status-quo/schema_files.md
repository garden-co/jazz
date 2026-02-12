# Schema Files — Status Quo

This is the developer-facing layer for schema management. While the [Schema Manager](schema_manager.md) handles runtime concerns (multi-version queries, lens transforms, catalogue sync), this layer handles build-time concerns: how developers define schemas, how versions are tracked on disk, and how migrations are generated.

The design philosophy is "schemas as code": developers edit `current.sql` (or `current.ts`), run `jazz build`, and the tool handles versioning, freezing, and migration generation automatically. Frozen schema files are immutable and content-hash-verified — if someone edits a frozen file, the build fails.

## SQL Dialect

### Schema DDL

```sql
CREATE TABLE todos (
    title TEXT NOT NULL,
    completed BOOLEAN NOT NULL
);
```

**Supported types**: `TEXT`, `INTEGER`, `BIGINT`, `BOOLEAN`, `TIMESTAMP`, `UUID`

**Constraints**: `NOT NULL` (omitting means nullable)

### Migration DDL

```sql
ALTER TABLE users ADD COLUMN age INTEGER DEFAULT 0;
ALTER TABLE users DROP COLUMN deprecated_field;
ALTER TABLE users RENAME COLUMN email TO email_address;
CREATE TABLE new_table (id TEXT NOT NULL);
DROP TABLE old_table;
```

> `crates/groove/src/schema_manager/sql.rs:556` (parse_schema), `582` (parse_lens)

## File Convention

```
schema/
├── current.sql                                           # Editable source of truth
├── schema_v1_455a1f10a158.sql                            # Frozen v1
├── schema_v2_add_description_357c464c4c43.sql            # Optional description
├── migration_v1_v2_fwd_455a1f10a158_357c464c4c43.sql     # Forward migration
├── migration_v1_v2_bwd_455a1f10a158_357c464c4c43.sql     # Backward migration
└── ...
```

- Schema: `schema_vN_{description}_{hash}.sql` (description optional)
- Migration: `migration_vA_vB_{fwd|bwd}_{hashA}_{hashB}.sql` (direction before hashes)
- Hash: 12-char hex (6 bytes of BLAKE3) via `SchemaHash::short()`
- Versions sequential from v1, no gaps allowed
- Frozen schemas are immutable (content hash verified on build)

> `crates/groove/src/schema_manager/files.rs:374-542` (filename parsing and generation)

## CLI: `jazz build`

```bash
jazz build [--schema-dir ./schema] [--ts]
```

Algorithm:

1. Load and validate all `schema_v*_*.sql` files (verify content hash matches filename)
2. Validate sequential versions
3. Compute hash of `current.sql`
4. If hash matches existing → "Schema unchanged"
5. If new → create `schema_vN+1_{hash}.sql`
6. Generate migrations for all adjacent version pairs (including cross-pair for branch merges)

> `crates/jazz-cli/src/commands/build.rs:69-194`

## TypeScript DSL

```typescript
import { table, col } from "jazz-ts";

table("todos", {
  title: col.string(),
  done: col.boolean(),
  description: col.string().optional(),
});
```

Migration:

```typescript
import { migrate, col } from "jazz-ts";

migrate("todos", {
  description: col.add().string({ default: "" }),
});
```

Uses side-effect collection (no export needed).

> `packages/jazz-ts/src/dsl.ts` (DSL implementation)
> `examples/todo-server-ts/schema/current.ts` (real example)

### TypeScript CLI: `jazz-ts build`

1. Compiles `current.ts` → `current.sql`
2. Compiles migration `.ts` → `_fwd.sql` + `_bwd.sql`
3. Runs `jazz build --ts` for validation and versioning

> `packages/jazz-ts/src/cli.ts`

### TypeScript Codegen

Generates `app.ts` with TypeScript interfaces, init types, WhereInput types, and QueryBuilder classes.

> `packages/jazz-ts/src/codegen/index.ts`
> `examples/todo-client-localfirst-ts/schema/app.ts` (generated output)

## Schema Diff

`diff_schemas(old, new)` returns `DiffResult` with a `LensTransform` and any `Ambiguity` items (possible renames, type changes).

> `crates/groove/src/schema_manager/diff.rs`

## Git Branch Merge

A subtle but important design: because frozen schema files include the content hash in their filename, two developers creating different v2 schemas on different git branches produce different files — no git conflict. After merge, both v2 variants coexist, and `jazz build` generates migrations from ALL v2 schemas to the new v3.

Only `current.sql`/`current.ts` can conflict in git, which is resolved by the developer creating the merged schema.

## API Summary

| Function                            | Location           |
| ----------------------------------- | ------------------ |
| `parse_schema(sql)`                 | `sql.rs:556`       |
| `parse_lens(sql)`                   | `sql.rs:582`       |
| `schema_to_sql(schema)`             | `sql.rs:630`       |
| `lens_to_sql(transform)`            | `sql.rs:656+`      |
| `SchemaDirectory`                   | `files.rs:105-372` |
| `diff_schemas(old, new)`            | `diff.rs:80`       |
| `parse_versioned_schema_filename()` | `files.rs:379-414` |
| `parse_migration_filename()`        | `files.rs:422-492` |

## Key Files

| File                                        | Purpose                             |
| ------------------------------------------- | ----------------------------------- |
| `crates/groove/src/schema_manager/sql.rs`   | SQL parsing/generation (700+ lines) |
| `crates/groove/src/schema_manager/files.rs` | File convention API (940+ lines)    |
| `crates/groove/src/schema_manager/diff.rs`  | Schema diffing (150+ lines)         |
| `crates/jazz-cli/src/commands/build.rs`     | CLI build command (370+ lines)      |
| `packages/jazz-ts/src/cli.ts`               | TypeScript CLI (195 lines)          |
| `packages/jazz-ts/src/dsl.ts`               | TypeScript DSL (180+ lines)         |
| `packages/jazz-ts/src/sql-gen.ts`           | TS → SQL generation                 |
| `packages/jazz-ts/src/codegen/index.ts`     | TS codegen (app.ts)                 |
