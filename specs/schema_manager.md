# Schema Manager Specification

Schema evolution through content-addressed schemas, environment-based branching, and bidirectional migration lenses.

## Conceptual Model

### Content-Addressed Schemas

Every schema version is identified by a **SchemaHash** - a BLAKE3 hash of the schema's structural elements:

```
SchemaHash = BLAKE3(canonicalized_schema)
```

Canonicalization ensures order-independence:
- Tables sorted by name
- Columns within each table sorted by name
- Structural elements (types, nullability, references) hashed deterministically

This means identical schemas always produce identical hashes, regardless of how they were constructed.

### Composed Branch Names

Branch names encode environment, schema version, and user branch:

```
{env}-{schemaHash8}-{userBranch}
```

Examples:
- `dev-a1b2c3d4-main` - Development, schema hash prefix a1b2c3d4, main branch
- `prod-f9e8d7c6-feature-x` - Production, different schema, feature branch

This naming scheme:
- Keeps different schema versions isolated in separate branches
- Allows multiple environments (dev/staging/prod) with same user branch
- Makes branch relationships discoverable from names alone

### Lenses

**Lenses** are bidirectional transformations between schema versions:

```
Lens {
    source_hash: SchemaHash
    target_hash: SchemaHash
    forward: LensTransform    // source → target
    backward: LensTransform   // target → source (auto-computed)
}
```

V1 lens operations:
- `AddColumn` / `RemoveColumn` - Add or remove a column with default value
- `RenameColumn` - Rename a column
- `AddTable` / `RemoveTable` - Add or remove a table

All operations are declarative and auto-invertible. The backward transform is computed automatically from the forward transform.

### Draft Lenses

Auto-generated lenses may contain **draft** operations - transformations where the system cannot be confident about correctness:

- Potential renames (same type, different name)
- Non-nullable columns without sensible defaults

Draft lenses fail fast at startup if found in the path to any live schema. Users must review and confirm draft operations before use.

### Multi-Schema Queries

When querying across schema versions:

1. **Live branches**: Current schema + explicitly selected live schemas (via lenses)
2. **Index access**: Column names translated through lens chain for each branch
3. **Row loading**: Lens transform applied after loading from storage
4. **Merge**: Union all results, existing LWW merge handles duplicates by ObjectId

### Copy-on-Write Updates

When updating a row that exists in an old schema branch:

1. Load row from old branch
2. Apply lens to transform to current schema
3. Apply update to transformed row
4. Write to current schema's branch (new object version)

Old data remains in old branch - no deletion or in-place modification.

## API Overview

### Schema Building

```rust
let schema = SchemaBuilder::new()
    .table(
        TableSchema::builder("users")
            .column("id", ColumnType::Uuid)
            .column("name", ColumnType::Text)
            .nullable_column("email", ColumnType::Text)
            .fk_column("org_id", "orgs")
    )
    .table(
        TableSchema::builder("posts")
            .column("id", ColumnType::Uuid)
            .fk_column("author_id", "users")
            .column("title", ColumnType::Text)
    )
    .build();
```

### Schema Hashing

```rust
let hash = SchemaHash::compute(&schema);
println!("Full: {}", hash);        // 64-char hex
println!("Short: {}", hash.short()); // 8-char hex prefix
```

### Lens Generation

```rust
// Automatic lens generation (may produce drafts)
let lens = generate_lens(&old_schema, &new_schema);

// Check for drafts
if lens.is_draft() {
    // Review draft_ops before use
}

// Manual lens creation
let lens = Lens::new(
    old_hash,
    new_hash,
    LensTransform::with_ops(vec![
        LensOp::RenameColumn {
            table: "users".into(),
            old_name: "email".into(),
            new_name: "email_address".into(),
        },
    ])
);
```

### Schema Context

```rust
let mut ctx = SchemaContext::new(current_schema, "dev", "main");

// Add live schema (old version still queryable)
ctx.add_live_schema(old_schema, lens);

// Validate no drafts in path
ctx.validate()?;

// Get all branch names for queries
let branches = ctx.all_branch_names();
```

### Integrated SchemaManager

```rust
// Create SchemaManager with integrated query support
let mut manager = SchemaManager::new(
    SyncManager::new(),
    current_schema,
    "dev",
    "main",
)?;

// Add previous schema version as live
manager.add_live_schema(old_schema)?;
manager.sync_context(); // Update QueryManager

// Insert a row (goes to current schema's branch)
let handle = manager.insert("users", &[id, name, email])?;
manager.process();

// Query across all live schema versions
// Rows from old schemas are automatically transformed via lens
let results = manager.execute(manager.query("users").build())?;

// Delete a row
manager.delete("users", object_id)?;
```

## Storage Format

### Schemas as Objects

Schemas are stored as Objects with:
- ObjectId = UUIDv5(content_hash) - deterministic from content
- Single immutable commit per schema version
- Content = binary-encoded schema structure

### Lenses as Objects

Lenses are stored as Objects with:
- ObjectId = UUIDv5(source_hash, target_hash) - deterministic from endpoints
- Multiple commits allowed (lens evolution)
- Content = binary-encoded LensTransform

## Error Handling

### SchemaError

- `DraftLensInPath` - Draft lens found in path to live schema
- `NoLensPath` - No lens chain connects two schemas
- `SchemaNotFound` - Schema hash not in context
- `LensNotFound` - No lens between specified hashes

## Implementation Status

### Completed (Phases 1-8 + Deep Integration + SchemaManager API)
- [x] Schema hashing with column/table order independence
- [x] Composed branch names (env-hash8-userBranch)
- [x] Lens types and operations (add/remove/rename column/table)
- [x] Bidirectional transforms (auto-computed backward)
- [x] Auto-lens generation from schema diffs
- [x] Draft detection for uncertain operations
- [x] Schema context with live schema tracking
- [x] Lens path finding for multi-hop migrations
- [x] SchemaManager coordination layer
- [x] LensTransformer for row transformation
- [x] Column name translation for index lookups
- [x] Branch-to-schema mapping helpers
- [x] CopyOnWriteWriter for cross-schema updates
- [x] Row write info tracking (source branch, target branch)
- [x] QueryManager.new_with_schema_context() for schema-aware queries
- [x] QueryGraph.compile_with_schema_context() for column translation
- [x] Row loader applies lens transforms for old schema branches
- [x] Automatic multi-branch query expansion from schema context
- [x] End-to-end tests with ObjectManager (insert v1 rows, query via v2, verify transform)
- [x] SchemaManager holds QueryManager (required, not optional)
- [x] SchemaManager.insert() - insert on schema branch
- [x] SchemaManager.execute() - query with automatic schema expansion
- [x] SchemaManager.delete() - soft delete on schema branch
- [x] SchemaManager.sync_context() - update QueryManager after adding live schemas
- [x] QueryManager.insert_on_branch() / delete_on_branch() for branch-aware writes

### Future Enhancements
- [ ] Type change lens operations
- [x] Multi-hop lens path traversal (v1 → v2 → v3)
- [ ] Schema/lens persistence as Objects
- [ ] GC for archived schema versions
