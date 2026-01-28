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

## App ID and Catalogue Discovery

### App ID Concept

An **App ID** is a UUID that identifies an application's schema family. All schemas and lenses for that app reference this ID in their metadata:

```
App ID: "my-todo-app" → UUIDv5(NAMESPACE_DNS, "my-todo-app")
```

### Schema/Lens Persistence

Schemas and lenses are persisted as Objects for discovery via sync:

**Schema Object:**
- `ObjectId = UUIDv5(NAMESPACE_DNS, schema_hash.as_bytes())` - deterministic from content
- Single commit on branch `"main"`
- Content = binary-encoded Schema
- Metadata: `{"type": "catalogue_schema", "app_id": "<uuid>", "schema_hash": "<64-char hex>"}`

**Lens Object:**
- `ObjectId = UUIDv5(NAMESPACE_DNS, source_hash || target_hash)` - deterministic from endpoints
- Single commit on branch `"main"`
- Content = binary-encoded LensTransform
- Metadata: `{"type": "catalogue_lens", "app_id": "<uuid>", "source_hash": "<hex>", "target_hash": "<hex>"}`

### Catalogue Query

When a client initializes with an app ID, it subscribes to a **catalogue query**. The server sends all matching schema/lens Objects. As new schemas/lenses are persisted, they automatically sync to all subscribed clients.

### Sync Flow: Schema Discovery

```
[v2 Client]                     [Server]                    [v1 Client]
     |                              |                             |
     |--subscribe(app_id)---------->|                             |
     |                              |<--subscribe(app_id)---------|
     |                              |                             |
     |--persist schema v2---------->|--schema v2 (app_id match)-->|
     |--persist lens(v1->v2)------->|--lens (app_id match)------->|
     |                              |                             |
     |                              |           [v1 discovers lens]
     |                              |           [adds v2 as live]
     |                              |                             |
     |--insert row on v2 branch---->|--forward to v1 (in scope)-->|
     |                              |                             |
     |                              |     [v1 queries both branches]
     |                              |     [v2 rows transformed via lens.backward]
```

When v1 client receives the lens via catalogue sync, it adds v2 as a "live" schema. When querying v2 data, it applies `lens.backward` to transform v2 rows to v1 format.

### Pending Schemas

Schemas received via catalogue without a lens path to current are stored as **pending**. They become live when:
1. A lens arrives that connects them to the current schema
2. Multi-hop paths are considered (v1→v2 lens may unlock pending v3 if v2→v3 exists)

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
// Create SchemaManager with app ID for catalogue discovery
let app_id = AppId::from_name("my-app");
let mut manager = SchemaManager::new(
    SyncManager::new(),
    current_schema,
    app_id,
    "dev",
    "main",
)?;

// Add previous schema version as live
manager.add_live_schema(old_schema)?;
manager.sync_context(); // Update QueryManager

// Persist schema and lens to catalogue for discovery
manager.persist_schema();
manager.persist_lens(&lens);

// Insert a row (goes to current schema's branch)
let handle = manager.insert("users", &[id, name, email])?;
manager.process(); // Drives sync and processes catalogue updates

// Query across all live schema versions
// Rows from old schemas are automatically transformed via lens
let results = manager.execute(manager.query("users").build())?;

// Delete a row
manager.delete("users", object_id)?;
```

## Error Handling

### SchemaError

- `DraftLensInPath` - Draft lens found in path to live schema
- `NoLensPath` - No lens chain connects two schemas
- `SchemaNotFound` - Schema hash not in context
- `LensNotFound` - No lens between specified hashes

## Implementation Status

### Completed
- [x] Schema hashing with column/table order independence
- [x] Composed branch names (env-hash8-userBranch)
- [x] Lens types and operations (add/remove/rename column/table)
- [x] Bidirectional transforms (auto-computed backward)
- [x] Auto-lens generation from schema diffs
- [x] Draft detection for uncertain operations
- [x] Schema context with live schema tracking
- [x] Lens path finding (bidirectional BFS, multi-hop)
- [x] SchemaManager coordination layer
- [x] LensTransformer for row transformation (direction-aware)
- [x] Column name translation for index lookups
- [x] Branch-to-schema mapping helpers
- [x] CopyOnWriteWriter for cross-schema updates
- [x] QueryManager.new_with_schema_context() for schema-aware queries
- [x] QueryGraph.compile_with_schema_context() for column translation
- [x] Automatic multi-branch query expansion from schema context
- [x] AppId type for application identification
- [x] Schema/Lens binary encoding (deterministic, versioned)
- [x] SchemaHash::to_object_id() for deterministic ObjectIds
- [x] Catalogue persistence (persist_schema, persist_lens)
- [x] Catalogue update processing (process_catalogue_update)
- [x] Pending schema tracking and activation
- [x] SchemaManager.process() drains catalogue updates
- [x] E2E tests for catalogue flow and multi-hop activation

### Known Limitations

1. **Draft lens handling**: Draft lenses are stored via catalogue but a TODO remains for proper logging.

2. **No realistic sync E2E test**: Catalogue tests call `process_catalogue_update()` directly. Full SyncManager wiring test would require `wire_up_sync()` / `pump_sync()` helpers.

### Future Enhancements
- [ ] Type change lens operations
- [ ] GC for archived schema versions
- [ ] Unify QueryManager constructors (see below)

## TODO: Unify QueryManager Constructors

Two QueryManager constructors exist with different behaviors:

1. **`QueryManager::new()`** - Auto-subscribes to all object updates. Sync'd data is automatically indexed.

2. **`QueryManager::new_with_schema_context()`** - Does NOT auto-subscribe because `handle_object_update()` doesn't support multi-schema decoding.

### Fix Required

`handle_object_update()` needs to be schema-aware:
1. Detect which schema the branch uses (via `branch_schema_map`)
2. Get the appropriate descriptor for that schema
3. Decode using that descriptor (or skip indexing and let query-time lens transform handle it)

Once fixed, both constructors should auto-subscribe uniformly.

## Code Quality Notes

Identified during implementation review - non-blocking but worth addressing:

1. **Wrapper delegation** (manager.rs): 9+ one-line delegates to SchemaContext could be reduced
2. **pending_schemas is public** (context.rs): Lifecycle not enforced; consider event-based activation
3. **Duplicate metadata building** (manager.rs): schema_metadata/lens_metadata share patterns
4. **process_catalogue_* duplication**: Similar error handling could be extracted
