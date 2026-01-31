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

> **⚠️ SECURITY TODO: Schema Push Authorization**
>
> Currently, any client can push schema/lens objects to the server via catalogue sync. This is a significant security gap:
>
> - Malicious clients could inject schemas with altered column types
> - Attackers could add tables/columns to exfiltrate data
> - Draft lenses could be pushed to cause server errors
>
> **Required:** Schema/lens pushes should require an **app admin token** separate from the normal session token. This token would:
> - Be issued to developers/operators, not end users
> - Be required for `type=catalogue_schema` and `type=catalogue_lens` objects
> - Be validated server-side before accepting catalogue updates
>
> Until implemented, treat schema sync as trusted-network-only.

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

## Server-Mode Schema Management

### The Client/Server Split

Clients and servers have fundamentally different schema contexts:

- **Client**: Fixed current schema (baked into app). Queries use implicit schema context.
- **Server**: No fixed current. Serves multiple clients with different schema versions.

### QuerySchemaContext

Minimal context for a query operation - what schema to target:

```rust
pub struct QuerySchemaContext {
    pub env: String,
    pub schema_hash: SchemaHash,
    pub user_branch: String,
}

impl QuerySchemaContext {
    pub fn branch_name(&self) -> ComposedBranchName { ... }
}
```

This travels with queries over the wire, allowing servers to execute queries using the client's schema as "current" for that operation.

### Server-Mode SchemaManager

Servers create a SchemaManager without a fixed current schema:

```rust
// Server mode - no fixed current schema
let manager = SchemaManager::new_server(sync_manager, app_id, "prod");

// Schemas are added when received from clients
manager.add_known_schema(client_schema);  // No lens path required

// Queries execute with explicit context
let ctx = QuerySchemaContext::new("dev", schema_hash, "main");
let results = manager.execute_with_schema_context(query, &ctx)?;
```

### known_schemas Storage

`SchemaManager` maintains `known_schemas: HashMap<SchemaHash, Schema>`:

- Populated automatically when `process_catalogue_schema()` receives schemas
- No lens path required for storage (unlike live schema activation)
- Used by `execute_with_schema_context()` for query execution
- Enables multi-tenant query handling on servers

### QueryManager Sync for Lazy Activation

In `SchemaManager::process()`, `known_schemas` is synced to QueryManager:

```rust
self.query_manager.set_known_schemas(self.known_schemas.clone());
```

This enables **lazy branch activation** in QueryManager:
- When a row arrives with unknown branch (e.g., `"client-a1b2c3d4-main"`)
- QueryManager parses the branch name to extract the short hash
- Looks up matching full hash in `known_schemas`
- If found, activates the branch by adding to `branch_schema_map`

This allows servers starting with no schema (`new_server()`) to automatically index row data as schemas arrive via catalogue sync.

### Query Execution Flow

**Client** (unchanged):
```rust
manager.execute(query)  // Uses implicit context from current schema
```

**Server**:
```rust
// Query arrives from client with explicit context
let ctx = request.schema_context;
manager.execute_with_schema_context(query, &ctx)?;
```

Internally, `execute_with_schema_context()`:
1. Looks up schema in `known_schemas` (returns `UnknownSchema` error if not found)
2. Builds temporary `SchemaContext` with target schema as "current"
3. Copies lenses from main context
4. Adds other known schemas as live if lens paths exist
5. Ensures indices exist for all reachable branches
6. Executes query using temporary context

### Lens Transforms Still Work

Even in server mode, multi-schema queries work correctly:

1. Server has schemas A and B in `known_schemas`
2. Server has lens between A and B
3. Client A sends query with `schema_hash_A`
4. Server builds context with A as current, B as live
5. Query reads from both branches, transforms B→A via lens

## Error Handling

### SchemaError

- `DraftLensInPath` - Draft lens found in path to live schema
- `NoLensPath` - No lens chain connects two schemas
- `SchemaNotFound` - Schema hash not in context
- `LensNotFound` - No lens between specified hashes

### QueryError

- `UnknownSchema` - Schema hash not in `known_schemas` (server mode)

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
- [x] QuerySchemaContext for parameterized queries
- [x] Server-mode SchemaManager (new_server, known_schemas)
- [x] execute_with_schema_context() for explicit context queries
- [x] SchemaHash serde serialization (hex format)
- [x] Lazy schema activation in QueryManager (server mode)
- [x] known_schemas sync to QueryManager via set_known_schemas()
- [x] E2E server schema bootstrap test (e2e_two_clients_server_schema_sync)

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
