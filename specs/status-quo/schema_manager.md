# Schema Manager — Status Quo

In a distributed system, different clients will inevitably run different schema versions — a user who hasn't updated their app shouldn't lose data, and a server needs to serve both old and new clients simultaneously. The Schema Manager solves this by making schema evolution a first-class runtime concept, not just a deployment-time migration.

Every schema version is content-addressed (BLAKE3 hash), so identical schemas are always recognized as identical regardless of where or when they were created. Bidirectional lenses transform data between versions — when a query touches data written under an old schema, the lens chain converts it transparently.

The Schema Manager wraps the [Query Manager](query_manager.md), adding schema versioning on top of the raw query engine. It coordinates with the [Sync Manager](sync_manager.md) for schema discovery: schemas and lenses are themselves objects, synced like any other data. See [Schema Files](schema_files.md) for the developer-facing tooling that produces schemas and lenses.

## Content-Addressed Schemas

Every schema version is identified by a **SchemaHash** — BLAKE3 hash of the canonicalized schema (tables sorted by name, columns sorted within each table). Identical schemas always produce identical hashes.

> `crates/groove/src/query_manager/types.rs:17-68` — `SchemaHash::compute()`, `short()` (12-char hex prefix)

## Composed Branch Names

Branch names encode environment, schema version, and user branch:

```
{env}-{schemaHash8}-{userBranch}
```

Example: `dev-a1b2c3d4-main`

This is the key mechanism for schema isolation. Data written under schema v1 lives on branches like `prod-a1b2c3d4-main`; data under schema v2 lives on `prod-e5f6g7h8-main`. They never collide. When a query runs, the Schema Manager knows which branches to target based on the current schema and any live schemas reachable via lenses. A query without explicit `.branch()` gets this automatically.

> `crates/groove/src/query_manager/types.rs:187-268` — `ComposedBranchName`

## Lenses

Bidirectional transformations between schema versions.

**V1 operations**: `AddColumn`, `RemoveColumn`, `RenameColumn`, `AddTable`, `RemoveTable`.

All operations are declarative and auto-invertible — the backward transform is computed automatically from the forward transform.

> `crates/groove/src/schema_manager/lens.rs:24-55` (LensOp), `117-168` (LensTransform), `172-338` (Lens)

Key properties:
- `Lens::object_id()` uses UUIDv5(NAMESPACE_DNS, source_hash || target_hash) — deterministic
- `translate_column()` for index lookups across schema versions
- `apply()` transforms row values according to lens operations

### Draft Lenses

Auto-generated lenses may contain **draft** operations — uncertain transformations (potential renames, non-nullable columns without sensible defaults). Draft lenses fail fast at startup if found in the path to any live schema.

### Auto-Lens Generation

`generate_lens()` compares schemas and detects added/removed tables/columns. Potential renames (same type, different name) marked as draft. Sensible defaults generated for most types; UUID and non-nullable columns flagged for review.

> `crates/groove/src/schema_manager/auto_lens.rs:18-200`

## Schema Context

Tracks current schema, environment, user branch, live schemas (reachable via lenses), and pending schemas (awaiting lens paths).

Key capabilities:
- `lens_path()` — BFS from source to current, multi-hop support
- `validate()` — ensures no draft lenses in paths to live schemas
- `try_activate_pending()` — activates pending schemas when lens paths become available

> `crates/groove/src/schema_manager/context.rs:113-286` — 29 tests covering lens paths, multi-hop, validation, pending activation

### QuerySchemaContext

Minimal serializable context for server-mode queries: `(env, schema_hash, user_branch)`. Travels with queries over the wire.

> `crates/groove/src/schema_manager/context.rs:15-51`

## SchemaManager Coordination

Top-level integration layer wrapping SchemaContext + QueryManager.

### Client Mode

Fixed current schema (baked into app). Queries use implicit schema context.

```
SchemaManager::new(sync_manager, schema, app_id, env, user_branch)
```

> `crates/groove/src/schema_manager/manager.rs:89-123`

### Server Mode

No fixed current schema. Serves multiple clients with different schema versions.

```
SchemaManager::new_server(sync_manager, app_id, env)
```

- `add_known_schema()` — adds schemas without requiring lens path
- `subscribe_with_schema_context()` — builds temporary context with target as current
- Lazy branch activation via `set_known_schemas()` sync to QueryManager

> `crates/groove/src/schema_manager/manager.rs:125-717`

### Multi-Schema Queries

This is how old data becomes visible to new code. When a v2 client queries, the Schema Manager includes branches from both v2 and v1 (if a lens path exists):

1. **Live branches**: current schema + all schemas reachable via lens chains
2. **Index access**: column names translated through lens chain per branch (e.g., v1's `email` → v2's `email_address`)
3. **Row loading**: lens transform applied after loading from storage (adding default values for new columns, etc.)
4. **Merge**: union all results, LWW handles duplicates by ObjectId

> `crates/groove/src/schema_manager/transformer.rs` (726 lines — LensTransformer)

### Copy-on-Write Updates

Update a row in an old schema branch: load → apply lens to current → apply update → write to current branch. Old data stays in old branch.

> `crates/groove/src/schema_manager/writer.rs` (439 lines — CopyOnWriteWriter)

## App ID & Catalogue Discovery

`AppId` identifies an application's schema family. Uses UUIDv5(NAMESPACE_DNS, app_name).

> `crates/groove/src/schema_manager/types.rs:19-59`

### Schema/Lens Persistence

Schemas and lenses are themselves Jazz objects — they sync between nodes like any other data. When a client connects to a server, its schema and lens objects propagate through the normal sync protocol. The server discovers what schemas exist by observing these catalogue objects, and can then serve queries for any known schema version.

| Type | ObjectId | Content | Key Metadata |
|------|----------|---------|-------------|
| Schema | UUIDv5(schema_hash) | Binary-encoded Schema | `type=catalogue_schema`, `app_id`, `schema_hash` |
| Lens | UUIDv5(source \|\| target) | Binary-encoded LensTransform | `type=catalogue_lens`, `app_id`, `source_hash`, `target_hash` |

> `crates/groove/src/schema_manager/manager.rs:430-479` (persist_schema, persist_lens)

### Catalogue Processing

`process_catalogue_update()` handles incoming schema/lens objects:
- Verifies app_id match
- Schemas: added to known_schemas, pending if no lens path yet
- Lenses: registered, triggers pending schema activation
- Idempotent (handles duplicates)

> `crates/groove/src/schema_manager/manager.rs:481-603`

## Binary Encoding

Deterministic encoding for schemas and lenses with version byte prefix for forward compatibility.

> `crates/groove/src/schema_manager/encoding.rs`

## Error Handling

**SchemaError**: `DraftLensInPath`, `NoLensPath`, `SchemaNotFound`, `LensNotFound`

> `crates/groove/src/schema_manager/context.rs:54-109`

**QueryError**: `UnknownSchema(SchemaHash)` for server-mode queries with unknown schema.

## Test Coverage

29 context tests + E2E integration tests including:
- `e2e_catalogue_sync_with_data_query`
- `e2e_two_clients_server_schema_sync`
- `copy_on_write_update`

> `crates/groove/src/schema_manager/integration_tests.rs`
