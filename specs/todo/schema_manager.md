# Schema Manager — TODO

Remaining work items and known limitations.

> Status quo: [specs/status-quo/schema_manager.md](../status-quo/schema_manager.md)

## Security: Schema Push Authorization

**Priority: High**

Currently, any client can push schema/lens objects to the server via catalogue sync. This is a significant security gap:

- Malicious clients could inject schemas with altered column types
- Attackers could add tables/columns to exfiltrate data
- Draft lenses could be pushed to cause server errors

**Required:** Schema/lens pushes should require an **app admin token** separate from the normal session token. This token would:
- Be issued to developers/operators, not end users
- Be required for `type=catalogue_schema` and `type=catalogue_lens` objects
- Be validated server-side before accepting catalogue updates

Until implemented, treat schema sync as trusted-network-only.

> `crates/groove/src/schema_manager/manager.rs` (catalogue processing has no auth check)
> See also: [specs/status-quo/sync_manager.md](../status-quo/sync_manager.md) — `CatalogueWriteDenied` error exists for User role, but Admin/Peer bypass

## Type Change Lens Operations

**Priority: Medium**

Cannot auto-generate lenses for column type changes. Currently only structural changes (add/remove/rename) are supported.

> `crates/groove/src/schema_manager/auto_lens.rs:172-174` — TODO comment

Workaround: manual lens creation with explicit operations.

## Draft Lens Logging

**Priority: Low**

Draft lenses are stored via catalogue processing but logging is incomplete.

> `crates/groove/src/schema_manager/manager.rs:591` — `// TODO: proper logging`

## Realistic Sync E2E Test

**Priority: Low**

Catalogue tests call `process_catalogue_update()` directly rather than pumping through SyncManager. A full end-to-end test with wire_up_sync() / pump_sync() helpers would exercise the complete flow.

> `crates/groove/src/schema_manager/integration_tests.rs`

## Unify QueryManager Constructors

**Priority: Low**

Two constructors with different behaviors:
1. `QueryManager::new()` — auto-subscribes to all object updates
2. `QueryManager::new_with_schema_context()` — does NOT auto-subscribe because `handle_object_update()` doesn't support multi-schema decoding

**Fix**: Make `handle_object_update()` schema-aware (detect branch schema via `branch_schema_map`, get appropriate descriptor, decode accordingly). Once fixed, both constructors should auto-subscribe uniformly.

> `crates/groove/src/query_manager/manager.rs`

## Code Quality (Non-Blocking)

1. 9+ one-line delegates to SchemaContext in manager.rs — minor boilerplate
2. `pending_schemas` is public in context.rs — lifecycle not enforced via private fields
3. Duplicate metadata building patterns in `persist_schema()` / `persist_lens()`
4. Similar error handling in `process_catalogue_schema()` / `process_catalogue_lens()` could be extracted

## GC for Archived Schema Versions

**Priority: Future**

No mechanism to garbage-collect old schema versions that are no longer reachable or needed. As apps evolve through many versions, the catalogue accumulates.
