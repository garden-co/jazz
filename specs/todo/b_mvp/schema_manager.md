# Schema Manager — TODO

Remaining work items and known limitations.

> Status quo: [specs/status-quo/schema_manager.md](../../status-quo/schema_manager.md)

## Phasing

- **First week**: Schema push authorization — see `../a_week_2026_02_09/schema_push_authorization.md`
- **MVP**: Type-change lens operations, realistic sync E2E test
- **Later**: Draft lens logging, constructor unification, code quality, schema GC

## MVP: Type Change Lens Operations

Cannot auto-generate lenses for column type changes. Currently only structural changes (add/remove/rename) are supported.

> `crates/groove/src/schema_manager/auto_lens.rs:172-174` — TODO comment

Workaround: manual lens creation with explicit operations.

## MVP: Realistic Sync E2E Test

Catalogue tests call `process_catalogue_update()` directly rather than pumping through SyncManager. A full end-to-end test with `wire_up_sync()` / `pump_sync()` helpers would exercise the complete flow.

> `crates/groove/src/schema_manager/integration_tests.rs`

## Later: Draft Lens Logging

Draft lenses are stored via catalogue processing but logging is incomplete.

> `crates/groove/src/schema_manager/manager.rs:591` — `// TODO: proper logging`

## Later: Unify QueryManager Constructors

Two constructors with different behaviors:

1. `QueryManager::new()` — auto-subscribes to all object updates
2. `QueryManager::new_with_schema_context()` — does NOT auto-subscribe because `handle_object_update()` doesn't support multi-schema decoding

**Fix**: Make `handle_object_update()` schema-aware (detect branch schema via `branch_schema_map`, get appropriate descriptor, decode accordingly).

> `crates/groove/src/query_manager/manager.rs`

## Later: Code Quality (Non-Blocking)

1. 9+ one-line delegates to SchemaContext in manager.rs — minor boilerplate
2. `pending_schemas` is public in context.rs — lifecycle not enforced via private fields
3. Duplicate metadata building patterns in `persist_schema()` / `persist_lens()`
4. Similar error handling in `process_catalogue_schema()` / `process_catalogue_lens()` could be extracted

## Later: GC for Archived Schema Versions

No mechanism to garbage-collect old schema versions that are no longer reachable or needed. As apps evolve through many versions, the catalogue accumulates.
