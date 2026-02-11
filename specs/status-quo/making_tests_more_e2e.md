# Making Tests More E2E — Status Quo

Testing philosophy for a multi-layered system. The insight: unit tests on individual components (SyncManager, QueryManager) construct "perfect" synthetic inputs that bypass production code paths. Bugs live at the boundaries — in how components interact, not in isolated logic.

## Core Recommendation: Adopted

**RuntimeCore is the primary correctness layer.** Tests create realistic 3-tier setups (A ↔ B[Worker] ↔ C[EdgeServer]) and exercise the full stack through the public API. Data flows through all layers — [Schema Manager](schema_manager.md) → [Query Manager](query_manager.md) → [Sync Manager](sync_manager.md) → [Storage](storage.md) — using real metadata and payloads constructed by the API, not hand-built test fixtures.

This approach caught real bugs that unit tests missed. For example, the row-object scope bypass bug (below) only manifested when realistic metadata flowed through the full stack — unit tests had always used empty metadata.

> `crates/groove/src/runtime_core.rs:959-1680+` (RuntimeCore tests)

## Example Bugs: Fixed

### Row-Object Scope Bypass

The original bug: `is_system_or_row_object` fast-path bypassed scope checks AND the pending queue for objects with `"table"` metadata. Tests never caught it because they used `metadata: HashMap::new()`.

**Fixed**: Role-based auth model (User/Admin/Peer) replaced scope-based permissions. User writes always go through ReBAC evaluation. Catalogue writes denied for User role.

> `crates/groove/src/sync_manager.rs:1237-1298` (role-based routing)

### PersistenceAck Without Consumer

Phase 6a added ack plumbing with no RuntimeCore API to consume it.

**Fixed**: Phase 6c added `insert_persisted()` / `update_persisted()` / `delete_persisted()` to RuntimeCore, tested at full-stack level.

> `crates/groove/src/runtime_core.rs` (durability API + tests)

## Current Test Landscape

| Layer | Tests | Purpose | E2E Level |
|-------|-------|---------|-----------|
| RuntimeCore | 20+ | Full-stack durability, sync, queries | High |
| QueryManager | ~91 | CRUD, subscriptions, filtering, sorting | Medium (below RuntimeCore) |
| ReBAC | 12 | Policy evaluation with realistic metadata | Medium |
| SyncManager | 44+ | Protocol correctness (message routing) | Low (surgical) |
| SchemaManager | 29+ | Schema versioning, lens transforms | Medium |
| Browser E2E | 10+ | Real Chromium + WASM + Worker + OPFS | Very High |

## Browser E2E Tests

The highest-fidelity tests: real Chromium via `@vitest/browser` + playwright. These exercise the complete path from TypeScript application code → generated query builders → Worker bridge → WASM runtime → OPFS persistence → server sync. If something works in these tests, it works in production.

> `packages/jazz-ts/tests/browser/worker-bridge.test.ts` (359 lines)
