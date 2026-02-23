# Weak Tests Audit — Week of 2026-02-09

This file replaces the old test-quality subsection from `general_cleanup.md` and tracks only remaining/new weak-test work.

## Scope

- Harden tests that still allow ambiguous pass conditions.
- Replace shape-only assertions with identity/content assertions.
- Add missing negative-path schema/catalogue coverage.

## Remaining Weak Assertions

## 2. Shape-Only Assertions (high priority)

- `crates/groove/src/query_manager/manager_tests.rs:insert_and_query`
  - Current: mostly count checks (`len()`).
  - Needed: verify row identities and key values for both unfiltered and filtered results.

- `crates/groove/src/sync_manager/tests.rs:local_commit_syncs_to_server`
  - Current: validates commit count/id only.
  - Needed: also assert destination server, object id, and branch.

## 3. Test Stub / Non-Behavioral Case (high priority)

- `crates/groove/src/schema_manager/integration_tests.rs:query_manager_queues_catalogue_updates`
  - Current: asserts only initial empty queue and then stops.
  - Needed: inject a real catalogue payload via sync path and assert pending catalogue update enqueue/dequeue behavior.

## 4. Broader Sweep (medium priority)

- Continue reducing `len()`-only checks in older tests (especially early `sync_manager/tests.rs` and older `manager_tests.rs` cases) where object identity/content can be asserted cheaply.
- Avoid broad `matches!` patterns that do not validate key payload fields.

## Missing Schema/Catalogue Negative Paths

## 5. Catalogue metadata filtering

- Add tests that non-matching `app_id` catalogue objects are ignored without side effects.
- Add tests that unknown catalogue `type` is ignored without side effects.

## 6. Malformed payload handling

- Add malformed schema-content test for `process_catalogue_update(... CatalogueSchema ...)` decode failure path.
- Add malformed lens-content test for `process_catalogue_update(... CatalogueLens ...)` decode failure path.

## 7. Invalid/missing lens metadata

- Missing `source_hash` or `target_hash` should return deterministic `SchemaError`.
- Invalid hash encoding/length in metadata should return deterministic `SchemaError`.

## Done in prior #7 passes

- Removed direct reliance on `test_get_row_if_loaded` / `test_subscriptions` in QueryManager tests.
- Deleted those test-only accessors from QueryManager.
- Replaced manual `create_with_id + add_commit + index_insert` setup in schema integration tests with ingest/public flows.
- Added runtime-core concurrency coverage, join invalid/circular/no-ON coverage, and non-cascading delete coverage.
