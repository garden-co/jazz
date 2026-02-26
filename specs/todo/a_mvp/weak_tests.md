# Weak Tests Audit — Week of 2026-02-09

This file replaces the old test-quality subsection from `general_cleanup.md` and tracks only remaining/new weak-test work.

## Scope

- Harden tests that still allow ambiguous pass conditions.
- Replace shape-only assertions with identity/content assertions.
- Add missing negative-path schema/catalogue coverage.

## Remaining Weak Assertions

## 4. Broader Sweep (medium priority) - Partially done but needs more work

- Continue reducing `len()`-only checks in older tests (especially early `sync_manager/tests.rs` and older `manager_tests.rs` cases) where object identity/content can be asserted cheaply.
- Avoid broad `matches!` patterns that do not validate key payload fields.

## Missing Schema/Catalogue Negative Paths

## 7. Invalid/missing lens metadata

- Missing `source_hash` or `target_hash` should return deterministic `SchemaError`.
- Invalid hash encoding/length in metadata should return deterministic `SchemaError`.

## Done in prior #7 passes

- Removed direct reliance on `test_get_row_if_loaded` / `test_subscriptions` in QueryManager tests.
- Deleted those test-only accessors from QueryManager.
- Replaced manual `create_with_id + add_commit + index_insert` setup in schema integration tests with ingest/public flows.
- Added runtime-core concurrency coverage, join invalid/circular/no-ON coverage, and non-cascading delete coverage.
