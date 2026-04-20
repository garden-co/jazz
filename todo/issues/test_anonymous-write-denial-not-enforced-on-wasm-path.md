# Anonymous-write denial not enforced on the WASM/NAPI insert path

## What

`AnonymousWriteDenied` fires for `query_manager::insert_with_write_context_and_id` (the direct query-manager path, exercised by `anonymous_insert_is_denied_before_policy_eval` in `manager_tests.rs`), but WASM/NAPI calls flow through `schema_manager::insert_with_write_context_and_id` → `query_manager::insert_on_branch_with_schema_and_write_context_and_id`, which has no anonymous check. So `db.insert(...)` and `db.insertDurable(...)` from an anonymous-JWT client succeed locally.

Blocks the "anonymous Db attempts a write, throws `AnonymousWriteDeniedError`" E2E test (rolled back in PR #586 after adding the check broke ~100 tests that rely on `createDb()` auto-minting an anonymous JWT).

## Priority

medium

## Notes

- Path-specific check at `query_manager/writes.rs:911` (hit) vs missing check at `query_manager/writes.rs:~1260` (the schema-manager-driven path).
- Attempted fix: adding the check at the schema-manager path fires denial for every test that uses `createDb` without `secret` — the whole test suite writes under anonymous. Options to unlock:
  - change `createDb` to NOT auto-mint an anonymous JWT when no credential is supplied (session-less writes skip the check); OR
  - migrate existing tests to pass a `secret` (local-first auth); OR
  - move the anonymous denial to the sync layer only — local writes OK, but syncing to the server is denied.
- Spec question: should anonymous mode allow _local-only_ writes, or should it deny all writes structurally? Today's behavior is split by accident.
