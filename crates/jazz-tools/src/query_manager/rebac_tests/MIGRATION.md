# Test Migration Guide

Use this guide when moving Jazz tests away from `RuntimeCore`/`SchemaManager`/`QueryManager`/storage internals
and toward `TestingServer` + `JazzClient`.

## Default Shape

- Start a real `TestingServer` with the test schema. If a single runtime is enough for the test,
  use `JazzClient::test_client` instead.
- Connect `JazzClient`s with `server.make_client_context_for_user(...)`.
  - To simulate untrusted clients, create them with
    `JazzClient::connect_with_row_policy_mode(..., RowPolicyMode::PermissiveLocal)`.
- Use public schema and permission builders (`SchemaBuilder`and `TableSchema::builder`).
  Do not use JSON-like schema, permission, or query definitions.
- Use `row_input!` for inserts.
- Assert user-visible effects through public client APIs: query rows,
  subscription deltas, accepted/rejected write settlement, or visible row state.
  - Use higher-level utils like `crate::test_support::wait_for_query` to wait for results

## What Should Stay Internal

Keep a lower-level test, or flag the migration as blocked, when the behavior is
not meaningfully observable through public APIs.

If an internal test can only be migrated by weakening what it proves, do not
rewrite it silently. Call out the mismatch.

## Migration Discipline

- Migrate one file or one behavior cluster at a time.
- Preserve the original behavioral claim, translated into visible effects.
- Keep unrelated refactors and warning cleanup out of the migration.
