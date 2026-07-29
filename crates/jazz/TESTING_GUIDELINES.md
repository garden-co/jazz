# Jazz Testing Guidelines - Rust

Always prefer black-boxed integration tests that exercise public APIs over unit tests or white-box tests.

- Canonical crate gate: `cargo test -p jazz --no-default-features --features test`, with a `-j`
  appropriate for the box (see `AGENTS.md`; the former fixed `-j 2` was a
  laptop-specific linker-OOM workaround, not a property of the build).
  This runs integration tests with Rust's default per-binary parallelism; fixtures
  must isolate app ids, ports, storage, and client state unless a test explicitly
  constructs a shared topology.
- Use public schema and permission builders (`SchemaBuilder`and `TableSchema::builder`).
  - Do not use JSON-like schema, permission, or query definitions.
- Set up the correct database topology.
  - If a single runtime is enough for the test, use `JazzClient::test_client`.
  - Otherwise use a `JazzServer` and connect `TestingClient`s to it
  - To simulate untrusted clients, create them with
    `JazzClient::connect_with_row_policy_mode(..., RowPolicyMode::PermissiveLocal)`.
- Assert user-visible effects through public client APIs: query rows,
  subscription deltas, accepted/rejected write settlement, or visible row state.
  - Use higher-level utils like `wait_for_query` to wait for results
- Use `row_input!` for inserts.

## What Should Stay Internal

Keep lower-level tests when the behavior is not meaningfully observable through public APIs.
Never do this silently: explicitly call out why an internal test is needed every time you write one.
