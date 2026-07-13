---
name: jazz-testing
description: Write or modify black-box tests for Jazz TypeScript applications through public APIs. Use when the requested work includes test code for database behavior, row permissions, local-first and offline behavior, cross-client sync, external JWT authentication, framework integration, durability, rejected mutations, or old/new schema migration compatibility. Do not load it merely because another Jazz task should eventually be tested.
---

# Jazz Testing

Test observable application behavior through the public Jazz API. Prefer a realistic in-memory
database or local sync server over mocks of runtime internals.

## Start from the existing suite

1. Read the project's test runner, setup files, schema, permissions, and nearby Jazz integration
   tests.
2. Preserve existing assertions unless the requested behavior intentionally changes.
3. Use the same package version and public entry points as the application.
4. Read [test-patterns.md](references/test-patterns.md), then choose the smallest topology that can
   prove the behavior.

## Choose the topology

- Pure query or mutation behavior without permissions or sync: use an isolated `createDb` setup.
- Row-policy behavior: use `createPolicyTestApp(app, permissions, expect)`.
- Server enforcement, cross-client sync, browser integration, or external auth: use
  `startLocalJazzServer(...)`, publish with `deploy(...)`, and connect real clients.
- Offline convergence: connect at least two clients, disconnect one, write on both sides, reconnect,
  and assert eventual observable state.
- Migration compatibility: deploy an old app, then the new app plus a migration, and exercise both
  schema clients.

## Keep tests black-boxed

- Build schemas, permissions, queries, and migrations with the public TypeScript DSL.
- Assert through `Db`, framework bindings, and `jazz-tools/testing` helpers.
- Do not inspect internal row-history tables, transport queues, query graphs, or private runtime
  fields to prove application behavior.
- Do not replace sync or permission enforcement with mocks when that behavior is the subject of the
  test.

## Synchronize on behavior

- Await `.wait({ tier: "edge" })` or `global` before asserting that another client or server can see
  a write.
- Use the test runner's retrying assertion (`vi.waitFor`, `expect.poll`, or equivalent) for eventual
  cross-client delivery.
- Make multi-row assertions deterministic: order the query explicitly, or normalize results by a
  stable key when row order is not the behavior under test.
- Avoid fixed sleeps. They hide missing durability waits and produce timing-dependent failures.
- Remember that denied reads resolve to filtered results, while denied writes reject.

## Isolate and clean up

- Use in-memory servers when persistence is not under test.
- Give persistent browser clients unique `dbName` and auth storage keys.
- Shut down every `Db`, stop every server and JWT issuer, and dispose every subscription the test
  creates.
- Prefer `beforeEach`/`afterEach` for per-test resources and global setup only when the suite
  deliberately shares one server.
- Keep test data explicit and local to the test.

## Verify permissions

- Test allowed and denied operations as different sessions.
- Test all relevant operations independently: read, insert, update, and delete.
- Test both old and new row conditions for ownership-sensitive updates.
- Assert denied reads as absent rows; do not expect a read-policy exception.
- Include relevant JWT claims in the test session when policies depend on them.

## Verify local-first behavior

- Assert the initiating client observes its local write immediately when that is the product
  behavior.
- Assert server or peer visibility only after the required durability or eventual-delivery boundary.
- Test reconnect and convergence when offline writes are part of the feature.
- Test rejection handling when a local write can later be denied by authority.

## Finish with the narrowest real checks

1. Run the new test in isolation.
2. Run the containing package's typecheck and test target.
3. Run broader integration or browser tests when the change crosses process or framework boundaries.
4. Report any behavior that cannot be tested through public APIs instead of reaching into internals.

## Avoid these failure modes

- Do not encode schemas, permissions, or migrations as JSON fixtures.
- Do not assert remote visibility immediately after an un-awaited local write.
- Do not assert array order from a query that has no explicit ordering.
- Do not reuse persistent storage names across parallel tests.
- Do not leave clients or servers running after a test.
- Do not rewrite an existing failing test to match new implementation behavior without permission.
