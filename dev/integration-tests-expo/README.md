# integration-tests-expo

An Expo app that **is** an on-device integration-test runner for the Jazz React
Native client (`jazz-rn`). It mirrors the browser integration tests
(`packages/jazz-tools/tests/browser/**`) so the same kinds of behaviours — bulk
writes, queries, subscriptions, relations, durability — are exercised against the
real native runtime.

It lives under `dev/` (not `examples/`) because it is internal QA tooling, not a
user-facing showcase.

## How it works

- On launch the app runs every suite in `tests/_registry.ts` **sequentially** and
  renders each test as a row that flips `pending → running → PASS/FAIL`.
- Each test gets a fresh, **local-only** `Db` (`createDb({ appId })`, no server) and
  has it shut down afterwards. Default-allow permissions mean no server or policy
  setup is needed.
- When every test passes, a `suite-passed` element renders. If any test fails (or the
  app freezes), it never appears.

## Maestro / CI

`dev/maestro/integration-tests/integration_tests.yaml` launches the app and waits for
`suite-passed`; a red test or a hang makes the wait time out and the flow throw. The
GitHub workflow `expo-android-integration-e2e.yml` builds a release APK and runs that
flow on Maestro Cloud (Android). No server, sandbox, or secrets beyond the Maestro
key.

## Run locally

```bash
pnpm --filter integration-tests-expo ios      # or: android
```

The runner starts automatically; watch the rows turn green.

## Unit tests (the runner itself, in Node)

The pure-TS runner modules (`runner/expect.ts`, `runner/support.ts`,
`runner/harness.ts`) are unit-tested in Node — they have no RN imports:

```bash
pnpm --filter integration-tests-expo test
```

## Add a test

1. Create `tests/<name>.suite.ts`:

   ```ts
   import { defineSuite } from "../runner/harness";
   import { app } from "../schema";

   export default defineSuite("<name>", ({ test }) => {
     test("does a thing", async ({ db, expect, waitForQuery }) => {
       db.insert(app.todos, { title: "x", done: false });
       const rows = await waitForQuery(db, app.todos, (r) => r.length === 1, "one lands");
       expect(rows).toHaveLength(1);
     });
   });
   ```

2. Register it in `tests/_registry.ts`.

Add tables to `schema.ts` (public DSL) as needed. Assertions use the bundled
Jest-style `expect` shim in `runner/expect.ts`.
