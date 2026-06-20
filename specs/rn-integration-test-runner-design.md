# RN Integration Test Runner — Design Spec

Date: 2026-06-03
Status: Approved (brainstormed), pending implementation
Branch: `feat/rn-integration-test-runner`

## Problem

CI exercises the React Native / Expo stack with a single Maestro flow that drives
the `todo-client-localfirst-expo` example app (`dev/maestro/flows/todo_crud.yaml`).
That is one happy-path UI walkthrough. Meanwhile the browser/JS client has a rich
integration suite (`packages/jazz-tools/tests/browser/**`, `tests/ts-dsl/**`) that
exercises real DB behaviour: bulk writes, queries, subscriptions, relations,
convergence. **RN has no equivalent.** Regressions in the `jazz-rn` runtime that
don't break the narrow todo flow can ship unnoticed.

## Goal

A reusable, on-device integration **test suite for RN**, authored with DX that
mirrors the existing browser tests. It is delivered as a standalone Expo app that
_is_ a test runner: it runs a list of integration tests sequentially, renders each
result, and exposes a deterministic pass/fail signal that Maestro asserts in CI. A
red test — or a freeze/hang — makes the Maestro flow throw.

It is explicitly a growing harness: PR #956-style regression scenarios are the kind
of thing it should host over time. #956 itself is **not** in Phase 1 (it needs
multi-peer sync; see Phasing).

## Non-goals

- Not a user-facing tutorial/example (lives outside the `examples/` showcase path).
- Not sharing test _files_ with the browser suite (chosen Option ②: mirror the
  style, separate files). No dynamic cross-runtime import abstraction.
- Phase 1 does **not** cover multi-peer sync, a server, or the in-process relay.
- iOS is out of scope for Phase 1 (Android-only, matching existing Maestro CI).

## Key decisions

1. **Option ② — mirrored RN suite.** RN-only test files written in the same
   ergonomic style as the browser tests, using ported helpers. Not literally shared
   with the browser suite.
2. **Placement (c) — out of the showcase path.** App lives at
   `dev/integration-tests-expo/` (co-located with the existing `dev/maestro/`
   infra), added as a one-line entry to `pnpm-workspace.yaml`. The Maestro flow lives
   in its **own** workspace dir `dev/maestro/integration-tests/` (not the shared
   `dev/maestro/flows/`, which holds the todo flow) so the new CI job runs only this
   flow.
3. **Local-only Phase 1.** Tests use `createDb` from `jazz-tools/react-native` with
   no `serverUrl`. No Vercel Sandbox, no jazz-tools server, no deploy, no secrets —
   which also sidesteps the known WS-upgrade flakiness
   (`specs/todo/issues/expo-android-maestro-e2e-ws-unverified.md`).
4. **Hand-rolled `expect`.** A small Jest-style assertion module (Hermes-safe,
   unit-tested in Node), behind a single `runner/expect.ts`. `@vitest/expect` (already
   in the tree, browser-proven) can be swapped in later if a CI spike confirms it
   runs on Hermes. Decision driven by: the on-device assertion path cannot be
   verified locally, and a controlled shim removes that risk with no DX cost for the
   matchers used.
5. **Auto-run on launch + deterministic terminal signal** for Maestro (below).

## Architecture

A normal Expo app (mirrors `todo-client-localfirst-expo`'s scaffold: `app.json`
Hermes + new arch, `index.js`, `metro.config.mjs` with `withJazz`, `babel.config.js`,
`tsconfig.json`, prebuild → `android/`). Its single screen is the test runner.

```
dev/integration-tests-expo/
├── App.tsx                     # runtime/schema bootstrap (local-only) → TestRunnerScreen
├── app.json                    # bundleId/package: dev.jazz.integrationtests, Hermes
├── index.js, metro.config.mjs, babel.config.js, tsconfig.json
├── schema.ts                   # test tables (todos, projects, orgs, teams, users) via public DSL
├── runner/
│   ├── harness.ts              # defineSuite/test, TestCtx, sequential runner + state machine
│   ├── expect.ts               # hand-rolled Jest-style matchers
│   ├── support.ts              # ported waitForQuery/waitForCondition/withTimeout/sleep/uniqueAppId/cleanup
│   └── types.ts                # TestStatus, SuiteResult, etc.
├── tests/
│   ├── _registry.ts            # explicit list of suites (deterministic order)
│   ├── bulk-writes.test.ts
│   ├── crud.test.ts
│   ├── queries.test.ts
│   ├── subscriptions.test.ts
│   ├── relations.test.ts
│   └── durability.test.ts
└── ui/
    └── TestRunnerScreen.tsx    # renders rows + summary + Maestro testIDs
```

### Runtime/runner flow

1. App mounts → native `jazz-rn` module + schema are bootstrapped local-only
   (mirroring the todo app's `withJazz`/metro wiring; `JazzProvider` only if the
   bootstrap needs it — otherwise the runner calls `createDb` directly). Each test
   creates its **own** `Db` via `createDb` so tests are isolated. Exact schema wiring
   is pinned during implementation.
2. `TestRunnerScreen` mounts → kicks off the runner over `_registry` suites,
   **sequentially**, updating observable state per test: `pending → running →
passed | failed(error)` with a duration.
3. Each test runs inside a per-test timeout (default ~30s) with auto-cleanup of any
   `Db`/subscriptions it created. A thrown assertion or timeout → `failed` with the
   message; the runner continues to the next test (so the UI shows the full picture)
   then settles into a terminal suite state.

### Test authoring DX (the priority)

```ts
// tests/bulk-writes.test.ts
import { defineSuite } from "../runner/harness";
import { todos, allTodos } from "../schema";

export default defineSuite("bulk writes", ({ test }) => {
  test("writes 100 todos and reads them all back", async ({ db, expect, waitForQuery }) => {
    for (let i = 0; i < 100; i++)
      db.insert(todos, { title: `t-${i}`, done: false, owner_id: "me" });
    await waitForQuery(db, allTodos, (r) => r.length === 100, "100 land", 10_000);
    expect((await db.all(allTodos)).length).toBe(100);
  });
});
```

`TestCtx` handed to each test:

```ts
type TestCtx = {
  db: Db; // fresh local Db, auto-created (unique appId) + auto-shutdown
  expect: Expect; // hand-rolled matchers
  waitForQuery;
  waitForCondition;
  withTimeout;
  sleep;
  uniqueAppId;
};
```

Adding a test = drop a `*.test.ts` in `tests/`, `export default defineSuite(...)`,
add one line to `_registry.ts`. (Explicit registry over `require.context` for
deterministic ordering.)

## Maestro pass/fail contract

UI state machine with stable testIDs:

| testID                          | meaning                                                                          |
| ------------------------------- | -------------------------------------------------------------------------------- |
| `test-row-<slug>` + status text | per-test row: pending → running → `PASS`/`FAIL` (the "one after another" visual) |
| `suite-status`                  | text = `RUNNING` → `PASSED` \| `FAILED`                                          |
| `suite-passed`                  | rendered **only** when every test passed                                         |
| `suite-failed`                  | rendered if any test failed; lists failing names + error messages                |

Flow `dev/maestro/integration-tests/integration_tests.yaml`:

```yaml
appId: dev.jazz.integrationtests
---
- launchApp
- extendedWaitUntil:
    visible: { id: "suite-passed" }
    timeout: 120000
- assertNotVisible: { id: "suite-failed" }
```

A red test → `suite-passed` never renders → `extendedWaitUntil` times out → Maestro
throws. A freeze/hang → identical outcome. One wait covers both failure and deadlock;
the failure screenshot captures the failing test name + message.

## CI

New workflow `.github/workflows/expo-android-integration-e2e.yml`, modeled on
`expo-android-maestro-e2e.yml` but **stripped of the server**:

- Reuse `rn-build-reusable.yml` for jazz-rn Android artifacts.
- `validate` (schema + `tsc --noEmit`) → `expo prebuild --platform android` → JDK/Gradle/SDK/NDK setup → `gradlew :app:assembleRelease`.
- **No** Vercel Sandbox, jazz-tools server, deploy, or secrets (local-only; `appId` baked in, no `serverUrl`).
- `mobile-dev-inc/action-maestro-cloud` runs the `dev/maestro/integration-tests/` workspace against the APK; upload screenshots/logs on failure.
- Triggers: push to `main`; PRs touching `crates/jazz-rn/**`,
  `packages/jazz-tools/src/react-native/**`, `dev/integration-tests-expo/**`,
  `dev/maestro/integration-tests/**`, and the workflow file.

## Phase-1 test list (local, no server)

1. **bulk writes** — insert 100 rows, read back all 100, assert count + sample shape.
2. **CRUD round-trip** — insert → update field → assert → delete → assert gone.
3. **query conditions** — mixed rows; `db.all` with `eq`/filter + ordering/limit.
4. **subscribeAll deltas** — assert add/update/remove deltas + final `all`.
5. **include relations** — orgs→teams→users→todos; nested query result.
6. **local durability** — `insert(...).wait({ tier: "local" })` resolves + queryable.

## Phasing

- **Phase 1 (this spec):** the app, runner, hand-rolled `expect`, support helpers,
  the 6 local tests, Maestro flow, CI workflow, workspace wiring.
- **Phase 2 (later):** multi-peer sync tests — either point at a server, or build the
  in-process `addClient`/`addServer`/`batchedTick` relay. Designed-for (registry +
  ctx accommodate a second `Db`), not built yet. PR #956-style deferred-subscription
  regression belongs here.

## Verification strategy

- **Locally (Node):** unit-test `runner/expect.ts` (matchers incl. deep `toEqual`),
  `runner/support.ts`, and the runner state machine; exercise the actual **test
  bodies** against Node `createDb` (`jazz-tools`) to prove the assertions are correct
  independent of RN. `tsc --noEmit` for the whole app.
- **CI (device):** `expo prebuild` + release APK build + Maestro flow are validated
  by the new workflow. The on-device Hermes assertion path and the end-to-end
  pass/fail gate are confirmed there.

## Risks / open implementation details

- **Schema/metro wiring:** how `withJazz` injects/compiles the schema for the RN
  runtime (todo app relies on it). Must be mirrored exactly; confirm local-only mode
  (no `serverUrl`) is happy with `withJazz`. Pin during implementation.
- **`expect` surface creep:** keep matchers minimal (what the 6 tests use); grow
  deliberately. Deep `toEqual` + a couple asymmetric matchers are the only non-trivial
  bits.
- **batched_tick / timing:** local writes are fast, but waiters (`waitForQuery`) must
  poll, not assume synchronous convergence. Generous per-test + Maestro timeouts.
- **Maestro workspace scoping:** ensure the new flow runs in a way that doesn't pull
  in the todo flow (separate workspace dir or appId filtering).

```

```
