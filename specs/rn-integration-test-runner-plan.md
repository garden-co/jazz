# RN Integration Test Runner — Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans / subagent-driven-development to implement task-by-task. Steps use `- [ ]` checkboxes.

**Goal:** A standalone Expo app at `dev/integration-tests-expo/` that runs Jazz integration tests on-device, renders pass/fail per test, and exposes a deterministic signal Maestro asserts in CI.

**Architecture:** Local-only `createDb` (no server/permissions). Pure-TS runner (`expect` shim + support helpers + sequential runner) decoupled from the RN test registry so it is Node-unit-testable. RN test bodies use the public DSL. A UI renders rows + a terminal `suite-passed`/`suite-failed` element. New CI workflow builds the APK and runs a dedicated Maestro flow.

**Tech Stack:** Expo 54, React Native 0.81, Hermes, `jazz-tools`/`jazz-rn` (workspace), vitest (Node unit tests only), Maestro Cloud, GitHub Actions.

**Verification split:** pure-TS modules → vitest in Node (local). RN bodies/UI/App → `tsc --noEmit` (local). APK build + Maestro pass/fail → CI only.

---

## Pinned API facts (verified, verbatim sources)

- RN `createDb(config: DbConfig): Promise<Db>` from `jazz-tools/react-native`. `appId` required; `serverUrl` optional → omit for local-only (`packages/jazz-tools/src/runtime/db.ts:92`, `react-native/db.ts:8`).
- Schema rides on queries: `app = s.defineApp(schema)`; `db.all(query)` reads `query._schema` (`typed-app.ts:1321`, `runtime/db.ts:1932`). No metro/`withJazz`/env needed for schema.
- Empty/absent permissions = default allow locally (`napi.integration.test.ts:763`, `schema-permissions.ts:529`). No permissions file required.
- No `secret` → runtime mints an anonymous session automatically (`react-native/db.test.ts:99`). `createDb({ appId })` alone is enough.
- Db API: `insert(table, data) → WriteResult` with `.value` and `.wait({tier})`; `update(table,id,data)`; `delete(table,id)`; `all(query)`; `one(query)`; `subscribeAll(query, cb) → () => void`; `shutdown()` (`runtime/db.ts:1674..2145`, `client.ts:657..686`). `DurabilityTier = "local"|"edge"|"global"`.
- Query DSL (public, NO JSON): `app.todos` (all), `.where({ col: { eq|gt|lt|... : v } })`, `.orderBy("col","asc"|"desc")`, `.limit(n)`, `.select(...)`, `.include({ rel: true })`. Forward ref `projectId: s.ref("projects")` → include key `project`.
- `subscribeAll` delta: `{ all: T[]; delta: RowDelta[] }`, kinds `Added=0, Removed=1, Updated=2` (`subscription-manager.ts:17`). Query type requires `T extends { id: string }`.
- Schema DSL: `s.table`, `s.string`, `s.boolean().default(false)`, `s.array(s.string()).default([])`, `s.ref("t")`, `.optional()`, `s.defineApp`, `s.RowOf<typeof app.todos>`.

---

## File structure

```
dev/integration-tests-expo/
├── package.json            # mirrors todo app deps
├── app.json                # bundleId/package dev.jazz.integrationtests, Hermes, newArch
├── babel.config.js         # babel-preset-expo + unstable_transformImportMeta
├── tsconfig.json           # extends expo/tsconfig.base
├── index.js                # imports jazz-tools/expo/polyfills + registerRootComponent
├── metro.config.mjs        # default expo config + symlinks (NO withJazz)
├── vitest.config.ts        # Node, includes runner/**/*.test.ts only
├── schema.ts               # test tables via public DSL
├── runner/
│   ├── types.ts            # TestStatus/TestResult/SuiteSummary
│   ├── expect.ts           # hand-rolled Jest-style matchers (+ expect.test.ts)
│   ├── support.ts          # sleep/uniqueAppId/waitForCondition/withTimeout/waitForQuery (+ support.test.ts)
│   └── harness.ts          # defineSuite/runSuites + summarize (+ harness.test.ts)
├── tests/
│   ├── _registry.ts        # suites: Suite[] (RN imports; not unit-tested)
│   ├── bulk-writes.test.ts
│   ├── crud.test.ts
│   ├── queries.test.ts
│   ├── subscriptions.test.ts
│   ├── relations.test.ts
│   └── durability.test.ts
└── ui/TestRunnerScreen.tsx # rows + summary + Maestro testIDs
App wiring: App.tsx
Maestro: dev/maestro/integration-tests/integration_tests.yaml
CI: .github/workflows/expo-android-integration-e2e.yml
Workspace: pnpm-workspace.yaml (+ one line)
```

---

## Task 1 — Scaffold app + workspace

**Files (create):** `dev/integration-tests-expo/{package.json,app.json,babel.config.js,tsconfig.json,index.js,metro.config.mjs}`; **modify:** `pnpm-workspace.yaml`.

- [ ] `package.json` (mirror todo app; drop server-only `expo-secure-store` is risky → keep to match proven app):

```json
{
  "name": "integration-tests-expo",
  "version": "0.1.0",
  "private": true,
  "main": "./index.js",
  "scripts": {
    "start": "expo start --clear",
    "android": "expo run:android",
    "ios": "expo run:ios",
    "build": "tsc --noEmit",
    "test": "vitest run --config vitest.config.ts",
    "prebuild": "expo prebuild --clean",
    "verify:expo:android": "CI=1 expo prebuild --platform android --clean --no-install"
  },
  "dependencies": {
    "expo": "^54.0.0",
    "expo-crypto": "~14.0.0",
    "expo-secure-store": "~14.0.0",
    "jazz-rn": "workspace:*",
    "jazz-tools": "workspace:*",
    "metro-runtime": "0.83.3",
    "react": "19.2.4",
    "react-native": "0.81.5"
  },
  "devDependencies": {
    "@babel/plugin-transform-flow-strip-types": "^7.27.1",
    "@types/react": "^19.0.0",
    "babel-preset-expo": "~54.0.10",
    "typescript": "catalog:default",
    "vitest": "catalog:default"
  }
}
```

- [ ] `app.json`:

```json
{
  "expo": {
    "name": "integration-tests-expo",
    "slug": "integration-tests-expo",
    "version": "1.0.0",
    "orientation": "portrait",
    "jsEngine": "hermes",
    "newArchEnabled": true,
    "android": { "package": "dev.jazz.integrationtests" },
    "ios": { "bundleIdentifier": "dev.jazz.integrationtests" }
  }
}
```

- [ ] `babel.config.js` (exact copy of todo app); `tsconfig.json` (todo app's, with `include: ["App.tsx","runner/**/*","tests/**/*","ui/**/*","schema.ts"]`); `index.js` (exact copy: import `jazz-tools/expo/polyfills`, register `App`).
- [ ] `metro.config.mjs` — default expo config + symlinks, **no** `withJazz`:

```js
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const { getDefaultConfig } = require("expo/metro-config");
const projectRoot = path.dirname(fileURLToPath(import.meta.url));
const config = getDefaultConfig(projectRoot);
config.resolver.unstable_enableSymlinks = true;
export default config;
```

- [ ] `pnpm-workspace.yaml`: add `  - dev/integration-tests-expo` under `packages:`.
- [ ] **Verify:** `pnpm install` (root) succeeds and links the package. Commit.

## Task 2 — schema.ts + vitest config

- [ ] `schema.ts`:

```ts
import { schema as s } from "jazz-tools";

const schema = {
  projects: s.table({ name: s.string() }),
  todos: s.table({
    title: s.string(),
    done: s.boolean().default(false),
    priority: s.string().optional(),
    projectId: s.ref("projects").optional(),
  }),
};
type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
export type Todo = s.RowOf<typeof app.todos>;
export type Project = s.RowOf<typeof app.projects>;
```

- [ ] `vitest.config.ts`:

```ts
import { defineConfig } from "vitest/config";
export default defineConfig({
  test: { environment: "node", include: ["runner/**/*.test.ts"] },
});
```

- [ ] **Verify:** `pnpm --filter integration-tests-expo exec tsc --noEmit` typechecks `schema.ts`. Commit.

## Task 3 — runner/types.ts + runner/expect.ts (TDD)

- [ ] `runner/types.ts`:

```ts
export type TestStatus = "pending" | "running" | "passed" | "failed";
export interface TestResult {
  suite: string;
  name: string;
  slug: string;
  status: TestStatus;
  error?: string;
  durationMs?: number;
}
export interface SuiteSummary {
  total: number;
  passed: number;
  failed: number;
  done: boolean;
  allPassed: boolean;
}
```

- [ ] **Step 1 — failing test** `runner/expect.test.ts`:

```ts
import { describe, it, expect as v } from "vitest";
import { expect } from "./expect";
describe("expect shim", () => {
  it("toBe / not.toBe", () => {
    expect(2).toBe(2);
    expect(2).not.toBe(3);
    v(() => expect(2).toBe(3)).toThrow();
  });
  it("toEqual deep", () => {
    expect({ a: [1, { b: 2 }] }).toEqual({ a: [1, { b: 2 }] });
    v(() => expect({ a: 1 }).toEqual({ a: 2 })).toThrow();
  });
  it("toMatchObject subset", () => {
    expect({ id: "x", title: "t", done: false }).toMatchObject({ title: "t" });
    v(() => expect({ title: "t" }).toMatchObject({ title: "z" })).toThrow();
  });
  it("toHaveLength / toContain / comparisons / nullish", () => {
    expect([1, 2, 3]).toHaveLength(3);
    expect([1, 2, 3]).toContain(2);
    expect(5).toBeGreaterThan(4);
    expect(5).toBeGreaterThanOrEqual(5);
    expect(null).toBeNull();
    expect(1).toBeDefined();
    expect(0).toBeFalsy();
    expect("x").toBeTruthy();
  });
});
```

- [ ] **Step 2 — run, expect FAIL:** `pnpm --filter integration-tests-expo test` → fails (no `expect`).
- [ ] **Step 3 — implement** `runner/expect.ts`: `deepEqual(a,b)`, `subsetMatch(actual,expected)`, a `Matchers` object with `not` negation, throwing `Error` with a readable message on mismatch. Matchers: `toBe,toEqual,toMatchObject,toHaveLength,toContain,toBeGreaterThan,toBeGreaterThanOrEqual,toBeDefined,toBeUndefined,toBeNull,toBeTruthy,toBeFalsy`. Export `expect` and types `Expect`/`Matchers`.
- [ ] **Step 4 — run, expect PASS.** Commit.

## Task 4 — runner/support.ts (TDD)

Structural `Queryable` so the module has no RN import.

- [ ] **Step 1 — failing test** `runner/support.test.ts`: test `waitForCondition` resolves when predicate flips true; rejects with the label on timeout; `withTimeout` rejects after ms; `waitForQuery` polls a fake `{ all: async () => rows }` until predicate true; `uniqueAppId("x")` returns distinct strings with the label prefix.
- [ ] **Step 2 — run, expect FAIL.**
- [ ] **Step 3 — implement** `runner/support.ts`:

```ts
export interface Queryable {
  all<T>(query: unknown): Promise<T[]>;
}
export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
let __seq = 0;
export function uniqueAppId(label: string): string {
  return `itest-${label}-${Date.now().toString(36)}-${(__seq++).toString(36)}`;
}
export async function waitForCondition(
  check: () => boolean | Promise<boolean>,
  timeoutMs: number,
  message: string,
): Promise<void> {
  /* poll every 50ms until deadline; throw Error(`Timeout ${timeoutMs}ms: ${message}`) */
}
export async function withTimeout<T>(p: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  /* Promise.race with a timer that rejects */
}
export async function waitForQuery<T>(
  db: Queryable,
  query: unknown,
  predicate: (rows: T[]) => boolean,
  label: string,
  timeoutMs = 10_000,
): Promise<T[]> {
  /* loop: rows = await db.all<T>(query); if predicate(rows) return rows; sleep(100); until deadline → throw with last count */
}
```

- [ ] **Step 4 — run, expect PASS.** Commit.

## Task 5 — runner/harness.ts (TDD)

Runner is decoupled: takes `suites` + injected `createDb` + `onUpdate`. `Db` type imported type-only.

- [ ] Types:

```ts
import type { Db } from "jazz-tools/react-native";
import type { Expect } from "./expect";
import type { TestResult, SuiteSummary } from "./types";
import { expect } from "./expect";
import * as support from "./support";
export interface TestCtx {
  db: Db;
  expect: Expect;
  waitForQuery: typeof support.waitForQuery;
  waitForCondition: typeof support.waitForCondition;
  withTimeout: typeof support.withTimeout;
  sleep: typeof support.sleep;
  uniqueAppId: typeof support.uniqueAppId;
}
export type TestBody = (ctx: TestCtx) => Promise<void>;
export interface Suite {
  name: string;
  tests: { name: string; body: TestBody }[];
}
export interface RunnerDeps {
  createDb: (config: { appId: string }) => Promise<Db>;
  onUpdate: (results: TestResult[]) => void;
  perTestTimeoutMs?: number; // default 30000
}
```

- [ ] **Step 1 — failing test** `runner/harness.test.ts`: build two suites via `defineSuite`; one test passes, one throws, one hangs (use a never-resolving promise) with a short `perTestTimeoutMs`; inject a fake `createDb` returning `{ all: async()=>[], shutdown: async()=>{} }`; assert final `TestResult[]` has statuses `passed/failed/failed`, the hang one has a timeout error, `summarize()` reports `done:true, allPassed:false`, and `onUpdate` was called with a `running` status before each terminal status. Also assert each test got a **distinct** `db` (fake createDb records appIds) and `shutdown` was called per test.
- [ ] **Step 2 — run, expect FAIL.**
- [ ] **Step 3 — implement** `defineSuite`, `slugify(name)`, `summarize(results): SuiteSummary`, and `runSuites(suites, deps)`: flatten to pending `TestResult[]`; `onUpdate`; for each: set `running`+`onUpdate`; `db = await deps.createDb({ appId: uniqueAppId(slug) })`; build ctx; `await withTimeout(body(ctx), perTestTimeoutMs, name)`; success→`passed` else `failed`+message; `finally` best-effort `db.shutdown()`; set `durationMs`; `onUpdate`. Return results. (`slug = suiteSlug + "-" + testSlug`.)
- [ ] **Step 4 — run, expect PASS.** Commit.

## Task 6 — test suites + registry

Each suite uses only the public DSL + ctx. Full bodies below; `_registry.ts` lists them in order.

- [ ] `tests/bulk-writes.test.ts`:

```ts
import { defineSuite } from "../runner/harness";
import { app } from "../schema";
export default defineSuite("bulk writes", ({ test }) => {
  test("writes 100 todos and reads them all back", async ({ db, expect, waitForQuery }) => {
    for (let i = 0; i < 100; i++) db.insert(app.todos, { title: `t-${i}`, done: false });
    const rows = await waitForQuery(db, app.todos, (r) => r.length >= 100, "100 land", 15_000);
    expect(rows).toHaveLength(100);
  });
});
```

- [ ] `tests/crud.test.ts` — insert (capture `value.id`), `update(app.todos, id, { done: true })`, `waitForQuery` until that row `done===true`, assert; `delete(app.todos, id)`, `waitForQuery` until absent, assert length 0.
- [ ] `tests/queries.test.ts` — insert 3 todos with mixed `done`; `db.all(app.todos.where({ done: { eq: false } }))`; assert only the not-done rows; then `.orderBy("title","asc").limit(2)` and assert length 2 + order.
- [ ] `tests/subscriptions.test.ts` — `const deltas=[]; const unsub = db.subscribeAll(app.todos, d => deltas.push(d));` insert one; `waitForCondition(() => deltas.some(d => d.delta.some(x => x.kind === 0)), 5000, "add delta")`; update it; assert an `Updated`(2) delta; finally `unsub()`.
- [ ] `tests/relations.test.ts` — insert a project (capture id); insert a todo with `projectId`; `db.all(app.todos.where({ id: { eq: todoId } }).include({ project: true }))`; assert `rows[0].project?.name` equals the project name.
- [ ] `tests/durability.test.ts` — `const row = await db.insert(app.todos, { title: "d" }).wait({ tier: "local" });` assert `row.id` defined; `db.one(app.todos.where({ id: { eq: row.id } }))` is non-null.
- [ ] `tests/_registry.ts`:

```ts
import type { Suite } from "../runner/harness";
import bulk from "./bulk-writes.test";
import crud from "./crud.test";
import queries from "./queries.test";
import subscriptions from "./subscriptions.test";
import relations from "./relations.test";
import durability from "./durability.test";
export const suites: Suite[] = [bulk, crud, queries, subscriptions, relations, durability];
```

- [ ] **Verify:** `tsc --noEmit`. Commit.

## Task 7 — UI + App wiring

- [ ] `ui/TestRunnerScreen.tsx`: props `{ suites }`. `useState<TestResult[]>([])`; `useEffect(run-once)` calling `runSuites(suites, { createDb, onUpdate: setResults })` with `createDb` from `jazz-tools/react-native`. Render:
  - `<Text testID="suite-status">{summary.done ? (summary.allPassed ? "PASSED" : "FAILED") : "RUNNING"}</Text>`
  - a `ScrollView` of rows: `<View testID={`test-row-${r.slug}`}><Text>{r.name}</Text><Text>{r.status === "passed" ? "PASS" : r.status === "failed" ? "FAIL" : r.status.toUpperCase()}</Text></View>` (failed rows also render `r.error`).
  - when `summary.done && summary.allPassed`: `<View testID="suite-passed"><Text>ALL {summary.total} PASSED</Text></View>`.
  - when `summary.done && !summary.allPassed`: `<View testID="suite-failed"><Text>{failed names + errors}</Text></View>`.
- [ ] `App.tsx`: `import { suites } from "./tests/_registry"; export default () => <SafeAreaView style={{flex:1}}><TestRunnerScreen suites={suites} /></SafeAreaView>;` (no JazzProvider needed — runner calls `createDb` directly).
- [ ] **Verify:** `tsc --noEmit`. Commit.

## Task 8 — Maestro flow

- [ ] `dev/maestro/integration-tests/integration_tests.yaml`:

```yaml
appId: dev.jazz.integrationtests
---
- launchApp
- extendedWaitUntil:
    visible:
      id: "suite-passed"
    timeout: 120000
- assertNotVisible:
    id: "suite-failed"
```

- [ ] Commit.

## Task 9 — CI workflow

- [ ] `.github/workflows/expo-android-integration-e2e.yml` — copy `expo-android-maestro-e2e.yml`, then: drop `build-jazz-tools-sandbox` job and all Vercel-sandbox/`deploy`/server-URL steps; in the e2e job keep checkout, `source-code`, download `jazz-rn-android` artifact, the JS build (`pnpm --filter jazz-wasm run build`, `pnpm --filter jazz-tools run build:runtime`), `tsc --noEmit` (drop `jazz-tools validate` — no schema deploy), `verify:expo:android` (filter `integration-tests-expo`), Java/Gradle/SDK/NDK, `gradlew :app:assembleRelease` **without** `EXPO_PUBLIC_JAZZ_*` env, then Maestro Cloud with `app-file: dev/integration-tests-expo/android/app/build/outputs/apk/release/app-release.apk` and `workspace: dev/maestro/integration-tests`. Update all `working-directory`/cache paths from `examples/todo-client-localfirst-expo` → `dev/integration-tests-expo`. Triggers: push `main`; PR paths `crates/jazz-rn/**`, `packages/jazz-tools/src/react-native/**`, `dev/integration-tests-expo/**`, `dev/maestro/integration-tests/**`, the workflow file.
- [ ] Commit.

## Task 10 — Final verification

- [ ] `pnpm --filter integration-tests-expo test` (vitest, Node) → all green.
- [ ] `pnpm --filter integration-tests-expo exec tsc --noEmit` → clean.
- [ ] `pnpm --filter integration-tests-expo run verify:expo:android` if the Android toolchain is available locally; otherwise note CI-pending.
- [ ] Final commit. (Push/PR only when the user asks.)

## Risks / CI-pending

- On-device Hermes behavior, the APK build, and the Maestro gate are validated only in CI.
- If RN unexpectedly denies a local write (shouldn't — default allow), add an allow-all `permissions.ts` and attach to `app`.
- `expo prebuild` for a fresh app generates `android/`/`ios/`; confirm `.gitignore` covers them (mirror todo app) so generated native dirs aren't committed.
- Maestro `workspace: dev/maestro/integration-tests` must contain only this flow.
