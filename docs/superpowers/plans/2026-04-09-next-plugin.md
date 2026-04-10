# Next.js Dev Plugin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `withJazz(...)` in `jazz-tools/dev/next` that mirrors the Vite plugin's dev-server/schema-watch lifecycle and injects `NEXT_PUBLIC_JAZZ_APP_ID` plus `NEXT_PUBLIC_JAZZ_SERVER_URL` for Next.js apps.

**Architecture:** Implement a dependency-light Next config wrapper that reuses `startLocalJazzServer(...)`, `pushSchemaCatalogue(...)`, and `watchSchema(...)` during async Next config resolution. Keep the package free of a hard `next` dependency by using local structural config types and the literal development phase string, and guard dev startup behind a module-level singleton with explicit test reset hooks.

**Tech Stack:** TypeScript, Vitest, Next.js config contract, `jazz-tools` dev helpers

---

## Scope Check

Single subsystem: a new `packages/jazz-tools` dev wrapper plus the minimal example migration needed to consume the new public env names. One plan.

---

## File Structure

### `packages/jazz-tools`

| File                                       | Responsibility                                                                                                                       |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| `packages/jazz-tools/src/dev/next.ts`      | **New.** Implements `withJazz(...)`, config merging, dev server startup, schema publish/watch, shutdown hooks, and test reset helper |
| `packages/jazz-tools/src/dev/next.test.ts` | **New.** Covers config merging, dev startup/env injection, misconfiguration, and singleton lifecycle                                 |
| `packages/jazz-tools/src/dev/index.ts`     | Re-export `withJazz` and its option types                                                                                            |
| `packages/jazz-tools/package.json`         | Add `./dev/next` export path                                                                                                         |

### `examples/nextjs-csr-ssr`

| File                                             | Responsibility                                                                                   |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------ |
| `examples/nextjs-csr-ssr/next.config.ts`         | Adopt `withJazz(...)` in explicit-server mode so the example uses the canonical Next plugin path |
| `examples/nextjs-csr-ssr/app/ClientTodo.tsx`     | Rename public env reads to `NEXT_PUBLIC_JAZZ_*`                                                  |
| `examples/nextjs-csr-ssr/lib/jazz-server.ts`     | Rename public env reads to `NEXT_PUBLIC_JAZZ_*` while keeping `BACKEND_SECRET` explicit          |
| `examples/nextjs-csr-ssr/scripts/sync-server.ts` | Rename public env reads to `NEXT_PUBLIC_JAZZ_*`                                                  |
| `examples/nextjs-csr-ssr/playwright.config.ts`   | Rename public env reads to `NEXT_PUBLIC_JAZZ_*`                                                  |
| `examples/nextjs-csr-ssr/README.md`              | Document the new env names and `withJazz(...)` wrapper                                           |

### Boundary Decisions

- Do **not** add a `next` dependency or peer dependency to `packages/jazz-tools`; match the Vite plugin's lightweight approach.
- Do **not** inject compatibility aliases for old env names.
- Do **not** invent backend-secret injection in this feature. The example keeps its explicit `BACKEND_SECRET` workflow for SSR/backend access.

---

### Task 1: Static Wrapper And Export Plumbing

**Files:**

- Create: `packages/jazz-tools/src/dev/next.test.ts`
- Create: `packages/jazz-tools/src/dev/next.ts`
- Modify: `packages/jazz-tools/src/dev/index.ts`
- Modify: `packages/jazz-tools/package.json`

- [ ] **Step 1: Write the failing static-config tests**

Create `packages/jazz-tools/src/dev/next.test.ts` with these initial tests and helpers:

```ts
import { afterEach, describe, expect, it } from "vitest";
import { __resetJazzNextPluginForTests, withJazz, type NextConfigLike } from "./next.js";

const DEVELOPMENT_PHASE = "phase-development-server";
const PRODUCTION_BUILD_PHASE = "phase-production-build";

async function resolveWrappedConfig(
  wrapped: ReturnType<typeof withJazz>,
  phase: string,
): Promise<NextConfigLike> {
  return wrapped(phase, { defaultConfig: {} });
}

afterEach(async () => {
  await __resetJazzNextPluginForTests();
  delete process.env.NEXT_PUBLIC_JAZZ_APP_ID;
  delete process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;
});

describe("withJazz", () => {
  it("preserves existing config fields and unions serverExternalPackages", async () => {
    const resolved = await resolveWrappedConfig(
      withJazz({
        reactStrictMode: true,
        env: { EXISTING_ENV: "1" },
        serverExternalPackages: ["sharp", "jazz-tools"],
      }),
      PRODUCTION_BUILD_PHASE,
    );

    expect(resolved.reactStrictMode).toBe(true);
    expect(resolved.env).toEqual({ EXISTING_ENV: "1" });
    expect(resolved.serverExternalPackages).toEqual(
      expect.arrayContaining(["sharp", "jazz-tools", "jazz-napi"]),
    );
    expect(resolved.serverExternalPackages?.filter((value) => value === "jazz-tools")).toHaveLength(
      1,
    );
  });

  it("supports config functions as input", async () => {
    const resolved = await resolveWrappedConfig(
      withJazz(async () => ({
        poweredByHeader: false,
        serverExternalPackages: ["better-sqlite3"],
      })),
      PRODUCTION_BUILD_PHASE,
    );

    expect(resolved.poweredByHeader).toBe(false);
    expect(resolved.serverExternalPackages).toEqual(
      expect.arrayContaining(["better-sqlite3", "jazz-tools", "jazz-napi"]),
    );
  });

  it("does not inject Jazz env vars outside the development phase", async () => {
    const resolved = await resolveWrappedConfig(withJazz({}), PRODUCTION_BUILD_PHASE);

    expect(resolved.env?.NEXT_PUBLIC_JAZZ_APP_ID).toBeUndefined();
    expect(resolved.env?.NEXT_PUBLIC_JAZZ_SERVER_URL).toBeUndefined();
    expect(process.env.NEXT_PUBLIC_JAZZ_APP_ID).toBeUndefined();
    expect(process.env.NEXT_PUBLIC_JAZZ_SERVER_URL).toBeUndefined();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
pnpm --dir packages/jazz-tools exec vitest run src/dev/next.test.ts --config vitest.config.ts
```

Expected: FAIL with a module resolution error for `./next.js` or missing exports such as `withJazz`.

- [ ] **Step 3: Write the minimal static implementation and export wiring**

Create `packages/jazz-tools/src/dev/next.ts` with the static wrapper shape and a no-op test reset helper:

```ts
export interface JazzServerOptions {
  port?: number;
  adminSecret?: string;
  appId?: string;
  allowAnonymous?: boolean;
  allowDemo?: boolean;
  dataDir?: string;
  inMemory?: boolean;
  jwksUrl?: string;
  catalogueAuthority?: "local" | "forward";
  catalogueAuthorityUrl?: string;
  catalogueAuthorityAdminSecret?: string;
}

export interface JazzPluginOptions {
  server?: boolean | string | JazzServerOptions;
  adminSecret?: string;
  schemaDir?: string;
  appId?: string;
}

export interface NextConfigLike {
  env?: Record<string, string | undefined>;
  serverExternalPackages?: string[];
  [key: string]: unknown;
}

interface NextConfigContextLike {
  defaultConfig: NextConfigLike;
}

type NextConfigFactory = (
  phase: string,
  context: NextConfigContextLike,
) => NextConfigLike | Promise<NextConfigLike>;

type NextConfigInput = NextConfigLike | NextConfigFactory;

const DEVELOPMENT_PHASE = "phase-development-server";

function mergeServerExternalPackages(existing: string[] | undefined): string[] {
  return Array.from(new Set([...(existing ?? []), "jazz-tools", "jazz-napi"]));
}

async function resolveConfig(
  input: NextConfigInput | undefined,
  phase: string,
  context: NextConfigContextLike,
): Promise<NextConfigLike> {
  if (!input) return {};
  if (typeof input === "function") {
    return (await input(phase, context)) ?? {};
  }
  return input;
}

export function withJazz(
  nextConfig?: NextConfigInput,
  _options: JazzPluginOptions = {},
): NextConfigFactory {
  return async (phase, context) => {
    const resolved = await resolveConfig(nextConfig, phase, context);

    const merged: NextConfigLike = {
      ...resolved,
      serverExternalPackages: mergeServerExternalPackages(resolved.serverExternalPackages),
    };

    if (phase !== DEVELOPMENT_PHASE) {
      return merged;
    }

    return merged;
  };
}

export async function __resetJazzNextPluginForTests(): Promise<void> {}
```

Update `packages/jazz-tools/src/dev/index.ts`:

```ts
export { withJazz, type JazzPluginOptions, type JazzServerOptions } from "./next.js";
```

Add this export entry to `packages/jazz-tools/package.json`:

```json
"./dev/next": {
  "types": "./dist/dev/next.d.ts",
  "default": "./dist/dev/next.js"
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run:

```bash
pnpm --dir packages/jazz-tools exec vitest run src/dev/next.test.ts --config vitest.config.ts
```

Expected: PASS for the three static wrapper tests.

- [ ] **Step 5: Commit**

```bash
git add packages/jazz-tools/src/dev/next.ts packages/jazz-tools/src/dev/next.test.ts packages/jazz-tools/src/dev/index.ts packages/jazz-tools/package.json
git commit -m "feat: add next wrapper scaffold"
```

---

### Task 2: Dev Startup, Env Injection, And Singleton Lifecycle

**Files:**

- Modify: `packages/jazz-tools/src/dev/next.test.ts`
- Modify: `packages/jazz-tools/src/dev/next.ts`

- [ ] **Step 1: Extend the test file with failing dev-lifecycle coverage**

Append these imports and tests to `packages/jazz-tools/src/dev/next.test.ts`:

```ts
import { writeFile } from "node:fs/promises";
import { join } from "node:path";
import { createTempRootTracker, getAvailablePort, todoSchema } from "./test-helpers.js";

const tempRoots = createTempRootTracker();
const originalJazzServerUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;
const originalJazzAppId = process.env.NEXT_PUBLIC_JAZZ_APP_ID;

afterEach(async () => {
  await tempRoots.cleanup();

  if (originalJazzServerUrl === undefined) {
    delete process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;
  } else {
    process.env.NEXT_PUBLIC_JAZZ_SERVER_URL = originalJazzServerUrl;
  }

  if (originalJazzAppId === undefined) {
    delete process.env.NEXT_PUBLIC_JAZZ_APP_ID;
  } else {
    process.env.NEXT_PUBLIC_JAZZ_APP_ID = originalJazzAppId;
  }
});

it("starts a local server in development and injects NEXT_PUBLIC_JAZZ_* env vars", async () => {
  const port = await getAvailablePort();
  const schemaDir = await tempRoots.create("jazz-next-test-");
  await writeFile(join(schemaDir, "schema.ts"), todoSchema());

  const wrapped = withJazz(
    { reactStrictMode: true },
    {
      server: { port, adminSecret: "next-test-admin" },
      schemaDir,
    },
  );

  const resolved = await resolveWrappedConfig(wrapped, DEVELOPMENT_PHASE);

  const healthResponse = await fetch(`http://127.0.0.1:${port}/health`);
  expect(healthResponse.ok).toBe(true);

  const schemasResponse = await fetch(`http://127.0.0.1:${port}/schemas`, {
    headers: { "X-Jazz-Admin-Secret": "next-test-admin" },
  });
  expect(schemasResponse.ok).toBe(true);

  const body = (await schemasResponse.json()) as { hashes?: string[] };
  expect(body.hashes?.length).toBeGreaterThan(0);
  expect(resolved.env?.NEXT_PUBLIC_JAZZ_APP_ID).toBeTruthy();
  expect(resolved.env?.NEXT_PUBLIC_JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
  expect(process.env.NEXT_PUBLIC_JAZZ_APP_ID).toBe(resolved.env?.NEXT_PUBLIC_JAZZ_APP_ID);
  expect(process.env.NEXT_PUBLIC_JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
}, 30_000);

it("throws when connecting to an existing server without adminSecret", async () => {
  process.env.NEXT_PUBLIC_JAZZ_SERVER_URL = "http://127.0.0.1:4000";
  process.env.NEXT_PUBLIC_JAZZ_APP_ID = "00000000-0000-0000-0000-000000000111";

  await expect(resolveWrappedConfig(withJazz({}), DEVELOPMENT_PHASE)).rejects.toThrow(
    "adminSecret is required when connecting to an existing server",
  );
});

it("throws when connecting to an existing server without appId", async () => {
  process.env.NEXT_PUBLIC_JAZZ_SERVER_URL = "http://127.0.0.1:4000";
  delete process.env.NEXT_PUBLIC_JAZZ_APP_ID;

  await expect(
    resolveWrappedConfig(withJazz({}, { adminSecret: "next-test-admin" }), DEVELOPMENT_PHASE),
  ).rejects.toThrow("appId is required when connecting to an existing server");
});

it("reuses the same managed server across repeated config resolution in one process", async () => {
  const port = await getAvailablePort();
  const schemaDir = await tempRoots.create("jazz-next-repeat-");
  await writeFile(join(schemaDir, "schema.ts"), todoSchema());

  const wrapped = withJazz(
    {},
    {
      server: { port, adminSecret: "next-repeat-admin" },
      schemaDir,
    },
  );

  const first = await resolveWrappedConfig(wrapped, DEVELOPMENT_PHASE);
  const second = await resolveWrappedConfig(wrapped, DEVELOPMENT_PHASE);

  expect(first.env?.NEXT_PUBLIC_JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
  expect(second.env?.NEXT_PUBLIC_JAZZ_SERVER_URL).toBe(first.env?.NEXT_PUBLIC_JAZZ_SERVER_URL);
  expect(second.env?.NEXT_PUBLIC_JAZZ_APP_ID).toBe(first.env?.NEXT_PUBLIC_JAZZ_APP_ID);
}, 30_000);
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
pnpm --dir packages/jazz-tools exec vitest run src/dev/next.test.ts --config vitest.config.ts
```

Expected: FAIL because the dev path does not start a server, inject env vars, or throw the required configuration errors yet.

- [ ] **Step 3: Implement the real dev lifecycle in `next.ts`**

Replace `packages/jazz-tools/src/dev/next.ts` with the real wrapper logic:

```ts
import { randomUUID } from "node:crypto";
import {
  pushSchemaCatalogue,
  startLocalJazzServer,
  type LocalJazzServerHandle,
} from "./dev-server.js";
import { watchSchema } from "./schema-watcher.js";

export interface JazzServerOptions {
  port?: number;
  adminSecret?: string;
  appId?: string;
  allowAnonymous?: boolean;
  allowDemo?: boolean;
  dataDir?: string;
  inMemory?: boolean;
  jwksUrl?: string;
  catalogueAuthority?: "local" | "forward";
  catalogueAuthorityUrl?: string;
  catalogueAuthorityAdminSecret?: string;
}

export interface JazzPluginOptions {
  server?: boolean | string | JazzServerOptions;
  adminSecret?: string;
  schemaDir?: string;
  appId?: string;
}

export interface NextConfigLike {
  env?: Record<string, string | undefined>;
  serverExternalPackages?: string[];
  [key: string]: unknown;
}

interface NextConfigContextLike {
  defaultConfig: NextConfigLike;
}

type NextConfigFactory = (
  phase: string,
  context: NextConfigContextLike,
) => NextConfigLike | Promise<NextConfigLike>;

type NextConfigInput = NextConfigLike | NextConfigFactory;

type ManagedRuntime = {
  appId: string;
  serverUrl: string;
  adminSecret: string;
};

const DEVELOPMENT_PHASE = "phase-development-server";
const PUBLIC_APP_ID_ENV = "NEXT_PUBLIC_JAZZ_APP_ID";
const PUBLIC_SERVER_URL_ENV = "NEXT_PUBLIC_JAZZ_SERVER_URL";
const LOG_PREFIX = "[jazz]";

let initPromise: Promise<ManagedRuntime> | null = null;
let runtime: ManagedRuntime | null = null;
let serverHandle: LocalJazzServerHandle | null = null;
let watcher: { close: () => void } | null = null;
let shutdownHooksInstalled = false;
let cleanupHandler: (() => void) | null = null;

function mergeServerExternalPackages(existing: string[] | undefined): string[] {
  return Array.from(new Set([...(existing ?? []), "jazz-tools", "jazz-napi"]));
}

async function resolveConfig(
  input: NextConfigInput | undefined,
  phase: string,
  context: NextConfigContextLike,
): Promise<NextConfigLike> {
  if (!input) return {};
  if (typeof input === "function") {
    return (await input(phase, context)) ?? {};
  }
  return input;
}

async function disposeManagedRuntime(): Promise<void> {
  watcher?.close();
  watcher = null;
  if (serverHandle) {
    await serverHandle.stop();
    serverHandle = null;
  }
  runtime = null;
  initPromise = null;
}

function installShutdownHooks(): void {
  if (shutdownHooksInstalled) return;

  cleanupHandler = () => {
    void disposeManagedRuntime();
  };

  process.once("SIGINT", cleanupHandler);
  process.once("SIGTERM", cleanupHandler);
  process.once("exit", cleanupHandler);
  shutdownHooksInstalled = true;
}

function resolveSchemaDir(options: JazzPluginOptions): string {
  return options.schemaDir ?? process.cwd();
}

async function initializeManagedRuntime(options: JazzPluginOptions): Promise<ManagedRuntime> {
  if (runtime) return runtime;
  if (initPromise) return initPromise;

  initPromise = (async () => {
    const serverOpt = options.server ?? true;
    const schemaDir = resolveSchemaDir(options);
    let serverUrl: string;
    let adminSecret: string;
    let appId: string;

    if (serverOpt === false) {
      throw new Error(`${LOG_PREFIX} server=false should bypass initialization`);
    }

    if (process.env[PUBLIC_SERVER_URL_ENV]) {
      serverUrl = process.env[PUBLIC_SERVER_URL_ENV]!;
      adminSecret = options.adminSecret ?? "";
      appId = process.env[PUBLIC_APP_ID_ENV] ?? options.appId ?? "";
      if (!adminSecret) {
        throw new Error(
          `${LOG_PREFIX} adminSecret is required when connecting to an existing server`,
        );
      }
      if (!appId) {
        throw new Error(`${LOG_PREFIX} appId is required when connecting to an existing server`);
      }
      console.log(`${LOG_PREFIX} using server from .env: ${serverUrl}`);
    } else if (typeof serverOpt === "string") {
      serverUrl = serverOpt;
      adminSecret = options.adminSecret ?? "";
      appId = options.appId ?? "";
      if (!adminSecret) {
        throw new Error(
          `${LOG_PREFIX} adminSecret is required when connecting to an existing server`,
        );
      }
      if (!appId) {
        throw new Error(`${LOG_PREFIX} appId is required when connecting to an existing server`);
      }
    } else {
      const serverConfig = typeof serverOpt === "object" ? serverOpt : {};
      adminSecret = serverConfig.adminSecret ?? `jazz-dev-${randomUUID().slice(0, 8)}`;
      appId = process.env[PUBLIC_APP_ID_ENV] ?? serverConfig.appId ?? options.appId ?? randomUUID();

      serverHandle = await startLocalJazzServer({
        appId,
        port: serverConfig.port ?? 0,
        adminSecret,
        allowAnonymous: serverConfig.allowAnonymous,
        allowDemo: serverConfig.allowDemo,
        dataDir: serverConfig.dataDir,
        inMemory: serverConfig.inMemory,
        jwksUrl: serverConfig.jwksUrl,
        catalogueAuthority: serverConfig.catalogueAuthority,
        catalogueAuthorityUrl: serverConfig.catalogueAuthorityUrl,
        catalogueAuthorityAdminSecret: serverConfig.catalogueAuthorityAdminSecret,
      });

      serverUrl = serverHandle.url;
      console.log(`${LOG_PREFIX} server started on ${serverUrl}`);
      if (serverHandle.dataDir) {
        console.log(`${LOG_PREFIX} data dir: ${serverHandle.dataDir}`);
      }
    }

    console.log(`${LOG_PREFIX} app id: ${appId}`);

    try {
      await pushSchemaCatalogue({ serverUrl, appId, adminSecret, schemaDir });
      console.log(`${LOG_PREFIX} schema published`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`${LOG_PREFIX} schema push failed:`, message);
      throw error;
    }

    watcher = watchSchema({
      schemaDir,
      serverUrl,
      appId,
      adminSecret,
      onPush: (hash) => {
        console.log(`${LOG_PREFIX} schema updated (${hash.slice(0, 12)})`);
      },
      onError: (error) => {
        console.error(`${LOG_PREFIX} schema push failed:`, error.message);
      },
    });

    installShutdownHooks();

    process.env[PUBLIC_APP_ID_ENV] = appId;
    process.env[PUBLIC_SERVER_URL_ENV] = serverUrl;

    runtime = { appId, serverUrl, adminSecret };
    return runtime;
  })();

  try {
    return await initPromise;
  } catch (error) {
    initPromise = null;
    throw error;
  }
}

export function withJazz(
  nextConfig?: NextConfigInput,
  options: JazzPluginOptions = {},
): NextConfigFactory {
  return async (phase, context) => {
    const resolved = await resolveConfig(nextConfig, phase, context);
    const merged: NextConfigLike = {
      ...resolved,
      serverExternalPackages: mergeServerExternalPackages(resolved.serverExternalPackages),
    };

    if (phase !== DEVELOPMENT_PHASE || options.server === false) {
      return merged;
    }

    const managed = await initializeManagedRuntime(options);

    return {
      ...merged,
      env: {
        ...merged.env,
        [PUBLIC_APP_ID_ENV]: managed.appId,
        [PUBLIC_SERVER_URL_ENV]: managed.serverUrl,
      },
    };
  };
}

export async function __resetJazzNextPluginForTests(): Promise<void> {
  if (cleanupHandler) {
    process.off("SIGINT", cleanupHandler);
    process.off("SIGTERM", cleanupHandler);
    process.off("exit", cleanupHandler);
  }
  cleanupHandler = null;
  shutdownHooksInstalled = false;
  await disposeManagedRuntime();
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run:

```bash
pnpm --dir packages/jazz-tools exec vitest run src/dev/next.test.ts --config vitest.config.ts
```

Expected: PASS for the static tests plus the dev startup, misconfiguration, and repeated-resolution tests.

- [ ] **Step 5: Commit**

```bash
git add packages/jazz-tools/src/dev/next.ts packages/jazz-tools/src/dev/next.test.ts
git commit -m "feat: add next dev plugin lifecycle"
```

---

### Task 3: Migrate The Next Example To The Canonical Public Env Names

**Files:**

- Modify: `examples/nextjs-csr-ssr/next.config.ts`
- Modify: `examples/nextjs-csr-ssr/app/ClientTodo.tsx`
- Modify: `examples/nextjs-csr-ssr/lib/jazz-server.ts`
- Modify: `examples/nextjs-csr-ssr/scripts/sync-server.ts`
- Modify: `examples/nextjs-csr-ssr/playwright.config.ts`
- Modify: `examples/nextjs-csr-ssr/README.md`

- [ ] **Step 1: Update the example config and code to use `withJazz(...)` plus `NEXT_PUBLIC_JAZZ_*`**

Change `examples/nextjs-csr-ssr/next.config.ts` to explicit-server mode so the example stays compatible with its existing SSR `sync-server` workflow:

```ts
import { withJazz } from "jazz-tools/dev/next";

export default withJazz(
  {},
  {
    server: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL ?? false,
    adminSecret: process.env.ADMIN_SECRET,
    appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID,
  },
);
```

Change `examples/nextjs-csr-ssr/app/ClientTodo.tsx`:

```tsx
<JazzProvider
  config={{
    appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID!,
    serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
    localAuthMode: "anonymous",
    driver: { type: "memory" },
  }}
>
```

Change `examples/nextjs-csr-ssr/lib/jazz-server.ts`:

```ts
const context = createJazzContext({
  appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID!,
  app: schemaApp,
  permissions: {},
  driver: { type: "memory" },
  serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
  backendSecret: process.env.BACKEND_SECRET!,
  tier: "worker",
});
```

Change `examples/nextjs-csr-ssr/scripts/sync-server.ts`:

```ts
const APP_ID = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;
```

Change `examples/nextjs-csr-ssr/playwright.config.ts`:

```ts
const SERVER_URL = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!;
const APP_ID = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;
```

and:

```ts
env: {
  NEXT_PUBLIC_JAZZ_APP_ID: APP_ID,
  BACKEND_SECRET,
  ADMIN_SECRET,
  JAZZ_SERVER_PORT: String(new URL(SERVER_URL).port),
},
```

and:

```ts
env: {
  NEXT_PUBLIC_JAZZ_SERVER_URL: SERVER_URL,
  NEXT_PUBLIC_JAZZ_APP_ID: APP_ID,
  BACKEND_SECRET,
  ADMIN_SECRET,
},
```

Update `examples/nextjs-csr-ssr/README.md` to reflect the wrapper and new env names:

```md
# Jazz + Next.js example

- `pnpm run sync-server`
- `pnpm run dev`

## Hot points

- `next.config.ts` uses `withJazz(...)` from `jazz-tools/dev/next`
- Public Jazz connection vars are `NEXT_PUBLIC_JAZZ_APP_ID` and `NEXT_PUBLIC_JAZZ_SERVER_URL`
- The SSR example still keeps `BACKEND_SECRET` explicit because backend access is server-only
```

- [ ] **Step 2: Type-check the example**

Run:

```bash
pnpm --dir examples/nextjs-csr-ssr exec tsc --noEmit
```

Expected: PASS with no TypeScript errors after the env-name migration.

- [ ] **Step 3: Commit**

```bash
git add examples/nextjs-csr-ssr/next.config.ts examples/nextjs-csr-ssr/app/ClientTodo.tsx examples/nextjs-csr-ssr/lib/jazz-server.ts examples/nextjs-csr-ssr/scripts/sync-server.ts examples/nextjs-csr-ssr/playwright.config.ts examples/nextjs-csr-ssr/README.md
git commit -m "chore: migrate next example to jazz env names"
```

---

### Task 4: Verification And Final Package Pass

**Files:**

- Modify: none

- [ ] **Step 1: Re-run the focused Next plugin tests**

Run:

```bash
pnpm --dir packages/jazz-tools exec vitest run src/dev/next.test.ts --config vitest.config.ts
```

Expected: PASS.

- [ ] **Step 2: Re-run the existing Vite plugin tests to catch regressions in shared dev helpers**

Run:

```bash
pnpm --dir packages/jazz-tools exec vitest run src/dev/vite.test.ts --config vitest.config.ts
```

Expected: PASS.

- [ ] **Step 3: Run the package test suite**

Run:

```bash
pnpm --dir packages/jazz-tools test
```

Expected: PASS for the `vitest.config.ts` and `vitest.config.svelte.ts` runs.

- [ ] **Step 4: Commit the verified state**

```bash
git add packages/jazz-tools/src/dev/next.ts packages/jazz-tools/src/dev/next.test.ts packages/jazz-tools/src/dev/index.ts packages/jazz-tools/package.json examples/nextjs-csr-ssr/next.config.ts examples/nextjs-csr-ssr/app/ClientTodo.tsx examples/nextjs-csr-ssr/lib/jazz-server.ts examples/nextjs-csr-ssr/scripts/sync-server.ts examples/nextjs-csr-ssr/playwright.config.ts examples/nextjs-csr-ssr/README.md
git commit -m "feat: add next dev plugin"
```
