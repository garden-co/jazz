# Inspector: own worker connection + minimal live-query link — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The embedded inspector overlay connects to the data layer as its own normal Jazz worker client (like standalone), the host page pushes only a stack-less active-subscription list to it, and the entire devtools-protocol bridge + the extension/devtools build are deleted.

**Architecture:** The overlay loader (host window) publishes a read-once connection-config handle on `window.__jazzInspectorHost` and pushes a serialized subscription list (no stacks) to the iframe one-way. The overlay (`embedded.tsx`) reads the config and opens its own `createJazzClient(...)` connection, resolving schema from the server exactly like the standalone build. Live Query renders the pushed list. The `dev-tools/` bridge, the proxy `Db`, the relay, and the extension/devtools build are removed.

**Tech Stack:** TypeScript, React 19, Vite (library + multi-entry builds), Vitest (unit), Playwright (browser), pnpm workspaces, turbo. Spec: `docs/superpowers/specs/2026-06-30-inspector-direct-worker-connection-design.md`.

**Reference convention:** Verify-and-match steps mean: open the cited `file:line`, confirm the current signature, and write code that matches it. They are not placeholders — the surrounding code is fully specified.

---

## File Structure

**New files**

- `packages/jazz-tools/src/dev/inspector-overlay/host-bridge.ts` — host-side: build the `JazzInspectorHost` handle from a `Db`, own the subscription listener, push stack-less snapshots to the iframe. (Replaces `attachDevTools` + `relay.ts` for the overlay.)
- `packages/inspector/src/contexts/host-link.ts` — overlay-side: read `window.parent.__jazzInspectorHost`, expose connection config + a React hook for the pushed subscription list.
- `packages/jazz-tools/src/dev-tools/inspector-host-types.ts` — shared `JazzInspectorHost` interface + `InspectorSubscriptionMessage` type + the `serializeActiveSubscriptions` helper (no stacks). (Small; survives the `dev-tools/` deletion — see Task 12.)

**Modified files**

- `packages/jazz-tools/src/dev/inspector-overlay/loader.ts` — drop the relay + `attachDevTools`; wire `host-bridge.ts`.
- `packages/jazz-tools/src/dev-tools/auto-attach.ts` — `startInspectorOnce` publishes the handle instead of attaching the bridge.
- `packages/inspector/src/embedded.tsx` — own `createJazzClient` from injected config.
- `packages/inspector/src/inspector-app.tsx` — drop bridge-coupled bootstrap; provide the overlay client + host-link.
- `packages/inspector/src/contexts/devtools-context.tsx` — `InspectorRuntime = "standalone" | "overlay"`; add the subscription source.
- `packages/inspector/src/pages/live-query/index.tsx` — overlay branch reads the pushed list; drop the stack column.
- `packages/inspector/src/pages/settings/index.tsx` — add the Admin⇄JWT identity toggle.
- `packages/inspector/src/components/data-explorer/TableDataGrid.tsx`, `TableSchemaDefinition.tsx`, `pages/data-explorer/index.tsx` — `runtime` branch audit.
- `packages/inspector/vite.config.ts`, `package.json` — remove the `extension` build.

**Deleted files** (Task 11–12)

- `packages/jazz-tools/src/dev-tools/{protocol.ts,dev-tools.ts,extension-panel.ts,parent-window-port.ts}` + their tests
- `packages/jazz-tools/src/dev/inspector-overlay/relay.ts`
- `packages/jazz-tools/src/react/create-embedded-jazz-client.ts` (+ `createExtensionJazzClient` in `create-jazz-client.ts`)
- `packages/inspector/src/devtools-main.tsx`, `src/devtools/main.js`, `devtools-tab.html`, `devtools.html`, `chrome-extension/`

---

## Phase ordering (why this order)

The overlay must work on the new path **before** the bridge is removed, or the overlay breaks mid-refactor. So: build the new host handle + link (1–3) → switch the overlay to its own connection (4–6) → identity toggle (7) → runtime audit (8) → **only then** remove the bridge + extension build (9–12) → rewrite the browser test (13). Each phase ends green.

---

### Task 1: Shared host-link types + stack-less serializer

**Files:**

- Create: `packages/jazz-tools/src/dev-tools/inspector-host-types.ts`
- Test: `packages/jazz-tools/src/dev-tools/inspector-host-types.test.ts`

- [ ] **Step 1: Confirm the trace shape.** Open `packages/jazz-tools/src/runtime/db.ts` and read the `ActiveQuerySubscriptionTrace` type (fields: `id, query, table, branches[], tier, propagation, createdAt, stack?`) and `getActiveQuerySubscriptions()` return type. Match these exactly below.

- [ ] **Step 2: Write the failing test**

```ts
// inspector-host-types.test.ts
import { describe, it, expect } from "vitest";
import { serializeActiveSubscriptions } from "./inspector-host-types.js";

describe("serializeActiveSubscriptions", () => {
  it("drops the stack and keeps the metadata", () => {
    const out = serializeActiveSubscriptions([
      {
        id: "s1",
        query: '{"from":"todos"}',
        table: "todos",
        branches: ["main"],
        tier: "edge",
        propagation: "full",
        createdAt: 123,
        stack: "Error\n  at X",
      } as any,
    ]);
    expect(out).toEqual([
      {
        id: "s1",
        query: '{"from":"todos"}',
        table: "todos",
        branches: ["main"],
        tier: "edge",
        propagation: "full",
        createdAt: 123,
      },
    ]);
    expect((out[0] as any).stack).toBeUndefined();
  });
});
```

- [ ] **Step 3: Run it — expect FAIL** (`pnpm --filter jazz-tools exec vitest run src/dev-tools/inspector-host-types.test.ts`) — "serializeActiveSubscriptions is not a function".

- [ ] **Step 4: Implement**

```ts
// inspector-host-types.ts
import type { ActiveQuerySubscriptionTrace } from "../runtime/db.js";
import type { WasmSchema } from "../runtime/db.js"; // verify export path at runtime/db.ts

/** Active subscription as sent to the overlay — the trace minus the JS stack. */
export type InspectorSubscription = Omit<ActiveQuerySubscriptionTrace, "stack">;

export interface InspectorConnectionConfig {
  appId: string;
  serverUrl: string;
  env: string;
  userBranch?: string;
  adminSecret?: string;
  jwtToken?: string;
  schemaHash: string;
}

/** Read-once handle the host publishes on window for the same-origin overlay. */
export interface JazzInspectorHost {
  getConnectionConfig(): InspectorConnectionConfig;
}

export const INSPECTOR_HOST_GLOBAL = "__jazzInspectorHost" as const;
export const INSPECTOR_SUBSCRIPTIONS_MESSAGE = "jazz-inspector:subscriptions" as const;

export interface InspectorSubscriptionsMessage {
  type: typeof INSPECTOR_SUBSCRIPTIONS_MESSAGE;
  list: InspectorSubscription[];
}

export function serializeActiveSubscriptions(
  traces: ActiveQuerySubscriptionTrace[],
): InspectorSubscription[] {
  return traces.map(({ stack: _stack, ...rest }) => rest);
}
```

- [ ] **Step 5: Run it — expect PASS.**

- [ ] **Step 6: Commit** — `git add packages/jazz-tools/src/dev-tools/inspector-host-types.* && git commit -m "feat(inspector): host-link types + stack-less subscription serializer"`

---

### Task 2: `Db` connection-config + schema-hash accessors (if missing)

The handle needs `appId/serverUrl/env/userBranch/adminSecret/jwtToken/schemaHash` from the live `Db`. The bridge got these via `sanitizeDbConfigForBridge` (`dev-tools.ts`) + private access.

**Files:**

- Modify: `packages/jazz-tools/src/runtime/db.ts`
- Test: `packages/jazz-tools/src/runtime/db.config-accessor.test.ts`

- [ ] **Step 1: Inventory what's already public.** Open `packages/jazz-tools/src/runtime/db.ts`; confirm `getConfig()` (returns a structuredClone of `DbConfig` incl. appId/serverUrl/env/userBranch/adminSecret/jwtToken) and whether a schema-hash is reachable. Open `packages/jazz-tools/src/dev-tools/dev-tools.ts:130-155` to see exactly how it reads schema + config today. If `getConfig()` already exposes everything except `schemaHash`, only add a schema-hash accessor.

- [ ] **Step 2: Write the failing test** — assert `db.getConfig()` returns the connection fields and `db.getSchemaHash()` (new) returns a non-empty string for a configured `Db`. Build the `Db` with the existing test helper (see `packages/jazz-tools/TESTING_GUIDELINES.md` — read it first, per repo rule) and the public API only.

- [ ] **Step 3: Run — expect FAIL.**

- [ ] **Step 4: Implement** a minimal public `getSchemaHash(): string` on `Db` returning the hash already computed for the connection (match the value the standalone `fetchStoredWasmSchema` uses — verify against `App.tsx` config + `fetchStoredWasmSchema`). Reuse `getConfig()` for the rest; do not add a second accessor if `getConfig()` suffices.

- [ ] **Step 5: Run — expect PASS.**

- [ ] **Step 6: Commit** — `feat(db): expose schema hash for the inspector host handle`

---

### Task 3: Host bridge — build the handle + push subscriptions

**Files:**

- Create: `packages/jazz-tools/src/dev/inspector-overlay/host-bridge.ts`
- Test: `packages/jazz-tools/src/dev/inspector-overlay/host-bridge.test.ts`

- [ ] **Step 1: Write the failing test** (jsdom). Build a fake `Db` (plain object) exposing `getConfig()`, `getSchemaHash()`, `setDevMode`, `getActiveQuerySubscriptions`, `onActiveQuerySubscriptionsChange`. Assert that `installInspectorHost(db, iframeWindow, origin)`:
      (a) calls `db.setDevMode(true)`,
      (b) sets `window.__jazzInspectorHost.getConnectionConfig()` to the expected config,
      (c) posts an initial `jazz-inspector:subscriptions` message to `iframeWindow` with the stack-less list,
      (d) posts again when the `onActiveQuerySubscriptionsChange` callback fires,
      (e) the returned `dispose()` removes the listener and deletes the global.

```ts
// host-bridge.test.ts (shape)
const posts: any[] = [];
const iframeWindow = { postMessage: (m: any) => posts.push(m) } as any;
let changeCb: () => void = () => {};
const db = {
  setDevMode: vi.fn(),
  getConfig: () => ({ appId: "a", serverUrl: "http://s", env: "e", adminSecret: "sek" }),
  getSchemaHash: () => "hash1",
  getActiveQuerySubscriptions: () => [
    {
      id: "s1",
      query: "{}",
      table: "t",
      branches: [],
      tier: "edge",
      propagation: "full",
      createdAt: 1,
      stack: "x",
    },
  ],
  onActiveQuerySubscriptionsChange: (cb: () => void) => {
    changeCb = cb;
    return () => {};
  },
} as any;
const dispose = installInspectorHost(db, iframeWindow, "http://localhost");
expect(db.setDevMode).toHaveBeenCalledWith(true);
expect((window as any).__jazzInspectorHost.getConnectionConfig().schemaHash).toBe("hash1");
expect(posts.at(-1)).toMatchObject({ type: "jazz-inspector:subscriptions", list: [{ id: "s1" }] });
expect(posts.at(-1).list[0].stack).toBeUndefined();
changeCb();
expect(posts.length).toBe(2);
dispose();
expect((window as any).__jazzInspectorHost).toBeUndefined();
```

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Implement**

```ts
// host-bridge.ts
import type { Db } from "../../runtime/db.js";
import {
  INSPECTOR_HOST_GLOBAL,
  INSPECTOR_SUBSCRIPTIONS_MESSAGE,
  serializeActiveSubscriptions,
  type JazzInspectorHost,
} from "../../dev-tools/inspector-host-types.js";

export function installInspectorHost(db: Db, iframeWindow: Window, origin: string): () => void {
  db.setDevMode(true);

  const handle: JazzInspectorHost = {
    getConnectionConfig() {
      const c = db.getConfig(); // verify fields at runtime/db.ts getConfig()
      return {
        appId: c.appId,
        serverUrl: c.serverUrl,
        env: c.env,
        userBranch: c.userBranch,
        adminSecret: c.adminSecret,
        jwtToken: c.jwtToken,
        schemaHash: db.getSchemaHash(),
      };
    },
  };
  (window as unknown as Record<string, unknown>)[INSPECTOR_HOST_GLOBAL] = handle;

  const push = () => {
    iframeWindow.postMessage(
      {
        type: INSPECTOR_SUBSCRIPTIONS_MESSAGE,
        list: serializeActiveSubscriptions(db.getActiveQuerySubscriptions()),
      },
      origin,
    );
  };
  // Host realm owns the listener (no dead-iframe-listener hazard).
  const stop = db.onActiveQuerySubscriptionsChange(push);
  push(); // initial snapshot

  return () => {
    stop();
    delete (window as unknown as Record<string, unknown>)[INSPECTOR_HOST_GLOBAL];
  };
}
```

- [ ] **Step 4: Run — expect PASS.**

- [ ] **Step 5: Commit** — `feat(inspector): host bridge publishes config handle + pushes subscriptions`

---

### Task 4: Wire the loader to the host bridge (replace the relay)

**Files:**

- Modify: `packages/jazz-tools/src/dev/inspector-overlay/loader.ts`

- [ ] **Step 1: Read the current wiring.** In `loader.ts` find where `createRelay({...})` is built and `attachDevTools` is called from `startInspectorOverlay(db)` (the relay is wired in the message handler near the iframe creation). Note the `iframe` ref and `window.location.origin`.

- [ ] **Step 2: Replace** the `createRelay` block + the `window.addEventListener("message", relay.handle ...)` with a call to `installInspectorHost(db, iframe.contentWindow!, window.location.origin)`, stored so `disconnectedCallback`/teardown calls the returned `dispose()`. Remove the `import { createRelay } from "./relay.js"` and the `attachDevTools` call from `startInspectorOverlay`.

```ts
// loader.ts — startInspectorOverlay(db)
import { installInspectorHost } from "./host-bridge.js";
// ...inside connectedCallback, after the iframe exists and the AbortController is set:
const disposeHost = installInspectorHost(db, iframe.contentWindow!, window.location.origin);
signal.addEventListener("abort", () => disposeHost(), { once: true });
```

- [ ] **Step 3: Build jazz-tools** — `pnpm --filter jazz-tools exec tsc` — expect exit 0 (the bridge files still exist; we only stopped using the relay here).

- [ ] **Step 4: Commit** — `refactor(inspector): overlay loader publishes the host handle instead of the relay`

---

### Task 5: Overlay host-link context (read config + subscription hook)

**Files:**

- Create: `packages/inspector/src/contexts/host-link.ts`
- Test: `packages/inspector/src/contexts/host-link.test.tsx`

- [ ] **Step 1: Write the failing test** — `readInspectorHostConfig()` returns the config from `window.parent.__jazzInspectorHost.getConnectionConfig()` (mock `window.parent`), and returns `null` when absent. `useHostSubscriptions()` returns `[]` initially and updates when a `jazz-inspector:subscriptions` `message` event fires.

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Implement**

```ts
// host-link.ts
import { useEffect, useState } from "react";
import {
  INSPECTOR_HOST_GLOBAL,
  INSPECTOR_SUBSCRIPTIONS_MESSAGE,
  type InspectorConnectionConfig,
  type InspectorSubscription,
  type InspectorSubscriptionsMessage,
  type JazzInspectorHost,
} from "jazz-tools"; // re-exported by inspector-host-types via the public barrel until removal; see Task 12

export function readInspectorHostConfig(): InspectorConnectionConfig | null {
  try {
    const host = (window.parent as unknown as Record<string, unknown>)[INSPECTOR_HOST_GLOBAL] as
      | JazzInspectorHost
      | undefined;
    return host ? host.getConnectionConfig() : null;
  } catch {
    return null; // cross-origin / no parent
  }
}

export function useHostSubscriptions(): InspectorSubscription[] {
  const [list, setList] = useState<InspectorSubscription[]>([]);
  useEffect(() => {
    const onMessage = (e: MessageEvent) => {
      const data = e.data as InspectorSubscriptionsMessage | undefined;
      if (data?.type === INSPECTOR_SUBSCRIPTIONS_MESSAGE) setList(data.list);
    };
    window.addEventListener("message", onMessage);
    return () => window.removeEventListener("message", onMessage);
  }, []);
  return list;
}
```

- [ ] **Step 4: Run — expect PASS.**

- [ ] **Step 5: Commit** — `feat(inspector): overlay host-link (config read + subscription hook)`

---

### Task 6: Overlay opens its own worker connection

**Files:**

- Modify: `packages/inspector/src/embedded.tsx`
- Modify: `packages/inspector/src/inspector-app.tsx`

- [ ] **Step 1: Read the standalone connection path.** Open `packages/inspector/src/App.tsx` around the `createJazzClient({...})` call (appId/serverUrl/env/userBranch/adminSecret/driver) and the parallel `fetchStoredWasmSchema(appId, adminSecret, schemaHash)` / `fetchStoredPermissions` calls. Open `packages/jazz-tools/src/react/create-jazz-client.ts` for the `createJazzClient(config: DbConfig)` signature and `JazzClient` shape. This is the exact path the overlay reuses.

- [ ] **Step 2: Implement `createOverlayClient`** in `embedded.tsx`: read `readInspectorHostConfig()`; if null, render an "inspector not attached" message; else call `createJazzClient({ appId, serverUrl, env, userBranch, adminSecret OR jwtToken per identity mode, driver: { type: "memory" } })` and `fetchStoredWasmSchema(appId, adminSecret, schemaHash)` — mirror `App.tsx` exactly. Identity mode comes from Task 7's hook (default admin; for this task hardcode admin, wire the toggle in Task 7).

```tsx
// embedded.tsx (shape — match App.tsx's exact createJazzClient + fetchStoredWasmSchema calls)
import { createJazzClient } from "jazz-tools/react";
import { readInspectorHostConfig } from "./contexts/host-link.js";
// build config -> createJazzClient(...) + fetchStoredWasmSchema(...), pass into <InspectorApp client schema isOverlay />
```

- [ ] **Step 3: Update `inspector-app.tsx`** — accept the resolved `client` + `wasmSchema` as props (like the standalone `App`), drop `getRegisteredWasmSchema()` and `onDevToolsPortDisconnect()` (bridge-coupled), set `runtime="overlay"` (Task 8 adds the type), keep `isOverlay`. Provide the host-subscription list via `DevtoolsProvider` (Task 8).

- [ ] **Step 4: Build embedded** — `pnpm --filter inspector run build:embedded` — expect exit 0.

- [ ] **Step 5: Manual verify** — run the host app on `localhost:5478`, hard-refresh, open the overlay; Data Explorer shows rows from the overlay's own connection. (Bridge still present; we just stopped using it for the overlay client.)

- [ ] **Step 6: Commit** — `feat(inspector): overlay opens its own worker connection`

---

### Task 7: Admin ⇄ App-user (JWT) identity toggle

**Files:**

- Modify: `packages/inspector/src/pages/settings/index.tsx`
- Modify: `packages/inspector/src/embedded.tsx` (re-create client on mode change)
- Create: `packages/inspector/src/utility/identity-mode.ts` (localStorage-backed `"admin" | "jwt"`, default `"admin"`)

- [ ] **Step 1: Implement `identity-mode.ts`** — `useIdentityMode()` reusing `useLocalStorageState` (pattern: `packages/inspector/src/utility/use-local-storage-state.ts`), key `jazz.inspector.identityMode`, default `"admin"`, validator.

- [ ] **Step 2: Add the toggle to Settings** — a new "Connection" section (only when `isOverlay` and the config has both an `adminSecret` and a `jwtToken`), reusing the existing `ToggleRow`/switch from `settings/index.tsx`. Label: "Admin (see everything)" vs "App user". Hide if the JWT is absent.

- [ ] **Step 3: Re-create the client on change** — in `embedded.tsx`, key the client creation on the identity mode: `adminSecret` when `"admin"`, `jwtToken` when `"jwt"`. On change, `await oldClient.shutdown()` before creating the new one (guard overlapping switches). Verify `createJazzClient` accepts a `jwtToken` field (check `DbConfig` in `runtime/db.ts`).

- [ ] **Step 4: Manual verify** — toggling reconnects and the grid re-resolves; toggle hidden when no JWT.

- [ ] **Step 5: Commit** — `feat(inspector): admin/app-user identity toggle for the overlay`

---

### Task 8: Runtime model `standalone | overlay` + subscription source

**Files:**

- Modify: `packages/inspector/src/contexts/devtools-context.tsx`
- Modify: `packages/inspector/src/pages/live-query/index.tsx`
- Modify: `packages/inspector/src/components/data-explorer/TableDataGrid.tsx`
- Modify: `packages/inspector/src/components/data-explorer/TableSchemaDefinition.tsx`
- Modify: `packages/inspector/src/pages/data-explorer/index.tsx`

- [ ] **Step 1: Change the type** — `devtools-context.tsx`: `InspectorRuntime = "standalone" | "overlay"`. Add an optional `hostSubscriptions?: InspectorSubscription[]` to the context value, fed by `useHostSubscriptions()` in `inspector-app.tsx` (overlay only).

- [ ] **Step 2: Audit each `runtime ===` branch** (grep `runtime ===` / `!==` in `packages/inspector/src`):
  - `TableDataGrid.tsx:713` durability — overlay → `"edge"` (now a real connection): `runtime === "standalone" ? "edge" : "local"` → `"edge"` for both `standalone` and `overlay` (replace with `"edge"`).
  - `TableSchemaDefinition.tsx:31,62` permissions — show for overlay (fetchable now): change the `=== "extension"`-hidden guard to show.
  - `pages/data-explorer/index.tsx:30,46` propagation switch — overlay uses `"full"` like standalone; re-point or drop the `=== "extension"` branch.
  - `live-query/index.tsx` — the `extension` branch becomes the `overlay` branch and reads `hostSubscriptions` from context (Task 9) instead of the bridge cache.

- [ ] **Step 3: tsc + tests** — `pnpm --filter inspector exec tsc -b` (expect type errors pointing at every remaining `"extension"` literal — fix each) and `pnpm --filter inspector exec vitest run` (fix any test asserting old runtime behavior **only** where behavior intentionally changed; otherwise surface to the human per repo rule).

- [ ] **Step 4: Commit** — `refactor(inspector): runtime model standalone|overlay + host subscription source`

---

### Task 9: Live Query reads the pushed list (drop the stack column)

**Files:**

- Modify: `packages/inspector/src/pages/live-query/index.tsx`

- [ ] **Step 1: Read the current extension branch** — `ExtensionLiveQuery` + `useActiveSubscriptions` (seeds from `getActiveQuerySubscriptions()` / `onActiveQuerySubscriptionsChange()` bridge cache) and the columns incl. `row.original.stack`.

- [ ] **Step 2: Repoint** — rename to `OverlayLiveQuery`; source rows from `hostSubscriptions` (context). Remove the `stack` column and any stack-detail UI. Keep table/tier/propagation/branches/createdAt columns + the existing filter UI (`LiveQueryFilters.tsx`).

- [ ] **Step 3: tsc + tests** — fix the live-query test if it asserted the stack column (intentional change).

- [ ] **Step 4: Manual verify** — open a `subscribeAll` in the host app → it appears in the overlay's Live Query; close it → it disappears. (Note the devMode caveat: subs opened before attach may be absent.)

- [ ] **Step 5: Commit** — `feat(inspector): Live Query reads the host subscription push (no stacks)`

---

### Task 10: Remove the `extension` / `devtools` build

**Files:**

- Modify: `packages/inspector/vite.config.ts`, `packages/inspector/package.json`
- Delete: `packages/inspector/src/devtools-main.tsx`, `src/devtools/main.js`, `devtools-tab.html`, `devtools.html`, `chrome-extension/`

- [ ] **Step 1:** Remove the `build:extension` script and its inclusion in `build` (`package.json:8,11`). Remove the `isExtensionBuild` branch in `vite.config.ts:19-38`.
- [ ] **Step 2:** Delete the entry files + `chrome-extension/` dir listed above.
- [ ] **Step 3:** `pnpm --filter inspector run build` — expect only `web` + `embedded` to build, exit 0.
- [ ] **Step 4: Commit** — `chore(inspector): drop the chrome extension / devtools build`

---

### Task 11: Remove the bridge consumers in the inspector

**Files:**

- Modify: `packages/inspector/src/embedded.tsx`, `inspector-app.tsx` (remove any remaining bridge imports)

- [ ] **Step 1:** grep `packages/inspector/src` for `dev-tools`, `getRegisteredWasmSchema`, `onDevToolsPortDisconnect`, `createEmbeddedJazzClient`, `DevtoolsBridge`. Remove every remaining usage (the overlay no longer needs them after Task 6/9).
- [ ] **Step 2:** `pnpm --filter inspector exec tsc -b` + `vitest run` — green.
- [ ] **Step 3: Commit** — `refactor(inspector): drop all devtools-bridge imports`

---

### Task 12: Delete the bridge from jazz-tools (the 5-package refactor)

**Files (delete):** `packages/jazz-tools/src/dev-tools/{protocol.ts,dev-tools.ts,extension-panel.ts,parent-window-port.ts}` + tests; `packages/jazz-tools/src/dev/inspector-overlay/relay.ts`; `packages/jazz-tools/src/react/create-embedded-jazz-client.ts`.
**Files (modify):** `packages/jazz-tools/src/index.ts`, the `react/vue/svelte/solid/web` barrels + their `create-jazz-client.ts`, `auto-attach.ts`.

- [ ] **Step 1: Decide the public-API change (record in the commit body).** `jazz-tools/src/index.ts:230` does `export * from "./dev-tools/index.js"`, and `attachDevTools` is re-exported from `react/index.ts:7`, `vue/index.ts:6`, `svelte/index.ts:10`, `solid/index.ts:6`. This is a **breaking removal** of `attachDevTools`/the dev-tools surface. Confirm with the user it's acceptable to remove (not deprecate-shim) before deleting.

- [ ] **Step 2: Keep the shared types.** `inspector-host-types.ts` (Task 1) must survive — move it out of `dev-tools/` if `dev-tools/index.ts` is deleted (e.g. to `packages/jazz-tools/src/dev/inspector-overlay/inspector-host-types.ts`) and re-export it where the inspector imports it. Update Task 1/5 import paths accordingly.

- [ ] **Step 3: Rewrite `auto-attach.ts`** — `startInspectorOnce(db)` already dynamic-imports the loader; it stays, but the loader now installs the host bridge (Task 4) instead of `attachDevTools`. Remove any `attachDevTools` import here.

- [ ] **Step 4: Remove `createExtensionJazzClient`** from `react/create-jazz-client.ts` and `createDbFromInspectedPage` imports in `react/web/vue/svelte create-jazz-client.ts`. Remove the `index.ts:230` dev-tools re-export and the `attachDevTools` barrel re-exports in all four framework `index.ts`.

- [ ] **Step 5: Delete** the files listed above.

- [ ] **Step 6: Build everything** — `pnpm build:core` — expect exit 0 across all packages (this is the blast-radius check; fix every dangling import).

- [ ] **Step 7: Commit** — `refactor!(jazz-tools): remove the devtools bridge (breaking: drop attachDevTools)`

---

### Task 13: Rewrite the overlay browser test

**Files:**

- Modify: `packages/inspector/tests/browser/overlay.spec.ts`

- [ ] **Step 1: Read the current spec** (it builds `dist-embedded`, serves it, and asserts the bridge-connected overlay renders). Read `overlay-host.html` (the test host page).

- [ ] **Step 2: Rewrite** so the host page builds a real `Db`, the loader installs the host handle, the overlay opens its own connection, and the test asserts: Data Explorer shows a seeded row; Live Query shows a host subscription (no stack column). Use a `bytea` + `timestamp` column in the seed to confirm the own-realm client renders/writes them (the cross-realm bug the reviews flagged — proven moot here since the client is own-realm).

- [ ] **Step 3: Run** — `pnpm --filter inspector run test:browser` — expect PASS.

- [ ] **Step 4: Commit** — `test(inspector): overlay browser test for the own-connection path`

---

### Task 14: Final verification

- [ ] `pnpm build:core` — exit 0.
- [ ] `pnpm --filter inspector exec vitest run` — all pass.
- [ ] `pnpm exec oxfmt --check <changed files>` + `pnpm exec oxlint packages/inspector/src packages/jazz-tools/src` — clean.
- [ ] Manual: `localhost:5478` overlay — Data Explorer (own connection), Live Query (host subs, no stacks), identity toggle reconnects.
- [ ] Update the spec `Status:` to "Implemented" and commit.

---

## Self-Review

**Spec coverage:** Data own-connection (T6), config handle (T1–4), one-way subscription link no-stacks (T1,T3,T5,T9), identity toggle (T7), runtime refactor (T8), build removal (T10), bridge deletion incl. blast radius/public-API (T11–12), devMode caveat (noted in T9), tests (T13). The "schema from server" path reuses standalone (T6) — covered.

**Placeholder scan:** "verify-and-match" steps cite exact `file:line` and specify surrounding code; they are confirmations of existing signatures, not deferred work. The two genuine unknowns (the exact `Db` config/schema-hash accessor in T2, and `createJazzClient`/`fetchStoredWasmSchema` arg order in T6) are gated behind explicit "read this file" steps because they are existing APIs in unread files — the engineer reads one line and matches it. No "TODO/handle errors/etc." left.

**Type consistency:** `JazzInspectorHost`, `InspectorConnectionConfig`, `InspectorSubscription`, `INSPECTOR_HOST_GLOBAL`, `INSPECTOR_SUBSCRIPTIONS_MESSAGE`, `installInspectorHost`, `readInspectorHostConfig`, `useHostSubscriptions`, `serializeActiveSubscriptions` are defined in T1/T3/T5 and used consistently in T4/T6/T9. Runtime literal `"overlay"` introduced in T8 is used in T6 (note: T6 sets `runtime="overlay"` which only type-checks after T8 — acceptable since T6's build step predates T8's type change; if executing strictly in order, set the literal in T8 and have T6 pass a placeholder `runtime` that T8 finalizes).
