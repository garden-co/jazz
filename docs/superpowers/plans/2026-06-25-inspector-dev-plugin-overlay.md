# Inspector as a Dev-Plugin Overlay — Implementation Plan (v2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the "copy a link to the hosted Vercel inspector" dev flow with a locally-served inspector overlay that the dev plugins (Vite, SvelteKit, Next) auto-inject into the running dev app, wired to the app's live in-process client over the existing `jazz-devtools-v1` bridge.

**Architecture:** The inspector runs in an iframe inside the dev page. The dev plugin serves the inspector's published embedded build (resolved from the **app's** `node_modules`, degrading gracefully when absent) at `/__jazz/embedded`, and injects an external `/__jazz/loader.js` that renders a toggle + iframe and **relays** bridge `postMessage` traffic between the top window (where `attachDevTools` lives) and the iframe. The inspector connects over a new parent-window transport that reuses the existing bridge plumbing. `JazzProvider` auto-attaches the bridge in dev only. `attachDevTools` itself is left untouched.

**Tech Stack:** TypeScript, React 19 (inspector), Vite/Rollup, Vitest (unit/integration), Playwright (browser), the existing `jazz-devtools-v1` postMessage bridge, esbuild (loader bundling).

**Spec:** `docs/superpowers/specs/2026-06-25-inspector-dev-plugin-overlay-design.md`

---

## Decisions baked into this plan (resolved during 3-way review)

Settled after GLM + Opus + Codex reviewed plan v1. Read before starting — several reverse v1 mistakes.

1. **Packaging — consumer installs the inspector.** `jazz-tools` takes **no dependency** on the inspector (that would create an `inspector → jazz-tools → inspector` cycle — the inspector imports `jazz-tools` — and break turbo's `^build`). Instead the inspector is **published** (currently `private: true`) under a real name, and the dev plugin resolves it from the **app's** `node_modules`. If absent, the plugin logs a one-line hint and skips the overlay (no crash). The overlay is "one devDependency," not literally zero-install.
2. **Dev-gating uses `process.env.NODE_ENV` only**, never `import.meta.env.DEV`. `jazz-tools` is built with `tsc`, so the gate is folded by the _consumer's_ bundler; both Vite and Next statically replace `process.env.NODE_ENV`, but Next does **not** define `import.meta.env.DEV`. The heavy `attachDevTools` import is **dynamic** inside the dev branch so it is absent from production chunks.
3. **The relay loop-safety test must exercise the re-injection round-trip** (feed the re-injected request back with `source === topWindow`), not just count one call.
4. **`transformIndexHtml` must be gated to dev/serve** (it otherwise runs during `vite build` and injects the loader into production HTML).
5. **Targeted edits, not full-file rewrites**, for `provider.tsx` (preserve its schema-hash side-effect import and re-exports).

## Execution model — model selection per task

When running subagent-driven:

- **GLM** (`glm` alias) handles tasks tagged **[GLM — simple/medium]**.
- **Opus** handles tasks tagged **[OPUS — hard]** (transport seam, relay/loader, provider auto-attach, serve middleware, Next.js).

---

## Phase ordering

0. **Phase 0 — Inspector publishability** (rename, un-private, exports).
1. **Phase 1 — Transport seam** (pluggable bridge client).
2. **Phase 2 — Embedded client** (parent-window connector + `createEmbeddedJazzClient`).
3. **Phase 3 — Embedded inspector entry + build** (`dist-embedded/`).
4. **Phase 4 — Loader + relay** (invariant comment + round-trip test + DOM loader).
5. **Phase 5 — Provider auto-attach** (React, then Svelte/Vue/Solid).
6. **Phase 6 — Vite plugin serve + inject** (first end-to-end framework).
7. **Phase 7 — SvelteKit plugin serve + inject.**
8. **Phase 8 — Next.js plugin serve + inject** (long pole; must not block 6/7).
9. **Phase 9 — Example migration + sweep.**

Phases 0–6 deliver a working overlay on Vite. Each phase ends green and committed.

---

## File structure

**Inspector (`packages/inspector`)**

- Modify: `package.json` — drop `private`, set publishable `name`, add `exports` for `./dist-embedded/*`, add `build:embedded`, `files`.
- Create: `embedded.html`, `src/embedded.tsx` — third entry (near-copy of `devtools-main.tsx`).
- Modify: `vite.config.ts` — dedicated embedded build → `dist-embedded/` (own outDir, `base:"./"`).
- Create: `src/embedded.browser.test.ts` (Playwright) — e2e relay + render.

**Bridge / client (`packages/jazz-tools/src`)**

- Modify: `dev-tools/extension-panel.ts` — `DevtoolsBridgePort` + pluggable connector (default chrome) + reset helper.
- Create: `dev-tools/parent-window-port.ts` (+ `.test.ts`).
- Modify: `dev-tools/dev-tools.ts` — `event.source === window` invariant comment (no logic change).
- Modify: `dev-tools/index.ts` — export connector setter + parent-window connector.
- Create: `react/create-embedded-jazz-client.ts`; Modify: `react/index.ts`.

**Loader / relay + serve (`packages/jazz-tools/src/dev/inspector-overlay`)**

- Create: `relay.ts` (+ `.test.ts`), `loader.ts`, `serve.ts` (+ `.test.ts`).

**Provider (`packages/jazz-tools/src`)**

- Modify (targeted): `react/provider.tsx` (+ `react/provider.devtools.test.tsx`); svelte/vue/solid providers.

**Dev plugins (`packages/jazz-tools/src/dev`)**

- Modify: `vite.ts`, `sveltekit.ts`, `next.ts`, `inspector-link.ts`.

**Example**

- Modify: `examples/todo-client-localfirst-react/{src/App.tsx,package.json}`.

---

## Conventions for every task

- For any Rust test, read `crates/jazz-tools/TESTING_GUIDELINES.md` first. For TS, prefer black-box tests through the public API; never use JSON-shaped schema/permission literals.
- Build core: `pnpm build:core`. Test all: `pnpm test`. Scope: `pnpm --filter <pkg> test`.
- Commit after each task; conventional commits; no AI/Claude attribution.
- Run the exact verification command and confirm the stated output before moving on.

---

## Phase 0 — Inspector publishability

### Task 0.1: Make the inspector a publishable package **[GLM — simple/medium]**

**Files:** Modify `packages/inspector/package.json`; references in root `package.json`, `.github/workflows/promote-inspector-production.yml`, `.github/workflows/publish-jazz-tools-alpha.yml`.

Today: `name: "inspector"`, `private: true`, no `exports`. `inspector` (unscoped) is taken on npm. **Use `jazz-inspector`** (verify; else use the project's publish scope). One constant in the serve middleware (Task 6.1) holds this name.

- [ ] **Step 1: Confirm the name is publishable.**

Run: `npm view jazz-inspector version 2>&1 | head -1`
Expected: `npm error code E404` (free). If taken, pick the project's scoped name and use it consistently below.

- [ ] **Step 2: Edit `packages/inspector/package.json`** — remove `"private": true`, set `"name": "jazz-inspector"`, add:

```json
"name": "jazz-inspector",
"exports": { "./dist-embedded/*": "./dist-embedded/*" },
"files": ["dist-embedded"],
```

(`files` keeps the 10 MB Vercel/extension builds out of the published tarball.)

- [ ] **Step 3: Update the three references** from `inspector` to `jazz-inspector`.

Run: `grep -rn "filter inspector\|filter=inspector\|\"inspector\"" package.json .github/workflows/ | grep -v node_modules`
Change `pnpm --filter inspector ...` → `pnpm --filter jazz-inspector ...` in root `package.json` and any workflow filters. Leave Vercel project identifiers unless they key on the package name.

- [ ] **Step 4: Reinstall.**

Run: `pnpm install && pnpm --filter jazz-inspector exec node -e "console.log('ok')"`
Expected: `ok`.

- [ ] **Step 5: Commit.**

```bash
git add packages/inspector/package.json package.json .github/workflows/promote-inspector-production.yml .github/workflows/publish-jazz-tools-alpha.yml pnpm-lock.yaml
git commit -m "chore(inspector): make package publishable (rename, drop private, add embedded export)"
```

---

## Phase 1 — Transport seam (inspector-side bridge client)

Line numbers verified against current `extension-panel.ts`.

### Task 1.1: Pluggable bridge-port connector **[OPUS — hard]**

**Files:** Modify `packages/jazz-tools/src/dev-tools/extension-panel.ts`.

`ensureDevtoolsPort()` (lines 217–344) is chrome-specific only in acquisition (222–244); the `onMessage`/`onDisconnect` handlers (246–341) use just `postMessage`/`onMessage.addListener`/`onDisconnect.addListener`, which a chrome `Port` already satisfies.

- [ ] **Step 1: Add the interface + connector registry** (~line 61):

```ts
export interface DevtoolsBridgePort {
  postMessage(message: unknown): void;
  onMessage: {
    addListener(cb: (message: unknown) => void): void;
    removeListener(cb: (message: unknown) => void): void;
  };
  onDisconnect: { addListener(cb: () => void): void; removeListener(cb: () => void): void };
}
export type DevtoolsBridgeConnector = () => Promise<DevtoolsBridgePort>;

let bridgeConnector: DevtoolsBridgeConnector = connectChromeDevtoolsPort;
export function setDevtoolsBridgeConnector(connector: DevtoolsBridgeConnector): void {
  bridgeConnector = connector;
}
export function resetDevtoolsBridgeConnector(): void {
  bridgeConnector = connectChromeDevtoolsPort;
}
```

- [ ] **Step 2: Extract `connectChromeDevtoolsPort()`** from lines 222–244 (returns the chrome `Port`, attaches no listeners):

```ts
async function connectChromeDevtoolsPort(): Promise<DevtoolsBridgePort> {
  const global = globalThis as any;
  const chromeApi = global?.chrome;
  if (
    !chromeApi ||
    !chromeApi.devtools ||
    !chromeApi.devtools.inspectedWindow ||
    !chromeApi.tabs ||
    typeof chromeApi.tabs.connect !== "function"
  ) {
    throw new Error("Chrome DevTools API is not available.");
  }
  const tabId = chromeApi.devtools.inspectedWindow.tabId;
  try {
    return await connectValidatedPort(chromeApi, tabId);
  } catch (error) {
    if (!isMissingReceivingEndError(error)) throw error;
    await installBridgeInInspectedTab(chromeApi, tabId);
    return await connectValidatedPort(chromeApi, tabId);
  }
}
```

- [ ] **Step 3: Rewrite the head of `ensureDevtoolsPort`** to use the connector; keep the `onMessage`/`onDisconnect` bodies verbatim:

```ts
async function ensureDevtoolsPort(): Promise<DevtoolsBridgePort> {
  if (devtoolsPort) return devtoolsPort;
  devtoolsPort = await bridgeConnector();
  // ... existing onMessage / onDisconnect definitions unchanged ...
  devtoolsPort.onMessage.addListener(onMessage);
  devtoolsPort.onDisconnect.addListener(onDisconnect);
  notifyDevtoolsPortConnected();
  return devtoolsPort;
}
```

- [ ] **Step 4: Change line 54** `let devtoolsPort: any | null = null;` → `let devtoolsPort: DevtoolsBridgePort | null = null;`.

- [ ] **Step 5: Build.** Run: `pnpm --filter jazz-tools build` — Expected: clean (extension path unchanged).
- [ ] **Step 6: Existing dev-tools tests.** Run: `pnpm --filter jazz-tools test -- dev-tools` — Expected: PASS.
- [ ] **Step 7: Commit.**

```bash
git add packages/jazz-tools/src/dev-tools/extension-panel.ts
git commit -m "refactor(dev-tools): make bridge client transport pluggable"
```

---

## Phase 2 — Embedded client (parent-window transport)

### Task 2.1: Parent-window bridge port **[GLM — simple/medium]**

**Files:** Create `packages/jazz-tools/src/dev-tools/parent-window-port.ts` (+ `.test.ts`).

- [ ] **Step 1: Write the failing test.**

```ts
import { describe, it, expect, vi } from "vitest";
import { DEVTOOLS_BRIDGE_CHANNEL } from "./protocol.js";
import { createParentWindowBridgePort } from "./parent-window-port.js";

describe("createParentWindowBridgePort", () => {
  it("posts outbound messages to window.parent with the page origin", async () => {
    const parentPost = vi.fn();
    vi.stubGlobal("window", {
      parent: { postMessage: parentPost },
      location: { origin: "http://localhost:5173" },
      addEventListener: vi.fn(),
    } as unknown as Window);
    const port = await createParentWindowBridgePort();
    port.postMessage({ channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "request" });
    expect(parentPost).toHaveBeenCalledWith(
      { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "request" },
      "http://localhost:5173",
    );
  });

  it("delivers inbound messages only from window.parent on the bridge channel", async () => {
    let handler: (e: MessageEvent) => void = () => {};
    const parent = { postMessage: vi.fn() };
    vi.stubGlobal("window", {
      parent,
      location: { origin: "http://localhost:5173" },
      addEventListener: (t: string, cb: (e: MessageEvent) => void) => {
        if (t === "message") handler = cb;
      },
    } as unknown as Window);
    const port = await createParentWindowBridgePort();
    const received: unknown[] = [];
    port.onMessage.addListener((m) => received.push(m));
    handler({
      source: parent,
      data: { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "response" },
    } as unknown as MessageEvent);
    handler({
      source: {},
      data: { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "response" },
    } as unknown as MessageEvent);
    handler({
      source: parent,
      data: { channel: "other", kind: "response" },
    } as unknown as MessageEvent);
    expect(received).toEqual([{ channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "response" }]);
  });
});
```

- [ ] **Step 2: Run to verify it fails.** Run: `pnpm --filter jazz-tools test -- parent-window-port` — Expected: FAIL.

- [ ] **Step 3: Implement `parent-window-port.ts`.**

```ts
import type { DevtoolsBridgePort } from "./extension-panel.js";
import { DEVTOOLS_BRIDGE_CHANNEL, isRecord } from "./protocol.js";

export function createParentWindowBridgePort(): Promise<DevtoolsBridgePort> {
  const messageListeners = new Set<(message: unknown) => void>();
  const disconnectListeners = new Set<() => void>();
  const onWindowMessage = (event: MessageEvent) => {
    if (event.source !== window.parent) return;
    const data = event.data;
    if (!isRecord(data) || data.channel !== DEVTOOLS_BRIDGE_CHANNEL) return;
    for (const listener of messageListeners) listener(data);
  };
  window.addEventListener("message", onWindowMessage);
  const port: DevtoolsBridgePort = {
    postMessage(message: unknown) {
      window.parent.postMessage(message, window.location.origin);
    },
    onMessage: {
      addListener: (cb) => messageListeners.add(cb),
      removeListener: (cb) => messageListeners.delete(cb),
    },
    onDisconnect: {
      addListener: (cb) => disconnectListeners.add(cb),
      removeListener: (cb) => disconnectListeners.delete(cb),
    },
  };
  return Promise.resolve(port);
}
```

- [ ] **Step 4: Run to verify it passes.** Run: `pnpm --filter jazz-tools test -- parent-window-port` — Expected: PASS.
- [ ] **Step 5: Commit.**

```bash
git add packages/jazz-tools/src/dev-tools/parent-window-port.ts packages/jazz-tools/src/dev-tools/parent-window-port.test.ts
git commit -m "feat(dev-tools): add parent-window bridge port transport"
```

### Task 2.2: `createEmbeddedJazzClient()` + exports **[GLM — simple/medium]**

**Files:** Create `react/create-embedded-jazz-client.ts`; Modify `react/index.ts`, `dev-tools/index.ts`.

Confirmed: `ensureDevtoolsPort()` reads `bridgeConnector` (module-level `let`) at call time, port acquired lazily — so setting the connector before delegating works.

- [ ] **Step 1: Export from `dev-tools/index.ts`:**

```ts
export {
  setDevtoolsBridgeConnector,
  resetDevtoolsBridgeConnector,
  type DevtoolsBridgePort,
  type DevtoolsBridgeConnector,
} from "./extension-panel.js";
export { createParentWindowBridgePort } from "./parent-window-port.js";
```

- [ ] **Step 2: Implement `create-embedded-jazz-client.ts`:**

```ts
import { setDevtoolsBridgeConnector, createParentWindowBridgePort } from "../dev-tools/index.js";
import { createExtensionJazzClient, type JazzClient } from "./create-jazz-client.js";

export function createEmbeddedJazzClient(): Promise<JazzClient> {
  setDevtoolsBridgeConnector(createParentWindowBridgePort);
  return createExtensionJazzClient();
}
```

- [ ] **Step 3: Export from `react/index.ts`:** `export { createEmbeddedJazzClient } from "./create-embedded-jazz-client.js";`
- [ ] **Step 4: Build.** Run: `pnpm --filter jazz-tools build` — Expected: clean.
- [ ] **Step 5: Commit.**

```bash
git add packages/jazz-tools/src/react/create-embedded-jazz-client.ts packages/jazz-tools/src/react/index.ts packages/jazz-tools/src/dev-tools/index.ts
git commit -m "feat(react): add createEmbeddedJazzClient for overlay iframe"
```

---

## Phase 3 — Embedded inspector entry + build

### Task 3.1: Embedded entry component **[GLM — simple/medium]**

**Files:** Create `packages/inspector/src/embedded.tsx`, `packages/inspector/embedded.html`.

- [ ] **Step 1: Create `embedded.tsx`.**

```tsx
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router";
import { createEmbeddedJazzClient, JazzClientProvider } from "jazz-tools/react";
import { getRegisteredWasmSchema, onDevToolsPortDisconnect } from "jazz-tools";
import { use, useEffect, useMemo } from "react";
import { DevtoolsProvider } from "./contexts/devtools-context";
import { InspectorRoutes } from "./routes";
import "./index.css";

const client = createEmbeddedJazzClient();

function App() {
  const embeddedClient = use(client);
  const wasmSchema = useMemo(() => getRegisteredWasmSchema(), [embeddedClient]);
  useEffect(() => onDevToolsPortDisconnect(() => window.location.reload()), []);
  if (!embeddedClient || !wasmSchema) return <p>Waiting for runtime devtools connection...</p>;
  return (
    <JazzClientProvider client={embeddedClient}>
      <DevtoolsProvider wasmSchema={wasmSchema} runtime="extension">
        <MemoryRouter>
          <InspectorRoutes />
        </MemoryRouter>
      </DevtoolsProvider>
    </JazzClientProvider>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
```

- [ ] **Step 2: Create `embedded.html`** (mirror `index.html`, point at `/src/embedded.tsx`, title "Jazz Inspector (embedded)").
- [ ] **Step 3: Typecheck.** Run: `pnpm --filter jazz-inspector exec tsc -b` — Expected: no errors.
- [ ] **Step 4: Commit.**

```bash
git add packages/inspector/src/embedded.tsx packages/inspector/embedded.html
git commit -m "feat(inspector): add embedded entry for overlay iframe"
```

### Task 3.2: Dedicated embedded build → `dist-embedded/` **[GLM — simple/medium]**

**Files:** Modify `packages/inspector/vite.config.ts`, `package.json`, `.gitignore`.

The plugin must serve **only** the embedded output, so it needs its own `outDir`. Use a dedicated build with `outDir: "dist-embedded"` and `base:"./"` — the minimal distinct-output approach (spec §A wording synced in Task 9.3).

- [ ] **Step 1: Add the embedded branch to `vite.config.ts`** (before the existing return):

```ts
if (mode === "embedded") {
  return {
    plugins: [react()],
    base: "./",
    worker: { format: "es" },
    build: {
      outDir: "dist-embedded",
      emptyOutDir: true,
      rollupOptions: { input: { index: resolve(__dirname, "embedded.html") } },
    },
  };
}
```

`base:"./"` is required so assets resolve when served from `/__jazz/embedded/`.

- [ ] **Step 2: Add the script** in `package.json`: `"build:embedded": "vite build --mode embedded"`, and append `&& pnpm run build:embedded` to `build`.
- [ ] **Step 3: Build and confirm.** Run: `pnpm --filter jazz-inspector run build:embedded && ls packages/inspector/dist-embedded/index.html` — Expected: exists; `dist-embedded/assets/` populated.
- [ ] **Step 4: Gitignore `dist-embedded`** alongside `dist`/`dist-extension`. Run: `grep -E "dist" packages/inspector/.gitignore`.
- [ ] **Step 5: Commit.**

```bash
git add packages/inspector/vite.config.ts packages/inspector/package.json packages/inspector/.gitignore
git commit -m "build(inspector): add embedded build → dist-embedded"
```

---

## Phase 4 — Loader + relay

### Task 4.0: Pin the `event.source === window` invariant **[GLM — simple/medium]**

**Files:** Modify `packages/jazz-tools/src/dev-tools/dev-tools.ts` (line 217, no logic change).

- [ ] **Step 1: Add a comment** above `if (event.source !== window) return;`:

```ts
// LOAD-BEARING for the inspector overlay: the relay (dev/inspector-overlay)
// re-injects iframe requests into THIS window so event.source === window.
// Loosening this guard would let any frame drive the bridge and could loop.
```

- [ ] **Step 2: Build.** Run: `pnpm --filter jazz-tools build` — Expected: clean.
- [ ] **Step 3: Commit.**

```bash
git add packages/jazz-tools/src/dev-tools/dev-tools.ts
git commit -m "docs(dev-tools): note overlay relay dependence on event.source guard"
```

### Task 4.1: Pure relay routing + loop/amplification tests **[OPUS — hard]**

**Files:** Create `packages/jazz-tools/src/dev/inspector-overlay/relay.ts` (+ `.test.ts`).

Rules (mirroring `extension-panel.ts:138–154`): iframe→top forwards only `request` from our iframe via `window.postMessage(msg,"*")`; top→iframe forwards only `response|event`. A re-injected request (`source===topWindow`, `kind==="request"`) hits the top→iframe arm, which drops it → no loop.

- [ ] **Step 1: Write the failing tests — including the round-trip.**

```ts
import { describe, it, expect, vi } from "vitest";
import { DEVTOOLS_BRIDGE_CHANNEL } from "../../dev-tools/protocol.js";
import { createRelay } from "./relay.js";

const makeWindow = () => ({ postMessage: vi.fn() });
const opts = (top: unknown, iframe: unknown) => ({
  topWindow: top as Window,
  iframeWindow: iframe as Window,
  origin: "http://localhost:5173",
});

describe("inspector overlay relay", () => {
  it("re-injects an iframe request into the top window exactly once", () => {
    const top = makeWindow(),
      iframe = makeWindow();
    const relay = createRelay(opts(top, iframe));
    relay.handle({
      source: iframe,
      data: { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "request", requestId: "r1" },
    } as unknown as MessageEvent);
    expect(top.postMessage).toHaveBeenCalledTimes(1);
    expect(top.postMessage).toHaveBeenCalledWith(
      { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "request", requestId: "r1" },
      "*",
    );
    expect(iframe.postMessage).not.toHaveBeenCalled();
  });

  // THE load-bearing invariant.
  it("does not echo a re-injected request (no loop)", () => {
    const top = makeWindow(),
      iframe = makeWindow();
    const relay = createRelay(opts(top, iframe));
    const req = { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "request", requestId: "r1" };
    relay.handle({ source: iframe, data: req } as unknown as MessageEvent); // hop 1
    relay.handle({ source: top, data: req } as unknown as MessageEvent); // hop 2
    expect(top.postMessage).toHaveBeenCalledTimes(1);
    expect(iframe.postMessage).not.toHaveBeenCalled();
  });

  it("does NOT re-inject a request from a foreign frame (NOTE: not the overlay+extension multi-peer case)", () => {
    const top = makeWindow(),
      iframe = makeWindow(),
      other = makeWindow();
    const relay = createRelay(opts(top, iframe));
    relay.handle({
      source: other,
      data: { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "request", requestId: "x" },
    } as unknown as MessageEvent);
    expect(top.postMessage).not.toHaveBeenCalled();
    expect(iframe.postMessage).not.toHaveBeenCalled();
  });

  it("forwards a runtime reply into the iframe and does not re-post to top", () => {
    const top = makeWindow(),
      iframe = makeWindow();
    const relay = createRelay(opts(top, iframe));
    relay.handle({
      source: top,
      data: { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "response", requestId: "r1", ok: true },
    } as unknown as MessageEvent);
    expect(iframe.postMessage).toHaveBeenCalledWith(
      { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "response", requestId: "r1", ok: true },
      "http://localhost:5173",
    );
    expect(top.postMessage).not.toHaveBeenCalled();
  });

  it("ignores non-bridge-channel messages", () => {
    const top = makeWindow(),
      iframe = makeWindow();
    const relay = createRelay(opts(top, iframe));
    relay.handle({
      source: iframe,
      data: { channel: "other", kind: "request" },
    } as unknown as MessageEvent);
    expect(top.postMessage).not.toHaveBeenCalled();
    expect(iframe.postMessage).not.toHaveBeenCalled();
  });
});
```

- [ ] **Step 2: Run to verify it fails.** Run: `pnpm --filter jazz-tools test -- inspector-overlay/relay` — Expected: FAIL.

- [ ] **Step 3: Implement `relay.ts`.**

```ts
import { DEVTOOLS_BRIDGE_CHANNEL } from "../../dev-tools/protocol.js";

export interface RelayOptions {
  topWindow: Window;
  iframeWindow: Window;
  origin: string;
}

function isBridgeMessage(data: unknown): data is { channel: string; kind: string } {
  return (
    typeof data === "object" &&
    data !== null &&
    (data as { channel?: unknown }).channel === DEVTOOLS_BRIDGE_CHANNEL &&
    typeof (data as { kind?: unknown }).kind === "string"
  );
}

export function createRelay({ topWindow, iframeWindow, origin }: RelayOptions) {
  function handle(event: MessageEvent): void {
    const data = event.data;
    if (!isBridgeMessage(data)) return;
    if (event.source === iframeWindow) {
      if (data.kind !== "request") return;
      topWindow.postMessage(data, "*");
      return;
    }
    if (event.source === topWindow) {
      if (data.kind !== "response" && data.kind !== "event") return;
      iframeWindow.postMessage(data, origin);
      return;
    }
  }
  return { handle };
}
```

- [ ] **Step 4: Run to verify it passes.** Run: `pnpm --filter jazz-tools test -- inspector-overlay/relay` — Expected: PASS (all five).
- [ ] **Step 5: Commit.**

```bash
git add packages/jazz-tools/src/dev/inspector-overlay/relay.ts packages/jazz-tools/src/dev/inspector-overlay/relay.test.ts
git commit -m "feat(dev): add inspector overlay relay with loop-safety invariant"
```

### Task 4.2: DOM loader (toggle + iframe + relay wiring) **[OPUS — hard]**

**Files:** Create `packages/jazz-tools/src/dev/inspector-overlay/loader.ts`. Bundled to `/__jazz/loader.js` (Task 6.1).

- [ ] **Step 1: Implement `loader.ts`.**

```ts
import { createRelay } from "./relay.js";

const PANEL_STATE_KEY = "jazz-inspector-overlay:open";
const readOpen = () => {
  try {
    return localStorage.getItem(PANEL_STATE_KEY) === "1";
  } catch {
    return false;
  }
};
const writeOpen = (open: boolean) => {
  try {
    localStorage.setItem(PANEL_STATE_KEY, open ? "1" : "0");
  } catch {
    /* ignore */
  }
};

function mount(): void {
  const w = window as unknown as Record<string, unknown>;
  if (w.__jazzInspectorOverlayMounted) return;
  w.__jazzInspectorOverlayMounted = true;

  const container = document.createElement("div");
  container.id = "jazz-inspector-overlay";
  container.style.cssText =
    "position:fixed;bottom:16px;right:16px;z-index:2147483647;font-family:system-ui,sans-serif;";

  const toggle = document.createElement("button");
  toggle.textContent = "⚡";
  toggle.setAttribute("aria-label", "Toggle Jazz inspector");
  toggle.style.cssText =
    "width:40px;height:40px;border-radius:50%;border:none;cursor:pointer;font-size:18px;box-shadow:0 2px 8px rgba(0,0,0,.25);background:#111;color:#fff;";

  const panel = document.createElement("div");
  panel.style.cssText =
    "position:fixed;bottom:64px;right:16px;width:480px;height:640px;max-width:90vw;max-height:80vh;background:#fff;border:1px solid #ddd;border-radius:8px;overflow:hidden;box-shadow:0 8px 32px rgba(0,0,0,.3);resize:both;display:none;";

  const iframe = document.createElement("iframe");
  // NOTE: the embedded Vite build emits embedded.html (Vite names HTML output
  // after the source filename), NOT index.html.
  iframe.src = "/__jazz/embedded/embedded.html";
  iframe.style.cssText = "width:100%;height:100%;border:none;";
  panel.appendChild(iframe);

  let open = readOpen();
  const apply = () => (panel.style.display = open ? "block" : "none");
  const setOpen = (next: boolean) => {
    open = next;
    writeOpen(open);
    apply();
  };
  toggle.addEventListener("click", () => setOpen(!open));
  window.addEventListener("keydown", (e) => {
    if (e.altKey && e.shiftKey && e.key.toLowerCase() === "j") setOpen(!open);
  });
  apply();

  container.appendChild(panel);
  container.appendChild(toggle);
  document.body.appendChild(container);

  const relay = createRelay({
    topWindow: window,
    iframeWindow: iframe.contentWindow!,
    origin: window.location.origin,
  });
  window.addEventListener("message", (event) => relay.handle(event));
}

if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", mount);
else mount();
```

- [ ] **Step 2: Typecheck.** Run: `pnpm --filter jazz-tools exec tsc --noEmit -p tsconfig.json 2>&1 | grep inspector-overlay || echo "ok"` — Expected: `ok`.
- [ ] **Step 3: Commit.**

```bash
git add packages/jazz-tools/src/dev/inspector-overlay/loader.ts
git commit -m "feat(dev): add inspector overlay loader (toggle + iframe + relay)"
```

---

## Phase 5 — Provider auto-attach

### Task 5.1: React provider auto-attach + opt-out + tests **[OPUS — hard]**

**Files:** Modify (targeted) `packages/jazz-tools/src/react/provider.tsx`; Create `react/provider.devtools.test.tsx`.

Dev-gate on `process.env.NODE_ENV !== "production"` only; dynamic-import `attachDevTools` so it never enters prod chunks; `autoAttachDevTools` prop (default true); `wasmSchema` prop supplies the schema; idempotency via a db `WeakSet`. **Preserve** the schema-hash import (lines 12–18) and all re-exports.

- [ ] **Step 1: Write the failing test.**

```tsx
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, waitFor } from "@testing-library/react";
import * as React from "react";

const attachSpy = vi.fn().mockResolvedValue({});
vi.mock("../dev-tools/dev-tools.js", () => ({ attachDevTools: attachSpy }));
const fakeDb = {};
vi.mock("../react-core/provider.js", () => ({
  JazzProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  useDb: () => fakeDb,
  useJazzClient: () => ({ db: fakeDb }),
  useSession: () => null,
}));
import { JazzProvider } from "./provider.js";
const fakeSchema = { tableA: {} } as never;

describe("JazzProvider dev auto-attach", () => {
  beforeEach(() => {
    attachSpy.mockClear();
    (process.env as Record<string, string>).NODE_ENV = "development";
  });
  it("auto-attaches in dev when wasmSchema is provided", async () => {
    render(
      <JazzProvider config={{} as never} wasmSchema={fakeSchema}>
        <div />
      </JazzProvider>,
    );
    await waitFor(() => expect(attachSpy).toHaveBeenCalledTimes(1));
  });
  it("does not auto-attach with autoAttachDevTools={false}", async () => {
    render(
      <JazzProvider config={{} as never} wasmSchema={fakeSchema} autoAttachDevTools={false}>
        <div />
      </JazzProvider>,
    );
    await new Promise((r) => setTimeout(r, 20));
    expect(attachSpy).not.toHaveBeenCalled();
  });
  it("does not attach when NODE_ENV=production", async () => {
    (process.env as Record<string, string>).NODE_ENV = "production";
    render(
      <JazzProvider config={{} as never} wasmSchema={fakeSchema}>
        <div />
      </JazzProvider>,
    );
    await new Promise((r) => setTimeout(r, 20));
    expect(attachSpy).not.toHaveBeenCalled();
  });
});
```

(Dynamic `import()` resolves to the `vi.mock` factory, so the spy fires.)

- [ ] **Step 2: Run to verify it fails.** Run: `pnpm --filter jazz-tools test -- provider.devtools` — Expected: FAIL.

- [ ] **Step 3: Apply the targeted edit.** Add imports + the component + two props; render the component inside `<CoreJazzProvider>`:

```tsx
import { useEffect } from "react";
import type { WasmSchema } from "../index.js";

const autoAttachedDbs = new WeakSet<object>();

function DevToolsAutoAttach({ wasmSchema }: { wasmSchema?: WasmSchema }) {
  const { db } = useCoreJazzClient() as JazzClientContextValue;
  useEffect(() => {
    if (!wasmSchema || autoAttachedDbs.has(db as object)) return;
    autoAttachedDbs.add(db as object);
    void import("../dev-tools/dev-tools.js").then(({ attachDevTools }) =>
      attachDevTools({ db }, wasmSchema),
    );
  }, [db, wasmSchema]);
  return null;
}

// JazzProviderProps gains:  autoAttachDevTools?: boolean;  wasmSchema?: WasmSchema;
// In JazzProvider(...):
const shouldAutoAttach = process.env.NODE_ENV !== "production" && autoAttachDevTools !== false;
// inside <CoreJazzProvider>, before {children}:
{
  shouldAutoAttach ? <DevToolsAutoAttach wasmSchema={wasmSchema} /> : null;
}
```

`process.env.NODE_ENV !== "production"` is statically replaced by the consumer's bundler; in prod `shouldAutoAttach` is `false`, the JSX is removed, and the dynamic `import("../dev-tools/dev-tools.js")` is never reached.

- [ ] **Step 4: Run to verify it passes.** Run: `pnpm --filter jazz-tools test -- provider.devtools` — Expected: PASS (all three).
- [ ] **Step 5: Build.** Run: `pnpm --filter jazz-tools build` — Expected: clean. (True prod-absence verified once manually in Task 9.2.)
- [ ] **Step 6: Commit.**

```bash
git add packages/jazz-tools/src/react/provider.tsx packages/jazz-tools/src/react/provider.devtools.test.tsx
git commit -m "feat(react): dev-only auto-attach devtools in JazzProvider"
```

### Task 5.2: Svelte/Vue/Solid provider auto-attach **[GLM — simple/medium]**

**Files:** svelte/vue/solid provider components.

Mirror 5.1: `autoAttachDevTools` (default true) + `wasmSchema`, `process.env.NODE_ENV !== "production"` gate, `WeakSet` db guard, dynamic `import("../dev-tools/dev-tools.js")` then `attachDevTools({ db }, wasmSchema)` in each mount hook.

- [ ] **Step 1: Locate providers.** Run: `pnpm --filter jazz-tools exec grep -rn "createJazzClient\|JazzProvider\|useDb" src/svelte src/vue src/solid | head -40`.
- [ ] **Step 2–4: Implement** in Svelte, Vue, Solid providers.
- [ ] **Step 5: Build.** Run: `pnpm --filter jazz-tools build` — Expected: clean.
- [ ] **Step 6: Commit.**

```bash
git add packages/jazz-tools/src/svelte packages/jazz-tools/src/vue packages/jazz-tools/src/solid
git commit -m "feat(svelte,vue,solid): dev-only auto-attach devtools in providers"
```

---

## Phase 6 — Vite plugin: serve + inject

### Task 6.1: Shared serve middleware **[OPUS — hard]**

**Files:** Create `packages/jazz-tools/src/dev/inspector-overlay/serve.ts` (+ `.test.ts`).

Resolve the inspector from the **app root**; **degrade gracefully** when absent; resolve `loader` relative to `import.meta.url`; `relative()` path containment.

- [ ] **Step 1: Write the failing test** (loader serving + graceful-degrade; no inspector install required):

```ts
import { describe, it, expect } from "vitest";
import { createOverlayHandler } from "./serve.js";

function fakeRes() {
  const headers: Record<string, string> = {};
  const state = { body: "", statusCode: 200 };
  return {
    res: {
      setHeader: (k: string, v: string) => (headers[k] = v),
      get statusCode() {
        return state.statusCode;
      },
      set statusCode(v: number) {
        state.statusCode = v;
      },
      end: (b?: string | Buffer) => (state.body = b ? b.toString() : ""),
    },
    headers,
    state,
  };
}

describe("overlay serve middleware", () => {
  it("serves the bundled loader.js", async () => {
    const handler = createOverlayHandler({ appRoot: process.cwd() });
    const r = fakeRes();
    expect(await handler({ url: "/__jazz/loader.js" }, r.res as never)).toBe(true);
    expect(r.headers["Content-Type"]).toContain("javascript");
    expect(r.state.body.length).toBeGreaterThan(0);
  });
  it("ignores unrelated urls", async () => {
    const handler = createOverlayHandler({ appRoot: process.cwd() });
    expect(await handler({ url: "/index.html" }, fakeRes().res as never)).toBe(false);
  });
  it("404s embedded requests when inspector is not installed (no crash)", async () => {
    const handler = createOverlayHandler({ appRoot: "/nonexistent-app-root" });
    const r = fakeRes();
    expect(await handler({ url: "/__jazz/embedded/index.html" }, r.res as never)).toBe(true);
    expect(r.state.statusCode).toBe(404);
  });
});
```

- [ ] **Step 2: Run to verify it fails.** Run: `pnpm --filter jazz-tools test -- inspector-overlay/serve` — Expected: FAIL.

- [ ] **Step 3: Implement `serve.ts`.**

```ts
import { createRequire } from "node:module";
import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { dirname, join, relative, isAbsolute, fileURLToPath } from "node:path";
import { build } from "esbuild";

export const INSPECTOR_PACKAGE = "jazz-inspector"; // Phase 0; one source of truth
export const OVERLAY_LOADER_PATH = "/__jazz/loader.js";
export const OVERLAY_EMBEDDED_PREFIX = "/__jazz/embedded";

const MIME: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".wasm": "application/wasm",
  ".json": "application/json; charset=utf-8",
  ".map": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
};
const ext = (p: string) => {
  const i = p.lastIndexOf(".");
  return i === -1 ? "" : p.slice(i);
};

function resolveEmbeddedDir(appRoot: string): string | null {
  try {
    const requireFromApp = createRequire(join(appRoot, "noop.js"));
    // Resolve an existing emitted file (the build outputs embedded.html, not
    // index.html) — require.resolve throws if the target file doesn't exist.
    return dirname(requireFromApp.resolve(`${INSPECTOR_PACKAGE}/dist-embedded/embedded.html`));
  } catch {
    return null;
  }
}

let loaderScriptPromise: Promise<string> | null = null;
function getLoaderScript(): Promise<string> {
  if (!loaderScriptPromise) {
    const here = dirname(fileURLToPath(import.meta.url));
    // Prefer source for vitest, built sibling for dist.
    const tsEntry = join(here, "loader.ts");
    const entry = existsSync(tsEntry) ? tsEntry : join(here, "loader.js");
    loaderScriptPromise = build({
      entryPoints: [entry],
      bundle: true,
      format: "iife",
      platform: "browser",
      write: false,
      legalComments: "none",
    }).then((r) => r.outputFiles[0].text);
  }
  return loaderScriptPromise;
}

export interface OverlayResponse {
  setHeader(name: string, value: string): void;
  statusCode: number;
  end(body?: string | Buffer): void;
}
export interface OverlayHandlerOptions {
  appRoot: string;
}

export function createOverlayHandler({ appRoot }: OverlayHandlerOptions) {
  let warnedMissing = false;
  return async function handle(req: { url?: string }, res: OverlayResponse): Promise<boolean> {
    const url = (req.url ?? "").split("?")[0];
    if (url === OVERLAY_LOADER_PATH) {
      res.setHeader("Content-Type", MIME[".js"]);
      res.end(await getLoaderScript());
      return true;
    }
    if (url === OVERLAY_EMBEDDED_PREFIX || url.startsWith(OVERLAY_EMBEDDED_PREFIX + "/")) {
      const dir = resolveEmbeddedDir(appRoot);
      if (!dir) {
        if (!warnedMissing) {
          warnedMissing = true;
          console.log(
            `[jazz] Inspector overlay: install \`${INSPECTOR_PACKAGE}\` as a devDependency to enable it.`,
          );
        }
        res.statusCode = 404;
        res.end("Inspector not installed");
        return true;
      }
      // The embedded build emits embedded.html (Vite names HTML output after the
      // source file), so that is the directory-root default — not index.html.
      const rel = url.slice(OVERLAY_EMBEDDED_PREFIX.length).replace(/^\//, "") || "embedded.html";
      const filePath = join(dir, rel);
      const within = relative(dir, filePath);
      if (within.startsWith("..") || isAbsolute(within)) {
        res.statusCode = 403;
        res.end("Forbidden");
        return true;
      }
      try {
        const body = await readFile(filePath);
        res.setHeader("Content-Type", MIME[ext(filePath)] ?? "application/octet-stream");
        res.end(body);
      } catch {
        res.statusCode = 404;
        res.end("Not found");
      }
      return true;
    }
    return false;
  };
}
```

- [ ] **Step 4: Confirm esbuild.** Run: `pnpm --filter jazz-tools exec node -e "require('esbuild');console.log('present')"` — Expected: `present`.
- [ ] **Step 5: Run the serve test.** Run: `pnpm --filter jazz-tools test -- inspector-overlay/serve` — Expected: PASS (all three).
- [ ] **Step 6: Build.** Run: `pnpm --filter jazz-tools build` — Expected: clean.
- [ ] **Step 7: Commit.**

```bash
git add packages/jazz-tools/src/dev/inspector-overlay/serve.ts packages/jazz-tools/src/dev/inspector-overlay/serve.test.ts
git commit -m "feat(dev): overlay serve middleware (app-root resolve, graceful degrade)"
```

### Task 6.2: Wire serve + inject into the Vite plugin **[GLM — simple/medium]**

**Files:** Modify `packages/jazz-tools/src/dev/vite.ts`, `packages/jazz-tools/src/dev/inspector-link.ts`.

- [ ] **Step 1: Register the middleware** in `configureServer` (dev/serve only, after managed-runtime init):

```ts
const overlay = createOverlayHandler({ appRoot: viteServer.config.root });
viteServer.middlewares.use((req: { url?: string }, res: OverlayResponse, next: () => void) => {
  void overlay(req, res).then((handled) => {
    if (!handled) next();
  });
});
```

Import `createOverlayHandler` + `OverlayResponse` from `./inspector-overlay/serve.js`; extend the local `ViteDevServer` interface (vite.ts:46–62) with `middlewares: { use(fn: (req: { url?: string }, res: OverlayResponse, next: () => void) => void): void };`.

- [ ] **Step 2: Inject the loader via a serve-gated `transformIndexHtml`** (gate on `ctx.server` so `vite build` never injects it):

```ts
transformIndexHtml(html: string, ctx: { server?: unknown }) {
  if (!ctx.server) return html;
  return { html, tags: [{ tag: "script", attrs: { type: "module", src: "/__jazz/loader.js" }, injectTo: "body" as const }] };
},
```

- [ ] **Step 3: Replace the primary inspector-link log** (`vite.ts:142–148`); keep `buildInspectorLink` exported for the admin path:

```ts
console.log(
  `${LOG_PREFIX} Inspector overlay enabled — click the ⚡ button in your app (Alt+Shift+J).`,
);
```

- [ ] **Step 4: Build + serve test (regression).** Run: `pnpm --filter jazz-tools build && pnpm --filter jazz-tools test -- inspector-overlay/serve` — Expected: clean + PASS. (Full asset-serving + injection covered by the inspector e2e in 6.3 and the manual check in 9.2.)
- [ ] **Step 5: Commit.**

```bash
git add packages/jazz-tools/src/dev/vite.ts packages/jazz-tools/src/dev/inspector-link.ts
git commit -m "feat(dev): serve + inject inspector overlay in the Vite plugin (serve-only)"
```

### Task 6.3: End-to-end browser test (inspector package) **[OPUS — hard]**

**Files:** Create `packages/inspector/src/embedded.browser.test.ts`.

The inspector depends on `jazz-tools` (existing edge), so it hosts the full e2e: a host page calls `attachDevTools` on a real db (schema via the public API per `TESTING_GUIDELINES.md`), wires the relay (reuse the built loader or inline a minimal relay), embeds `dist-embedded/index.html` in an iframe, and asserts the inspector leaves "Waiting for runtime devtools connection..." and renders.

- [ ] **Step 1: Inspect the existing harness.** Run: `pnpm --filter jazz-inspector exec cat playwright.config.ts` and list current `*.browser.test.ts`.
- [ ] **Step 2: Write the Playwright test** mirroring that harness; schema via public API.
- [ ] **Step 3: Run it.** Run: `pnpm --filter jazz-inspector run build:embedded && pnpm --filter jazz-inspector test:browser -- embedded` — Expected: PASS.
- [ ] **Step 4: Commit.**

```bash
git add packages/inspector/src/embedded.browser.test.ts
git commit -m "test(inspector): e2e overlay relay + embedded entry"
```

---

## Phase 7 — SvelteKit plugin: serve + inject

### Task 7.1: Wire serve + inject into the SvelteKit plugin **[GLM — simple/medium]**

**Files:** Modify `packages/jazz-tools/src/dev/sveltekit.ts`. Reuse Task 6.2 exactly.

- [ ] **Step 1: Confirm hooks.** Run: `pnpm --filter jazz-tools exec grep -n "configureServer\|transformIndexHtml\|middlewares\|enforce" src/dev/sveltekit.ts`.
- [ ] **Step 2: Add the middleware** (`createOverlayHandler({ appRoot: viteServer.config.root })`), dev-only.
- [ ] **Step 3: Add the serve-gated `transformIndexHtml`** (identical to 6.2 Step 2).
- [ ] **Step 4: Build.** Run: `pnpm --filter jazz-tools build` — Expected: clean.
- [ ] **Step 5: Commit.**

```bash
git add packages/jazz-tools/src/dev/sveltekit.ts
git commit -m "feat(dev): serve + inject inspector overlay in the SvelteKit plugin"
```

---

## Phase 8 — Next.js plugin: serve + inject (the long pole)

Next has no `transformIndexHtml` and no dev-server middleware hook. **Serving:** copy resolved embedded build + bundled loader into the user's `public/__jazz/` in the development phase only, guarded against prod. **Injection:** export a dev-only `<JazzInspectorScript />` for the root layout (a documented one-line deviation from "zero app code," Next only).

### Task 8.1: Next serving via dev-only public-dir copy **[OPUS — hard]**

**Files:** Modify `packages/jazz-tools/src/dev/next.ts`.

- [ ] **Step 1: Add `copyOverlayToPublic(appRoot)`** mirroring `copyWasmToPublic` (`next.ts:65–72`): resolve the embedded dir from the app root (reuse `INSPECTOR_PACKAGE` / `resolveEmbeddedDir` logic) + bundled loader, write to `<appRoot>/public/__jazz/embedded/*` and `/__jazz/loader.js`. If the inspector is absent, log the hint and skip. Call **only** in `phase-development-server`.
- [ ] **Step 2: Guard prod.** In `phase-production-build`, never copy; warn (once) if `public/__jazz` exists.
- [ ] **Step 3: Build.** Run: `pnpm --filter jazz-tools build` — Expected: clean.
- [ ] **Step 4: Commit.**

```bash
git add packages/jazz-tools/src/dev/next.ts
git commit -m "feat(dev): serve inspector overlay assets in the Next plugin (dev only)"
```

### Task 8.2: Next injection via dev-only `<JazzInspectorScript />` **[OPUS — hard]**

**Files:** Modify `packages/jazz-tools/src/dev/next.ts` (+ `jazz-tools/next` export).

- [ ] **Step 1: Export a component** rendering `<script src="/__jazz/loader.js">` only when `process.env.NODE_ENV !== "production"`, for the user's root layout. Document it as the Next-only step.
- [ ] **Step 2: Build.** Run: `pnpm --filter jazz-tools build` — Expected: clean.
- [ ] **Step 3: Commit.**

```bash
git add packages/jazz-tools/src/dev/next.ts
git commit -m "feat(dev): export JazzInspectorScript for Next overlay injection (dev only)"
```

---

## Phase 9 — Example migration + sweep

### Task 9.1: Migrate the todo example **[GLM — simple/medium]**

**Files:** Modify `examples/todo-client-localfirst-react/{src/App.tsx,package.json}`.

- [ ] **Step 1: Add the inspector devDep.** Run: `pnpm --filter todo-client-localfirst-react add -D jazz-inspector@workspace:*`.
- [ ] **Step 2: Remove the manual `DevToolsRegistration`** (App.tsx:30–50) + usage; drop unused imports. Pass the schema:

```tsx
<JazzProvider
  config={resolvedConfig}
  wasmSchema={app.wasmSchema}
  fallback={fallback ?? <p>Loading...</p>}
>
  <h1>Todos</h1>
  <TodoList />
</JazzProvider>
```

- [ ] **Step 3: Build the example.** Run: `pnpm --filter todo-client-localfirst-react build` — Expected: clean.
- [ ] **Step 4: Commit.**

```bash
git add examples/todo-client-localfirst-react/src/App.tsx examples/todo-client-localfirst-react/package.json pnpm-lock.yaml
git commit -m "chore(example): use JazzProvider wasmSchema + inspector overlay"
```

### Task 9.2: Full build + test + manual verify **[GLM — simple/medium]**

- [ ] **Step 1: Build everything.** Run: `pnpm build:core` — Expected: all build (no cycle, since jazz-tools has no inspector dep).
- [ ] **Step 2: Full test suite.** Run: `pnpm test` — Expected: green.
- [ ] **Step 3: Manual overlay verify + prod-absence check.** Start the example dev server, confirm the ⚡ toggle opens the panel and shows live data. Then:

Run: `pnpm --filter todo-client-localfirst-react build && grep -rn "__jazz/loader.js\|attachDevTools" examples/todo-client-localfirst-react/dist || echo "clean prod build"`
Expected: `clean prod build`.

### Task 9.3: Sync the spec **[GLM — simple/medium]**

**Files:** Modify `docs/superpowers/specs/2026-06-25-inspector-dev-plugin-overlay-design.md`.

- [ ] **Step 1: Update the spec** to record: packaging = consumer installs `jazz-inspector` (dev plugin resolves from app root, degrades gracefully) — replacing §F's "resolve from jazz-tools' node_modules" and the false "already a build-time dependency" claim; dev-gate = `process.env.NODE_ENV` only; embedded build = dedicated `dist-embedded/`; toggle = Alt+Shift+J; Next injection = `JazzInspectorScript`; multi-peer namespacing deferred.
- [ ] **Step 2: Commit.**

```bash
git add docs/superpowers/specs/2026-06-25-inspector-dev-plugin-overlay-design.md
git commit -m "docs: sync inspector overlay spec with resolved implementation decisions"
```

---

## Self-review notes (author, v2)

- **Spec coverage:** A=3.1/3.2; B=2.1/2.2; C=4.1/4.2; D=5.1/5.2; E=6.2/7.1/8.1/8.2; F=Phase 0 + 6.1. Testing → 2.1, 4.1 (round-trip), 5.1, 6.1, 6.3. Invariant comment → 4.0. Multi-peer → deferred (4.1 comment + 9.3).
- **Resolved review blockers:** packaging cycle → Phase 0 + app-root resolution + graceful degrade (no jazz-tools→inspector dep); DCE/gate → `NODE_ENV`-only + dynamic import (5.1); `transformIndexHtml` prod injection → `ctx.server` gate (6.2); relay loop test → round-trip case (4.1); provider full-rewrite → targeted edit (5.1); path containment → `relative()` (6.1); loader resolution → relative to `import.meta.url` (6.1).
- **Acknowledged deviations:** overlay is "one devDependency," not zero-install; Next needs one line of app code (`<JazzInspectorScript />`). Both documented (9.3 / 8.2).
- **Type consistency:** `DevtoolsBridgePort`/`DevtoolsBridgeConnector`/`setDevtoolsBridgeConnector`/`resetDevtoolsBridgeConnector` (1.1, 2.x); `createOverlayHandler`/`OverlayResponse`/`INSPECTOR_PACKAGE` (6.1, 6.2, 7.1); `createRelay`/`RelayOptions` (4.1, 4.2).
