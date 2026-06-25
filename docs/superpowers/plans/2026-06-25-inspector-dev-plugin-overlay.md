# Inspector as a Dev-Plugin Overlay — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the "copy a link to the hosted Vercel inspector" dev flow with a zero-config, locally-served inspector overlay that the dev plugins (Vite, SvelteKit, Next) auto-inject into the running dev app, wired to the app's live in-process client over the existing `jazz-devtools-v1` bridge.

**Architecture:** The inspector runs in an iframe inside the dev page. The dev plugin serves the inspector's embedded build (resolved from `node_modules`) at `/__jazz/embedded` and injects an external `/__jazz/loader.js` that renders a toggle + iframe and **relays** bridge `postMessage` traffic between the top window (where `attachDevTools` lives) and the iframe. The inspector connects over a new parent-window transport that reuses the existing bridge request/subscription plumbing. `JazzProvider` auto-attaches the devtools bridge in dev. `attachDevTools` itself is left untouched.

**Tech Stack:** TypeScript, React 19 (inspector), Vite/Rollup, Vitest (unit/integration), Playwright (browser), the existing `jazz-devtools-v1` postMessage bridge.

**Spec:** `docs/superpowers/specs/2026-06-25-inspector-dev-plugin-overlay-design.md`

---

## Execution model — model selection per task

Per the user's instruction, when running subagent-driven:

- **GLM** (`glm` alias) handles tasks tagged **[GLM — simple/medium]**.
- **Opus** handles tasks tagged **[OPUS — hard]** (transport seam, relay/loader, provider auto-attach, Next.js).

Each task lists its tag in its heading.

---

## Phase ordering and rationale

1. **Phase 1 — Transport seam** (inspector-side): make the bridge client transport-pluggable. Nothing else can connect the iframe without this.
2. **Phase 2 — Embedded client**: the parent-window connector + `createEmbeddedJazzClient()`.
3. **Phase 3 — Embedded inspector entry + build**: `embedded.html`/`embedded.tsx` and the build output the plugin serves.
4. **Phase 4 — Loader + relay**: the served asset and its critical loop/amplification tests.
5. **Phase 5 — Provider auto-attach**: React first (hard), then Svelte/Vue/Solid.
6. **Phase 6 — Vite plugin serve + inject** — first end-to-end working framework.
7. **Phase 7 — SvelteKit plugin serve + inject.**
8. **Phase 8 — Next.js plugin serve + inject** — the long pole, sequenced last; must not block 6/7.
9. **Phase 9 — Example migration + cleanup.**

Phases 1–6 deliver a fully working overlay on Vite. Each phase ends green and committed.

---

## File structure

**Inspector (`packages/inspector`)**

- Create: `embedded.html` — third Vite entry, mirrors `devtools.html`.
- Create: `src/embedded.tsx` — embedded entry; near-copy of `src/devtools-main.tsx`.
- Modify: `vite.config.ts` — add an `embedded` build mode → `dist-embedded/`.
- Modify: `package.json` — add `build:embedded`; include in `build`.
- Create: `src/embedded.browser.test.ts` (Playwright) — relay handshake + render.

**Bridge / client (`packages/jazz-tools/src`)**

- Modify: `dev-tools/extension-panel.ts` — extract `DevtoolsBridgePort` + pluggable connector; default = chrome.
- Create: `dev-tools/parent-window-port.ts` — the iframe→parent postMessage connector.
- Modify: `dev-tools/index.ts` — export the connector setter + parent-window connector.
- Create: `react/create-embedded-jazz-client.ts` — `createEmbeddedJazzClient()`.
- Modify: `react/index.ts` — export `createEmbeddedJazzClient`.
- Create: `dev-tools/parent-window-port.test.ts` (Vitest, jsdom) — transport unit test.

**Loader / relay (served by plugins, source in `packages/jazz-tools/src/dev`)**

- Create: `dev/inspector-overlay/loader.ts` — source compiled to the served `loader.js`.
- Create: `dev/inspector-overlay/relay.ts` — pure relay routing (unit-testable).
- Create: `dev/inspector-overlay/relay.test.ts` (Vitest, jsdom) — **loop/amplification invariant**.
- Create: `dev/inspector-overlay/serve.ts` — shared middleware: resolve embedded build + serve `/__jazz/*`.

**Provider auto-attach (`packages/jazz-tools/src`)**

- Modify: `react/provider.tsx` — `autoAttachDevTools` + `wasmSchema` props + dev-gated auto-attach.
- Modify: `svelte/index.ts`, `vue/index.ts`, `solid/index.ts` provider equivalents.
- Create: `react/provider.devtools.test.tsx` (Vitest) — opt-out, no-double-attach, prod-absent.

**Dev plugins (`packages/jazz-tools/src/dev`)**

- Modify: `vite.ts` — serve `/__jazz/*` + inject loader via `transformIndexHtml`.
- Modify: `sveltekit.ts` — same via its Vite-style hooks.
- Modify: `next.ts` — route handler/public copy + `<Script>` injection.
- Modify: `inspector-link.ts` — keep `buildInspectorLink` for the admin/standalone path; stop logging it as the primary entry.
- Create: `dev/inspector-overlay/serve.integration.test.ts` (Vitest) — `/__jazz/embedded` + `/__jazz/loader.js` served; loader injected into HTML.

**Example**

- Modify: `examples/todo-client-localfirst-react/src/App.tsx` — drop the manual `DevToolsRegistration`; pass `wasmSchema` to `JazzProvider`.

---

## Conventions for every task

- **Read `crates/jazz-tools/TESTING_GUIDELINES.md` before writing any Rust test.** (No Rust here, but if a task drifts into a crate, follow it.) For TS, prefer black-box integration tests through the public API; no JSON-shaped schema/permission literals — build via the public API.
- Build core: `pnpm build:core`. Test all: `pnpm test`. Scope a package test with `pnpm --filter <pkg> test`.
- Commit after each task with a conventional-commit message. No Claude/AI attribution in messages.
- Run the exact verification command shown and confirm the stated expected output before moving on.

---

## Phase 1 — Transport seam (inspector-side bridge client)

Goal: make the bridge client connect over a pluggable transport, defaulting to the existing Chrome behavior with **zero behavior change** for the extension.

### Task 1.1: Define the bridge-port interface and pluggable connector **[OPUS — hard]**

**Files:**

- Modify: `packages/jazz-tools/src/dev-tools/extension-panel.ts`

Current `ensureDevtoolsPort()` (lines 217–344) is chrome-specific: it calls `connectValidatedPort` / `installBridgeInInspectedTab`, then attaches `onMessage`/`onDisconnect` listeners to the returned chrome `Port`. A chrome `Port` already exposes `postMessage`, `onMessage.addListener/removeListener`, `onDisconnect.addListener/removeListener` — so we can describe it as an interface and inject the connector.

- [ ] **Step 1: Add the interface + connector registry near the top of the module (after the imports/types block, around line 60).**

```ts
export interface DevtoolsBridgePort {
  postMessage(message: unknown): void;
  onMessage: {
    addListener(cb: (message: unknown) => void): void;
    removeListener(cb: (message: unknown) => void): void;
  };
  onDisconnect: {
    addListener(cb: () => void): void;
    removeListener(cb: () => void): void;
  };
}

export type DevtoolsBridgeConnector = () => Promise<DevtoolsBridgePort>;

// Default connector = the Chrome DevTools port. Overridable so the in-page
// iframe overlay can supply a postMessage-backed port (see parent-window-port.ts).
let bridgeConnector: DevtoolsBridgeConnector = connectChromeDevtoolsPort;

export function setDevtoolsBridgeConnector(connector: DevtoolsBridgeConnector): void {
  bridgeConnector = connector;
}
```

- [ ] **Step 2: Extract the chrome-specific acquisition into `connectChromeDevtoolsPort()`.** Move the body of `ensureDevtoolsPort` that obtains the port (lines 222–244: the `chromeApi` checks, `connectValidatedPort`, the `installBridgeInInspectedTab` retry) into a new function that **returns** the chrome `Port` and does not attach the message/disconnect listeners:

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
    if (!isMissingReceivingEndError(error)) {
      throw error;
    }
    await installBridgeInInspectedTab(chromeApi, tabId);
    return await connectValidatedPort(chromeApi, tabId);
  }
}
```

- [ ] **Step 3: Rewrite `ensureDevtoolsPort` to use the connector.** Replace lines 217–244 (the chrome acquisition) with `const port = await bridgeConnector();` and assign `devtoolsPort = port;`. Keep the existing `onMessage`/`onDisconnect` handler bodies (lines 246–341) exactly as-is — they only use the `DevtoolsBridgePort` surface. The `onDisconnect` body that nulls `devtoolsPort` etc. stays unchanged.

```ts
async function ensureDevtoolsPort(): Promise<DevtoolsBridgePort> {
  if (devtoolsPort) {
    return devtoolsPort;
  }
  devtoolsPort = await bridgeConnector();
  // ... existing onMessage / onDisconnect definitions unchanged ...
  devtoolsPort.onMessage.addListener(onMessage);
  devtoolsPort.onDisconnect.addListener(onDisconnect);
  notifyDevtoolsPortConnected();
  return devtoolsPort;
}
```

- [ ] **Step 4: Update the `devtoolsPort` declaration type.** Change line 54 `let devtoolsPort: any | null = null;` to `let devtoolsPort: DevtoolsBridgePort | null = null;`. Fix any resulting `any` accesses (there should be none — all uses go through the interface surface).

- [ ] **Step 5: Build to verify no type/behavior regression.**

Run: `pnpm --filter jazz-tools build`
Expected: builds clean (no TS errors). The extension code path is unchanged (default connector is chrome).

- [ ] **Step 6: Run the existing dev-tools tests to confirm no regression.**

Run: `pnpm --filter jazz-tools test -- dev-tools`
Expected: PASS (existing `dev-tools.test.ts` still green).

- [ ] **Step 7: Commit.**

```bash
git add packages/jazz-tools/src/dev-tools/extension-panel.ts
git commit -m "refactor(dev-tools): make bridge client transport pluggable"
```

---

## Phase 2 — Embedded client (parent-window transport)

### Task 2.1: Parent-window bridge port (unit-tested transport) **[GLM — simple/medium]**

**Files:**

- Create: `packages/jazz-tools/src/dev-tools/parent-window-port.ts`
- Create: `packages/jazz-tools/src/dev-tools/parent-window-port.test.ts`

The iframe inspector reaches the top window only via `window.parent`. Inbound relayed messages arrive with `event.source === window.parent`. Outbound goes to `window.parent.postMessage(msg, origin)`.

- [ ] **Step 1: Write the failing test.**

```ts
// parent-window-port.test.ts
import { describe, it, expect, vi, beforeEach } from "vitest";
import { DEVTOOLS_BRIDGE_CHANNEL } from "./protocol.js";
import { createParentWindowBridgePort } from "./parent-window-port.js";

describe("createParentWindowBridgePort", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

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
      addEventListener: (type: string, cb: (e: MessageEvent) => void) => {
        if (type === "message") handler = cb;
      },
    } as unknown as Window);

    const port = await createParentWindowBridgePort();
    const received: unknown[] = [];
    port.onMessage.addListener((m) => received.push(m));

    // from parent, correct channel -> delivered
    handler({
      source: parent,
      data: { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "response" },
    } as unknown as MessageEvent);
    // from a different source -> ignored
    handler({
      source: {},
      data: { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "response" },
    } as unknown as MessageEvent);
    // wrong channel -> ignored
    handler({
      source: parent,
      data: { channel: "other", kind: "response" },
    } as unknown as MessageEvent);

    expect(received).toEqual([{ channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "response" }]);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails.**

Run: `pnpm --filter jazz-tools test -- parent-window-port`
Expected: FAIL ("createParentWindowBridgePort is not a function" / module not found).

- [ ] **Step 3: Implement `parent-window-port.ts`.**

```ts
import type { DevtoolsBridgePort } from "./extension-panel.js";
import { DEVTOOLS_BRIDGE_CHANNEL, isRecord } from "./protocol.js";

// Bridge port backed by window.parent postMessage, for the inspector running
// in the overlay iframe. The plugin-injected loader relays these messages
// to/from the top window where attachDevTools listens (see dev/inspector-overlay).
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

- [ ] **Step 4: Run the test to verify it passes.**

Run: `pnpm --filter jazz-tools test -- parent-window-port`
Expected: PASS (both tests).

- [ ] **Step 5: Commit.**

```bash
git add packages/jazz-tools/src/dev-tools/parent-window-port.ts packages/jazz-tools/src/dev-tools/parent-window-port.test.ts
git commit -m "feat(dev-tools): add parent-window bridge port transport"
```

### Task 2.2: `createEmbeddedJazzClient()` + exports **[GLM — simple/medium]**

**Files:**

- Create: `packages/jazz-tools/src/react/create-embedded-jazz-client.ts`
- Modify: `packages/jazz-tools/src/react/index.ts`
- Modify: `packages/jazz-tools/src/dev-tools/index.ts`

`createExtensionJazzClient` (`create-jazz-client.ts:73`) delegates to `createExtensionJazzClientInternal` which calls `createDbFromInspectedPage()`. The only difference for embedded is **which connector is installed first**.

- [ ] **Step 1: Export the connector setter + parent-window connector from `dev-tools/index.ts`.** Add:

```ts
export {
  setDevtoolsBridgeConnector,
  type DevtoolsBridgePort,
  type DevtoolsBridgeConnector,
} from "./extension-panel.js";
export { createParentWindowBridgePort } from "./parent-window-port.js";
```

- [ ] **Step 2: Implement `create-embedded-jazz-client.ts`.**

```ts
import { setDevtoolsBridgeConnector, createParentWindowBridgePort } from "../dev-tools/index.js";
import { createExtensionJazzClient, type JazzClient } from "./create-jazz-client.js";

// Same bridge protocol as the extension client, but the transport talks to the
// top window via postMessage (the overlay iframe case) instead of a chrome port.
export function createEmbeddedJazzClient(): Promise<JazzClient> {
  setDevtoolsBridgeConnector(createParentWindowBridgePort);
  return createExtensionJazzClient();
}
```

- [ ] **Step 3: Export it from `react/index.ts`.** Add to the existing dev-tools export line region:

```ts
export { createEmbeddedJazzClient } from "./create-embedded-jazz-client.js";
```

- [ ] **Step 4: Build to verify exports resolve.**

Run: `pnpm --filter jazz-tools build`
Expected: builds clean; `createEmbeddedJazzClient` present in `dist/react`.

- [ ] **Step 5: Commit.**

```bash
git add packages/jazz-tools/src/react/create-embedded-jazz-client.ts packages/jazz-tools/src/react/index.ts packages/jazz-tools/src/dev-tools/index.ts
git commit -m "feat(react): add createEmbeddedJazzClient for overlay iframe"
```

---

## Phase 3 — Embedded inspector entry + build

### Task 3.1: Embedded entry component **[GLM — simple/medium]**

**Files:**

- Create: `packages/inspector/src/embedded.tsx`
- Create: `packages/inspector/embedded.html`

`src/devtools-main.tsx` is the model. The only change: use `createEmbeddedJazzClient()` instead of `createExtensionJazzClient()`. Reuse `DevtoolsProvider runtime="extension"` (gives `local-only` propagation with toggle — the live-client view we want; **do not** add a new runtime value).

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

  useEffect(() => {
    return onDevToolsPortDisconnect(() => {
      window.location.reload();
    });
  }, []);

  if (!embeddedClient || !wasmSchema) {
    return <p>Waiting for runtime devtools connection...</p>;
  }

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

- [ ] **Step 2: Create `embedded.html`** (mirror `index.html`, point at the embedded entry):

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Jazz Inspector (embedded)</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/embedded.tsx"></script>
  </body>
</html>
```

- [ ] **Step 3: Typecheck the inspector.**

Run: `pnpm --filter inspector exec tsc -b`
Expected: no errors.

- [ ] **Step 4: Commit.**

```bash
git add packages/inspector/src/embedded.tsx packages/inspector/embedded.html
git commit -m "feat(inspector): add embedded entry for overlay iframe"
```

### Task 3.2: Embedded build mode → `dist-embedded/` **[GLM — simple/medium]**

**Files:**

- Modify: `packages/inspector/vite.config.ts`
- Modify: `packages/inspector/package.json`

- [ ] **Step 1: Add an `embedded` mode to `vite.config.ts`.** Replace the body to branch on three modes:

```ts
import { resolve } from "node:path";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";

export default defineConfig(({ mode }) => {
  const isExtensionBuild = mode === "extension";
  const isEmbeddedBuild = mode === "embedded";

  if (isEmbeddedBuild) {
    return {
      plugins: [react()],
      base: "./",
      worker: { format: "es" },
      build: {
        outDir: "dist-embedded",
        emptyOutDir: true,
        rollupOptions: {
          input: { index: resolve(__dirname, "embedded.html") },
        },
      },
    };
  }

  return {
    plugins: [react()],
    base: isExtensionBuild ? "./" : "/",
    publicDir: isExtensionBuild ? "chrome-extension" : "public",
    worker: { format: "es" },
    build: isExtensionBuild
      ? {
          outDir: "dist-extension",
          emptyOutDir: true,
          rollupOptions: {
            input: {
              index: resolve(__dirname, "devtools-tab.html"),
              devtools: resolve(__dirname, "devtools.html"),
            },
          },
        }
      : { outDir: "dist", emptyOutDir: true },
  };
});
```

Note: `base: "./"` (relative asset URLs) is required so the build works when served from `/__jazz/embedded/` rather than the domain root.

- [ ] **Step 2: Add the build script in `package.json`.** Add `"build:embedded": "vite build --mode embedded"` and append it to `build`:

```json
"build": "tsc -b && pnpm run build:web && pnpm run build:extension && pnpm run build:embedded",
"build:embedded": "vite build --mode embedded",
```

- [ ] **Step 3: Build the embedded bundle and confirm output exists.**

Run: `pnpm --filter inspector run build:embedded && ls packages/inspector/dist-embedded/index.html`
Expected: `dist-embedded/index.html` exists; `dist-embedded/assets/` populated.

- [ ] **Step 4: Add `dist-embedded/` to the inspector `.gitignore` if `dist/` is ignored there.** Check `packages/inspector/.gitignore`; if `dist` / `dist-extension` are listed, add `dist-embedded`.

Run: `grep -E "dist" packages/inspector/.gitignore`
Expected: `dist-embedded` present after edit (matching the others).

- [ ] **Step 5: Commit.**

```bash
git add packages/inspector/vite.config.ts packages/inspector/package.json packages/inspector/.gitignore
git commit -m "build(inspector): add embedded build mode"
```

---

## Phase 4 — Loader + relay (served asset)

This is the highest-risk code. The relay must enforce the `event.source` invariant or it loops. We split it into a **pure routing module** (`relay.ts`, unit-tested with the critical invariant) and a thin DOM `loader.ts` that wires it up.

### Task 4.1: Pure relay routing + loop/amplification test **[OPUS — hard]**

**Files:**

- Create: `packages/jazz-tools/src/dev/inspector-overlay/relay.ts`
- Create: `packages/jazz-tools/src/dev/inspector-overlay/relay.test.ts`

Routing rules (mirroring `extension-panel.ts:138–154`, adapted for an iframe instead of a chrome port):

- **iframe → top:** a message whose `event.source === iframe.contentWindow`, on the bridge channel, `kind === "request"` → re-inject into the top window via `window.postMessage(msg, "*")` so `attachDevTools` (which requires `event.source === window`) receives it.
- **top → iframe:** a message whose `event.source === window` (a reply/event the runtime posted), on the bridge channel, `kind === "response" | "event"` → forward to `iframe.contentWindow.postMessage(msg, origin)`.
- Anything else is ignored. Critically, a `request` posted by a **different** frame must NOT be re-injected (prevents amplification/loops).

- [ ] **Step 1: Write the failing test (the invariant is the point of this task).**

```ts
import { describe, it, expect, vi } from "vitest";
import { DEVTOOLS_BRIDGE_CHANNEL } from "../../dev-tools/protocol.js";
import { createRelay } from "./relay.js";

function makeWindow() {
  return { postMessage: vi.fn() };
}

describe("inspector overlay relay", () => {
  it("re-injects exactly one request from the iframe into the top window (no echo/loop)", () => {
    const topWindow = makeWindow();
    const iframeWindow = makeWindow();
    const relay = createRelay({
      topWindow: topWindow as unknown as Window,
      iframeWindow: iframeWindow as unknown as Window,
      origin: "http://localhost:5173",
    });

    relay.handle({
      source: iframeWindow,
      data: { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "request", requestId: "r1" },
    } as unknown as MessageEvent);

    // exactly one re-injection into the top window, none back to the iframe
    expect(topWindow.postMessage).toHaveBeenCalledTimes(1);
    expect(topWindow.postMessage).toHaveBeenCalledWith(
      { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "request", requestId: "r1" },
      "*",
    );
    expect(iframeWindow.postMessage).not.toHaveBeenCalled();
  });

  it("does NOT re-inject a bridge request posted by a different frame", () => {
    const topWindow = makeWindow();
    const iframeWindow = makeWindow();
    const otherFrame = makeWindow();
    const relay = createRelay({
      topWindow: topWindow as unknown as Window,
      iframeWindow: iframeWindow as unknown as Window,
      origin: "http://localhost:5173",
    });

    relay.handle({
      source: otherFrame,
      data: { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "request", requestId: "x" },
    } as unknown as MessageEvent);

    expect(topWindow.postMessage).not.toHaveBeenCalled();
    expect(iframeWindow.postMessage).not.toHaveBeenCalled();
  });

  it("forwards a runtime reply (event.source === topWindow) into the iframe", () => {
    const topWindow = makeWindow();
    const iframeWindow = makeWindow();
    const relay = createRelay({
      topWindow: topWindow as unknown as Window,
      iframeWindow: iframeWindow as unknown as Window,
      origin: "http://localhost:5173",
    });

    relay.handle({
      source: topWindow,
      data: { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "response", requestId: "r1", ok: true },
    } as unknown as MessageEvent);

    expect(iframeWindow.postMessage).toHaveBeenCalledTimes(1);
    expect(iframeWindow.postMessage).toHaveBeenCalledWith(
      { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "response", requestId: "r1", ok: true },
      "http://localhost:5173",
    );
    // must NOT re-inject the reply back into the top window
    expect(topWindow.postMessage).not.toHaveBeenCalled();
  });

  it("ignores non-bridge-channel messages", () => {
    const topWindow = makeWindow();
    const iframeWindow = makeWindow();
    const relay = createRelay({
      topWindow: topWindow as unknown as Window,
      iframeWindow: iframeWindow as unknown as Window,
      origin: "http://localhost:5173",
    });
    relay.handle({
      source: iframeWindow,
      data: { channel: "other", kind: "request" },
    } as unknown as MessageEvent);
    expect(topWindow.postMessage).not.toHaveBeenCalled();
    expect(iframeWindow.postMessage).not.toHaveBeenCalled();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails.**

Run: `pnpm --filter jazz-tools test -- inspector-overlay/relay`
Expected: FAIL (module not found).

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

export function createRelay(options: RelayOptions) {
  const { topWindow, iframeWindow, origin } = options;

  function handle(event: MessageEvent): void {
    const data = event.data;
    if (!isBridgeMessage(data)) return;

    // iframe -> top: only requests, only from OUR iframe. Re-inject so
    // attachDevTools (event.source === window guard) accepts it.
    if (event.source === iframeWindow) {
      if (data.kind !== "request") return;
      topWindow.postMessage(data, "*");
      return;
    }

    // top -> iframe: runtime replies/events the page posted to itself.
    if (event.source === topWindow) {
      if (data.kind !== "response" && data.kind !== "event") return;
      iframeWindow.postMessage(data, origin);
      return;
    }

    // Any other source (OAuth popups, third-party frames) is ignored.
  }

  return { handle };
}
```

- [ ] **Step 4: Run the test to verify it passes.**

Run: `pnpm --filter jazz-tools test -- inspector-overlay/relay`
Expected: PASS (all four tests).

- [ ] **Step 5: Commit.**

```bash
git add packages/jazz-tools/src/dev/inspector-overlay/relay.ts packages/jazz-tools/src/dev/inspector-overlay/relay.test.ts
git commit -m "feat(dev): add inspector overlay relay with loop-safety invariant"
```

### Task 4.2: DOM loader (toggle + iframe + relay wiring) **[OPUS — hard]**

**Files:**

- Create: `packages/jazz-tools/src/dev/inspector-overlay/loader.ts`

This module is bundled to the served `/__jazz/loader.js`. It runs in the dev page. It must be self-contained (no imports the page can't resolve) — so it inlines the bridge channel constant and the relay logic at build time. We import `createRelay` and the channel; the plugin bundles this file with esbuild before serving (Task 6.1 builds it).

- [ ] **Step 1: Implement `loader.ts`.**

```ts
import { createRelay } from "./relay.js";

const PANEL_STATE_KEY = "jazz-inspector-overlay:open";
const TOGGLE_SHORTCUT = { key: "j", altKey: true, shiftKey: true }; // Alt+Shift+J

function readOpen(): boolean {
  try {
    return localStorage.getItem(PANEL_STATE_KEY) === "1";
  } catch {
    return false;
  }
}
function writeOpen(open: boolean): void {
  try {
    localStorage.setItem(PANEL_STATE_KEY, open ? "1" : "0");
  } catch {
    /* ignore */
  }
}

function mount(): void {
  if ((window as unknown as Record<string, unknown>).__jazzInspectorOverlayMounted) return;
  (window as unknown as Record<string, unknown>).__jazzInspectorOverlayMounted = true;

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
  iframe.src = "/__jazz/embedded/index.html";
  iframe.style.cssText = "width:100%;height:100%;border:none;";
  panel.appendChild(iframe);

  let open = readOpen();
  const apply = () => {
    panel.style.display = open ? "block" : "none";
  };
  const setOpen = (next: boolean) => {
    open = next;
    writeOpen(open);
    apply();
  };
  toggle.addEventListener("click", () => setOpen(!open));
  window.addEventListener("keydown", (e) => {
    if (
      e.key.toLowerCase() === TOGGLE_SHORTCUT.key &&
      e.altKey === TOGGLE_SHORTCUT.altKey &&
      e.shiftKey === TOGGLE_SHORTCUT.shiftKey
    ) {
      setOpen(!open);
    }
  });
  apply();

  container.appendChild(panel);
  container.appendChild(toggle);
  document.body.appendChild(container);

  // Relay must be wired regardless of panel visibility: the iframe announces
  // and subscribes as soon as it loads.
  const relay = createRelay({
    topWindow: window,
    iframeWindow: iframe.contentWindow!,
    origin: window.location.origin,
  });
  window.addEventListener("message", (event) => relay.handle(event));
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", mount);
} else {
  mount();
}
```

- [ ] **Step 2: Typecheck (compiles as part of the package build later; quick check now).**

Run: `pnpm --filter jazz-tools exec tsc --noEmit -p tsconfig.json 2>&1 | grep inspector-overlay || echo "no errors in inspector-overlay"`
Expected: `no errors in inspector-overlay`.

- [ ] **Step 3: Commit.**

```bash
git add packages/jazz-tools/src/dev/inspector-overlay/loader.ts
git commit -m "feat(dev): add inspector overlay loader (toggle + iframe + relay)"
```

---

## Phase 5 — Provider auto-attach

### Task 5.1: React provider auto-attach + opt-out + tests **[OPUS — hard]**

**Files:**

- Modify: `packages/jazz-tools/src/react/provider.tsx`
- Create: `packages/jazz-tools/src/react/provider.devtools.test.tsx`

Requirements (spec §D): dev-gated via `import.meta.env.DEV`-style build-time elimination with a `process.env.NODE_ENV` fallback; `autoAttachDevTools` prop (default true); `wasmSchema` prop supplies the schema (the provider has no other source — verified: the example imports `app.wasmSchema`); idempotency keyed on the client/db so a manual `attachDevTools` call does not double-attach (note: `attachDevTools` already de-dupes on `db` via `registeredRuntimeBridgeDbs`, so calling it again with the same db is safe — the provider guard is a belt-and-suspenders early return + avoids re-running the effect).

- [ ] **Step 1: Write the failing test.**

```tsx
// provider.devtools.test.tsx — jsdom
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, waitFor } from "@testing-library/react";

const attachSpy = vi.fn().mockResolvedValue({
  isConnected: () => false,
  onConnectionChange: () => () => {},
  updateSchema: () => {},
});

vi.mock("../dev-tools/dev-tools.js", () => ({
  attachDevTools: attachSpy,
}));

// minimal client/db stub the provider passes through
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

  it("auto-attaches devtools in dev when wasmSchema is provided", async () => {
    render(
      <JazzProvider config={{} as never} wasmSchema={fakeSchema}>
        <div />
      </JazzProvider>,
    );
    await waitFor(() => expect(attachSpy).toHaveBeenCalledTimes(1));
  });

  it("does not auto-attach when autoAttachDevTools={false}", async () => {
    render(
      <JazzProvider config={{} as never} wasmSchema={fakeSchema} autoAttachDevTools={false}>
        <div />
      </JazzProvider>,
    );
    await new Promise((r) => setTimeout(r, 20));
    expect(attachSpy).not.toHaveBeenCalled();
  });

  it("does not attach in production", async () => {
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

- [ ] **Step 2: Run to verify it fails.**

Run: `pnpm --filter jazz-tools test -- provider.devtools`
Expected: FAIL (`wasmSchema`/`autoAttachDevTools` not accepted; no attach call).

- [ ] **Step 3: Implement the auto-attach.** Add an internal component rendered inside the provider, plus the two props. Full new `provider.tsx`:

```tsx
import type { ReactNode } from "react";
import { useEffect, useRef } from "react";
import type { Session } from "../runtime/context.js";
import type { Db, DbConfig } from "../runtime/db.js";
import type { WasmSchema } from "../index.js";
import {
  JazzProvider as CoreJazzProvider,
  useDb as useCoreDb,
  useJazzClient as useCoreJazzClient,
  useSession,
} from "../react-core/provider.js";
import { createJazzClient, type JazzClient as CreatedJazzClient } from "./create-jazz-client.js";
import { attachDevTools } from "../dev-tools/dev-tools.js";

if (process.env.NODE_ENV === "development" && typeof window !== "undefined") {
  import("jazz-tools/_dev/schema-hash").catch(() => {});
}

export { JazzClientProvider, type JazzClientProviderProps } from "../react-core/provider.js";

interface JazzClientContextValue {
  db: Db;
  manager: CreatedJazzClient["manager"];
  session: Session | null;
  shutdown: CreatedJazzClient["shutdown"];
}

export type JazzProviderProps = {
  config: DbConfig;
  fallback?: ReactNode;
  children: ReactNode;
  onJWTExpired?: () => Promise<string | null | undefined>;
  /** Dev-only: auto-attach the inspector devtools bridge. Default true. */
  autoAttachDevTools?: boolean;
  /** Schema for the inspector overlay. Required for auto-attach to surface data. */
  wasmSchema?: WasmSchema;
};

// Module-level so a manual attachDevTools elsewhere and this guard converge on
// db identity. attachDevTools itself also de-dupes on db.
const autoAttachedDbs = new WeakSet<object>();

function DevToolsAutoAttach({ wasmSchema }: { wasmSchema?: WasmSchema }) {
  const { db } = useCoreJazzClient() as JazzClientContextValue;
  const attempted = useRef(false);
  useEffect(() => {
    if (attempted.current) return;
    if (!wasmSchema) return;
    if (autoAttachedDbs.has(db as object)) return;
    attempted.current = true;
    autoAttachedDbs.add(db as object);
    void attachDevTools({ db }, wasmSchema);
  }, [db, wasmSchema]);
  return null;
}

export function JazzProvider({
  config,
  fallback,
  children,
  onJWTExpired,
  autoAttachDevTools = true,
  wasmSchema,
}: JazzProviderProps) {
  // Build-time DCE in Vite via import.meta.env.DEV; runtime fallback for bundlers
  // that only define NODE_ENV. Both must be false in prod for the branch to drop.
  const devEnabled =
    (typeof import.meta !== "undefined" && (import.meta as { env?: { DEV?: boolean } }).env?.DEV) ||
    process.env.NODE_ENV !== "production";
  const shouldAutoAttach = Boolean(devEnabled && autoAttachDevTools);

  return (
    <CoreJazzProvider
      config={config}
      fallback={fallback}
      createJazzClient={createJazzClient}
      onJWTExpired={onJWTExpired}
    >
      {shouldAutoAttach ? <DevToolsAutoAttach wasmSchema={wasmSchema} /> : null}
      {children}
    </CoreJazzProvider>
  );
}

export function useJazzClient(): JazzClientContextValue {
  return useCoreJazzClient() as JazzClientContextValue;
}

export function useDb(): Db {
  return useCoreDb<Db>();
}

export { useSession };
export type { JazzClientContextValue };
```

- [ ] **Step 4: Run to verify it passes.**

Run: `pnpm --filter jazz-tools test -- provider.devtools`
Expected: PASS (all three tests).

- [ ] **Step 5: Build to confirm types.**

Run: `pnpm --filter jazz-tools build`
Expected: clean.

- [ ] **Step 6: Commit.**

```bash
git add packages/jazz-tools/src/react/provider.tsx packages/jazz-tools/src/react/provider.devtools.test.tsx
git commit -m "feat(react): dev-only auto-attach devtools in JazzProvider"
```

### Task 5.2: Svelte/Vue/Solid provider auto-attach **[GLM — simple/medium]**

**Files:**

- Modify: `packages/jazz-tools/src/svelte/index.ts` (and the Svelte provider component it points to)
- Modify: `packages/jazz-tools/src/vue/index.ts` (and its provider)
- Modify: `packages/jazz-tools/src/solid/index.ts` (and its provider)

For each binding, mirror Task 5.1: add `autoAttachDevTools` (default true) + `wasmSchema` to the provider, and in a dev-gated mount effect call `attachDevTools({ db }, wasmSchema)` once per db (reuse a `WeakSet` guard). Use each framework's lifecycle (`onMount`/`$effect` for Svelte, `onMounted` for Vue, `onMount`/`createEffect` for Solid).

- [ ] **Step 1: Inspect each provider to find the mount hook + db accessor.**

Run: `pnpm --filter jazz-tools exec grep -rn "createJazzClient\|provider\|JazzProvider\|useDb" src/svelte src/vue src/solid | head -40`
Expected: identifies each framework's provider entry to edit.

- [ ] **Step 2: Implement the dev-gated auto-attach in the Svelte provider** (component file identified in Step 1), guarded by `import.meta.env.DEV` with the same `WeakSet` pattern, calling `attachDevTools({ db }, wasmSchema)`.

- [ ] **Step 3: Implement the same in the Vue provider.**

- [ ] **Step 4: Implement the same in the Solid provider.**

- [ ] **Step 5: Build all bindings.**

Run: `pnpm --filter jazz-tools build`
Expected: clean.

- [ ] **Step 6: Commit.**

```bash
git add packages/jazz-tools/src/svelte packages/jazz-tools/src/vue packages/jazz-tools/src/solid
git commit -m "feat(svelte,vue,solid): dev-only auto-attach devtools in providers"
```

---

## Phase 6 — Vite plugin: serve + inject (first end-to-end framework)

### Task 6.1: Shared serve middleware (resolve embedded build + bundle loader) **[GLM — simple/medium]**

**Files:**

- Create: `packages/jazz-tools/src/dev/inspector-overlay/serve.ts`

This module: (a) resolves the inspector embedded build from `node_modules` via `require.resolve`, (b) bundles `loader.ts` once (esbuild) into a string, (c) exposes a connect-style middleware `handleOverlayRequest(req, res)` that serves `/__jazz/loader.js` and `/__jazz/embedded/*`.

- [ ] **Step 1: Implement `serve.ts`.**

```ts
import { createRequire } from "node:module";
import { readFile } from "node:fs/promises";
import { dirname, join, normalize } from "node:path";
import { build } from "esbuild";

const require = createRequire(import.meta.url);

const MIME: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".wasm": "application/wasm",
  ".json": "application/json; charset=utf-8",
  ".map": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
};

function extname(p: string): string {
  const i = p.lastIndexOf(".");
  return i === -1 ? "" : p.slice(i);
}

// inspector ships dist-embedded/index.html as the embedded build entry.
function resolveEmbeddedDir(): string {
  const indexHtml = require.resolve("inspector/dist-embedded/index.html");
  return dirname(indexHtml);
}

let loaderScriptPromise: Promise<string> | null = null;
function getLoaderScript(): Promise<string> {
  if (!loaderScriptPromise) {
    const entry = require.resolve("jazz-tools/dev/inspector-overlay/loader");
    loaderScriptPromise = build({
      entryPoints: [entry],
      bundle: true,
      format: "iife",
      platform: "browser",
      write: false,
      legalComments: "none",
    }).then((result) => result.outputFiles[0].text);
  }
  return loaderScriptPromise;
}

export const OVERLAY_LOADER_PATH = "/__jazz/loader.js";
export const OVERLAY_EMBEDDED_PREFIX = "/__jazz/embedded";

export interface OverlayRequest {
  url: string;
}
export interface OverlayResponse {
  setHeader(name: string, value: string): void;
  statusCode: number;
  end(body?: string | Buffer): void;
}

// Returns true if it handled the request.
export async function handleOverlayRequest(
  req: OverlayRequest,
  res: OverlayResponse,
): Promise<boolean> {
  const url = (req.url ?? "").split("?")[0];

  if (url === OVERLAY_LOADER_PATH) {
    const script = await getLoaderScript();
    res.setHeader("Content-Type", MIME[".js"]);
    res.end(script);
    return true;
  }

  if (url === OVERLAY_EMBEDDED_PREFIX || url.startsWith(OVERLAY_EMBEDDED_PREFIX + "/")) {
    const rel = url.slice(OVERLAY_EMBEDDED_PREFIX.length).replace(/^\//, "") || "index.html";
    const dir = resolveEmbeddedDir();
    const filePath = normalize(join(dir, rel));
    if (!filePath.startsWith(dir)) {
      res.statusCode = 403;
      res.end("Forbidden");
      return true;
    }
    try {
      const body = await readFile(filePath);
      res.setHeader("Content-Type", MIME[extname(filePath)] ?? "application/octet-stream");
      res.end(body);
    } catch {
      res.statusCode = 404;
      res.end("Not found");
    }
    return true;
  }

  return false;
}
```

- [ ] **Step 2: Add `esbuild` to `jazz-tools` devDependencies if absent.**

Run: `pnpm --filter jazz-tools exec node -e "require('esbuild'); console.log('present')" || echo "MISSING"`
Expected: `present`. If `MISSING`, run `pnpm --filter jazz-tools add -D esbuild` and re-check.

- [ ] **Step 3: Ensure `inspector` is a (dev) dependency of `jazz-tools`** so `require.resolve("inspector/...")` works.

Run: `pnpm --filter jazz-tools exec node -e "require.resolve('inspector/package.json'); console.log('resolves')" || echo "MISSING"`
Expected: `resolves`. If `MISSING`, add `"inspector": "workspace:*"` to `jazz-tools` devDependencies and `pnpm install`.

- [ ] **Step 4: Add a package export so `require.resolve('jazz-tools/dev/inspector-overlay/loader')` works.** In `packages/jazz-tools/package.json` `exports`, add a subpath mapping for `./dev/inspector-overlay/loader` pointing to the built loader (`dist/dev/inspector-overlay/loader.js`).

- [ ] **Step 5: Build.**

Run: `pnpm --filter jazz-tools build`
Expected: clean; `dist/dev/inspector-overlay/serve.js` and `loader.js` present.

- [ ] **Step 6: Commit.**

```bash
git add packages/jazz-tools/src/dev/inspector-overlay/serve.ts packages/jazz-tools/package.json
git commit -m "feat(dev): add inspector overlay serve middleware"
```

### Task 6.2: Wire serve + inject into the Vite plugin + integration test **[GLM — simple/medium]**

**Files:**

- Modify: `packages/jazz-tools/src/dev/vite.ts`
- Modify: `packages/jazz-tools/src/dev/inspector-link.ts`
- Create: `packages/jazz-tools/src/dev/inspector-overlay/serve.integration.test.ts`

- [ ] **Step 1: Add the middleware + HTML injection to the Vite plugin.** In `vite.ts`, extend the returned plugin object: add a `transformIndexHtml` hook and register the middleware in `configureServer`. The `ViteDevServer` interface in this file (lines 46–62) must gain `middlewares: { use(fn): void }`.

```ts
// in the returned plugin object, add:
transformIndexHtml(html: string) {
  return {
    html,
    tags: [
      {
        tag: "script",
        attrs: { type: "module", src: "/__jazz/loader.js" },
        injectTo: "body" as const,
      },
    ],
  };
},
```

```ts
// inside configureServer, after the managed runtime is initialized and only in serve:
viteServer.middlewares.use((req: { url?: string }, res: OverlayResponse, next: () => void) => {
  void handleOverlayRequest({ url: req.url ?? "" }, res).then((handled) => {
    if (!handled) next();
  });
});
```

Add the import: `import { handleOverlayRequest, type OverlayResponse } from "./inspector-overlay/serve.js";` and extend the local `ViteDevServer` interface with `middlewares: { use(fn: (req: { url?: string }, res: OverlayResponse, next: () => void) => void): void };`.

- [ ] **Step 2: Replace the primary inspector-link log** (`vite.ts:142–148`) with a local-overlay message; keep `buildInspectorLink` for the admin path but demote it:

```ts
console.log(
  `${LOG_PREFIX} Inspector overlay enabled — open your app and click the ⚡ button (Alt+Shift+J).`,
);
```

- [ ] **Step 3: Write the integration test.** Boot the Vite plugin's middleware in isolation (call `handleOverlayRequest` directly is unit-level; for integration, assert the plugin registers the hook and the middleware serves both paths). Minimal integration via the serve module + a fake res:

```ts
import { describe, it, expect } from "vitest";
import { handleOverlayRequest } from "./serve.js";

function fakeRes() {
  const headers: Record<string, string> = {};
  let body = "";
  let statusCode = 200;
  return {
    res: {
      setHeader: (k: string, v: string) => (headers[k] = v),
      get statusCode() {
        return statusCode;
      },
      set statusCode(v: number) {
        statusCode = v;
      },
      end: (b?: string | Buffer) => (body = b ? b.toString() : ""),
    },
    get headers() {
      return headers;
    },
    get body() {
      return body;
    },
    get statusCode() {
      return statusCode;
    },
  };
}

describe("inspector overlay serve middleware", () => {
  it("serves the bundled loader.js", async () => {
    const r = fakeRes();
    const handled = await handleOverlayRequest({ url: "/__jazz/loader.js" }, r.res as never);
    expect(handled).toBe(true);
    expect(r.headers["Content-Type"]).toContain("javascript");
    expect(r.body.length).toBeGreaterThan(0);
  });

  it("serves the embedded index.html", async () => {
    const r = fakeRes();
    const handled = await handleOverlayRequest(
      { url: "/__jazz/embedded/index.html" },
      r.res as never,
    );
    expect(handled).toBe(true);
    expect(r.headers["Content-Type"]).toContain("text/html");
  });

  it("ignores unrelated urls", async () => {
    const r = fakeRes();
    const handled = await handleOverlayRequest({ url: "/index.html" }, r.res as never);
    expect(handled).toBe(false);
  });
});
```

This test requires the inspector embedded build to exist. Add a prerequisite note: run `pnpm --filter inspector run build:embedded` before this test (and ensure CI builds it).

- [ ] **Step 4: Run the integration test.**

Run: `pnpm --filter inspector run build:embedded && pnpm --filter jazz-tools test -- inspector-overlay/serve.integration`
Expected: PASS (all three).

- [ ] **Step 5: Build.**

Run: `pnpm --filter jazz-tools build`
Expected: clean.

- [ ] **Step 6: Commit.**

```bash
git add packages/jazz-tools/src/dev/vite.ts packages/jazz-tools/src/dev/inspector-link.ts packages/jazz-tools/src/dev/inspector-overlay/serve.integration.test.ts
git commit -m "feat(dev): serve + inject inspector overlay in the Vite plugin"
```

### Task 6.3: End-to-end browser test (Playwright, inspector package) **[OPUS — hard]**

**Files:**

- Create: `packages/inspector/src/embedded.browser.test.ts`

Validate the whole chain in a real browser: a host page calls `attachDevTools` on a real (or stubbed) db, loads `embedded/index.html` in an iframe, the relay wired, and the inspector reaches its connected state (no longer showing "Waiting for runtime devtools connection...").

- [ ] **Step 1: Write the Playwright test** following the existing `playwright.config.ts` patterns in the package. The test serves a small fixture page that: imports `attachDevTools` + a minimal schema built via the public API (NOT a JSON literal — use the schema builder), mounts the relay (reuse the loader by injecting `/__jazz/loader.js` equivalent, or inline a minimal relay), embeds the iframe, and asserts the inspector renders the data-explorer shell.

(Fixture construction detail: reuse the existing browser-test harness in `packages/inspector` — inspect `playwright.config.ts` and any current `*.browser.test.ts`/`tests/` to match how a page+app is served. Build the schema with the public API per `TESTING_GUIDELINES.md`.)

- [ ] **Step 2: Run it.**

Run: `pnpm --filter inspector run build:embedded && pnpm --filter inspector test:browser -- embedded`
Expected: PASS — iframe leaves the "Waiting..." state and shows the inspector UI.

- [ ] **Step 3: Commit.**

```bash
git add packages/inspector/src/embedded.browser.test.ts
git commit -m "test(inspector): e2e overlay relay + embedded entry"
```

---

## Phase 7 — SvelteKit plugin: serve + inject

### Task 7.1: Wire serve + inject into the SvelteKit plugin **[GLM — simple/medium]**

**Files:**

- Modify: `packages/jazz-tools/src/dev/sveltekit.ts`

`sveltekit.ts` already uses Vite-style `config()` + `configureServer()` hooks (spec §E). Reuse the exact same middleware + `transformIndexHtml` injection as Task 6.2.

- [ ] **Step 1: Inspect the SvelteKit plugin's hooks to confirm `transformIndexHtml` + `middlewares` availability.**

Run: `pnpm --filter jazz-tools exec grep -n "configureServer\|transformIndexHtml\|middlewares\|enforce" src/dev/sveltekit.ts`
Expected: shows the existing hook structure to extend.

- [ ] **Step 2: Add the same middleware registration** (`viteServer.middlewares.use(... handleOverlayRequest ...)`) inside the SvelteKit plugin's `configureServer`, dev-only.

- [ ] **Step 3: Add the `transformIndexHtml` script injection** (identical tag to Task 6.2). If SvelteKit's app template injection differs, inject into the document via the same `tags` mechanism Vite exposes.

- [ ] **Step 4: Build + run the serve integration test against the shared module (already covers serving).**

Run: `pnpm --filter jazz-tools build && pnpm --filter inspector run build:embedded && pnpm --filter jazz-tools test -- inspector-overlay/serve.integration`
Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add packages/jazz-tools/src/dev/sveltekit.ts
git commit -m "feat(dev): serve + inject inspector overlay in the SvelteKit plugin"
```

---

## Phase 8 — Next.js plugin: serve + inject (the long pole)

Next has no `transformIndexHtml` and no dev-server middleware hook (spec §E). Two new mechanisms:

- **Serving:** add a rewrite from `/__jazz/:path*` to a bundled route handler that calls `handleOverlayRequest`, OR copy the embedded build into the user's `public/__jazz/` at dev time (wasm-style), guarded so it never ships to prod.
- **Injection:** inject a `<Script src="/__jazz/loader.js" strategy="afterInteractive">` via the App Router root layout, or a `_document`/`<Head>` for Pages Router.

### Task 8.1: Next serving via public-dir copy (dev only) **[OPUS — hard]**

**Files:**

- Modify: `packages/jazz-tools/src/dev/next.ts`

- [ ] **Step 1: Add a `copyOverlayToPublic(appRoot)` mirroring `copyWasmToPublic` (`next.ts:65–72`).** It resolves the inspector embedded dir + the bundled loader and writes them to `<appRoot>/public/__jazz/embedded/*` and `<appRoot>/public/__jazz/loader.js`. Only call it in the development phase (`phase-development-server`), never in `phase-production-build`. Add the written paths to a cleanup list and to `.gitignore` guidance in the log.

- [ ] **Step 2: Guard against prod.** In the production-build phase, explicitly skip the copy and (if present) note that `public/__jazz` is dev-only. Log a one-line warning if `public/__jazz` exists during a prod build so users can gitignore it.

- [ ] **Step 3: Build.**

Run: `pnpm --filter jazz-tools build`
Expected: clean.

- [ ] **Step 4: Commit.**

```bash
git add packages/jazz-tools/src/dev/next.ts
git commit -m "feat(dev): serve inspector overlay assets in the Next plugin (dev only)"
```

### Task 8.2: Next injection via root layout `<Script>` **[OPUS — hard]**

**Files:**

- Modify: `packages/jazz-tools/src/dev/next.ts`

- [ ] **Step 1: Determine the injection approach** the existing plugin supports. Inspect `next.ts` for any existing client-entry/script injection precedent.

Run: `pnpm --filter jazz-tools exec grep -n "Script\|webpack\|turbopack\|phase\|injectTo\|entry" src/dev/next.ts`
Expected: shows whether a webpack entry injection point exists.

- [ ] **Step 2: Implement injection.** Preferred: document + provide a tiny `JazzInspectorScript` component exported from `jazz-tools/next` that renders `<Script src="/__jazz/loader.js" strategy="afterInteractive" />` only in dev, which users drop into their root layout. (Fully-automatic injection into arbitrary Next layouts is unreliable across App/Pages router; an exported one-liner component is the robust, documented path.) This is a deliberate, documented deviation from "zero app code" for Next only — record it in the spec's open-details + the plugin's docs.

- [ ] **Step 3: Build.**

Run: `pnpm --filter jazz-tools build`
Expected: clean.

- [ ] **Step 4: Commit.**

```bash
git add packages/jazz-tools/src/dev/next.ts
git commit -m "feat(dev): inject inspector overlay loader for Next (dev only)"
```

---

## Phase 9 — Example migration + cleanup

### Task 9.1: Migrate the todo example to the new zero-boilerplate flow **[GLM — simple/medium]**

**Files:**

- Modify: `examples/todo-client-localfirst-react/src/App.tsx`

- [ ] **Step 1: Remove the manual `DevToolsRegistration` component** (lines 30–50) and its `<DevToolsRegistration />` usage; remove the now-unused `attachDevTools`/`useJazzClient` imports if not otherwise used. Pass the schema to the provider instead:

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

Keep the `window.jazzClient` debug assignment if desired by moving it into a tiny dev-only effect, or drop it.

- [ ] **Step 2: Run the example's tests/build to confirm it still works.**

Run: `pnpm --filter todo-client-localfirst-react build`
Expected: builds clean.

- [ ] **Step 3: Commit.**

```bash
git add examples/todo-client-localfirst-react/src/App.tsx
git commit -m "chore(example): use JazzProvider wasmSchema for inspector overlay"
```

### Task 9.2: Full build + test sweep **[GLM — simple/medium]**

- [ ] **Step 1: Build everything.**

Run: `pnpm build:core`
Expected: all packages build.

- [ ] **Step 2: Run the full test suite.**

Run: `pnpm test`
Expected: green (including the new parent-window-port, relay, provider.devtools, serve.integration, and inspector browser tests).

- [ ] **Step 3: Update the spec's "Open implementation details"** to record the resolved decisions: toggle shortcut = Alt+Shift+J; `inspector-link.ts` retained for the admin path; Next injection via exported `JazzInspectorScript` component; multi-peer namespacing deferred (documented). Commit the spec edit.

```bash
git add docs/superpowers/specs/2026-06-25-inspector-dev-plugin-overlay-design.md
git commit -m "docs: record resolved implementation decisions for inspector overlay"
```

---

## Self-review notes (author)

- **Spec coverage:** A=Tasks 3.1/3.2; B=Tasks 2.1/2.2; C=Tasks 4.1/4.2; D=Tasks 5.1/5.2; E=Tasks 6.2/7.1/8.1/8.2; F=Task 6.1. Testing section → Tasks 2.1, 4.1 (loop invariant), 5.1, 6.2, 6.3. Multi-peer conflict → deferred + documented (Task 9.2 Step 3).
- **Known risk / deviation:** Next "zero app code" is relaxed to a one-line exported `<JazzInspectorScript />` (Task 8.2) — flagged for reviewers. Provider auto-attach requires a `wasmSchema` prop (the provider has no other schema source); this is "near-zero-config," not literally zero — flagged for reviewers.
- **Type consistency:** `DevtoolsBridgePort` / `DevtoolsBridgeConnector` / `setDevtoolsBridgeConnector` used consistently across Tasks 1.1, 2.1, 2.2. `handleOverlayRequest` / `OverlayResponse` consistent across 6.1, 6.2, 7.1.
