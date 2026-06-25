# Inspector as a Dev-Plugin Overlay — Design

**Date:** 2026-06-25
**Status:** Approved design, pending implementation plan

## Summary

Make the Jazz inspector part of the dev plugins. Today the dev plugins
(`packages/jazz-tools/src/dev`) print a link to a **hosted** standalone inspector
(`https://jazz2-inspector.vercel.app/`) with `serverUrl`/`appId`/`adminSecret` in the
URL fragment. Developers have to open a separate tab (or install the Chrome extension)
and the inspector connects either with an admin secret (standalone) or through the
extension's content-script bridge.

This design replaces that with a **zero-config, locally-served inspector overlay**:

- The dev plugin **serves the inspector assets locally** (no external Vercel) and
  **auto-injects an overlay** (floating toggle + iframe) into the running dev app.
- The overlay is wired to the app's **live in-process client** — it shows exactly what
  the current session sees under its permissions. No admin secret in the browser.
- It is fully zero-config: the developer adds nothing to their app code.

The hosted standalone (Vercel) and Chrome-extension inspector builds keep working
unchanged; this adds a third host for the same inspector UI.

## Goals

- Developer runs `vite`/`next dev`/`svelte` dev and gets a working inspector overlay
  with **no code changes and no external site**.
- The overlay reflects the **live client's** permission-scoped view, via the existing
  `jazz-devtools-v1` bridge.
- Reuse the existing inspector React app and bridge protocol; keep the new surface area
  small and dev-only.

## Non-goals

- **Expo / React Native** — excluded. The overlay is DOM-based; RN has no DOM.
- Admin/full-DB inspection from the overlay (that remains the hosted standalone's job).
- Changing the bridge protocol or the inspector's data-explorer / live-query UIs.
- Removing the hosted standalone or Chrome extension.

## Scope

DOM-based dev plugins only: **Vite**, **Next.js**, **SvelteKit** (plus Vue/Solid hosted
under Vite). The React overlay mounts into an injected DOM node, so it is
host-framework-agnostic; each plugin only needs its own way to inject that node and serve
the assets.

## Background — current state

- **Inspector** (`packages/inspector`): a React app with two existing entries/modes.
  - Standalone (`index.html` → `src/standalone.tsx` → `App`): reads connection from the
    URL fragment, connects to a server with an admin secret. Built via `--mode web` →
    `dist/`, deployed to Vercel.
  - Extension (`devtools.html`/`devtools-tab.html` → `src/devtools-main.tsx`): builds its
    client with `createExtensionJazzClient()` and renders
    `DevtoolsProvider runtime="extension"`. Connects to the page's live client over the
    `jazz-devtools-v1` bridge. Built via `--mode extension` → `dist-extension/`.
- **Bridge** (`packages/jazz-tools/src/dev-tools`):
  - Runtime side: `attachDevTools(client, wasmSchema)` (`dev-tools.ts`) speaks the bridge
    purely via the **top window's** `window.postMessage` / `window` `message` events.
  - Extension transport: the content script (`extension-panel.ts`) **relays** those window
    messages to/from the devtools panel over a `chrome.runtime` port. The panel side uses
    `createExtensionJazzClient()`.
  - Apps call `attachDevTools(client, app.wasmSchema)` manually today (see
    `examples/todo-client-localfirst-react/src/App.tsx`).
- **Dev plugins** (`packages/jazz-tools/src/dev`): framework adapters (`vite.ts`,
  `next.ts`, `sveltekit.ts`, `expo.ts`) sharing `ManagedDevRuntime`. The Vite plugin's
  `configureServer` currently `console.log`s `buildInspectorLink(...)`
  (`inspector-link.ts`) pointing at Vercel.

## Architecture

```
┌─ dev app page (e.g. localhost:5173) ───────────────────────────┐
│                                                                 │
│  JazzProvider ──auto in dev──► attachDevTools(client, schema)   │
│        │                                ▲                        │
│        │                      jazz-devtools-v1 (window.postMsg)  │
│        │                                ▼                        │
│   injected loader / relay  ◄──────────────────────────┐         │
│        │  (forwards bridge msgs  window ⇄ iframe)      │  [⚡]   │
│        ▼                                               │ toggle  │
│  ┌─ overlay panel ──────────────────────────────────┐ │         │
│  │  <iframe src="/__jazz/embedded">                  │ │         │
│  │     inspector (embedded entry, bridge peer)       │ │         │
│  └───────────────────────────────────────────────────┘         │
└─────────────────────────────────────────────────────────────────┘
         ▲ assets at /__jazz/embedded served by the dev plugin
         └── from jazz-tools' copied-in inspector embedded build
```

### Data flow

1. The dev plugin, in dev/`serve` only, (a) serves the inspector's embedded build at
   `/__jazz/embedded`, and (b) injects a small loader script into the page.
2. `JazzProvider` auto-calls `attachDevTools(client, wasmSchema)` in dev, exposing the
   live client on the existing `jazz-devtools-v1` window bridge.
3. The loader renders the `⚡` toggle and the `<iframe src="/__jazz/embedded">`, and acts
   as the **relay** between the top window (where `attachDevTools` lives) and the iframe —
   the same role the extension's content script plays.
4. Inside the iframe, the inspector's **embedded entry** connects as a bridge peer via a
   new `createEmbeddedJazzClient()` transport (`window.parent.postMessage` out, `window`
   `message` in), then runs the existing devtools React tree (handshake →
   `devtools.announce` for schema → queries / subscriptions).

### The load-bearing invariant (`event.source === window`)

The whole relay design rests on one fact, verified at
`packages/jazz-tools/src/dev-tools/dev-tools.ts:217`: the runtime listener **rejects every
message whose `event.source !== window`**, and it sends replies via
`window.postMessage(reply, "*")` (`dev-tools.ts:248-256`). The bridge is plain
serialized-payload `window.postMessage` — **no `MessagePort`/`MessageChannel`/transferred
ports** — which is what makes a relay possible at all.

Consequences the implementation must honor:

- The iframe **cannot** talk to `attachDevTools` directly: a `window.parent.postMessage`
  arrives at the top window with `event.source === iframe.contentWindow`, which the guard
  rejects. So the relay re-injecting iframe→top via `window.postMessage(msg, "*")` (giving
  `event.source === window`) is **mandatory, not optional** — it is the only way a request
  reaches the runtime.
- The relay must mirror the content-script routing in `extension-panel.ts:138-154`:
  forward `kind: "request"` iframe→top, forward `kind: "response" | "event"` top→iframe,
  and **gate the iframe→top path on `event.source === iframe.contentWindow`**. Without that
  source check, any other frame or script that posts a bridge-channel message to the top
  window (OAuth popups, third-party embeds, the app's own iframes) gets re-injected and
  amplified into a loop.
- Any future change to that `event.source !== window` line silently breaks the overlay.
  Add a comment at `dev-tools.ts:217` tying it to the overlay relay, and a test asserting
  one request produces **exactly one** re-injection (no echo, no loop) — see Testing.

`attachDevTools` itself is left **completely untouched**; all new routing lives in the
relay.

## Components

### A. Inspector "embedded" entry — `packages/inspector` (new, minimal)

- New `embedded.html` → `src/embedded.tsx`, a ~10-line near-copy of `src/devtools-main.tsx`
  (43 lines today) that builds its client with the new `createEmbeddedJazzClient()` and
  renders the existing devtools React tree.
- **Reuse `DevtoolsProvider runtime="extension"`.** That already provides exactly the
  desired semantics — `local-only` query propagation by default with a toggle
  (`devtools-context.tsx:28-35`), i.e. the live client's view, not `"full"`/admin. Do
  **not** add a new `InspectorRuntime` value or fork the propagation logic. (If a label
  reading "embedded" is wanted for clarity later, make it a thin alias that routes to the
  `"extension"` code path — not a parallel branch.)
- Add `embedded.html` as an extra Rollup input on the **existing extension build config**
  rather than introducing a separate `--mode embedded`; the output directory the dev
  plugin serves from is the only thing that needs to be distinct.

### B. `createEmbeddedJazzClient()` transport — `jazz-tools/react` (new)

- Sibling to `createExtensionJazzClient()`. Same bridge protocol, but the transport uses
  `window.parent.postMessage(msg, targetOrigin)` to send and
  `window.addEventListener("message")` to receive. No `chrome.*` — works in any iframe.
- **`targetOrigin` is the app's own origin, not `"*"`.** The overlay is same-origin with
  the app; posting to a concrete origin avoids leaking bridge traffic to an unexpected
  parent. (The runtime reply path uses `"*"` today and validates no origin — see Production
  safety for why that is acceptable in dev only.)
- Exported from `jazz-tools/react` (and the other framework entrypoints as needed).

### C. Injected loader + relay — served by the plugin (new)

- **Served as an external, same-origin file** (`/__jazz/loader.js`), and injected as
  `<script src="/__jazz/loader.js">` — **not** as an inline script. Any app with
  `Content-Security-Policy: script-src 'self'` (common in SvelteKit/Next) would block an
  inlined loader; a same-origin external file is allowed under `'self'` and avoids the
  nonce/hash dance. The same-origin iframe at `/__jazz/embedded` is allowed under
  `frame-src`/`child-src 'self'`.
- Responsibilities:
  1. Render a floating `⚡` toggle in its own container (high z-index, isolated so the app
     can't style it) and a resizable panel holding `<iframe src="/__jazz/embedded">`.
  2. **Relay** bridge messages per the invariant above: iframe→top by re-injecting via
     `window.postMessage` (gated on `event.source === iframe.contentWindow` and the bridge
     channel), top→iframe via `iframe.contentWindow.postMessage`. Mirror
     `extension-panel.ts:138-154`.
- Collapsed by default; open/closed state and panel size persisted in `localStorage`.
- Optional keyboard shortcut to toggle (default chosen during implementation).

### D. Provider auto-attach — `jazz-tools` react/svelte/vue/solid (change)

- In dev only, the provider calls `attachDevTools(client, wasmSchema)` itself.
- **Opt-out:** an `autoAttachDevTools` prop (default `true` in dev) lets users disable it —
  some won't want the client instrumented (perf, console noise) or drive the extension
  manually.
- **Idempotency keyed on the client instance**, sharing the guard with existing manual call
  sites (e.g. `examples/todo-client-localfirst-react` still calls `attachDevTools`
  directly). Otherwise auto + manual double-attach the same client. `attachDevTools`
  already de-dupes on the `db` (`registeredRuntimeBridgeDbs`), so the provider guard must
  resolve to the same identity.
- **Build-time dead-code elimination, not just a runtime check.** Gate on
  `import.meta.env.DEV` (statically replaced by Vite) so the call is removed from prod
  bundles entirely. A bare runtime `process.env.NODE_ENV !== "production"` is a fallback,
  but a misconfigured staging env with `NODE_ENV !== "production"` would silently activate
  the bridge and expose live data to every same-origin script — prefer the build-time gate
  where the bundler supports it, per framework.
- `attachDevTools` stays exported for manual and extension use.

### E. Dev plugin: serve + inject — `vite.ts`, `next.ts`, `sveltekit.ts` (change)

- Serve the embedded build (resolved from `node_modules`, see F) as static assets at
  `/__jazz/embedded`, and `/__jazz/loader.js`.
- Inject the loader `<script src="/__jazz/loader.js">`. Guarded to dev/`serve` only.
- **Vite + SvelteKit** are straightforward: both expose `transformIndexHtml` for injection
  and a `configureServer` middleware hook for serving (`viteServer.middlewares.use(...)`).
- **Next.js is the long pole and materially harder than the others.** The Next plugin
  (`next.ts`) is a pure config wrapper: it has **no `transformIndexHtml` equivalent, no
  page-HTML injection precedent, no app-router vs pages-router handling, and no dev-server
  middleware hook** (Next runs its own server). Both serving and injection need new
  mechanisms there:
  - _Serving:_ either a route handler / rewrite to the embedded assets, or the wasm-style
    copy into the user's `public/` (with a guard so it never reaches a prod build).
  - _Injection:_ an injected client entry or a `<Script>` added via the framework's
    documented hooks, with router-mode awareness.
    Next stays in scope (all DOM frameworks), but the plan should sequence it **after** Vite
  * SvelteKit and budget for the extra work; it must not block their release.
- Replace the `buildInspectorLink(...)` → Vercel `console.log` with a message pointing at
  the local overlay (the hosted link/`inspector-link.ts` may be retired or kept for the
  admin/standalone path — decided during implementation).

### F. Packaging — resolve from `node_modules`, do **not** vendor into the published package

The earlier plan ("copy the embedded build into the published `jazz-tools` package") was
based on a misread of the `jazz-wasm` precedent and is rejected. The inspector build is
**~10 MB on disk, dominated by a 9.3 MB wasm blob** (verified: `du -sh dist` = 10M).
Vendoring that into `jazz-tools` would ship 10 MB of dev-only assets to **every** consumer,
including production installs.

What the wasm precedent actually does (`copyWasmToPublic`, `next.ts:65-72`):
`require.resolve("jazz-wasm/package.json")` to locate the package in `node_modules`, then
copy/serve from there at dev/build time — it does **not** bundle wasm into the published
`jazz-tools`.

So, mirroring that precedent correctly:

- `inspector` is already a build-time dependency of `jazz-tools`. The dev plugin does
  `require.resolve("inspector/<embedded-build-entry>")` to locate the embedded assets in
  `node_modules` and **serves them through dev-server middleware** (e.g. `sirv` mounted at
  `/__jazz/embedded`). For Vite/SvelteKit this is `viteServer.middlewares.use(...)`; for
  Next, the route-handler/copy approach in §E.
- Zero added weight in the published `jazz-tools` package; assets exist only where
  `inspector` is already installed as a dev dependency, and are served only in dev.
- The inspector remains its own source and still produces the Vercel (`dist/`) and
  extension (`dist-extension/`) builds. Build ordering: the inspector's embedded build runs
  before the dev plugin needs to resolve it.

## Why an iframe (not a shadow-DOM portal)

The iframe is a deliberate isolation boundary, not incidental. The inspector bundles its
own **React 19**, `react-data-grid`, and `@tanstack/table`. Mounting it into the host
document via a shadow-DOM portal would put two React copies (and possibly two React
_versions_ — a host on React 18, or a non-React renderer) in the same JS realm, which
breaks. The iframe gives JS-realm + React-version isolation **and** CSS isolation for free,
and lets us serve the inspector build verbatim. An implementer must not "simplify" this to
a same-document portal.

## UX

- Floating `⚡` toggle, fixed bottom-right corner, isolated container.
- Click toggles a resizable panel containing the iframe; collapsed by default; state + size
  persisted in `localStorage`.
- During app load, before `attachDevTools` announces, the embedded entry shows its existing
  "Waiting for runtime devtools connection…" state, then data appears.

## Production safety

- Plugin serves `/__jazz/*` and injects the loader **only** when `command === "serve"` /
  dev (matches the existing `configureServer` guard). Nothing ships to production builds.
- Provider auto-attach is dev-gated and build-time dead-code-eliminated (§D), so
  `attachDevTools` is fully absent from production bundles.
- No admin secret in the browser — the overlay connects via the in-process bridge only.
- **The overlay is not "sealed" — it is dev-only trust, not isolation.** In dev the bridge
  is plain `window.postMessage` with no origin validation, so it trusts _all_ same-origin
  contexts. The safety story is "this code does not exist in production," not "the bridge is
  locked down." State this explicitly; don't imply the absence of an admin secret makes the
  dev bridge secure.

### Multi-peer conflict (document, ideally namespace)

The bridge has **no peer IDs and no dedup** — the runtime keys subscriptions in a shared
Map by `bridgeSubscriptionId` (`dev-tools.ts:441-469`). Today two devtools surfaces on one
tab is rare. The overlay is **always present** in dev, so overlay + Chrome extension open
together, or two overlay iframes after HMR churn, will create overlapping subscriptions and
race. Minimum: document the limitation. Better: tag relay messages with a peer id and have
the runtime namespace subscriptions per peer. Decide scope in the plan.

## Testing

Black-box / public-API tests, per `crates/jazz-tools/TESTING_GUIDELINES.md` and the repo's
preference for integration tests:

- **Inspector (browser):** a Playwright test (alongside the existing `test:browser`) loads a
  small host page that calls `attachDevTools`, embeds the embedded entry in an iframe, and
  asserts the relay handshake completes and data renders.
- **Relay loop/amplification (critical):** assert that one iframe→top request produces
  **exactly one** re-injection — no echo, no infinite loop — and that a bridge-channel
  message posted by a _different_ frame is **not** re-injected (the `event.source ===
iframe.contentWindow` gate). This pins the load-bearing invariant against regressions in
  the `dev-tools.ts:217` guard.
- **Plugin (integration):** boot the Vite plugin's dev server and assert `/__jazz/embedded`
  - `/__jazz/loader.js` serve assets and the loader `<script>` is injected into the served
    HTML.
- **Provider auto-attach:** through the public provider API, assert the bridge becomes
  active in dev, respects `autoAttachDevTools={false}`, does not double-attach when a manual
  `attachDevTools` call is also present, and is absent in a production build.

## Open implementation details (not blocking the design)

- Exact keyboard shortcut for the toggle.
- Whether `inspector-link.ts` / the Vercel link is retired or retained for the admin path.
- Precise Next.js serving + injection mechanism (route handler vs. `public/` copy; client
  entry vs. `<Script>`; app-router vs. pages-router) — the long pole, sequenced last.
- Whether to add per-peer subscription namespacing now or defer (multi-peer conflict).
- Where `createEmbeddedJazzClient` is re-exported across framework entrypoints.

## Review provenance

This spec was reviewed by an external model (GLM-5.2 via the `glm`/Z.ai Claude alias); a
second reviewer (DeepSeek via opencode) produced no usable output. The reviewer's
highest-impact findings — the `event.source` relay invariant, the 10 MB packaging
correction, CSP-safe external loader, auto-attach opt-out/DCE, Next.js as the long pole,
collapsing the redundant `embedded` build mode, multi-peer conflict, and the iframe
isolation rationale — were independently verified against the source and folded into the
sections above.
