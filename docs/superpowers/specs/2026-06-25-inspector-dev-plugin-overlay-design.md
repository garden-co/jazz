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

Because the runtime side already uses `window.postMessage` and the iframe can only reach
the parent via `window.parent`, the loader relay is required: it forwards messages from
the iframe into the top window (so `attachDevTools` receives them) and from the top window
into `iframe.contentWindow` (so the inspector receives replies). `attachDevTools` is left
**completely untouched**.

## Components

### A. Inspector "embedded" entry — `packages/inspector` (new)

- New `embedded.html` → `src/embedded.tsx`, a near-copy of `src/devtools-main.tsx`.
- Builds its client via the new `createEmbeddedJazzClient()` and renders
  `DevtoolsProvider runtime="embedded"`, reusing `InspectorRoutes` and the existing
  schema-over-`announce` flow.
- A new `InspectorRuntime` value `"embedded"`. It behaves like `"extension"` for query
  propagation (`local-only` default, toggleable) — i.e. it sees the live client's view,
  not `"full"`/admin. (`devtools-context.tsx` updated accordingly.)
- New build: `--mode embedded` → `dist-embedded/`. The existing `build:web` and
  `build:extension` are unchanged; `build` runs all three.

### B. `createEmbeddedJazzClient()` transport — `jazz-tools/react` (new)

- Sibling to `createExtensionJazzClient()`. Same bridge protocol, but the transport uses
  `window.parent.postMessage(msg, "*")` to send and `window.addEventListener("message")`
  to receive. No `chrome.*` — works in any iframe.
- Exported from `jazz-tools/react` (and the other framework entrypoints as needed).

### C. Injected loader + relay — served by the plugin (new)

- A tiny script the plugin injects into the dev page. Responsibilities:
  1. Render a floating `⚡` toggle in its own container (high z-index, isolated so the app
     can't style it) and a resizable panel holding `<iframe src="/__jazz/embedded">`.
  2. **Relay** bridge messages between the app's top `window` and `iframe.contentWindow`,
     mirroring `extension-panel.ts`'s content-script role.
- Collapsed by default; open/closed state and panel size persisted in `localStorage`.
- Optional keyboard shortcut to toggle (default chosen during implementation).

### D. Provider auto-attach — `jazz-tools` react/svelte/vue/solid (change)

- In dev only (`import.meta.env.DEV` / `process.env.NODE_ENV !== "production"`), the
  provider calls `attachDevTools(client, wasmSchema)` itself, with the same idempotency
  guard the example uses (track already-attached clients).
- `attachDevTools` stays exported for manual and extension use. In production the
  auto-attach is a no-op, so nothing is wired and nothing leaks.

### E. Dev plugin: serve + inject — `vite.ts`, `next.ts`, `sveltekit.ts` (change)

- Serve `dist-embedded/` (copied into jazz-tools, see F) as static assets at
  `/__jazz/embedded`.
- Inject the loader: Vite via `transformIndexHtml`; SvelteKit via its Vite-style hooks;
  Next via an injected client entry/script. Guarded to dev/`serve` only.
- Replace the `buildInspectorLink(...)` → Vercel `console.log` with a message pointing at
  the local overlay (the hosted link/`inspector-link.ts` may be retired or kept for the
  admin/standalone path — decided during implementation).

### F. Packaging — copy the embedded build into jazz-tools

- At jazz-tools build time, **copy `inspector`'s `dist-embedded/` into jazz-tools'
  published package** (e.g. `jazz-tools/dist/inspector-embedded/`), mirroring how the Next
  plugin already copies `jazz-wasm` bytes.
- Keeps a single install (`jazz-tools`); no new published package. The inspector remains
  its own source and still produces the Vercel (`dist/`) and extension (`dist-extension/`)
  builds. Build ordering: inspector's embedded build runs before jazz-tools packages it.

## UX

- Floating `⚡` toggle, fixed bottom-right corner, isolated container.
- Click toggles a resizable panel containing the iframe; collapsed by default; state + size
  persisted in `localStorage`.
- During app load, before `attachDevTools` announces, the embedded entry shows its existing
  "Waiting for runtime devtools connection…" state, then data appears.

## Production safety

- Plugin serves `/__jazz/*` and injects the loader **only** when `command === "serve"` /
  dev (matches the existing `configureServer` guard). Nothing ships to production builds.
- Provider auto-attach is dev-gated, so `attachDevTools` is a no-op in production.
- No admin secret in the browser — the overlay connects via the in-process bridge only.

## Testing

Black-box / public-API tests, per `crates/jazz-tools/TESTING_GUIDELINES.md` and the repo's
preference for integration tests:

- **Inspector (browser):** a Playwright test (alongside the existing `test:browser`) loads a
  small host page that calls `attachDevTools`, embeds the embedded entry in an iframe, and
  asserts the relay handshake completes and data renders.
- **Plugin (integration):** boot the Vite plugin's dev server and assert `/__jazz/embedded`
  serves assets and the loader is injected into the served HTML.
- **Provider auto-attach:** through the public provider API, assert the bridge becomes
  active in dev and stays a no-op in production.

## Open implementation details (not blocking the design)

- Exact keyboard shortcut for the toggle.
- Whether `inspector-link.ts` / the Vercel link is retired or retained for the admin path.
- Precise injection mechanism for Next.js (client entry vs. injected script tag).
- Where `createEmbeddedJazzClient` is re-exported across framework entrypoints.
