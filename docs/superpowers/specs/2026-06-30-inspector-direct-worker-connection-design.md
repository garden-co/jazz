# Inspector: direct worker connection (drop the devtools protocol)

- **Date:** 2026-06-30
- **Status:** Approved design — ready for implementation plan
- **Scope:** `packages/inspector`, `packages/jazz-tools` (dev-tools + overlay loader)

## Goal

Make the embedded inspector overlay connect to the data layer as a **normal Jazz
worker client** (the same path the standalone web build already uses), and delete
the entire devtools bridge protocol. Kill the browser-extension / devtools build.
Keep only two builds: `web` (standalone) and `embedded` (overlay).

A second worker client cannot see the host app's active subscription traces
(those are client-local, in the host page's `Db`, with JS call-site stacks). We
keep those by reading the host `Db` **directly across the same-origin iframe
boundary** — no message protocol.

This is an experiment; the bar is "works end-to-end for the overlay against a
real backend," not "feature-parity with every bridge edge case."

## Decisions (locked)

1. **Subscription tracking:** keep the host app's traces _with stacks_, via a thin
   same-origin link — the overlay iframe reads the host `Db`'s
   `getActiveQuerySubscriptions()` / `onActiveQuerySubscriptionsChange()` directly.
   No request/response protocol, no proxy `Db`.
2. **Connection identity:** the inspector connects as its own client with the
   host's injected credentials. **Both** modes, user-switchable: **Admin**
   (adminSecret, bypasses policies — default) ⇄ **App-user** (JWT, policies
   enforced). Switching reconnects.
3. **Handoff mechanism (Approach A):** the host loader publishes a read-only
   `window.__jazzInspectorHost` handle; the same-origin overlay reads
   `window.parent.__jazzInspectorHost`. No postMessage, no serialization.

### Accepted trade-offs

- The inspector shows **server-synced state**, not the host's exact local `Db`.
  Local-only / not-yet-synced rows do not appear. (Inherent to "normal worker
  connection"; acceptable for this experiment.)
- Data path is identical to standalone, so the overlay gains standalone's
  behaviors: mutation durability `"edge"`, permissions panel available.

## Current architecture (for reference)

- **Builds** (`packages/inspector/vite.config.ts`): `web`→`dist` (standalone,
  `App.tsx`, `createJazzClient`, direct connection); `extension`→`dist-extension`
  (Chrome devtools panel, chrome-port bridge); `embedded`→`dist-embedded`
  (overlay iframe, `embedded.tsx`→`inspector-app.tsx`, postMessage bridge).
- **Bridge** (`packages/jazz-tools/src/dev-tools/`): `protocol.ts`
  (`DEVTOOLS_BRIDGE_CHANNEL`, request/response/event envelopes, ~9 commands),
  `dev-tools.ts` (`attachDevTools` host side, drives the host's live `JazzClient`,
  `setDevMode(true)`), `extension-panel.ts` (`DevToolsDb`/`DevToolsJazzClient`
  proxy — the inspector's entire "data layer" today, every call a bridge
  round-trip), `parent-window-port.ts` (overlay transport). The overlay relay is
  `packages/jazz-tools/src/dev/inspector-overlay/relay.ts`, wired in `loader.ts`.
- **Embedded client:** `createEmbeddedJazzClient()`
  (`packages/jazz-tools/src/react/create-embedded-jazz-client.ts`) =
  `createExtensionJazzClient()` + parent-window transport — a remote proxy, never
  a real worker.
- **Standalone client (reuse target):** `createJazzClient(config)`
  (`packages/jazz-tools/src/react/create-jazz-client.ts:41`) → `createDb` →
  `DirectConnectionManager.onClientCreated` →
  `client.connectTransport(serverUrl, { jwt_token, admin_secret })`. Config shape:
  `{ appId, serverUrl, env, userBranch, adminSecret, jwtToken?, driver }`.
- **Subscription traces:** `Db.subscribeAll(...)` registers an
  `ActiveQuerySubscriptionTrace` (`{ id, query, table, branches[], tier,
propagation, createdAt, stack? }`) in `Db.activeQuerySubscriptionTraces`, gated
  on `config.devMode`. Exposed via `getActiveQuerySubscriptions()` /
  `onActiveQuerySubscriptionsChange()` / `setDevMode()`
  (`packages/jazz-tools/src/runtime/db.ts`). The bridge is the only thing
  carrying these out of the host runtime today.

## Target architecture

### The host handle

The overlay loader runs in the host window and already receives `db` via
`startInspectorOverlay(db)`. Instead of `attachDevTools(db)` + relay, it:

1. Calls `db.setDevMode(true)` (so traces are captured).
2. Publishes a read-only handle on the host window:

```ts
// set by the loader (host window); read by the overlay via window.parent
interface JazzInspectorHost {
  /** Connection config sourced from the host db; both credentials present. */
  getConnectionConfig(): {
    appId: string;
    serverUrl: string;
    env: string;
    userBranch?: string;
    adminSecret?: string;
    jwtToken?: string;
  };
  /** The host's already-resolved runtime schema (avoids a server fetch). */
  wasmSchema: WasmSchema;
  /** Host app's live subscription traces (with stacks), devMode-gated. */
  getActiveQuerySubscriptions(): ActiveQuerySubscriptionTrace[];
  onActiveQuerySubscriptionsChange(cb: () => void): () => void; // returns unsubscribe
}
window.__jazzInspectorHost = host; // dev-only; `__` signals internal
```

The handle is a thin façade over the existing `Db` methods — no new runtime
capability, just a same-origin read surface. It persists on the host window
across iframe reloads.

### Overlay client creation (replaces the bridge)

`embedded.tsx` / a new helper:

1. Read `window.parent.__jazzInspectorHost`. If absent → render a clear
   "inspector not attached" state (host not in dev / loader didn't run).
2. Build connection config from `getConnectionConfig()`, choosing credential by
   the current identity mode (Admin → `adminSecret`; App-user → `jwtToken`),
   with `driver: { type: "memory" }` (ephemeral).
3. `createJazzClient(config)` → real `Db` → worker connection.
4. Take `wasmSchema` from the handle (no server fetch needed).
5. Provide the subscription accessors to Live Query via context.

`inspector-app.tsx` stops using `getRegisteredWasmSchema()` /
`onDevToolsPortDisconnect()` (bridge-coupled) and the hardwired
`runtime="extension"`.

### Connection identity toggle

A control in the **Settings** tab: `Admin ⇄ App-user (JWT)`, default Admin.
State stored in the overlay (localStorage, like other inspector prefs). Switching
tears down the current client (`client.shutdown()`) and re-creates it with the
other credential, then re-resolves schema/queries. Disabled/hidden if the
corresponding credential is absent in the handle (e.g., no JWT).

### Subscription tracking

`DevtoolsContext` gains a subscription source. For the overlay it is the host
handle's `getActiveQuerySubscriptions()` + `onActiveQuerySubscriptionsChange()`;
for standalone it stays `fetchServerSubscriptions(...)`. Live Query's two existing
branches (`ExtensionLiveQuery` / `StandaloneLiveQuery`) are renamed/repointed:
the "extension" branch reads from the handle instead of the jazz-tools bridge
cache. Data shape (`ActiveQuerySubscriptionTrace[]`) is unchanged → the table UI
is untouched. On iframe unload, call the unsubscribe returned by
`onActiveQuerySubscriptionsChange` so the host doesn't retain a dead callback.

### Runtime-model refactor

`InspectorRuntime = "standalone" | "extension"` → `"standalone" | "overlay"`.
Re-audit each branch:

| Site                                         | Today (`extension`) | New (`overlay`, direct client)               |
| -------------------------------------------- | ------------------- | -------------------------------------------- |
| `live-query/index.tsx`                       | bridge cache        | host handle feed (keeps stacks)              |
| `TableDataGrid.tsx` durability               | `"local"`           | `"edge"` (server-backed)                     |
| `TableSchemaDefinition.tsx` permissions      | hidden              | shown (fetchable)                            |
| `data-explorer/index.tsx` propagation switch | shown               | re-evaluate; likely `"full"` like standalone |

`isOverlay` stays orthogonal (overlay chrome: Close button, launcher-hide
setting). It continues to be passed by `embedded.tsx`.

### Build & code removal

**Remove:**

- `package.json` `build:extension` + its entry in `build`; the `isExtensionBuild`
  branch in `vite.config.ts`.
- Entries/assets: `devtools-tab.html`, `devtools.html`, `src/devtools-main.tsx`,
  `src/devtools/main.js`, `chrome-extension/`.
- The bridge: `packages/jazz-tools/src/dev-tools/` (`protocol.ts`,
  `dev-tools.ts`/`attachDevTools`, `extension-panel.ts`, `parent-window-port.ts`,
  `index.ts`, bridge tests), the bridge parts of `auto-attach.ts`, and
  `dev/inspector-overlay/relay.ts` + its wiring in `loader.ts`.
- `react/create-embedded-jazz-client.ts`, `createExtensionJazzClient` in
  `react/create-jazz-client.ts`, and their `react/index.ts` exports.

**Keep:**

- On `Db`: `setDevMode`, `getActiveQuerySubscriptions`,
  `onActiveQuerySubscriptionsChange`, `ActiveQuerySubscriptionTrace` — the
  runtime-side source of truth.
- Overlay serving + chrome: `dev/inspector-overlay/serve.ts`, `loader.ts`
  (minus the relay), the resize/close/launcher behavior already shipped.
- `createJazzClient` (the direct path) and the whole standalone build.

**Builds left:** `web` (standalone, `dist`) + `embedded` (overlay,
`dist-embedded`).

## Error handling / edge cases

- **Handle missing:** `window.parent.__jazzInspectorHost` undefined → overlay
  shows "inspector not attached" (no crash).
- **Connection failure / bad config / server down:** error state with retry.
- **Expired/invalid JWT (App-user mode):** surface the error; offer switch back
  to Admin.
- **`devMode` off:** Live Query shows an empty state with a hint ("enable devMode
  to capture subscriptions").
- **Identity switch mid-session:** clean teardown (`shutdown()`) before
  reconnect; guard against overlapping reconnects.
- **iframe reload:** re-reads the host global (persists); re-establishes its own
  client and subscription listener.
- **Listener leak:** unsubscribe the host `onChange` on iframe unload / client
  teardown.

## Testing

- **Standalone:** unchanged; existing inspector unit tests must stay green.
- **Overlay direct connection:** rewrite the Playwright spec
  (`tests/browser/overlay.spec.ts`) from bridge-based to: host attaches → loader
  publishes `__jazzInspectorHost` → overlay opens its own client → Data Explorer
  shows rows → Live Query shows a host subscription with a stack.
- **Identity toggle:** unit/integration — switching credentials reconnects and
  re-resolves; missing-credential disables the mode.
- **Subscription feed adapter:** unit — overlay reads handle, standalone reads
  server introspection; both produce `ActiveQuerySubscriptionTrace[]`.

## Out of scope / risks

- **Out of scope:** local-only (unsynced) data visibility; reviving the Chrome
  extension; multi-tab/multi-Db host pages (assume one host `db`).
- **Risk — credential exposure:** adminSecret/JWT live on a host `window` global.
  Dev-only (already in the dev bundle), `__`-prefixed; acceptable for a dev tool,
  but call it out in code comments.
- **Risk — runtime branch audit:** the `runtime === "extension"` → `"overlay"`
  swap touches several components; each must be verified against a real backend,
  not just typechecked.
- **Risk — schema drift:** injecting the host's `wasmSchema` assumes it matches
  the server schema the inspector connects to; if they differ, prefer a server
  fetch (like standalone). Validate during implementation.
