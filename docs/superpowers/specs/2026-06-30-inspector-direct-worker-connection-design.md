# Inspector: own worker connection + minimal live-query link (drop the devtools protocol)

- **Date:** 2026-06-30
- **Status:** Approved design — ready for implementation plan
- **Scope:** `packages/inspector`, `packages/jazz-tools` (dev-tools + overlay loader, framework bindings)

## Goal

Make the embedded inspector overlay connect to the data layer as its **own
normal Jazz worker client** (the standalone path), and replace the heavy postMessage
"devtools protocol" bridge with a **minimal one-way parent link** that carries
only the host app's active subscription list (no call-site stacks). Kill the
browser-extension / devtools build. Keep two builds: `web` (standalone) +
`embedded` (overlay).

## Why this shape (and why NOT host-`Db` reuse)

An earlier iteration proposed the overlay reusing the host page's live `Db`
directly across the same-origin iframe boundary. Two independent reviews
(Opus + GLM) showed that's riskier than it looks:

- **Cross-realm `instanceof` breaks on values, not just the query.** Host-realm
  `Uint8Array`/`Date` fail the inspector's own-realm checks → bytea/timestamp
  columns misrender on read and **throw on write** (`query-adapter.ts:120-137`);
  `instanceof Error` loses mutation-rejection reasons (`TableDataGrid.tsx:916`).
- **A dead iframe listener can break the host app** — the inspector's
  subscription callback runs synchronously inside the host's own `subscribeAll`
  (`db.ts:1536-1571`).

The overlay's **own** worker client keeps every `Db` value/error in the iframe
realm, so all of that evaporates. We accept the inherent trade-off (a separate
client sees **server-synced state**, not the host's local-only rows) — and we
keep the live-query feature by linking just the subscription _list_ from the host.

## Decisions (locked)

1. **Data: own worker connection.** The overlay calls `createJazzClient(config)`
   (the standalone path) → real `Db` → `DirectConnectionManager.connectTransport`.
   The loader injects the host's connection config (incl. `schemaHash`) into the
   same-origin iframe; the overlay resolves schema from the server like standalone.
2. **Live queries: a minimal one-way parent link.** The loader (host realm) owns
   the `db.onActiveQuerySubscriptionsChange` listener, serializes the active
   subscription list **without stacks**, and posts it to the iframe. The overlay's
   Live Query shows the host app's active subscriptions. **Stacks are dropped
   (accepted).**
3. **Identity: admin by default, switchable to app-user JWT.** Both credentials
   are injected; a Settings toggle switches and reconnects. (Carried from the
   earlier decision; valid for the own-connection model.)
4. **Handoff:** connection config via a read-once same-origin handle
   (`window.parent.__jazzInspectorHost`); subscriptions via a one-way postMessage
   pushed on change. No request/response protocol, no proxy `Db`.

### Accepted trade-offs

- Server-synced view, not the host's local `Db` (local-only/unsynced rows don't
  appear). Inherent to a normal worker connection.
- Live Query loses the JS call-site stacks; it shows the subscription metadata
  (table, query, tier, propagation, branches, createdAt).
- Mutation durability `"edge"`, permissions panel available (== standalone).

## Current architecture (for reference)

- **Builds** (`packages/inspector/vite.config.ts`): `web`→`dist` (standalone,
  `App.tsx`, `createJazzClient`); `extension`→`dist-extension` (Chrome devtools
  panel); `embedded`→`dist-embedded` (overlay iframe, postMessage bridge).
- **Bridge** (`packages/jazz-tools/src/dev-tools/`): `protocol.ts` (~9 commands),
  `dev-tools.ts` (`attachDevTools`, `setDevMode(true)`), `extension-panel.ts`
  (`DevToolsDb`/`DevToolsJazzClient` proxy — the inspector's data layer today),
  `parent-window-port.ts`. Overlay relay: `dev/inspector-overlay/relay.ts`.
- **Standalone client (the reuse target):** `createJazzClient(config)`
  (`react/create-jazz-client.ts:41`) → `createDb` →
  `DirectConnectionManager.onClientCreated` →
  `client.connectTransport(serverUrl, { jwt_token, admin_secret })`. Config:
  `{ appId, serverUrl, env, userBranch, adminSecret, jwtToken?, driver }`. Schema
  resolved via `fetchStoredWasmSchema(appId, adminSecret, schemaHash)`.
- **Subscription traces** (`runtime/db.ts`): `subscribeAll` registers an
  `ActiveQuerySubscriptionTrace` (`{ id, query, table, branches[], tier,
propagation, createdAt, stack? }`) when `config.devMode`;
  `getActiveQuerySubscriptions()` (public traces only) /
  `onActiveQuerySubscriptionsChange()` / `setDevMode()`.

## Target architecture

### Host side (overlay loader)

`startInspectorOverlay(db)` (host window) replaces `attachDevTools(db)` + relay with:

1. `db.setDevMode(true)` (so traces are captured) — **as early as possible**
   (see devMode caveat below).
2. Publish a read-once config handle:
   ```ts
   window.__jazzInspectorHost = {
     getConnectionConfig(): {
       appId; serverUrl; env; userBranch?; adminSecret?; jwtToken?; schemaHash;
     };
   };
   ```
3. Own the subscription listener **in the host realm** and push serialized
   snapshots (no stacks) to the iframe, one-way:
   ```ts
   const push = () =>
     iframe.contentWindow?.postMessage(
       {
         type: "jazz-inspector:subscriptions",
         list: serializeNoStacks(db.getActiveQuerySubscriptions()),
       },
       origin,
     );
   const stop = db.onActiveQuerySubscriptionsChange(push);
   push(); // initial
   ```
   Because the host realm owns the listener and only plain JSON crosses, there's
   no dead-iframe-listener hazard and no cross-realm value issue.

### Overlay (iframe)

`embedded.tsx`:

1. Read `window.parent.__jazzInspectorHost.getConnectionConfig()`. Absent →
   "inspector not attached" state.
2. `createJazzClient({ ...config, credential-by-mode, driver: { type: "memory" } })`
   → own worker connection. Resolve schema from the server via `schemaHash`
   (standalone path) — no host-schema injection, no cross-realm schema.
3. Live Query subscribes to the `window` `message` listener for
   `jazz-inspector:subscriptions` and renders the pushed list.

### Connection identity toggle

Settings tab: **Admin ⇄ App-user (JWT)**, default Admin, persisted in
localStorage. Switching `shutdown()`s the current client and re-creates it with
the other credential. Hidden per missing credential.

### Live-query link details

- One-way, host→iframe, plain JSON, **no `stack` field**.
- The serialized shape is `ActiveQuerySubscriptionTrace` minus `stack` — the Live
  Query table already renders these columns; the stack column is removed.
- **devMode caveat:** traces are registered only for subscriptions opened _after_
  `setDevMode(true)` (`db.ts:1552`). Subscriptions the host app opened before the
  inspector attached won't appear. Mitigate by calling `setDevMode(true)` as early
  as the dev plugin can, and document the limitation.

### Runtime-model refactor

`InspectorRuntime = "standalone" | "extension"` → `"standalone" | "overlay"`.
Overlay behaves like standalone for data (own client: `"edge"` durability,
permissions panel shown, propagation `"full"`); its Live Query source is the
parent-link message instead of the server-introspection poll. `isOverlay` stays
orthogonal (Close button, launcher-hide setting), still passed by `embedded.tsx`.

### Build & code removal (bigger than it looks — review H1)

Deleting the bridge is a **cross-package, public-API-affecting refactor**, not a
localized delete:

- `jazz-tools/src/index.ts:230` re-exports `dev-tools/*` (public); `attachDevTools`
  is re-exported from `react/`, `vue/`, `svelte/`, `solid/` barrels — decide
  explicitly: breaking removal vs deprecated shim.
- `createDbFromInspectedPage` is imported by `react/`, `web/`, `vue/`, `svelte/`
  `create-jazz-client.ts`; `startInspectorOnce` by the react/vue/solid providers
  (`auto-attach.ts:32`). Rewrite `startInspectorOnce` to publish the handle + own
  the subscription push instead of `attachDevTools` + relay.

**Remove:** `build:extension` + the `isExtensionBuild` vite branch;
`devtools-tab.html`, `devtools.html`, `src/devtools-main.tsx`, `src/devtools/main.js`,
`chrome-extension/`; the `dev-tools/` bridge (protocol, `attachDevTools`,
`extension-panel` proxy, `parent-window-port`) + the overlay `relay.ts` and its
`loader.ts` wiring; `create-embedded-jazz-client.ts`, `createExtensionJazzClient`,
their exports.

**Keep:** on `Db` — `setDevMode`, `getActiveQuerySubscriptions`,
`onActiveQuerySubscriptionsChange`, `ActiveQuerySubscriptionTrace`;
`createJazzClient` + `fetchStoredWasmSchema`; the standalone build; the overlay
serving + chrome (`serve.ts`, `loader.ts` minus relay; resize/close/launcher).

**Builds left:** `web` + `embedded`.

## Error handling / edge cases

- **Handle missing** → "inspector not attached" state.
- **Connection failure / expired JWT** → error + retry; offer switch to Admin.
- **Identity switch** → `shutdown()` before reconnect; guard overlapping switches;
  hide a mode when its credential is absent; the admin/memory driver needs
  `serverUrl` (`db.ts:1820`) — guard if absent.
- **devMode off / pre-existing subs** → Live Query may be empty or partial; hint.
- **iframe reload** → re-reads config handle, reconnects, re-listens for pushes.

## Testing

No cross-realm spike needed (the overlay uses a normal own-realm client). Focus:

- **Standalone:** unchanged; existing inspector unit tests stay green.
- **Overlay own connection:** rewrite `tests/browser/overlay.spec.ts` — loader
  injects config → overlay opens its own client → Data Explorer shows rows →
  Live Query shows a host subscription (no stack column).
- **Identity toggle:** switching reconnects/re-resolves; hidden when credential
  absent.
- **Parent link:** opening a `subscribeAll` in the host app pushes an updated list
  to the overlay; closing it removes it.

## Out of scope / risks

- **Out of scope:** reviving the Chrome extension; local-only data visibility;
  call-site stacks in Live Query.
- **Risk — deletion blast radius (review H1):** the bridge is woven through all
  framework bindings + a public export; the refactor + the public-API decision is
  the largest implementation risk. Sequence it carefully.
- **Risk — devMode trace timing:** pre-attach subscriptions are untraced; set
  devMode early, document the limit.
- **Risk — credential exposure:** adminSecret/JWT reachable via the host handle
  (dev-only, `__`-prefixed); call out in code.
