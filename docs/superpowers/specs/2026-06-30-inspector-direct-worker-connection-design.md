# Inspector: reuse the host Db (drop the devtools protocol)

- **Date:** 2026-06-30
- **Status:** Approved design — ready for implementation plan
- **Scope:** `packages/inspector`, `packages/jazz-tools` (dev-tools + overlay loader)

## Goal

Make the embedded inspector overlay use the host app's **live, already
worker-connected `Db` directly** instead of proxying every call over the devtools
bridge. Delete the bridge protocol. Kill the browser-extension / devtools build.
Keep only two builds: `web` (standalone) and `embedded` (overlay).

Because the overlay iframe is **same-origin** with the host page, it can read the
host `Db` straight off `window.parent` — no message protocol, no proxy `Db`, no
second connection. "Use the normal worker connection for the data layer" is
satisfied by reusing the host `Db`, which _is_ the normal worker connection.

This is an experiment; the bar is "works end-to-end for the overlay against a
real backend," not feature-parity with every bridge edge case.

## Decisions (locked)

1. **Data layer (default): reuse the host `Db` directly.** The host publishes its
   live `db` on `window.__jazzInspectorHost`; the overlay reads
   `window.parent.__jazzInspectorHost.db` and wraps it in its own
   `SubscriptionsOrchestrator` (iframe-local query cache). The inspector sees the
   host's exact state — **including local-only / unsynced rows** — and inherits
   the host app's identity. Its own queries are tagged
   `visibility: "hidden_from_live_query_list"` (already done in
   `TableDataGrid.tsx:718`) so they don't pollute the host's Live Query.
2. **Admin override (optional).** A toggle opens a **separate**
   `createJazzClient(adminConfig)` connection using the host's adminSecret to
   bypass row-level policies and "see everything." Default off. Toggling swaps the
   active _data_ client (host-Db ⇄ admin connection) and tears the other down.
3. **Subscriptions: always from the host `Db`.** Live Query reads
   `host.db.getActiveQuerySubscriptions()` / `onActiveQuerySubscriptionsChange()`
   (traces _with JS call-site stacks_), **independent of the data mode** — even in
   admin mode the subscription feed stays the host's. No protocol.
4. **Handoff (same-origin handle).** The loader publishes a read-only
   `window.__jazzInspectorHost = { db, getConnectionConfig(), wasmSchema,
session }`. The overlay reads it via `window.parent`. No postMessage.

### Central risk — validate first

**Cross-realm `Db` use.** The overlay is a separate iframe realm with its own
jazz-tools bundle; it would drive the host-realm `Db` instance. The boundary is
plain data + callbacks (query JSON in, rows out, subscription callbacks), and the
WASM runtime + broker worker stay in the host realm — so it _should_ work, and in
dev the host and the `embedded` build share the same jazz-tools version. But this
is the load-bearing assumption. **Plan step 1 is a spike** proving `useAll`,
mutations, and the subscription feed work against
`window.parent.__jazzInspectorHost.db`.

- **Fallback if a method misbehaves across realms:** wrap only the offending `Db`
  methods in a thin same-origin in-realm shim (still no request/response
  protocol), or — worst case — make the separate-connection model the default and
  keep host-Db reuse for the subscription feed only.

### Accepted trade-offs

- **Default mode** depends on cross-realm `Db` access (the risk above) and edits
  go through the host's identity/session.
- **Admin mode** uses a separate synced client: loses local-only visibility, gains
  see-everything; mutations are admin-attributed.

## Current architecture (for reference)

- **Builds** (`packages/inspector/vite.config.ts`): `web`→`dist` (standalone,
  `App.tsx`, `createJazzClient`); `extension`→`dist-extension` (Chrome devtools
  panel, chrome-port bridge); `embedded`→`dist-embedded` (overlay iframe,
  postMessage bridge).
- **Bridge** (`packages/jazz-tools/src/dev-tools/`): `protocol.ts`, `dev-tools.ts`
  (`attachDevTools`, `setDevMode(true)`), `extension-panel.ts`
  (`DevToolsDb`/`DevToolsJazzClient` proxy — the inspector's data layer today,
  every call a round-trip), `parent-window-port.ts`. Overlay relay:
  `dev/inspector-overlay/relay.ts`, wired in `loader.ts`.
- **Embedded client:** `createEmbeddedJazzClient()` =
  `createExtensionJazzClient()` + parent-window transport — a remote proxy.
- **`JazzClient` shape** (`react/create-jazz-client.ts`): `{ db, session,
manager: SubscriptionsOrchestrator, shutdown }`. `createJazzClient(config)`
  builds one with a fresh `createDb(config)` + a `SubscriptionsOrchestrator`.
- **Subscription traces** (`runtime/db.ts`): `subscribeAll` registers an
  `ActiveQuerySubscriptionTrace` (`{ id, query, table, branches[], tier,
propagation, createdAt, stack? }`), gated on `config.devMode`;
  `getActiveQuerySubscriptions()` returns only `visibility === "public"` traces;
  `onActiveQuerySubscriptionsChange()` / `setDevMode()` round it out.

## Target architecture

### The host handle

The loader runs in the host window and gets `db` via `startInspectorOverlay(db)`.
Instead of `attachDevTools(db)` + relay it:

1. Calls `db.setDevMode(true)`.
2. Publishes:

```ts
// set by the loader (host window); read by the overlay via window.parent
interface JazzInspectorHost {
  db: Db; // the host's live, worker-connected Db (default data source + sub feed)
  /** For the admin-override connection only. */
  getConnectionConfig(): {
    appId: string;
    serverUrl: string;
    env: string;
    userBranch?: string;
    adminSecret?: string;
  };
  wasmSchema: WasmSchema; // host's resolved runtime schema
  session: Session | null; // host identity, for the wrapping client
}
window.__jazzInspectorHost = host; // dev-only; `__` signals internal
```

### Default data client (host-Db reuse)

The overlay reads `window.parent.__jazzInspectorHost` and builds a thin
`JazzClient` around the host `db` — no new connection:

```ts
const host = window.parent.__jazzInspectorHost;
const client: JazzClient = {
  db: host.db,
  session: host.session,
  manager: new SubscriptionsOrchestrator({ appId }, host.db, host.session),
  async shutdown() {
    await this.manager.shutdown();
  }, // do NOT shut down the host db
};
```

`JazzClientProvider client={client}` then drives the existing `useAll`/`useDb`
hooks against the host `db`. The orchestrator (cache) is iframe-local; only `Db`
calls cross the realm boundary.

### Admin override

A toggle in the **Settings** tab (default off, persisted in localStorage). When
on, build a **separate** `createJazzClient({ ...getConnectionConfig(),
adminSecret, driver: { type: "memory" } })` and provide _that_ client instead;
on toggle-off, `shutdown()` it and fall back to the host-Db client. Hidden if no
`adminSecret` in the handle.

### Subscription tracking

Live Query always reads `host.db.getActiveQuerySubscriptions()` +
`onActiveQuerySubscriptionsChange()` (with stacks) — regardless of data mode.
Same `ActiveQuerySubscriptionTrace[]` shape as today, so the table UI is
unchanged. Unsubscribe on iframe unload. Standalone keeps `fetchServerSubscriptions`.

### "Add APIs on the Db" (per the brainstorm)

Anything the inspector currently gets from the bridge but isn't a plain `Db`
method should become one (e.g., listing tables from schema, permissions, schema
hashes), rather than a bridge command. Audit during implementation; in admin mode
the server-fetch paths (permissions, schema hashes) remain available.

### Runtime-model refactor

`InspectorRuntime = "standalone" | "extension"` → `"standalone" | "overlay"`.
Re-audit each branch:

| Site                                    | Today (`extension`) | New (`overlay`)                               |
| --------------------------------------- | ------------------- | --------------------------------------------- |
| `live-query/index.tsx`                  | bridge cache        | host `db` feed (keeps stacks)                 |
| `TableDataGrid.tsx` durability          | `"local"`           | default: host `db` semantics; admin: `"edge"` |
| `TableSchemaDefinition.tsx` permissions | hidden              | shown (host `db` / admin fetch)               |
| `data-explorer/index.tsx` propagation   | shown               | re-evaluate under host `db`                   |

`isOverlay` stays orthogonal (Close button, launcher-hide setting); still passed
by `embedded.tsx`.

### Build & code removal

**Remove:** `build:extension` + its entry in `build`; the `isExtensionBuild`
vite branch; `devtools-tab.html`, `devtools.html`, `src/devtools-main.tsx`,
`src/devtools/main.js`, `chrome-extension/`; the bridge
(`packages/jazz-tools/src/dev-tools/` protocol/attachDevTools/extension-panel/
parent-window-port/index + tests, the bridge parts of `auto-attach.ts`),
`dev/inspector-overlay/relay.ts` + its `loader.ts` wiring;
`react/create-embedded-jazz-client.ts`, `createExtensionJazzClient`, their exports.

**Keep:** on `Db` — `setDevMode`, `getActiveQuerySubscriptions`,
`onActiveQuerySubscriptionsChange`, `ActiveQuerySubscriptionTrace`; the overlay
serving + chrome (`dev/inspector-overlay/serve.ts`, `loader.ts` minus relay,
resize/close/launcher); `createJazzClient` (for the admin override) + the
standalone build.

**Builds left:** `web` + `embedded`.

## Error handling / edge cases

- **Handle missing** → "inspector not attached" state, no crash.
- **Cross-realm method failure** → in-realm shim for that method (see fallback).
- **Admin toggle** → clean `shutdown()` of the outgoing client before swapping;
  guard overlapping switches; hide toggle if no `adminSecret`.
- **`devMode` off** → empty Live Query with a hint.
- **iframe reload** → re-reads the host global; rebuilds its wrapping client +
  subscription listener.
- **Listener leak** → unsubscribe `onChange` on unload; never `shutdown()` the
  host `db` (only the inspector's own orchestrator / admin client).

## Testing

- **Step 0 — cross-realm spike** (gating): prove `useAll` + a mutation + the
  subscription feed work against `window.parent.__jazzInspectorHost.db` in a real
  two-realm setup. Decide host-Db-direct vs fallback before building further.
- **Standalone:** unchanged; existing inspector unit tests stay green.
- **Overlay default path:** rewrite `tests/browser/overlay.spec.ts` — host
  publishes handle → overlay wraps host `db` → Data Explorer shows rows (incl. a
  local-only row) → Live Query shows a host subscription with a stack.
- **Admin override:** toggling opens/closes the separate connection and re-resolves
  data; hidden when no adminSecret.

## Out of scope / risks

- **Out of scope:** reviving the Chrome extension; multi-Db host pages (assume one
  host `db`).
- **Risk — cross-realm (load-bearing):** see "Central risk"; spike first.
- **Risk — credential exposure:** adminSecret reachable via the host handle
  (dev-only, `__`-prefixed); call out in code.
- **Risk — runtime branch audit:** `"extension"` → `"overlay"` touches several
  components; verify against a real backend, not just typecheck.
