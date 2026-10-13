# Jazz Lite experiment

Question: what if a browser app used a "lite" Jazz client with roughly the same
query/transaction API, but no local storage, no worker and no WASM, talking
directly to the server, which sends results per subscription? How much smaller
would the browser bundle be?

Not for merge. Everything here runs against the published `jazz-tools@2.0.0-alpha.59`
so the numbers match what apps ship today.

## Answer

For the same todo app (the `todo-client-localfirst-ts` example), what the page can load
(1 KB = 1024 bytes; gzip -9, brotli q11):

| Build                                                  |         raw |        gzip |      brotli |
| ------------------------------------------------------ | ----------: | ----------: | ----------: |
| Full Jazz (WASM + broker workers + main JS)            |     20.2 MB |     6.50 MB |     4.38 MB |
| of which WASM                                          |     18.4 MB |     6.08 MB |     4.03 MB |
| of which workers                                       |     1.17 MB |     0.25 MB |     0.21 MB |
| of which main-thread JS                                |     0.60 MB |     0.17 MB |     0.14 MB |
| **Jazz Lite, as built here**                           | **99.7 KB** | **30.1 KB** | **26.1 KB** |
| Jazz Lite with a DSL-only schema entry (projected)     |     45.9 KB |     13.7 KB |     12.2 KB |
| Lite client runtime alone (`src/client.ts` + protocol) |      6.2 KB |      2.2 KB |      2.0 KB |

So roughly **220x smaller over gzip** as built (0.45% of today's bundle), and
about 490x with a schema entry point that leaves out what only servers and the
CLI need. The lite runtime itself is ~2 KB; nearly all of the lite bundle is the
typed schema/query-builder DSL from `jazz-tools`.

Reproduce: `npm install && npm run sizes` in this directory.

### What the lite bundle is made of

`import { schema as s } from "jazz-tools"` brings in the whole `schema`
namespace object (`Object.assign({}, col, { ..., definePermissions, defineMigration })`),
which bundlers cannot tree-shake. Of the ~100 KB minified:

- 31.6 KB `permissions/index` and 6.0 KB `migrations`: only needed where permissions
  and migrations are defined (CLI, server), never by browser query code.
- 19.7 KB `json-schema-metaschemas` + validator/format helpers: JSON column validation.
- 12.5 KB `typed-app` + 7.7 KB `dsl` + 4.9 KB `schema-reader`: the query builder and
  schema lowering a lite client actually needs.
- ~9 KB `@noble/hashes` (blake3 for the schema hash), not needed by a lite client.
- 6.3 KB lite client + protocol, 2.7 KB app code.

The projected row imports `dsl`, `relationships` and `typed-app` directly.

Side finding: a lite build that imports from the `jazz-tools` root still makes
Vite _emit_ the 19 MB WASM and both worker bundles into `dist/`, because the
modules that reference them with `new URL(..., import.meta.url)` are in the
module graph before tree-shaking. Nothing references them at runtime, so they
are never downloaded, but they are deployed. A DSL-only entry point fixes this too.

## What was built

- `src/client.ts`: `createLiteDb({ url, getToken })` with `all`, `one`,
  `subscribe`, `insert`, `update`, `upsert`, `delete`, `transaction`,
  `exclusiveTransaction`. Queries are built with the app's normal `defineApp`
  handles (`app.todos.where(...).include(...)`), and only query JSON
  (`_build()`) crosses the wire. Only types are imported from `jazz-tools`.
- `src/proxy.ts`: the "client proxy on the server". One backend Jazz client
  (NAPI runtime, memory storage, connected upstream like any backend) serves all
  lite connections. Each connection authenticates once with a bearer token and
  gets a session-scoped `Db` via `forRequest()`, so every read and write runs
  with that user's permissions; the proxy never widens access. Per subscription
  it keeps the last result and sends only changed rows, removed ids, and the order
  when it moved.
- `src/protocol.ts`: JSON frames plus a small value codec (Date, bigint, bytes).
- `tests/e2e.test.ts` (5 passing): subscriptions follow inserts, updates and
  deletes; another user can neither read nor write a private row; filtered
  queries with includes; interactive transactions with read-your-writes, rollback
  on throw, and exclusive commits; a one-row update sends ~200 B, not the result.
- `tests/cost.test.ts`: the cost probes below (`JAZZ_LITE_COST=1 npx vitest run tests/cost.test.ts`).

## What it costs

Measured on localhost (4-core container), upstream server in its own process.

Write latency. Nothing is optimistic in the browser any more, so a write becomes
visible after one round trip to the proxy:

| Subscription read tier on the proxy | write waits for | write ack p50 | visible in subscription p50 |
| ----------------------------------- | --------------- | ------------: | --------------------------: |
| `local-first`                       | `local`         |        4.6 ms |                      4.6 ms |
| `local-first`                       | `global`        |        8.1 ms |                      8.1 ms |
| `remote` (backend default)          | `global`        |        6.4 ms |                     18.5 ms |

With full Jazz the same write appears in the subscription synchronously.

Wire: a 100-row subscription's first result is 17.9 KB; flipping one row then
costs 226 B including the write ack.

Memory per connected user (100 users, one subscription of 10 rows each):

| Step                       |     proxy RSS | upstream server RSS |
| -------------------------- | ------------: | ------------------: |
| connected, no subscription |       +32 KiB |                  ~0 |
| 10 writes                  |      +138 KiB |            +156 KiB |
| one live subscription      | **+1.25 MiB** |        **+4.8 MiB** |

The server-side 4.8 MiB per subscription is not lite-specific: it is the
per-subscription query graph every client subscription costs on alpha.59 (the
cost #3993 and the graph-sharing follow-up address). The lite design adds the
proxy's own 1.25 MiB per subscription, because the proxy is a full Jazz client
that materialises each user's query a second time.

## Trade-offs this reveals

1. **No local-first behaviour.** No offline reads or writes, no optimistic
   updates, no instant `insert` return value (writes are promises). Everything
   is one RTT away. Writes in flight when the socket drops are rejected with
   `LiteConnectionLostError` (outcome unknown), never silently retried;
   subscriptions resubscribe with a full result after reconnecting.
2. **Server cost moves from the browser to the server.** Every live query is
   now held server-side twice (proxy runtime + upstream) in this Node design.
3. **The proxy is a second Jazz client in the path.** It adds a hop and its
   own query evaluation. A native endpoint inside `jazz-server` that evaluates
   the session-scoped query directly on server state would remove both, and
   would share compiled graphs across subscribers. That is the shape I'd build
   if this goes further; the Node proxy proves the API and the numbers.
4. **Interactive transactions are held open on the proxy** across round trips.
   Exclusive transactions keep their usual authority validation; a dropped
   connection rolls them back.
5. **Not supported yet** (explicit errors or absent): token refresh on a live
   connection (closes with "not supported yet"), streaming/large values,
   permission preflights (`canInsert` etc.), `subscribeDelta`, column transforms
   whose outputs are not plain JSON/Date/bigint/bytes, and framework bindings
   (React hooks would be a thin wrapper over `subscribe`).

## Files

- `app/`: the example's schema and UI (`main-full.ts` is the unchanged example,
  `main-lite.ts` the same UI on `LiteDb`), plus the owner-only permissions from
  `todo-server-ts`.
- `scripts/sizes.mjs`: builds both pages with Vite, reports every emitted asset
  (marking assets nothing references), attributes the lite bundle by module, and
  measures the projections.

## Follow-up: start on lite, hot-swap to full local-first Jazz

Question: can an app start on Jazz Lite, lazy-load full Jazz, and swap over
while keeping subscriptions alive? Yes. `src/upgrade.ts` does it, and it is
seamless in the measured runs: zero extra renders, no empty or shrinking
results.

### How the swap works

`createUpgradableDb({ lite, loadFull })` is one facade over both backends.
`loadFull` is the app's `() => import("jazz-tools").then(...createDb...)`, so
the WASM, workers and full client stay out of the initial bundle.
`upgrade()` then:

1. Loads full Jazz for the same account (same JWT). From that moment, new
   reads, writes and subscriptions use full Jazz.
2. Re-opens every live lite subscription on full Jazz, with the app's own
   options. Lite keeps delivering results while full Jazz syncs.
3. Switches each subscription on the first full result that equals the latest
   lite result. The switch is one synchronous step, followed by unsubscribing
   lite. Because the two views agree, the app sees no render at the swap. A
   cold store's first full results are partial (often empty) and are never
   shown. If the views never agree within `handoverServerWaitMs` (default
   10 s), the swap is forced, accepting one visible jump.
4. Lite writes and interactive transactions already in flight finish on lite;
   the server orders them and full Jazz receives them by sync. The lite socket
   closes once nothing uses it, and the proxy frees its subscriptions.

Why step 3 matches results instead of trusting the first full result: in
Node, a fresh full client's first local-first result was empty even with
`firstLoadRemoteWaitMs` set (alpha.59). I did not dig further; matching makes
the swap independent of that.

Tests: `tests/upgrade.test.ts` (Node, 4 passing). Subscriptions keep running
with no re-render, no empty result and no shrink, and the proxy ends with no
subscriptions or connections. Writes after the swap reach the same
subscriptions, and writes from another client are seen. A change made while
full Jazz is loading is delivered, and a subscription opened mid-upgrade also
moves. A lite transaction that is open when the upgrade starts commits.
`tests/browser-upgrade.test.ts` runs in real Chromium with brotli assets
(`JAZZ_LITE_BROWSER=1`, after `npm run sizes`).

### Bundle

| Upgrade demo (`index-upgrade.html`)   |    gzip |  brotli |
| ------------------------------------- | ------: | ------: |
| Initial (lite + app + schema DSL)     | 27.8 KB | 24.5 KB |
| Lazy: full client JS + workers + WASM | 6.63 MB | 4.46 MB |

### What the upgrade costs in a browser

Chromium, 20-row subscription. Times run from calling `upgrade()` until every
subscription has moved. Cold means a first visit; warm means a reload, with
the HTTP cache and IndexedDB already filled.

| Profile                             | lite first result | upgrade, cold | upgrade, warm | renders at swap |
| ----------------------------------- | ----------------: | ------------: | ------------: | --------------: |
| desktop, unthrottled                |            103 ms |         1.3 s |         0.7 s |               0 |
| fast 4G (9 Mbps, 60 ms)             |            140 ms |         5.7 s |         0.6 s |               0 |
| slow 4G (1.6 Mbps, 150 ms) + 4x CPU |            268 ms |        25.1 s |         1.8 s |               0 |

On cold runs, almost all of the time is fetching and compiling the 4.1 MB
(brotli) WASM: 4.6 s on fast 4G and 22 s on slow 4G. The handover itself takes
0.3 to 2.2 s. A full-only app pays that same download before its _first_
result, so starting on lite moves first data from about 5 s to 140 ms on
fast 4G, and from about 25 s to 270 ms on slow 4G.

### When to lazy-load full Jazz

The trade: lite has no offline mode, no optimistic writes (each write is one
round trip), and no large-value streaming, and the proxy holds about 1.25 MiB
per live subscription. Full Jazz costs a 4.5 MB download plus compile once,
and almost nothing on later visits.

Signals, in order of precedence:

1. **Returning visitor** (full Jazz loaded before, so the cache and store are
   warm; a localStorage flag set after the first upgrade): start the upgrade
   right after the first lite paint. It costs 0.6 to 1.8 s and nothing on the
   network.
2. **Intent to write continuously**: focus in an editor or composer,
   pointerdown on a canvas, sequencer or board, or the second write within a
   few seconds. Upgrade at once. Lite keeps working until the swap, at one
   round trip per write.
3. **Needs a full-only feature**: opening an upload dialog (streaming large
   values is not in lite), an explicit offline toggle, or installing the PWA.
   Upgrade at once. Going offline is too late, so apps that promise offline
   use rule 4 eagerly.
4. **Idle prefetch for signed-in or engaged sessions**: after first paint and
   `load`, in `requestIdleCallback`, but only on a good connection
   (`effectiveType` 4g, no Save-Data, `deviceMemory` > 2). A sign-in screen is
   a free prefetch window: start loading during sign-in.
5. **Many live subscriptions** (about 10 or more) or long dwell (about 30 s
   active): upgrade. This moves per-subscription cost off the proxy, and the
   download is amortised.
6. **Otherwise stay lite**: anonymous, read-mostly visitors on constrained
   connections never pay the 4.5 MB.

Applied to the example apps (from a survey of their queries and writes):

| App                                          | Entry / pattern                                                             | Start              | Upgrade when                                                                                    |
| -------------------------------------------- | --------------------------------------------------------------------------- | ------------------ | ----------------------------------------------------------------------------------------------- |
| jamazon                                      | public catalogue browsing; guest cart works offline                         | lite               | add-to-cart or sign-in (cart must work offline); idle prefetch only on good connections         |
| world-tour                                   | public fan page, read-mostly, narrow queries                                | lite               | a band member opens an edit form; fans never upgrade                                            |
| todo-client-localfirst-\*, nextjs-csr-ssr    | 1 small subscription, rare writes                                           | lite               | idle prefetch on good connections; returning visitors at once                                   |
| chat-react                                   | anonymous; 8 to 12 subscriptions incl. per-message; strokes on pointer move | lite (paint)       | composer focus or canvas pointerdown; idle prefetch right after paint (many subscriptions)      |
| stage-plan                                   | anonymous, offline-first by design (offline switch), transactional          | lite (paint)       | immediately after first paint (offline is the product)                                          |
| text-editor                                  | public doc; continuous Yjs byte splices                                     | lite (paint)       | first focus or keystroke in the editor                                                          |
| band-chat, wequencer, poster-shop, band-book | sign-in first; frequent or continuous writes; streaming uploads             | lite after sign-in | prefetch during sign-in; swap as soon as loaded                                                 |
| record-player, epic-drop                     | metadata browsing; large uploads and range reads                            | lite               | upload dialog opens or playback/preview starts (byte ranges need full); otherwise idle prefetch |

### Not covered yet

- Anonymous local-first accounts (chat-react, stage-plan, epic-drop) would
  need the lite client to present a local-first bearer token without WASM.
  The proxy accepts local-first JWTs via `forRequest()`, but minting one in
  plain JS in the browser was not tried.
- Writes made through lite are not optimistic, so a write issued just before
  the swap shows up after its round trip, as on lite.
- Framework hooks (`useAll`) would subscribe through the facade; not built.
