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
