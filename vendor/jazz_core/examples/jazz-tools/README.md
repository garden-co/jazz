# jazz-tools Alpha Example

This package starts the actual alpha-style TypeScript package surface named
`jazz-tools` over direct WASM objects. It runs in Node, keeps row writes encoded
at the hot boundary, and uses `src/direct-codec.ts` plus
`src/direct-row-codec.ts` for the small amount of postcard/record encoding that
the binding layer needs to understand.

## Public API Shape

The initial public surface is deliberately small:

```ts
import { createDb, createJazzClient, createJazzContext, defineSchema } from "jazz-tools";
import { createJazzContext as createBackendContext } from "jazz-tools/backend";
import { createJazzHooks, JazzProvider, useLocalFirstAuth } from "jazz-tools/react";

const schema = defineSchema({
  todos: {
    columns: [
      { name: "title", column_type: "Text", nullable: false },
      { name: "done", column_type: "Boolean", nullable: false },
      { name: "owner", column_type: "Uuid", nullable: true },
    ],
  },
});

const db = await createDb({ schema: schema._schema, node, accountAuthor, accountId });
const todos = db.table<{ id: string; title: string; done: boolean; owner?: string }>("todos");
const liveTodos = db.subscribe(todos, (rows) => {
  console.log("live todos", rows.length);
});
const created = db.insert(todos, { title: "Ship alpha", done: false }, { id: "11111111-1111-1111-1111-111111111111" });
await created.wait({ tier: "local" });
db.update(todos, created.id, { done: true }, { updatedAt: new Date() });
db.upsert(todos, { title: "Patch or create", done: false }, { id: created.id, updatedAt: new Date() });
db.all(todos);
await db.delete(todos, created.id, { updatedAt: new Date() }).wait();
await db.restore(todos, created.id, { title: "Restored alpha", done: false }).wait();
liveTodos.unsubscribe();

const { secret, isLoading } = useLocalFirstAuth();
if (!isLoading && secret) {
  await createDb({ schema: schema._schema, appId: "my-app", secret });
}

const client = await createJazzClient({ schema: schema._schema, appId: "my-app" });
JazzProvider({
  client,
  children: ({ db }) => db.all(todos),
});

const { useDb, useTable, useAll } = createJazzHooks(client);
const todoTable = useTable<{ id: string; title: string; done: boolean }>("todos");
const liveTodoRows = useAll(todoTable);
useDb().insert(todoTable, { title: "Try hooks", done: false });
liveTodoRows.unsubscribe();

const context = await createJazzContext({ appId: "my-app", app: schema, driver: "memory" });
context.db().all(todoTable);
context.asBackend().db.insert(todoTable, { title: "Use backend context", done: false });
await context.shutdown();

const backendContext = await createBackendContext({ appId: "my-app", schema: schema._schema });
await backendContext.shutdown();
```

`createDb`, `defineSchema`, `db.table`, `insert`, `update`, `upsert`, `delete`, `restore`,
`all` and `subscribe` are backed directly by current ABI helpers. Write
methods preserve synchronous row visibility and return small write results:
`insert`, `update`, `upsert`, and `restore` remain row-like while adding `.value`, `.handle`,
and `.wait({ tier: "local" })`; `delete` returns the same write-result shape with
`value: undefined`. `restore(table, id, data)` uses the real direct WasmDb restore
write and refuses currently visible rows so it does not silently behave like an
overwrite. One-shot reads support `{ includeDeleted: true }` and the exported
`isDeleted(row)` helper; deleted state is a non-enumerable row marker, not a user
column. Subscriptions remain live-row only in this slice. This first slice
supports scalar `Boolean`, `Integer`, `Text`, `Uuid`, `Bytea`, and array
columns plus nullable literal comparisons and one-shot `select`, `orderBy`, `limit`,
and `offset` query shaping; array `contains`, whole-array `eq`/`in`, Bytea `eq`/`in`; includes, joins,
permissions/session APIs, durable storage selection, and full query lowering
remain future alpha surface.

`createJazzClient` is a thin convenience helper over `createDb` and the existing
local-first auth secret store. It returns `{ db, auth, getAuthState,
onAuthChanged, updateAuthToken }` without adding a second runtime abstraction.
The package root still exports a dependency-free `JazzProvider` and
`createJazzHooks` facade for framework-neutral compatibility checks. The
`react` entrypoint is a small real React provider/hook layer over the same thin
client: `JazzProvider` places a client in context, `useDb` reads it, `useTable`
creates a table handle, and `useAll` subscribes through `db.subscribe(...)` with
`useSyncExternalStore`.

`createJazzHooks(clientOrGetter)` adds framework-neutral hook-shaped helpers:
`useJazzClient`, `useDb`, `useTable`, and `useAll`. They are synchronous
factories over the existing client and `Db` APIs, not a rendering runtime.
`useAll` opens a real `db.subscribe(...)`, exposes the current rows, and
updates from the subscription callback. Its explicit `refresh()` path is a
one-shot read convenience for tests and non-rendering callers, not subscription
delivery semantics.

The replacement surface uses `db.subscribe(query, callback)` directly. We do not
export a separate `subscribeAll` helper in this package; alpha-style callers
should route that shape through `db.subscribe` until a higher-level binding layer
has a real reason to add the alias.

`db.beginTransaction()` and `db.transaction((tx) => ...)` are the first public
transaction slice. They use real ABI transaction handles for staged
insert/update/upsert/delete/restore, commit, rollback, custom ids, same-row
write coalescing, and sync or async transaction callbacks. Mergeable transaction
reads and query-builder reads through transaction objects are still future work.

`createJazzContext({ appId, app/schema, driver? })` is the smallest
`jazz-tools/backend` compatibility slice for this direct-WasmDb package. It returns
`{ db(), asBackend(), shutdown() }`; both `db()` and `asBackend().db` point at
the same `createDb` memory-backed direct WasmDb instance for now. `driver` may be
omitted or set to `"memory"`/`"local"`. Persistent backend storage is not exposed
honestly in this slice yet, so `"persistent"` or other driver kinds throw before
opening a DB.

## Quick Start

```sh
npm install
npm test
```

`npm test` typechecks, compiles TypeScript, and runs the current direct
`jazz-tools` compatibility gates: auth/session helpers, client/provider helpers,
package-root public API, schema DSL, transaction facade,
backend context, and file/blob helpers.

## Scripts

- `npm run build:wasm` builds `../../jazz-wasm/pkg` with `wasm-pack --target nodejs`.
- `npm run check` typechecks the TypeScript sources.
- `npm run build` compiles the TypeScript package.
- `npm run test:auth-secret-store` runs the local-first auth secret store and
  framework-neutral `useLocalFirstAuth` factory tests.
- `npm run test:jazz-client` runs the thin `createJazzClient` and
  dependency-free `JazzProvider` plus hook-factory checks.
- `npm run test:transaction-compat` runs the bounded public transaction facade
  checks over real ABI transaction handles.
- `npm run test:backend` runs the smallest `jazz-tools/backend` compatibility
  check for `createJazzContext`, `db()`, `asBackend()`, and the persistent-driver
  guard.
- `npm run test:alpha-public-flow` runs the smallest public-flow adoption check.
  It is intentionally not part of `npm test` until identity-scoped owner-policy
  reads are fixed on the direct facade.
- `npm test` runs the direct public API, auth, React/provider, subscription,
  schema, transaction, backend, and file/blob gates.

## Scenario

The example package:

- opens the `jazz-tools.ts` alpha-style public surface with `defineApp`,
  `schema.table`, `createDb`, and a `todos` table handle, then verifies
  `insert`, `update`, `delete`, `subscribe`, `all`, bool equality reads,
  scalar boolean/text/integer `in` reads, nullable literal comparisons, chained
  `where` clauses, and a narrow title substring query with `limit`;
- opens a memory-only files/blob slice with one `files` row per file containing
  `mime_type` and `data`, then verifies `createFileFromBlob`,
  `loadFileAsBlob`, direct byte reads, and delete.
- keeps older server/WebSocket smoke coverage out of this package until it can be
  rebuilt directly on `createDb`/`WasmDb.connectUpstream()` without the removed
  command/event helper layer.

The dedicated `alpha-public-flow-gate.ts` script is the current smallest
automated adoption check for the real `jazz-tools` package surface: account/open
DB, schema/table token, create/update/delete, one-shot reads, and
callback `db.subscribe(query, rows => ...)`. Its query coverage includes
equality, scalar `in`, integer/text ranges, nullable comparisons, text and array
`contains`, whole-array `eq`/`in`, Bytea `eq`/`in`, multiple `where` clauses,
result `limit`, and one-shot `include`/`hop`/`gather` facade reads over schema
references. It currently fails at identity-scoped owner-policy reads, where the
direct facade returns both owner rows instead of filtering to the requested
identity.

The auth/session slice is package-local in `jazz-tools.ts`. `createDb` accepts
`appId`, `secret`, `jwtToken`, and `cookieSession`; `secret` deterministically
derives a local-first session and direct account identity, while JWT `sub` maps
to `session.user_id` for local policy state. The DB exposes `getAuthState`,
`onAuthChanged`, and `updateAuthToken`.

The files/blob helpers deliberately diverge from the current alpha
`files`/`file_parts` convention. They store each file on a single file-like row
with `mime_type` and native binary large-value `data`; no `file_parts` table is
created or read.

## Deferred Server/Transport Gates

The old HTTP, chat, shared-todo, and WebSocket smoke files were deleted with the
command/event helper layer. They should come back as direct examples that own
`Db`/`WasmDb` objects and use `WasmDb.connectUpstream()` for byte-frame sync,
not as resurrected runtime handles or event polling.
