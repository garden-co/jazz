# Alpha-style example target matrix

This repo branch is the alpha graft: the public `jazz-tools` package is being
forced onto `jazz_core` through the shared direct-core runtime and WASM/NAPI
bindings. The purpose is to pin app-shaped flows that alpha users and
maintainers can recognize while deleting the old parallel engine paths, not to
host a separate compatibility facade beside the real package.

Operational rule for this branch: direct core is the product runtime. The
remaining work should be expressed as public `jazz-tools` API gates, direct
core/Groove correctness gaps, or missing storage/server/auth surfaces. Do not
add a second row-batch/sync-manager compatibility implementation to make old
tests pass.

The old graft-era `batch_fate`, `row_histories`, and generic `storage` modules
have been deleted outright. Transaction identity now lives in the neutral
`transaction` module, schema catalogue persistence uses its own narrow storage
trait, and remaining persistence work should target the direct-core browser,
server, and native storage paths rather than reintroducing the deleted row
history engine.

## Alpha targets surveyed

- Upstream targets surveyed for this slice:
  - `crates/jazz-wasm/tests/wasm.rs`
  - deleted legacy `crates/jazz-tools/src/runtime_core/tests/basic.rs`
  - `packages/jazz-tools/tests/browser/db.all.test.ts`
- `starters/ts-localfirst`
  - `schema.ts` defines a tiny `todos` app with `s.table`, `s.string`, and
    `s.boolean`.
  - `src/todo-widget.ts` mounts a plain DOM todo app over `Db` with
    `insert`, `update`, `delete`, and live subscriptions.
  - `e2e/todo-flow.spec.ts` verifies that a todo can be added and survives a
    browser reload in pure local-first mode.
- `examples/todo-client-localfirst-react`
  - Uses `JazzProvider`, local-first auth, `useDb`, `useAll`, and app schema
    objects to drive add/toggle/delete/filter behavior.
- `examples/todo-server-ts`
  - Uses `createJazzContext`, `context.forSession`, CRUD endpoints, live
    snapshots, and a restart persistence test over a durable data path.
- `examples/chat-react`
  - Exercises the richer future target: related tables, membership, messages,
    routing, upload/file APIs, and edge/local write waits.

## Current Example

The current example emulates the local-first todo shape without pretending to expose
the alpha API yet:

1. define a `todos` schema with `title`, `done`, `owner`, and owner-only
   policy;
2. open a local account-like context over a WASM memory DB;
3. insert a todo and observe it through a subscription;
4. update the todo and observe the changed subscription state;
5. delete the todo and observe removal;
6. sync the local DB through the byte transport to a history-complete server
   memory DB and assert the server sees the expected final state;
7. expose an in-process server-style facade with create/list/update/delete and
   subscribe/snapshot operations over the same memory authority;
8. expose the `jazz-tools.ts` alpha-style public surface with `defineApp`,
   `schema.table`, `createDb`, a `todos` table handle, `insert`, `update`,
   `delete`, `restore`, row-like write results with `.value`, `.handle`, and
   `.wait(...)`, `subscribe`, `all`, `one`, object-style query
   filters, bool equality reads, scalar boolean/text/integer `in` reads,
   integer/text range reads, nullable owner null/not-null reads, nullable
   literal comparisons, chained `where` clauses, and one-shot title `contains`
   reads with `limit`, plus one-shot nested include selection over a three-table
   `users -> teams -> parent team` read;
9. expose a small memory-only Node HTTP/SSE wrapper around that facade,
   including identity-shaped policy reads and update denial;
10. add a snapshot-backed restart subsection to the HTTP example, where two todos
    are created through HTTP, the underlying memory storage bytes are exported,
    the first server closes, and a fresh server imports those bytes before
    `dbOpenMemory` so `GET /todos` returns both rows;
11. add a memory-only chat-lite scenario with `rooms`, `room_members`, and
    `messages`, where message reads are scoped by a related membership table via
    `dbReadForIdentity`;
12. add a memory-only shared-todo scenario with `todos` and `todo_shares`,
    where creator access is represented by an initial owner share row and
    recipient reads are granted and revoked through real `dbReadForIdentity`
    policy checks, including an alpha-style shared-with-me include view over
    already-authorized share rows and todos;
13. add a files/blob slice that deliberately diverges from the current alpha
    `files`/`file_parts` tables by storing each file on one file-like `files`
    row with `mime_type` and native binary-large-value `data`, then exercise
    `createFileFromBlob` -> `loadFileAsBlob`/raw byte reads -> delete in the
    Node demo and package runtime checks;
14. add a TypeScript WebSocket transport smoke check that owns a local `upstream`
    transport, uses app-scoped `/apps/<app>/ws` URLs plus the auth prelude, and
    pumps batched postcard/raw `WireFrame` bytes without decoding rows; by
    default it spawns a Rust `jazz-server` sync server process and asserts
    two-client todo convergence through that listener, then proves fresh
    reconnect catch-up by closing one client's sync, writing while it is
    offline, and reconnecting it with a new WebSocket/upstream transport;
15. add a shared-todo WebSocket policy smoke check over the same process boundary:
    owner and recipient clients connect with deterministic identities, the owner
    creates a shared todo plus share rows, and the smoke asserts that the
    recipient cannot read before grant, can read after grant, and hydrates the
    authorized todo on a fresh connection after grant;
16. add a chat WebSocket policy smoke check over the same process boundary: owner and
    member clients connect with deterministic identities, the owner creates a
    room, membership rows, and a message, the member hydrates and reads the
    message through sync, and a fresh outsider identity reads no messages through
    the existing membership policy;
17. run all of the above in automated Node checks, with the WebSocket checks
    included in `npm test`.
18. add a dedicated `npm run test:alpha-public-flow` check for the smallest
    alpha-like public flow that can run today through `jazz-tools.ts`: define an
    app/schema, create the DB, use table/query objects, subscribe through
    `db.subscribe(query, callback)`, insert/update/delete/restore
    with write handles, one-shot `all`/`one` reads, limit, and delete through the
    public surface.
19. add a durable chat WebSocket restart check: write room membership
    relation state and a visible message before restart, restart the Rust
    listener with the same data directory, hydrate that message into a fresh
    authorized member client through the relation-backed read policy, write one
    accepted post-restart message, and assert both messages are visible.
20. add a public predicate-movement browser canary in
    `packages/jazz-tools/tests/browser/alpha-public-flow-gate.test.ts`: a
    persistent direct-core `createDb` client subscribes to
    `app.todos.where({ done: false }).orderBy("title")`, then local updates move
    one row out of the predicate and another row into it. This is the minimum
    public subscription movement gate; broader ordered/windowed and websocket
    predicate movement remain separate direct-core/Groove gates.

Rows remain descriptor/raw encoded at the ABI boundary. This package may add
small app-facing helpers when they express real app semantics, but it should not
grow a method-per-ABI forwarding layer or full alpha query builder.

## Gap matrix from first upstream targets

| Upstream target                                                    | Covered now                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | Still missing                                                                                                                                                                                               |
| ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `crates/jazz-wasm/tests/wasm.rs`                                   | WASM module loading from TypeScript, schema bytes, deterministic row IDs, `WasmDb` create/update/delete, and query reads are covered by `npm run test:alpha-public-flow` plus the broader demo.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Rust-side `wasm-pack test --node` parity, exported `generate_id`, `current_timestamp`, `parse_schema`, and public query-builder API tests are not present in this repo's WASM package.                      |
| deleted legacy `crates/jazz-tools/src/runtime_core/tests/basic.rs` | WasmDb-shaped insert/query, update/delete, callback subscription rows, restart-by-snapshot, owner-policy reads, and Rust WebSocket server durable restart are represented in the `WasmDb` binding checks.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | Default materialization API parity and broader direct-core Rust regression coverage still need to be rebuilt against `Db`/`CoreRuntime`; the old runtime_core harness is intentionally gone.                |
| `packages/jazz-tools/tests/browser/db.all.test.ts`                 | `jazz-tools.ts` covers `createDb`, `defineApp`/schema tokens, table/query objects, `insert`, `update`, `delete`, `restore`, row-like write results with `.value`/`.handle`/`.wait(...)`, `all`, `one`, `allForIdentity`, `subscribe(query, callback)`, boolean/text/integer equality, signed integer `lt`/range comparisons, scalar boolean/text/integer `in`, array-element `in`, integer/text `gt`/`gte`/`lt`/`lte`, nullable UUID `isNull`/`isNotNull`, nullable literal equality/inequality and mixed nullable `in`, text inequality, chained positional and object-style filters, one-shot text and array `contains`/`limit`, selected projections, result ordering, offset pagination, text-array insert/update/readback, whole-array `eq`/`in`, Bytea `eq`/`in`, binary-large-value file helpers, one-shot facade include/hop/gather reads over schema references, required object-style includes, one-shot nested include selection for scalar/reference paths, simple/nested forward include subscription callbacks, hop/gather app-shaped subscriptions, and a direct websocket depth-3 reverse include subscription gate with selected include projection materialization rebuilt from subscription rows. The browser example now proves reload persistence through `WasmDb.openBrowser(namespace, schema, config)` OPFS storage and keeps a subscription useful after reload/update. | Full browser `jazz-tools` test runner, broader native relation query/subscription lowering in core rather than TS recompute scaffolding, and durable/upload/streaming object-storage semantics are missing. |

The grafted package now also has a browser alpha public-flow gate in
`packages/jazz-tools/tests/browser/alpha-public-flow-gate.test.ts`. The running
slice proves public `schema.defineApp`, `createDb({ driver: "persistent" })`,
CRUD, `db.one`, `subscribeAll`, and read-your-writes in one browser session.
The package persistent path now uses a direct-core dedicated worker for OPFS and
the same file proves a local write can survive shutdown/reopen. The same gate
also runs a persistent OPFS client through the real Rust websocket server
boundary, reopens that client, and verifies a second persistent client converges
over the websocket path. It now also covers includeDeleted reads after
edge-confirmed deletes and binary-large-value file/blob persistence plus
websocket convergence. Delete/restore over websocket is unskipped: the writer
can edge-accept the restore, and a fresh websocket client can query the
restored row. The same gate is now also the public predicate-movement canary for
filtered local subscriptions.

The alpha docs React todo app now also has a direct-core browser websocket
canary in
`examples/todo-client-localfirst-react/tests/browser/todo-app-direct-core.test.tsx`.
That gate runs the real `JazzProvider`, `useDb`, and `useAll` flow with two
persistent OPFS clients connected to one local Rust Jazz server. It verifies
DOM-driven create/update/delete and bidirectional subscription observation over
the direct websocket path. The old core/edge browser setup has been collapsed
out of this canary. The DOM-driven OPFS remount/reopen assertion is now
unskipped: `TodoList` waits for the public `db.insert(...).wait({ tier:
"local" })` path and emits a test-observable durability event before the test
shuts down and remounts the app.

## Current gaps versus alpha

- Rust Axum todo-server examples are currently kept outside the root Cargo
  workspace. They put `JazzClient` directly in shared router state, which
  requires `Send + Sync`; the direct-core `Db` owner is local-thread by design
  (`Rc` / `RefCell`). Re-enable these examples by rebuilding them around the
  real server boundary or another explicit local-owner gateway, not by reviving
  the legacy runtime. The standalone examples now compile through an
  example-local `TodoClient` owner task that keeps the direct-core
  `JazzClient` on a current-thread `LocalSet` and gives Axum a `Send + Sync`
  request handle. This must stay example-local until we decide whether Rust HTTP
  apps should talk to the direct `jazz-server` boundary or whether
  `jazz-tools` should expose a real native server gateway.
- Browser persistent creation in the grafted `jazz-tools` package now uses a
  direct-core dedicated worker for OPFS instead of the deleted broker/leader
  topology. The current positive browser gate covers public CRUD,
  read-your-writes, subscriptions in the direct path, and local OPFS
  shutdown/reopen for a simple inserted row. It also covers persistent OPFS plus
  direct websocket convergence over a real Rust server for a todo-shaped flow,
  includeDeleted reads for edge-confirmed deletes, and binary-large-value
  file/blob persistence plus websocket convergence. Rust `JazzClient` also has
  a first offline persistent direct-core RocksDB rehydrate gate for public
  row insert/query. Remaining persistence gaps are broader history/index
  correctness and production storage coverage beyond the first browser/Rust
  vertical slices.
  `examples/browser-wasm` still has older OPFS reload coverage for the vendored
  example path, but the package gate is the integration source of truth.
- Public TypeScript API compatibility is intentionally thin. Since this repo is
  separate from the alpha repo, the replacement package is named `jazz-tools`;
  new integration coverage should enter through that public package surface
  first, then force direct-core gaps to close underneath it. The old todo-only
  alpha facade has been deleted. It now
  includes `allForIdentity(tableOrQuery, identity)` for deterministic
  identity-scoped one-shot reads, object-style `where({ ... })`, object-style
  `include({ relation: true })`, `db.one(...)`, callback-first
  `subscribe(query, callback)`, `db.beginTransaction()`, and synchronous
  `db.transaction(cb)`. It also exposes
  `restore(table, id, data)` over `dbRestoreEncoded`, returning the restored row
  with local `.wait(...)` support and refusing currently visible rows so restore
  does not silently overwrite live content. One-shot `all`/`one`/
  `allForIdentity` reads now accept `{ includeDeleted: true }`; rows remain
  row-shaped and expose deleted state through a non-enumerable marker read by
  `isDeleted(row)`. The package root is now library-only and browser-import
  gated; Node-only executable demo code lives in `demo.ts`, and the Node runtime
  loader is only used lazily when callers do not inject a runtime. Subscriptions
  still remain live-row only. Missing API surfaces should be named directly:
  full upstream query-builder vocabulary, selected include projection
  subscriptions, app-level durability wait hooks for React todo remount tests,
  auth-loss refresh/failure callbacks with browser coverage, and durable
  upload/streaming file helpers are not complete.
- Public `jazz-tools` one-shot query coverage now includes boolean, text, and
  integer equality, scalar boolean/text/integer `in`, integer/text
  `gt`/`gte`/`lt`/`lte`, nullable UUID `isNull`/`isNotNull`, nullable literal
  equality/inequality and mixed nullable `in`, text inequality, text
  `contains`, array `contains`, array-element `in` lowered to `contains`
  disjunctions, whole-array `eq`/`in`, Bytea `eq`/`in`, chained
  positional and object-style `where` clauses, `db.one(...)`, selected
  projections, result ordering, general `limit`, `offset` through the `WasmDb`
  query encoder, and one-shot positional/object-style `include`, nested
  `include({ relation: { select, include, required } })`, `hop`, and
  `gather` facade reads. Relation reads are resolved after a base
  `db.all`/`allForIdentity` read using schema `references`, plus explicit
  `schema.table(..., { relations })` aliases for reverse include names. A
  direct websocket depth-3 reverse include subscription gate now passes by
  recursively materializing nested include arrays and subscribing to schema-
  aware child-table triggers. Selected include projection subscriptions remain
  query-builder gaps.
- Public signed `Integer` values now bridge to direct core through an
  order-preserving `i32` bias before entering Groove's unsigned `U32` value
  representation. This keeps negative values round-trippable and makes direct
  core range predicates such as `priority < 0` sort the same way public Jazz
  signed integers sort. This is a boundary encoding rule, not a new public
  type.
- Title `contains` is covered for one-shot reads and maintained subscriptions.
  Maintained subscriptions also support unordered `limit(1)` with offset `0`,
  lowering through Groove `ArgMinBy` over `row_uuid`; this closes one maintained
  subscription lowering gap without introducing a full-recompute path. Explicit
  ordering, `limit > 1`, and nonzero `offset` still skip maintained subscription
  lowering until Groove has a `TopBy`/ordered-window primitive. The remaining
  ordered/windowed subscription work should be attacked as a pure Groove and
  Jazz completeness slice, independent of alpha API wrapper work.
- Include/hop/gather-style related-record semantics now have a one-shot
  `jazz-tools.ts` facade marker in `alpha-public-flow-gate.ts`, including an
  identity-scoped include over an owner policy table. Core now has a focused
  maintained subscription view proof for `sharedTodos.include(owner)` relation
  deltas without full recompute, and ABI subscription snapshot/delta row batches now carry
  flat included closure rows as ordinary descriptor/raw records. The TS direct-WasmDb
  `subscribe(query, callback)` facade now accepts simple and nested forward
  includes such as `todos.include("owner")` and `users.include({ team: { include:
{ parent: true } } })`, plus a depth-3 reverse include path over the direct
  websocket server, and materializes the alpha-shaped included object from flat
  subscription rows. Hop and forward-gather app-shaped subscriptions now also
  pass through the public browser `useAll`/`useAllSuspense` gates. Those
  relation subscriptions are still maintained by the TypeScript direct-core
  runtime evaluating the supported relation shape and refreshing from direct
  subscription chunks; the remaining integration gap is native relation
  lowering/deltas in direct core, not another facade fallback. Selected include
  projections remain query-builder gaps. The nested-forward slice is pinned by
  a Rust prepared subscription regression so root rows survive multi-segment
  include paths.
- Public transaction compatibility now has a first bounded slice over real ABI
  transaction handles: `beginTransaction`, synchronous `transaction(cb)`,
  transactional insert/update/upsert/delete/restore, commit with local write
  waiting, rollback, custom ids, exclusive transaction commit/rollback through
  direct core WASM, and exclusive transaction `tx.all`/`tx.one` reads over
  staged state. Same-row insert/update, update/delete, and
  restore/update sequences are coalesced before commit so the public facade can
  present alpha-shaped staged-row semantics while still replaying a minimal ABI
  operation set. Focused compat tests also pin rollback after coalesced staged
  changes, repeated same-row upsert sequences, sync and async `transaction(cb)`
  commit return/throw rollback behavior, and rollback after rejected async
  callbacks. Mergeable transaction reads and transaction query-builder reads
  remain future work; session-scoped exclusive writes fail closed until direct
  core exposes identity-aware exclusive staging.
- Auth now has the first pure TypeScript alpha-shaped foundation in
  `jazz-tools.ts`: `Session`/`AuthMode`, JWT payload parsing, deterministic
  local-first `secret` resolution, `cookieSession`, auth state, and same-principal
  `updateAuthToken` events. `createJazzClient`, package-root dependency-free
  provider-shaped helpers, and the `jazz-tools/react` entrypoint now cover the
  first real React provider/hook app slice with `JazzProvider`, `useDb`,
  `useTable`, and `useAll` over `useSyncExternalStore`. Server-side Ed25519
  local-first JWT admission is now wired for WebSocket Authorization headers and
  first-frame auth handshakes when explicitly enabled. TS-side signed
  local-first proof generation is covered by the async WebCrypto helper used by
  `createDb({ secret })` and the todo WebSocket auth smoke, while sync Node
  signing remains available from the explicit `jazz-tools/auth` subpath.
  Local-first JWTs now carry standard `aud` equal to the app id, and TS session
  claims expose it as `claims.audience`; client-side session resolution rejects
  mismatched JWT audiences. Server-side audience rejection, browser auth-loss
  refresh behavior, richer framework integration, and a first-class direct-WasmDb
  subscription listener contract remain future work.
- In-process server-style create/list/update/delete and subscribe/snapshot are
  present over the memory authority, with a small HTTP/SSE wrapper that covers
  health, list, identity-bound policy list, create, read, update, delete, and
  live snapshots. `GET /todos/as/:userId` exercises real `dbReadForIdentity`
  The Rust server builder now keeps the old HTTP catalogue store explicitly
  separate from the direct-core server database: catalogue metadata lives under
  `catalogue.rocksdb`/`catalogue.sqlite`, while sync state lives under
  `core-server.rocksdb`. The remaining cleanup is to replace or shrink that
  legacy catalogue persistence path, not to let it grow back into a second
  engine.
  filtering for deterministic 16-byte identities, and
  `PUT /todos/:id/as/:userId` uses `dbCanUpdateEncodedForIdentity` to allow the
  owner update while returning 403 for a non-owner without changing the row. The
  HTTP example includes snapshot-backed restart coverage by exporting memory
  storage bytes from one server instance and importing them into a fresh memory
  storage before `dbOpenMemory`; this is not true durable RocksDB-style
  persistence. Durable persistence is covered on the Rust WebSocket listener via
  data-dir restart gates for todo and chat relation/policy flows. `jazz-server`
  now has an alpha-shaped `server <APP_ID>` entrypoint with `./data` default
  storage, `/apps/<APP_ID>/ws` WebSocket routing, upstream-shaped auth/config
  aliases, and a JSON-only admin schema API for
  `POST /apps/<app>/admin/schemas`, `GET /apps/<app>/schemas`, and
  `GET /apps/<app>/schema/<hash>`. The admin schema catalogue can now persist as
  `admin-schemas.json` beside a data-dir and survive listener restart. Accepted
  upstream-shaped schema JSON is converted to local `JazzSchema`, published into
  the live in-memory runtime catalogue, and set as the current write schema on
  publish, while public GET/list responses still preserve the raw schema JSON;
  the loopback HTTP listener now reloads persisted admin schemas into the runtime
  catalogue before reporting ready. The combined `server <APP_ID>` HTTP/admin
  plus WebSocket lifecycle and the `createJazzContext` server package/API
  compatibility are still partial.
- Chat-lite related tables and membership-scoped message reads are covered in
  memory and over the Rust WebSocket sync server. The chat and shared-todo
  WebSocket smokes now also include first-frame auth handshakes without
  `?identity`, using the same auth envelope shape as the todo WebSocket smoke.
  Full chat features such as invites, routing, profiles, durable delivery, and
  richer auth/session refresh remain future targets.
- `subscribeAll` is now part of the public package path used by the browser
  alpha-flow gates, while lower-level direct-runtime coverage still exercises
  `db.subscribe(query, callback)` directly over `WasmDb.subscribe` snapshot and
  delta chunks. Keep new public integration checks on `createDb`/table/query
  objects unless the gap is specifically in the internal runtime adapter.
- Shared-todo related-table reads and identity-scoped update dry-runs are
  covered in memory only. The schema encodes `can_edit` on share rows and uses
  it in the todo update policy; the example reads `todo_shares` through an
  identity-scoped `user == identity` UUID query and asserts that a reader share
  can include the authorized todo but cannot update, an editor share can pass
  `dbCanUpdateEncodedForIdentity`, and revoked shares return no included todos.
- Files/blob behavior intentionally diverges from the current alpha
  `files`/`file_parts` style: each file is a single file-like `files` row with
  `mime_type` and native binary-large-value `data`. The public package runtime
  now uses that model for `createFileFromBlob`/`createFileFromStream` and
  `loadFileAsBlob`/`loadFileAsStream`, with NAPI persistence coverage for
  `files.data`. Treat this as the alpha convention for this graft unless a
  product decision reverses it; do not rebuild `file_parts` chunk tables as a
  compatibility layer. This is still not upload routing, bounded streaming,
  durable object storage, resumable transfer, or full public file API coverage.
- The old browser worker/broker package path has been deleted from the graft.
  Browser memory clients still use the in-process direct `CoreRuntime` path, but
  persistent browser clients now use one dedicated OPFS-owner worker through
  `PersistentBrowserOpfsProxyRuntime`. That worker owns the real
  `WasmDb.openBrowser(...)` database because OPFS requires worker ownership; it
  is not a compatibility broker, tab leader, or second engine.
  Keep the small direct-core JS boundary glue (`core-runtime` codecs,
  websocket framing, and persistent-browser worker/proxy runtime) because it
  adapts package calls to the shared WASM/NAPI core. Do not recreate the old
  parallel browser broker, worker bridge, sync transport, leader-election, or
  package-worker protocol paths around it.
- Node HTTP, todo WebSocket, shared-todo WebSocket, and chat WebSocket smokes
  now run directly on `createDb`/`WasmDb.connectUpstream()`. WebSocket frames are
  opaque byte batches; row decoding stays at the app/test edge.
- Legacy alpha websocket transport, `runtime_tokio`, and the old Rust
  `runtime_core` module have been deleted from the active graft. React Native is
  temporarily unsupported rather than kept alive through the deleted runtime.
  The legacy `onMutationError` callback/event path has also been removed from
  the TypeScript runtime surface; direct-core writes report policy/durability
  rejection through retained write handles and `.wait(...)`. Old
  SyncManager integration test modules have been removed from active source so
  they no longer act as the semantic oracle for the replacement engine; new
  coverage must assert public direct-core client/server/browser behavior.
  The old `query_manager` execution engine (`manager`, graph/graph_nodes,
  subscriptions, writes, old indexes, old policy graph/IR/counters, server
  query helpers, and their manager/rebac tests) has been deleted from the
  active source. The remaining `query_manager` modules are vocabulary and
  codecs only: schema/query/policy/session/value types, relation IR, magic
  columns, and row/query encoding. `SchemaManager` now owns catalogue, schema,
  lens, and permissions-head state; it is no longer a query/write bridge and no
  longer stores or accepts `SyncManager`. Schema catalogue publication now uses
  a local catalogue clock plus direct storage writes, and `SchemaManager` drains
  its own pending catalogue updates instead of the old sync inbox bridge. Any
  old `sync_manager` code has been deleted, and remaining storage/schema-manager
  code should be treated as public schema/query vocabulary or catalogue
  scaffolding until it is ported to core-native types, not as a second engine to
  extend. `SchemaManager` now depends on a narrow `SchemaCatalogueStorage`
  interface instead of the full old row-history `Storage` trait; row-history
  mutation helpers are gated out of the default live build. The
  server admin catalogue now depends on dedicated catalogue-only memory/RocksDB
  storage instead of `Box<dyn Storage>` or old storage backend adapters. SQLite
  remains a native/client storage implementation, but direct-core
  `jazz-tools server` rejects SQLite for catalogue and sync storage.
  Admin subscription introspection no longer records fabricated
  `SyncPayload::QuerySubscription` entries in the catalogue store; the endpoint
  currently returns an authenticated empty shell until it can be backed by
  direct-core subscription telemetry. Server test utilities no longer expose
  `JazzServer::block_messages_to(...)` or buffered `SyncPayload::BatchFate`
  assertions; tests should assert public query/subscription/write outcomes
  instead of pinning old semantic sync frames.
  The remaining `SyncPayload` / `SyncTracer` vocabulary is transitional
  catalogue/admin/test observability scaffolding. The old row/batch/fate tracer
  path is being removed, not relocated: do not add new tests that assert
  buffered `SyncPayload` row batches, per-row batch fates, or old sync-manager
  message ordering. New sync confidence should come from public
  `createDb`/`JazzClient` outcomes, websocket convergence, durable restart, and
  direct-core telemetry surfaces if those are added.
  Transaction ids and transaction-kind parsing now live in neutral
  `jazz_tools::transaction` vocabulary rather than being sourced from
  `row_histories::BatchId` or `batch_fate::BatchMode`. `WriteContext` keeps the
  transaction id needed for direct-core staged writes, but it no longer carries
  `batch_mode`, and the TypeScript write-context payload no longer advertises
  that dead field. The old `rocksdb_storage_integration` test has been deleted
  so row-history/batch-fate storage is no longer pinned as an integration
  contract; future persistence checks should be narrow catalogue-storage gates
  or direct-core app gates.
  Schema-manager catalogue persistence and server catalogue storage now use
  catalogue-only traits/errors instead of `crate::storage::SchemaCatalogueStorage`
  or `crate::storage::StorageError`. The old storage module has been deleted;
  do not reintroduce it for remaining row-history/client-test fallout or as a
  catalogue abstraction to extend.
  The live server-side `SyncPayload` fanout hub
  (`ConnectionEventHub`/`SequencedSyncUpdate`/registration dispatch) has also
  been deleted. `SyncPayload` remains only as transitional tracer/test
  vocabulary until that observability API is migrated to direct-core event
  terms. `SyncTracer`, server tracer hooks, and the legacy `SyncPayload`
  vocabulary are now gated behind tests/test-utils rather than exported as
  product sync APIs.
  Persistent browser OPFS writes now expose their main-thread transaction ids
  as pending worker writes and `.wait(...)` resolves through the worker-owned
  direct-core transaction id only. Fully removing pending write semantics
  requires changing the synchronous runtime mutation interface or returning a
  write handle that resolves to the authoritative worker result.
- The Rust server's direct-core authority is now named
  `LocalCoreServerHandle`, with a private local owner around the non-`Send`
  core server shell. This is not intended as a second engine; it is the current
  required ownership boundary because the direct core server and underlying
  `Db`/`Node` stack are local-owner `Rc`/`RefCell` structures while Axum shares
  server state across tasks.
- Rust client/server schema conversion names now say public-schema/direct-core
  instead of alpha/core. The conversion boundary remains real until the public
  Rust API itself adopts direct-core-native schema/value types.
- The TypeScript `DbRuntimeModule` / `WasmRuntimeModule` naming has been
  collapsed into `DirectCoreSource` / `WasmCoreSource`. The remaining seam is a
  platform loader/source boundary for direct core, not a swappable engine
  abstraction.
- Public framework client objects no longer expose the subscription
  orchestrator as `.manager`. Framework hooks read an internal symbol-backed
  subscription store attached to the client, while the advanced shared package
  exports only the small cache/result types needed by binding authors.
- TypeScript query normalization no longer silently falls back to a runtime
  schema or caller-provided table when public query objects omit table/schema
  metadata. `Db.all`, transaction reads, and `subscribeAll` now require the
  built query's explicit table to exist in the query's schema. The deferred
  local-update default remains documented in `Db` because immediate local
  updates still fail several public query shapes; close that as direct-core
  read/subscription completeness work, not as a facade fallback.
- The retired alpha disconnect-marker/sweep lifecycle has been deleted from
  server state and tests. Direct websocket lifecycle is accounted for through
  shutdown admission/drain tracking and explicit direct-core session close, not
  the old client TTL/reap loop.
- Browser `useAll`/`useAllSuspense` tests now cover direct-core
  `propagation: "full"` and positive local persistent reads with
  `propagation: "local-only"`. The local-only path is passed through to native
  read options and direct core skips upstream subscription registration for
  explicit local-only subscriptions. The same hook gates now cover signed
  integer comparisons plus hop and gather relation subscriptions on the
  direct-core path.

## Next targets

1. **Create the real `jazz-tools` package surface.** Because this is a separate
   repo, the compatibility package should be named `jazz-tools`. Add alpha
   tests/examples directly as soon as the APIs they need exist; do not build a
   separate compatibility-package abstraction just to host those tests.
2. **Broaden browser storage coverage.** `WasmDb.openBrowser(namespace, schema, config)`
   uses the repo's OPFS-backed browser storage path and the browser smoke
   has example-level reload coverage. The grafted `jazz-tools` package now
   bypasses the old worker broker and uses a direct-core dedicated worker
   runtime for OPFS; next it needs persistent transactions, websocket
   convergence, file/blob reopen, and broader storage coverage. Resolve the
   pending-worker-write API shape before treating browser persistent writes as
   authoritative synchronous core writes. Expand coverage for true transactions,
   history/index/table partitions, cursor correctness after reload, ABI error
   mapping, format versioning, and quota/cleanup handling.
3. **Make durable `jazz-server` real.** The server now has an alpha-shaped
   `server <APP_ID>` command, `/apps/<APP_ID>/ws` route, auth/config aliases,
   durable WebSocket restart gates, and admin schema publish/list/fetch that
   converts accepted schemas into the live runtime catalogue. The loopback HTTP
   listener reloads active admin schemas on durable startup. The old
   `SyncPayload`-backed subscription introspection shim is not a target; next,
   compose the admin surface with the app-scoped WebSocket command, add a
   direct-core subscription telemetry API only if the product still needs that
   endpoint, and broaden lifecycle coverage before treating the Rust server as
   the default public server.
4. **Complete alpha-shaped auth/session admission.** Server-side signed
   local-first JWT admission, TS-side signed local-first proof generation, and
   WebSocket routing are covered for the current slice. Next, add audience/app-id
   binding hardening, auth-loss signaling, refresh, and rejection behavior.
5. **Aggressively close maintained subscription semantics.** Ordering,
   pagination beyond unordered `limit(1)`, query changes, and related lowering
   gaps should be handled as Groove/Jazz correctness work first, then surfaced
   through `jazz-tools`. TypeScript callback APIs should be driven by runtime
   subscription stream chunks; facade-side local-write refresh hooks are not an
   acceptable compatibility layer. The public filtered-subscription movement
   canary exists for local OPFS; add concrete failing examples for remaining
   movement gaps, such as ordered websocket predicate moves, offset/window
   reshuffles, selected include projection changes, or identity-scope changes.
6. **Close include/hop/gather semantics.** Add app-shaped checks for related
   record reads/subscriptions and unreadable/missing include targets. Keep
   alpha `requireIncludes()` compatibility focused on required include match
   semantics; it must not imply a traversed/failed-path include payload mode.
   For subscriptions, simple/nested forward includes, hop/gather app-shaped
   subscriptions, and one depth-3 reverse include websocket gate with selected
   include projection materialization work. The remaining gap is moving this
   relation maintenance out of TS recompute scaffolding and into native direct
   core lowering/deltas.
   This is large but should be mechanically clearer than filtered subscription
   completeness.
7. **Expand file/blob API coverage.** Keep the intentional divergence from
   alpha `files`/`file_parts`: files are single rows with `mime_type` and native
   binary-large-value `data`. The package runtime no longer exports chunk-size
   constants or `ConventionalFile*` types. Grow public upload/download/file
   helpers around this model and update remaining examples/docs that still
   teach `file_parts`. Any missing work should be phrased as a public helper or
   storage guarantee, not as `file_parts` compatibility.
8. **Polish WebSocket protocol details later.** Current batched raw
   `WireFrame` bytes are good enough unless they block another target; full
   handshake/resume/control envelope design can wait.
   The immediate browser blockers are persistent-worker transaction/file/auth
   coverage and cross-client subscription convergence over the real Rust server
   boundary.
9. **Defer React Native; keep NAPI as a parity gate.** NAPI now has direct-core
   CRUD/subscription/policy/persistence/edge-sync coverage and should stay in
   the green path. React Native remains deferred until the browser/server/API
   semantics above are in place and the storage story is clearer.
10. **Run alpha tests as APIs appear.** Pull over the relevant upstream alpha
    tests/examples incrementally rather than waiting for a monolithic
    compatibility milestone.
11. **Revisit benchmarks after semantic gaps.** Keep benchmark/product
    viability in view, but prioritize the API, storage, server, auth,
    subscription, and include gaps before another broad benchmark push.

## Browser reload-persistence slice

The alpha starter behavior to match is exact but small: a local-first todo app
adds a todo, the browser reloads, the app reopens local state, and the same todo
is rendered from browser-owned durable bytes. The package now has a positive
public `createDb` gate for that shape backed by a direct-core dedicated worker
and real OPFS storage. The browser-wasm example also has a two-load smoke:
`?smoke=reload-write&ns=...` opens `WasmDb.openBrowser(...)`, creates
`Survive reload`, waits for local durability, and closes the DB/storage after
the write flushes. `?smoke=reload-verify&ns=...` starts a fresh worker,
reopens the same browser storage namespace, subscribes to `todos`, asserts
`Survive reload:open`, and removes the OPFS file for that namespace.

The old temporary reload path has been removed, and there is no separate
TypeScript OPFS adapter in this example. Remaining browser durability work is
about breadth and correctness on the current storage path: transactional batch
behavior, durable history/index/table partition coverage, cursor and
subscription correctness after reload, ABI error mapping, format
versioning/migration rules, quota and cleanup behavior, and worker-safe
concurrency.

### Direct-Core Worker Runtime Shape

The clean package path is not the deleted alpha browser broker. The package now
has a narrow single direct-core dedicated worker runtime for browser
persistence:

- `createDb({ driver: { type: "persistent", dbName } })` opens the
  OPFS-backed direct core in a dedicated module worker, because
  `WasmDb.openBrowser(namespace, schema, config)` rejects on the main thread.
- Browser memory DBs, Node, NAPI, and server paths should keep the current
  in-process direct `CoreRuntime` path.
- The public write API must keep synchronous mutation handles:
  `db.insert/update/delete/upsert/restore(...)` returns immediately with a row
  result/handle, and `.wait({ tier })` remains async.
- The worker protocol must not restore `browser-broker`, `leader-lock`,
  `WorkerBridge`, `createWithWorker`, or the old package worker protocol.
- Row and cell payloads should keep using the direct row/record encoding path;
  do not introduce a JSON row-batch protocol for hot write/read payloads.

The landed smallest vertical slice is:

1. Add a `core-runtime` browser worker host that loads `jazz-wasm` from
   `runtimeSources`, calls `WasmDb.openBrowser(...)`, and hosts a normal
   `CoreRuntime` around the opened DB.
2. Add a main-thread `Runtime` implementation backed by that dedicated worker,
   responsible for request ids, pending write waits, subscription callbacks,
   shutdown, and transport forwarding. Auth failure callback propagation is
   still listed below as follow-up work.
3. Preserve synchronous writes either by keeping a tiny in-memory direct-core
   mirror on the main thread for immediate row materialization while sending the
   encoded mutation to the worker for OPFS durability, or by using a
   cross-origin-isolated `SharedArrayBuffer`/`Atomics.wait` synchronous RPC path
   where available. The mirror is the preferred default because public package
   users should not need cross-origin isolation just to write local state.
4. The local OPFS reopen gate is unskipped for the package-level direct `Db`
   API, but still skipped for the real React DOM todo flow until app code can
   wait for local durability before unmount.
5. The persistent OPFS websocket gate is unskipped for React todo CRUD and
   second-client convergence through one direct-core server; shutdown/reopen of
   that DOM-driven todo flow remains the durability gap above.
6. The includeDeleted websocket gate is unskipped for edge-confirmed deletes.
7. The file/blob websocket gate is unskipped: binary-large-value `files.data`
   survives persistent OPFS reopen and converges to a second websocket client.
8. The package alpha public browser gate now covers a mixed direct-core
   boundary: one public `createDb` memory writer syncs over the direct websocket
   server to a persistent OPFS worker reader, the reader observes the row via
   `subscribeAll`, then shuts down/reopens and reads the same binary-rich row
   locally.
9. The same alpha public browser gate now covers a public reconnect/offline
   slice: a persistent OPFS websocket client shuts down, another public memory
   client writes while it is offline, then the OPFS client reopens and catches
   up through `subscribeAll`, `all`, and `one` without private transport hooks.

Current blockers:

- Keep broadening worker-owned persistent DB and main-thread memory DB
  convergence under local writes, edge sync, subscriptions, and shutdown. The
  mixed memory-writer/OPFS-reader and shutdown/reopen reconnect canaries cover
  the first public end-to-end slices, but they are not a full transaction,
  auth-expiry, or short-lived transport-disconnect matrix.
- Transactions are not implemented on the persistent worker runtime yet. A
  worker-only attempt exposed deeper missing direct-core semantics:
  transaction-scoped reads, session-scoped staged writes, and exclusive
  transaction support cannot be solved honestly in the wrapper alone.
- Auth failure and mutation-error callbacks are now forwarded from the worker to
  the main thread, but they still need focused browser failure tests.
- Delete/restore websocket convergence is unskipped: a fresh websocket client
  can query the restored row after `restore(...).wait({ tier: "edge" })`.
