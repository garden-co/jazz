# Alpha-style example target matrix

This repo branch is the alpha graft: the public `jazz-tools` package is being
forced onto `jazz_core` through the shared direct-core runtime and WASM/NAPI
bindings. The purpose is to pin app-shaped flows that alpha users and
maintainers can recognize while deleting the old parallel engine paths, not to
host a separate compatibility facade beside the real package.

## Alpha targets surveyed

- Upstream targets surveyed for this slice:
  - `crates/jazz-wasm/tests/wasm.rs`
  - `crates/jazz-tools/src/runtime_core/tests/basic.rs`
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

Rows remain descriptor/raw encoded at the ABI boundary. This package may add
small app-facing helpers when they express real app semantics, but it should not
grow a method-per-ABI forwarding layer or full alpha query builder.

## Gap matrix from first upstream targets

| Upstream target                                     | Covered now                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | Still missing                                                                                                                                                                                                                                                |
| --------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `crates/jazz-wasm/tests/wasm.rs`                    | WASM module loading from TypeScript, schema bytes, deterministic row IDs, `WasmDb` create/update/delete, and query reads are covered by `npm run test:alpha-public-flow` plus the broader demo.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | Rust-side `wasm-pack test --node` parity, exported `generate_id`, `current_timestamp`, `parse_schema`, and public query-builder API tests are not present in this repo's WASM package.                                                                       |
| `crates/jazz-tools/src/runtime_core/tests/basic.rs` | WasmDb-shaped insert/query, update/delete, callback subscription rows, restart-by-snapshot, owner-policy reads, and Rust WebSocket server durable restart are represented in the `WasmDb` binding checks.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | A local `jazz-tools` runtime_core surface, default materialization API parity, durable restart through the exact runtime_core harness, and the exact Rust runtime_core test harness are still absent.                                                        |
| `packages/jazz-tools/tests/browser/db.all.test.ts`  | `jazz-tools.ts` covers `createDb`, `defineApp`/schema tokens, table/query objects, `insert`, `update`, `delete`, `restore`, row-like write results with `.value`/`.handle`/`.wait(...)`, `all`, `one`, `allForIdentity`, `subscribe(query, callback)`, boolean/text/integer equality, scalar boolean/text/integer `in`, array-element `in`, integer/text `gt`/`gte`/`lt`/`lte`, nullable UUID `isNull`/`isNotNull`, nullable literal equality/inequality and mixed nullable `in`, text inequality, chained positional and object-style filters, one-shot text and array `contains`/`limit`, selected projections, result ordering, offset pagination, text-array insert/update/readback, whole-array `eq`/`in`, Bytea `eq`/`in`, binary-large-value file helpers, one-shot facade include/hop/gather reads over schema references, required object-style includes, one-shot nested include selection for scalar/reference paths, and simple/nested forward include subscription callbacks rebuilt from subscription rows. The browser example now proves reload persistence through `WasmDb.openBrowser(namespace, schema, config)` OPFS storage and keeps a subscription useful after reload/update. | Full browser `jazz-tools` test runner, reverse include subscriptions, hop/gather subscriptions, selected include projection subscriptions, broader relation query lowering/subscriptions, and durable/upload/streaming object-storage semantics are missing. |

## Current gaps versus alpha

- Browser reload persistence is covered by real browser storage in
  `examples/browser-wasm`: the write page opens
  `WasmDb.openBrowser(namespace, schema, config)`, writes a Jazz todo row, and closes the
  DB/storage after the write flushes through OPFS; the verify page starts a
  fresh worker, reopens the same namespace, reads the row back from
  browser-owned durable bytes, keeps a subscription open, updates the restored row, and
  observes the subscription move from open to done. The old temporary reload path and
  TypeScript OPFS adapter are gone.
- Public TypeScript API compatibility is intentionally thin. Since this repo is
  separate from the alpha repo, the replacement package is named `jazz-tools`
  and currently routes simple public-flow/todo-shaped examples through
  `jazz-tools.ts`; the old todo-only alpha facade has been deleted. It now
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
  still remain live-row only. It does not implement the full upstream query
  builder.
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
  `schema.table(..., { relations })` aliases for reverse include names.
  Maintained reverse relation subscriptions and selected include projection
  subscriptions remain query-builder gaps.
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
{ parent: true } } })`, and materializes the alpha-shaped included object from
  flat subscription rows. Reverse includes, selected include projections, hops,
  and gather subscriptions remain query-builder gaps. The nested-forward slice
  is pinned by a Rust prepared subscription regression so root rows survive
  multi-segment include paths.
- Public transaction compatibility now has a first bounded slice over real ABI
  transaction handles: `beginTransaction`, synchronous `transaction(cb)`,
  transactional insert/update/upsert/delete/restore, commit with local write
  waiting, rollback, custom ids, and exclusive transaction `tx.all`/`tx.one`
  reads over staged state. Same-row insert/update, update/delete, and
  restore/update sequences are coalesced before commit so the public facade can
  present alpha-shaped staged-row semantics while still replaying a minimal ABI
  operation set. Focused compat tests also pin rollback after coalesced staged
  changes, repeated same-row upsert sequences, sync and async `transaction(cb)`
  commit return/throw rollback behavior, and rollback after rejected async
  callbacks. Mergeable transaction reads, transaction query-builder reads, and
  exclusive restore remain future work.
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
- `subscribeAll` is not exported from this replacement package. The current TS
  surface uses `db.subscribe(query, callback)` directly over `WasmDb.subscribe`
  snapshot and delta chunks.
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
  `files.data`. This is still not upload routing, bounded streaming, durable
  object storage, resumable transfer, or full public file API compatibility.
- The browser worker smoke also exercises `dbCanUpdateEncodedForIdentity` over
  the worker boundary for the basic owner-policy todo flow, allowing the owner
  and denying a different deterministic identity.
- The browser worker example now owns its local direct worker helpers instead of
  importing the deleted alpha `abi-helpers` package fragment. The worker now
  owns direct `WasmDb` and transport objects, with rows/cells crossing the
  worker boundary as Record-encoded bytes.
- Node HTTP, todo WebSocket, shared-todo WebSocket, and chat WebSocket smokes
  now run directly on `createDb`/`WasmDb.connectUpstream()`. WebSocket frames are
  opaque byte batches; row decoding stays at the app/test edge.

## Next targets

1. **Create the real `jazz-tools` package surface.** Because this is a separate
   repo, the compatibility package should be named `jazz-tools`. Add alpha
   tests/examples directly as soon as the APIs they need exist; do not build a
   separate compatibility-package abstraction just to host those tests.
2. **Broaden browser storage coverage.** `WasmDb.openBrowser(namespace, schema, config)`
   uses the repo's OPFS-backed browser storage path and the browser smoke
   proves reload persistence plus a post-reload subscription/update, worker-safe
   same-namespace handoff, and a multi-write reopen/subscription durability gate. Expand
   coverage for true transactions, history/index/table partitions, cursor
   correctness after reload, ABI error mapping, format versioning, and
   quota/cleanup handling.
3. **Make durable `jazz-server` real.** The server now has an alpha-shaped
   `server <APP_ID>` command, `/apps/<APP_ID>/ws` route, auth/config aliases,
   durable WebSocket restart gates, and admin schema publish/list/fetch that
   converts accepted schemas into the live runtime catalogue. The loopback HTTP
   listener reloads active admin schemas on durable startup; next, compose that
   admin surface with the app-scoped WebSocket command and broaden lifecycle
   coverage before treating the Rust server as a drop-in replacement.
4. **Complete alpha-shaped auth/session admission.** Server-side signed
   local-first JWT admission, TS-side signed local-first proof generation, and
   WebSocket routing are covered for the current slice. Next, add audience/app-id
   binding hardening, auth-loss signaling, refresh, and rejection behavior.
5. **Aggressively close maintained subscription semantics.** Ordering,
   pagination beyond unordered `limit(1)`, query changes, and related lowering
   gaps should be handled as Groove/Jazz correctness work first, then surfaced
   through `jazz-tools`. TypeScript callback APIs should be driven by runtime
   subscription stream chunks; facade-side local-write refresh hooks are not an
   acceptable compatibility layer.
6. **Close include/hop/gather semantics.** Add app-shaped checks for related
   record reads/subscriptions and unreadable/missing include targets. Keep
   alpha `requireIncludes()` compatibility focused on required include match
   semantics; it must not imply a traversed/failed-path include payload mode.
   For subscriptions, simple and nested forward includes work. Reverse includes
   need a query/ABI representation for child-table membership edges; hop/gather
   subscriptions and selected include projections remain explicit gaps.
   This is large but should be mechanically clearer than filtered subscription
   completeness.
7. **Expand file/blob API coverage.** Keep the intentional divergence from
   alpha `files`/`file_parts`: files are single rows with `mime_type` and native
   binary-large-value `data`. The package runtime no longer exports chunk-size
   constants or `ConventionalFile*` types. Grow public upload/download/file
   helpers around this model and update remaining examples/docs that still
   teach `file_parts`.
8. **Polish WebSocket protocol details later.** Current batched raw
   `WireFrame` bytes are good enough unless they block another target; full
   handshake/resume/control envelope design can wait.
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
is rendered from browser-owned durable bytes. The browser example now has a
positive two-load smoke for that shape backed by real OPFS storage.
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
