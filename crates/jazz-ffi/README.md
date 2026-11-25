# jazz-ffi

A dynamic, JSON-in/JSON-out layer over [`jazz-app-support`](../jazz-app), so the
Swift, Kotlin and Go bindings only (de)serialize JSON. Two surfaces over one
handle:

- **UniFFI** (proc-macros, no UDL), for Swift and Kotlin: `JazzHandle`
  (`open`, `account`, `all`, `one`, `insert`, `update`, `delete`,
  `subscribe`, `shutdown`), `Subscription` (`cancel`), `SubscriptionListener`
  (`on_delta`, `on_error`) and `JazzError` (`Refused { reason }`,
  `Io { reason }`, `Closed`). The names `shutdown` and `reason` avoid
  clashing with Kotlin's `AutoCloseable.close` and `Throwable.message`.
- **A C ABI** declared in [`include/jazz_ffi.h`](include/jazz_ffi.h), for Go
  via cgo: `jazz_open`, `jazz_call`, `jazz_subscribe`, `jazz_unsubscribe`,
  `jazz_close`, `jazz_string_free`.

The crate builds as `lib`, `cdylib` and `staticlib`. UniFFI is pinned to
`=0.32.2`, and only this crate depends on it (jazz-wasm, jazz-napi and
jazz-rn don't).

## Generating Swift and Kotlin sources

```sh
cargo build -p jazz-ffi
cargo run -p jazz-ffi --features bindgen --bin uniffi-bindgen -- \
  generate --library target/debug/libjazz_ffi.so --language swift --out-dir out/swift
cargo run -p jazz-ffi --features bindgen --bin uniffi-bindgen -- \
  generate --library target/debug/libjazz_ffi.so --language kotlin --out-dir out/kotlin
```

On macOS the library is `libjazz_ffi.dylib`. The namespace is `jazz_ffi`, so
the Kotlin package is `uniffi.jazz_ffi`, and there are no `uniffi.toml`
overrides. CI runs [`dev/gates/jazz-ffi.sh`](../../dev/gates/jazz-ffi.sh) in
the `rust-workspace` partition. It links `libjazz_ffi.a` into
[`tests/c/jazz_ffi_test.c`](tests/c/jazz_ffi_test.c) and runs it, writes both
languages' sources to `target/jazz-ffi-bindings`, and uploads them as the
`jazz-ffi-bindings` artifact. The `test-jazz-ffi-bindings` job runs
[`dev/gates/jazz-ffi-compile-bindings.sh`](../../dev/gates/jazz-ffi-compile-bindings.sh),
which compiles the generated Kotlin (with JNA) and Swift sources together
with small smoke programs ([`tests/kotlin`](tests/kotlin),
[`tests/swift`](tests/swift)) and runs them against the library.

## Config

```json
{ "app": "<jazz.app.json text>", "appId": "<uuid>",
  "serverUrl": "https://my-app.com/jazz",
  "auth": { "deviceKey": "…" } | { "bearer": true } | { "credential": true },
  "permissions": "<jazz.permissions.json text>",
  "storage": "memory" | "persistent", "dataDir": "./data", "contract": 2 }
```

An end user's persistent store, or a scoped backend client's, goes in
`dataDir/<account>/`, so each user of one directory has their own. An app
without a user, and a trusted backend client, keep theirs in `dataDir`
itself. An app without a user refuses a store there that a signed-in user
wrote (an earlier version kept users' stores there): open it as that user,
or delete it.

**Backend clients** (Go and Rust servers; TS's `createJazzClient` with a
`credential`, see `docs/content/docs/setup/backend-client.mdx`): a trusted
machine that syncs through the app's main backend, the TS `createJazz` that
lists it in `backendClients`. `"serverUrl"` is that backend's `/jazz`,
`"auth": {"credential": true}`, and `"permissions"` (required) the app's
`jazz.permissions.json`, as `createJazzClient` takes it: not published, but
how the client checks the users it acts for. The host's token callback gives the machine's own token (its
workload's OIDC token), fresh for each request and socket (UniFFI
`open_with_tokens(config, TokenProvider)`, C `jazz_open_user`). The main
backend decides what the client is, reported by `backend_client()` /
`"backendClient"`:

- `"trusted"`: it acts as the system (its session's user has kind
  `"system"`), and `for_user(user_id, claims_json)` / `jazz_for_user`
  returns a handle acting as one of the app's users, under their
  permissions. That never creates an account: `NoAccount` (kind
  `"noAccount"`, with `userId`) when the user has none yet, `Permission`
  when the main backend refuses the identity (its 403), and
  `BundleMismatch` (kind `"bundleMismatch"`, with `own` and `served`, the
  two bundle ids) when the client's `permissions` aren't the ones the main
  backend serves: reopen it with the app's published
  `jazz.permissions.json`. Closing such a handle leaves its parent open.
  The session is the parent's, shared by every user: `session_state`,
  `refresh`, `link`, `sign_out` and watching the session fail with kind
  `"notSupported"` on such a handle.
- `"scoped"`: it acts as its one user; `for_user` is refused (`Refused`,
  kind `"refused"`), as on a `for_user` handle or any other app.

A token trusted neither way fails the open with `Refused`.
`shutdown_within(timeout_ms)` / `jazz_close_within` first waits for the
writes the client holds (its `for_user` handles' too) to reach the backend,
as TS's `close({ timeoutMs })` (10 000 there by default): with memory
storage, writes still unsent fail it with `Unsynced { pending }` (kind
`"unsynced"`), and the app closes all the same. `0` waits for no writes; it
isn't instant (see "Closing").

**End users** (contract v2): `"serverUrl"` is the app's Jazz endpoint (its
backend's `createJazz` handler, never the core) and `"auth"` says who signs
in: `{"deviceKey": "jazz-auth-v1:…"}` (a device account; make one
with `generate_device_key()` / `jazz_generate_device_key`, and keep it as a
password in the platform's keychain) or `{"bearer": true}`, the app's own
sign-in, whose tokens a host callback gives (UniFFI
`JazzHandle.open_with_tokens(config, TokenProvider)`, C
`jazz_open_user(config, token_cb, ctx, err)`): it is called on a thread of
its own with `connect`, `renew`, `expired` or `refresh` and returns the token
or none (signed out). `session_state()` / `"session"` gives
`{"status": "signedIn"|"expired"|"signedOut", "user"?: {account, issuer,
subject, claims, kind}}`; `sync_status()` / `"syncStatus"` gives `{"link":
…}`; `watch_session` / `watch_sync` (C `jazz_watch(h, "session"|"sync", …)`,
`jazz_unwatch`) call back with each state, the current one first;
`refresh()` asks for a fresh token; `link(bearer)` / `"link"` links a
device account to the app's own sign-in (the session becomes that user's, on
the device's account); `sign_out()` signs out and closes the app. The sync
status is TS's `SyncStatus` (`{link, pendingUploads, oldestPendingAt,
rejected, lastError, synced}`). Anonymous users are not supported (as in TS).

Without `auth`, it opens as a local client. `appId` is required for
`"persistent"` storage.

## Sessions (TS's `JazzSession`)

A session follows who is signed in, and hands out a client for that user,
replacing it when the user changes. UniFFI `Session.open(config, tokens?)`,
C `jazz_session_open(config, token_cb, ctx, err)`. The config is the app's,
with `serverUrl` (the app's `/jazz`), `appId`, `storage` / `dataDir` (each
account's store under `dataDir/<account>/`, the device key in
`dataDir/device.key`), `"auth": {"bearer": true}` with a token provider,
and `"initial"`: `"backend"` (follow the bearer; the default, which needs
one), `"device"` or `"signed-out"`. `"linkGuest"` (default `true`, as TS):
a user the bearer names who has no account yet takes over the device
guest's account; `false` leaves the guest until `link_device_account()`.
Linking reopens the account on the bearer: `clientEpoch` moves on.

- `snapshot()` / `"snapshot"`: `{"status": "starting"|"ready"|"signed-out"|
  "transitioning"|"error"|"closed", "user"?, "clientEpoch", "error"?:
  {"kind", "message"}, "recovery"?: "action"|"session"}`, as TS's.
- `jazz()` / `jazz_session_jazz`: a `JazzHandle` for the user signed in now
  (`NoUser`, kind `"noUser"` with `"status"`, while there is none). Once the
  user changes (or signs out) every call on it fails with `Replaced` (kind
  `"replaced"`; `Closed` without contract 2): fetch the new one when
  `clientEpoch` changes. Its `shutdown` / `jazz_close` leaves the session
  open.
- A client that ends by itself (its bearer names no one at a renewal or a
  handle's `refresh`) signs the session out: `signed-out`, a new
  `clientEpoch`, and the old handles `Replaced`.
- `refresh()` (the app's auth changed: ask the bearer and follow),
  `create_device_account()`, `restore_device_account(key)`,
  `reopen_device_account()`, `link_device_account()`,
  `export_device_key()`, `sign_out()` (the session stays, signed out),
  `retry()` (as `recovery` says); C `jazz_session_call(s, method, args)`.
- `watch(listener)` / `jazz_session_watch`: each snapshot, the current one
  first, and a last `{"kind": "closed"}` when the session closes.
- On a handle it gave, `sign_out` and `link` fail with kind `"notSupported"`
  and leave the handle open: use the session's `sign_out` and
  `link_device_account`.
- `shutdown()` / `jazz_session_close`: closes the session; its handles fail
  `Closed` and its watches end. No C token callback runs after; a Swift or
  Kotlin token provider already asked may outlive it (see "Closing").

Not yet: deleting a session account's store through its handle (refused).

## Closing

Every close (`shutdown`, `shutdown_within`, C `"close"` and the functions
that free) gives the calls in flight the same guarantees:

- Calls waiting on the server or the network (`wait_for_sync`, a global
  tier, remote reads, `for_user`, `link`, `refresh`) fail `Closed`: when the
  write drain (`timeout_ms`) ends on a root handle, at once with `0` and on
  a `for_user` or session handle. What they sent may still apply, and a
  later refusal of it is still reported.
- Local work already started finishes, with 30 s as its backstop, and
  stream sessions get up to 2 s to end. So closing takes about `timeout_ms`,
  plus that: `0` is not instant. Nor is that a hard limit: work can't be
  interrupted while it runs, and closing also waits for the store to close
  and for a host name lookup in progress. Don't close on a UI thread.
- A `for_user` or session handle closes only itself, and rolls back what it
  began.
- A session doesn't wait for the operation in flight, which fails `Closed`
  and installs no client; an open it interrupted gets up to 2 s to let go of
  its store. `Replaced` is only for a session that goes on: closing is
  always `Closed`, and wins when both hold.
- On the C ABI, closing waits for a token callback that is running, as the
  host frees its `ctx` after: hosts make theirs cancellable. Through UniFFI
  (Swift, Kotlin) it doesn't wait: a token provider already asked may still
  run after `shutdown` returns, and what it gives is dropped.

On the C ABI, `jazz_close`, `jazz_close_within` and `jazz_session_close`
free, so nothing may run beside them. To close with calls in flight:

1. `jazz_call(h, "close", {"timeoutMs"?})`, or
   `jazz_session_call(s, "close", {})`: closes without freeing, beside any
   other call, and again if need be. Keep its error (`unsynced`, say).
2. Start no more calls, and wait for every call in flight, other closes
   included.
3. Free with `jazz_close` or `jazz_session_close`.
4. Only then free the token callback's `ctx`.

From inside a token callback do step 1 only and the rest later from
another thread: waiting there deadlocks.

The private store's callback (or UniFFI `PrivateStore`) runs on the
client's own thread, which every other call waits for: from inside it, call
nothing on the handle but `"close"` (UniFFI `shutdown`). There it waits for
nothing and returns at once: the handle is closed, its calls fail `Closed`,
no store or token callback starts after this one, and the client stops once
the callback returns. It ignores a `timeoutMs` (UniFFI `shutdown_within`):
nothing drains, so the writes it asks for and loses (memory storage,
unsent) fail it `Unsynced { pending }` at once, mapped as any close's
(`"unsynced"` in C, `Io` without contract 2). Free the handle later, from
another thread.

## Lifecycle (as TS's)

- `begin_mergeable()` / `txBegin {"serializable": false}`: a mergeable
  transaction (TS's default `transaction`). Its writes commit together and
  merge with concurrent ones, and its commit is never refused as a
  conflict. `begin()` / `txBegin {}` stays serializable.
- **One open app per `dataDir`:** opening a persistent store another app
  has open, in this process or another, is refused (kind `"refused"`): the
  engine is its file's only writer. An open that follows a close, sign-out
  or last drop in this process waits up to 5 s for it to finish. Share the
  one handle, or give each app a `dataDir` of its own.
- `disconnect()` / `reconnect()` (C `"disconnect"`, `"reconnect"`): leave
  the server, with local reads and writes going on, then resume sync.
- `delete_client_storage()` / `"deleteClientStorage"`: closes the app and
  deletes its local store, so the next open starts empty; open transactions
  roll back and uploads abort first. The store stays locked until it is
  gone.
  On the C ABI, free the handle with `jazz_close` afterwards, as after
  `signOut`.
- `watch_mutation_errors(listener)` / `jazz_watch(h, "mutationErrors", …)`:
  each write the server refused that no durable wait (a `global` tier,
  `wait_for_sync`) reported, as TS's `onMutationError`:
  `{"transaction", "message", "code", "reason", "denied", "serializable"}`,
  where `code` and `reason` are TS's `MutationErrorEvent`'s (`code`:
  `permission_denied`, `serialization_conflict`, `transaction_conflict`,
  `client_clock_too_far_ahead` or `write_rejected`). Refusals from
  before the first watch come first.
- Every watch (session, sync, mutation errors) ends when the app closes,
  whichever way (`shutdown`, `sign_out`, `delete_client_storage`): a last
  state `{"kind": "closed", "message"}` (UniFFI), or a last callback with
  kind `"error"` and `{"kind": "closed", …}` (C), after which nothing
  comes.

## Values, queries, rows, deltas

Values are plain JSON, typed by the schema:

| Column type        | JSON                                           |
| ------------------ | ---------------------------------------------- |
| string, uuid, enum | string                                         |
| int, float         | number                                         |
| bigint             | decimal string (a number is accepted on input) |
| boolean            | boolean                                        |
| timestamp          | number, ms since the epoch                     |
| json               | any JSON                                       |
| bytes              | base64 string                                  |
| array              | JSON array                                     |
| optional           | `null`                                         |

A float that isn't finite (NaN, ±infinity) reads as `null`; JSON can't
write one. Text-merge
columns (`merge: "text"`) take plain string writes.

- **Query:** `{ "table", "where": [{ "column", "op", "value"? }], "orderBy":
  [{ "column", "direction" }], "limit", "offset" }`. The `where` entries are
  ANDed. Ops: `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `contains` (string or
  array column), `in`, `notIn`, `isNull` (`"value": false` means "is not
  null"), `isNotNull`; `eq`/`ne` with `null` test for null. They mean what
  TS's `where` means (`jazz::query::where_condition`). Columns also include
  `id`, `$createdAt` and `$updatedAt`.
- **Include:** `"include": [{ "relation": "<name>", "required"?: bool,
  "query"?: { "where", "orderBy", "limit", "select", "include" } }]`, at the
  root or nested in an include's `query`, which has no `table` and no
  `offset`. Relations are the ones `jazz.app.json` declares. In a row, an
  included relation sits under its name: a forward relation as a row or
  `null`, a reverse one as an array. `required` (as TS's `requireIncludes`)
  keeps only rows whose target is readable, for forward relations through a
  non-optional column. Subscriptions deliver included rows too, and deliver
  an `updated` row when its related rows change.
- **Select:** `"select": ["<column>", …]` returns those columns (and `id`,
  always) instead of all of them, at the root or in an include. `"*"` selects
  every column. An entry may read part of a large value instead of all of
  it: `{"column", "from", "to", "units": "utf8"|"utf16"|"bytes"}` (half-open;
  UTF-8 on code point boundaries; bytes come back base64) or `{"column",
  "at": "<JSON pointer>"}` for a json column. Parts are for the root query,
  not inside includes.
- A forward relation through a `uuid[]` column reads as an array of the
  referenced rows; `required` on it keeps only rows whose every reference is
  readable. Stricter than TS: `required` on reverse relations or optional
  references, an `offset` other than 0 in an include, and a relation named
  like a column of its table are refused by name.
- **Transactions:** UniFFI `JazzHandle.begin() -> Transaction` (`all`,
  `one`, `insert`, `update`, `delete`, `commit(tier?)`, `rollback`; dropped
  uncommitted, it rolls back). C ABI: `jazz_call` methods `txBegin`
  `{serializable?}` → `{tx}`, `"tx"` in the args of any read or write, then
  `txCommit {tx, tier?}` or `txRollback {tx}`. Transactions are
  serializable: reads see a snapshot plus their own writes; writes commit
  together or not at all. A transaction that only read just ends. Closing
  the app rolls back open transactions.
- **Tiers:** reads and subscriptions take `"local"`, `"localFirst"`
  (default) or `"remote"`; writes and commits take `"local"` or `"global"`,
  the durability they wait for (`"edge"` is refused). UniFFI: `all_at`,
  `one_at`, `insert_at`, `update_at`, `delete_at`, `upsert_at`, `subscribe_at`. C ABI:
  `"tier"` in the args, or `{"query", "tier"}` for `jazz_subscribe`.
- **waitForSync:** UniFFI `wait_for_sync(tier)`, C ABI `waitForSync {tier}`:
  returns once every write committed so far reaches `tier` (`local` returns
  at once), and fails if the server refused one. It checks up to the last
  4096 writes individually; older ones rely on the core wait, which can miss
  a refusal that is no longer pending until jazz#5728 is fixed.
- **Partial edits:** UniFFI `edit_text(table, id, column, edited_from,
  text)` sets a text-merge column (`merge: "text"`) to `text`, an edit of
  `edited_from`, the value its writer last saw: changes others made since are
  kept, as the server merges them (TS's `editedFrom`). `edit(table, id,
  patch?, edits)` takes a list of `{"kind": "text", "column", "text",
  "editedFrom"}`, `{"kind": "splice", "column", "within": {"units":
  "utf8"|"utf16"|"bytes", "from", "to"}, "splices": [{"at", "delete",
  "insert"}]}` (`insert` a string, or base64 for bytes; `at` relative to
  `from`) and `{"kind": "jsonSet", "column", "edits": [{"at": "<JSON
  pointer>", "value"}]}`, plus an optional whole-column patch, as one write.
  Without `within`, a splice's page is the whole current value, in
  `"units"` (default `"utf8"`). Both on `Transaction` too. C ABI: `jazz_call`
  `textEdit {table, id, column, units?, edits: [{pos, delete, insert}]}`
  (splices over the whole value), `editText {table, id, column,
  editedFrom, text}` and `edit {table, id, patch?, edits}`, with `"tx"` and
  `"tier"`.
- **Permission advice:** UniFFI `can(table, op, args_json) -> Option<bool>`,
  C ABI `can {table, op, id?, row?}` → `true` | `false` | `null`: whether the
  server would let this user `read` / `insert` / `update` / `delete`, without
  doing it (TS's `canRead`, …). `insert` takes `row`; `read` and `delete`
  take `id`; `update` takes `id` and an optional `row` patch. `null` (no
  server, or no decision) means unknown. It is advice: `true` doesn't promise
  the write will succeed, as the data or permissions may change first. Not
  inside a transaction.
- **Upsert and restore:** UniFFI `upsert(table, id, row)` and C ABI
  `upsert {table, id, row}` write the row with this id: for a new id an
  insert (`row` then needs every required column, else `missing required
  field`), for an existing row the columns `row` names (a partial row
  updates). At a tier (UniFFI `upsert_at(table, id, row, tier)`, C ABI
  `"tier"`) they return once the write reached it, with the row as written
  here: not as someone else may have changed it by then.
  `restore(table, id, patch?)` / `restore {table, id, patch?}` brings back a
  deleted row as it was, or with `patch` (a whole row) as its content; a
  patch naming another row's `id` is refused. Both
  return the row, take `"tx"` and `"tier"`, and are on `Transaction` too.
- **Streaming uploads:** UniFFI `begin_upload(table, column, kind, id?,
  row?) -> Upload` (`push(bytes)`, `finish() -> row`, `abort()`); C ABI
  `uploadBegin {table, column, kind?: "insert"|"update"|"upsert", id?,
  row?}` → `{upload}`, `uploadPush {upload, bytes: base64}`, `uploadFinish
  {upload, tier?}` → the row, `uploadAbort {upload}`. An insert takes the
  row's other columns as `row` and no `id`; update and upsert take the `id`.
  Not inside a transaction. Read a large value back in pages with `select`
  parts.
- **Contract version:** `"contract": 1 | 2` in the open config (default 1).
  With 2, errors have three more kinds: `permission` (the app's permissions
  refuse it, e.g. a write waiting for global durability), `notSupported`
  (anything "not supported yet") and `conflict` (a serializable transaction
  lost to another write; retry it). UniFFI: `JazzError.Permission`,
  `NotSupported`, `Conflict`, each with a `reason`. A contract 1 host sees
  these as `refused`, as before.
- **hopTo, gather, union, history, includeDeleted** (lowered by jazz-app-support's
  `untyped` builder, as the typed Rust API and TS lower them):
  - `{"table", "where"?, "orderBy"?, "limit"?, "hopTo": "<rel>" | ["<rel>",
    …], "after"?: {"where", "orderBy", "limit", "offset", "select"}}`: rows
    of the last hop's table; the top-level clauses filter the rows hopped
    from, `after` the rows reached.
  - `{"table", "where"?, "gather": {"relation", "maxDepth"?}}`: the rows
    matching `where` and every row reached along a forward relation of the
    table to itself, up to `maxDepth` (default 10).
  - `{"union": [Q, …], "orderBy"?, "limit"?, "offset"?}`: every arm's rows,
    as TS's union: a row matched by two arms comes twice, and `limit` /
    `offset` count every occurrence; arms read one table, with `where` and
    an optional `"label"` only. An arm without a label is labelled by what
    it reads, as every client labels it, so two identical arms need labels.
    Subscribing to a union whose arms overlap can mis-apply removals (deltas
    name rows by id), as in jazz-app-support.
  - `{"table", "history": true, "where"?, "orderBy"?, "limit"?}`: the
    versions the server accepted, each row with `"$history":
    {"transactionId", "acceptedAt"}` (order by `"$acceptedAt"`); read it at
    tier `remote`. Not subscribable yet.
  - `"includeDeleted": true` on any of these or a plain query reads deleted
    rows too.
- **Row:** `{ "id", "<column>": … }`, with the authored column names. An
  insert may give its own `id`.
- **Delta:** `{ "added": [row], "updated": [row], "removed": [id], "reset",
  "coalesced" }`. The first delta has `reset: true` and holds the full result.
  `coalesced` stays `false` until the engine coalesces lagging readers
  ([jazz#5635](https://github.com/garden-co/jazz/pull/5635)). Deltas carry no
  positions, so an ordered subscription re-sorts locally. A row that only
  moved is left out.
- **Result keys:** each `added` and `updated` row carries `"$key"`, an
  opaque string naming that result, and `"removedKeys"` runs beside
  `removed` (same order). One row can be several results: each version of a
  history subscription (which carries `$history`), or each arm of a union
  that matches it (UNION ALL). A host that holds such results applies
  `updated` and `removedKeys` by `$key`; elsewhere the key and the `id`
  name the same result, and applying by `id` stays correct.

Every way a subscription ends, other than being cancelled, ends with one
last error, after which no callback comes: a rejection (`Refused`), or
`Closed` when the app closes or the stream ends for any other reason (the
client stopping, the server ending the query). A row that permissions stop
granting arrives as `removed`; the subscription goes on. After the app
closes, each open subscription gets one last error, `Closed`
(kind `"closed"` on the C ABI). `jazz_unsubscribe` returns only once no
callback is running, so the host may free `ctx` right after. Called from
inside the callback, it doesn't wait. A subscription may be freed before or
after `jazz_close`. No call on a handle may run concurrently with, or follow,
its `jazz_close`: with calls in flight, use `"close"` first (see "Closing").

Listeners run on a thread owned by the subscription, so they may call back
into the handle. Every method blocks and is safe to call from any thread.

On the C ABI, `*err` is `{"kind","message"}`, kind `"refused"`, `"io"` or `"closed"`; with `"contract": 2` also `"permission"`, `"notSupported"`, `"conflict"`, `"noAccount"` (with `"userId"`) and `"unsynced"` (with `"pending"`).
Returned strings belong to the caller, who frees them with
`jazz_string_free`. Strings passed to a callback are valid only during that
call.

## Not supported yet

These are refused with `Refused`, and the message starts with "not supported
yet":

- `offset` in an include, and `required` on reverse relations or on optional
  references;

Computed columns and transforms are the language bindings' job: this layer
reads and writes the stored values.

Writes are local-first, as in jazz-app-support. A write that the server's permissions
refuse is not refused by the call. It just never syncs.

## End-to-end encryption

An app that declares encrypted columns opens with a private store the host supplies: `JazzHandle.open_with_store`
(UniFFI) or `jazz_open_encrypted` (C ABI) take a callback that answers the store's requests. UniFFI
uses typed methods, with unsigned 64-bit revisions and incarnations and byte buffers. The config's
`"environment"` names the deployment's environment (default `dev`). Opening such an app any other way is
refused, naming the option. A device's schema has the plaintext types of its encrypted columns, so rows go
in and out as ordinary JSON; writes of a space whose key the device does not hold yet wait for it
(ten seconds at most). The store is called on the client's thread, one request at a time.

The required UniFFI `load_all(scope, incarnation)` reads a single snapshot at startup. The host
checks the scope marker in the same transaction as the document read and returns
`PrivateStoreError::Wiped` if the scope is absent or its incarnation differs. An intact scope with
no documents returns an empty sequence. Each document carries the name, bytes and revision from
that snapshot. Jazz authenticates the records and uses the revisions for later saves.

The UniFFI scope snapshot v1 result is a sequence of `ScopeDocument` records in field order
`doc` (UTF-8), `bytes` (raw byte sequence), `revision` (unsigned 64-bit). The sequence, string
and byte lengths are 32-bit big-endian and the revision is 64-bit big-endian. This adds a required
method to the prerelease UniFFI callback interface. Regenerate the bindings with the matching
library and update Swift and Kotlin stores together. The on-disk private-store formats are unchanged.

A C callback request is JSON text `{ "op": ..., ... }`. Bytes are base64. Numbers that may exceed 2^53 use decimal text.

| `op`        | Fields                                                    | Answer (JSON)                        |
| ----------- | --------------------------------------------------------- | ------------------------------------ |
| `openScope` | `scope`                                                   | the incarnation, as text             |
| `load`      | `scope`, `doc`                                            | `{ "bytes", "revision" }`, or `null` |
| `save`      | `scope`, `incarnation`, `doc`, `bytes`, `expected`/`null` | the new revision, as text            |
| `list`      | `scope`                                                   | the document names                   |
| `delete`    | `scope`                                                   | `null`                               |
| `protect`   | `secret`                                                  | the protected blob, base64           |
| `unprotect` | `blob`                                                    | the secret, base64                   |

The C callback can opt into the existing `loadAllV1` encoding. When the `openScope` request
includes `"loadAll":"v1"`, a host that supports it answers with
`{"incarnation":"…","loadAll":"v1"}`. Jazz then sends `loadAllV1` with `scope` and `incarnation`.
Return a JSON array of `{"doc":"…","bytes":"…","revision":"…"}` objects, or answer
`wiped` for a missing or changed scope. Hosts that answer `openScope` with the incarnation
as text continue to receive single-document requests.

An answer is ok with its JSON, or not ok with `conflict` (the document's revision is not `expected`), `wiped`
(the scope is not of that incarnation) or a message.

Use `encryption_call(op, args_json)` for the runtime operations below. In C, pass `"encryptionCall"` to
`jazz_call` with `{"op", "args"?}`.

| Operations                                                                                | Purpose                                                                                                                                                                     |
| ----------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `waitUntilReady`, `state`, `reports`                                                      | Wait for account startup, inspect the loop, or drain pending reports as JSON objects. `waitUntilReady` takes an optional non-negative integer `timeoutMs` (default 30,000). |
| `ready`, `resetAccount`                                                                   | Verify the account on demand, or reset it when the app enables account reset.                                                                                               |
| `deviceId`, `devices`                                                                     | Read this device or list the account's devices.                                                                                                                             |
| `challenge`, `answer`, `approvalCode`, `approve`                                          | Approve a device after comparing codes.                                                                                                                                     |
| `remove`, `requestRemoval`, `completeRemovals`                                            | Remove a device or process removal requests.                                                                                                                                |
| `recoveryCreate`, `recoveryRegister`, `recoveryRestore`, `recoveryRetire`, `recoveryList` | Manage recovery. Register and restore accept a 24-word `phrase` or a 64-character hexadecimal `secret`.                                                                     |
| `keysDeliver`, `keysRotate`, `keysRecoverRotation`                                        | Deliver keys or rotate a space's key.                                                                                                                                       |
| `spaceRow`                                                                                | Resolve the address of a space. Pass its `table` and row `id`, then use the returned `scopeId` and `identifier` in the member row.                                          |
| `prepareSpace`, `retryWithCurrentKey`                                                     | Prepare a space for a write or retry the write with the current key.                                                                                                        |
| `retryWrite`                                                                              | Wait for the refused write’s space. Returns `"retry"`, `"last"` (one attempt with the current key), or `"stop"`.                                                            |

`keysRotate` returns `{"delivered": number}` with the number of deliveries in the
transaction that commits the new key, up to 100. The runtime delivers the key to the
remaining members in the background. A failure in this pass produces a `delivery-failed`
report with the space and reason.

An encryption failure carries a `code` field. Store and database failures have the I/O kind. Other
encryption failures have the refusal kind. A session (`jazz_session_open`) takes no store yet.

### Space membership

Pass the name of the table declared as the encryption scope and the space row's ID.
Use `member_add(table, id, account_id, tier)`, `member_list(table, id)` and
`member_remove(table, id, account_id)` on the UniFFI handle. In C, use these
`jazz_call` methods:

| Method         | Arguments                                 | Result                 |
| -------------- | ----------------------------------------- | ---------------------- |
| `memberAdd`    | `{ "table", "id", "accountId", "tier"? }` | Inserted member row    |
| `memberList`   | `{ "table", "id" }`                       | Visible member rows    |
| `memberRemove` | `{ "table", "id", "accountId" }`          | Number of rows removed |

A member row contains `id`, `scopeId`, `identifier` (the space row ID) and `accountId`.
Add settles like an insert, with optional `local` or `global` durability. Remove deletes
the account's visible rows for the space in a serializable transaction and waits for
the authority to accept it. A count of zero means there were no matching rows visible
to the caller.
