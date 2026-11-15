# jazz-ffi

A dynamic, JSON-in/JSON-out layer over [`jazz-app`](../jazz-app), so the
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
{ "app": "<jazz.app.json text>", "permissions": "<jazz.permissions.json text>",
  "appId": "<uuid>", "serverUrl": "http://localhost:1625",
  "storage": "memory" | "persistent", "dataDir": "./data",
  "backendSecret": "…", "adminSecret": "…",
  "session": { "issuer": "jazz:backend", "subject": "alice" } }
```

With `permissions`, the handle opens as the app's backend. It publishes the
permissions bundle (which takes `adminSecret`), then acts as the session's
user, and `account()` returns that user's account id (`session.user`). The
backend needs `serverUrl`, `appId`, `backendSecret`, `adminSecret` and
`session.subject`.

Backend mode is for server-side processes only. It keeps the sync server's
`backendSecret` and `adminSecret` in the process, so those secrets must never
ship inside a mobile or desktop app. In .65, apps on users' devices open
local-only clients (no `serverUrl`), because connecting as an end user with a
JWT is not supported yet.

**End users** (contract v2): `"serverUrl"` is the app's Jazz endpoint (its
backend's `createJazz` handler, never the core) and `"auth"` says who signs
in: `{"deviceKey": "jazz-auth-v1:…"}` (a local-first device account; make one
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
rejected, lastError, synced}`). A backend's `bundle()` / `"bundle"` is the
permissions bundle it published. Anonymous users are not supported (as in TS).

**A backend's own `/jazz` endpoint** (TS `createJazz`'s handler, over
jazz-app's `Endpoint`), for a backend serving its users' clients from the
host's own HTTP server: UniFFI `JazzHandle.endpoint(authenticator,
base_path?, session_ttl_secs?) -> Endpoint` (`is_socket(path)`,
`handle(request) -> response`, `for_user(request) -> JazzHandle`,
`socket(upgrade, SocketListener) -> Socket` with `push`/`close`); C
`jazz_endpoint_new`/`_handle`/`_is_socket`/`_for_user`/`_socket`,
`jazz_socket_push`/`_free`, `jazz_endpoint_free`. Requests and responses
are JSON (bodies base64); the host upgrades the socket path itself and
pumps its WebSocket's binary messages through the socket. The
`Authenticator` (called on a thread of its own) maps a request to
`{"kind": "user", "userId", "claims"?, "expiresAt"}`, `{"kind":
"deviceAccount"}` or none.

Without `permissions` or `auth`, it opens as a local client. `appId` is required for
`"persistent"` storage.

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
write one. Enums with payloads read as `{ "case": …, "fields": { … } }`. Text-merge
columns (`merge: "text"`) take plain string writes.

- **Query:** `{ "table", "where": [{ "column", "op", "value"? }], "orderBy":
[{ "column", "direction" }], "limit", "offset" }`. The `where` entries are
  ANDed. Ops: `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `contains` (string or
  array column), `in`, `isNull`, `isNotNull`. Columns also include `id`,
  `$createdAt` and `$updatedAt`.
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
  `one_at`, `insert_at`, `update_at`, `delete_at`, `subscribe_at`. C ABI:
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
  `upsert {table, id, row}` write the row with this id: an insert if there is
  none, else its columns replaced; `row` is a whole row, as for an insert.
  `restore(table, id, patch?)` / `restore {table, id, patch?}` brings back a
  deleted row as it was, or with `patch` (a whole row) as its content. Both
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
- **hopTo, gather, union, history, includeDeleted** (lowered by jazz-app's
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
    `offset` count every occurrence; arms read one table, with `where` only.
    Subscribing to a union whose arms overlap can mis-apply removals (deltas
    name rows by id), as in jazz-app.
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
its `jazz_close`.

Listeners run on a thread owned by the subscription, so they may call back
into the handle. Every method blocks and is safe to call from any thread.

On the C ABI, `*err` is `{"kind":"refused"|"io"|"closed","message":…}`.
Returned strings belong to the caller, who frees them with
`jazz_string_free`. Strings passed to a callback are valid only during that
call.

## Not supported yet

These are refused with `Refused`, and the message starts with "not supported
yet":

- `offset` in an include, and `required` on reverse relations or on optional
  references;
- connecting to a sync server as an end user (with a JWT);
- writing enums with payloads;
- a session issuer other than `jazz:backend`;
- transactions, and with them reads inside transactions.

Computed columns and transforms are the language bindings' job: this layer
reads and writes the stored values.

Writes are local-first, as in jazz-app. A write that the server's permissions
refuse is not refused by the call. It just never syncs.
