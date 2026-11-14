# cojson-core-rn

## 2.0.0-alpha.63

### Patch Changes

- 687f9b8: On iOS, React Native's sync link now follows the device's network path: when Wi-Fi goes away the link is dropped and reconnected at once, and when the network comes back it reconnects without waiting out the retry backoff. A link on cellular is kept when Wi-Fi takes over. Android is unchanged. Thanks to Xelson for this.
- 9e594cc: React Native now gives up a link the server has gone silent on (Wi-Fi off, a suspended server, a half-open connection) after 45 s and reconnects, instead of showing a live link with nothing arriving. Thanks to Xelson for this fix.
  - jazz-rn-ios@2.0.0-alpha.63
  - jazz-rn-android@2.0.0-alpha.63

## 2.0.0-alpha.62

### Patch Changes

- 56b3e92: Update live results with a forward include when the included row changes through a nullable reference.
- 3e1622a: React Native now tells a relay's refused upstream apart by its error code, as Node and browser clients do. When the server serves another authority history than the device's store, the client reports an `AuthorityChangedError` and the link reads `{ state: "offline", reason: "authority-changed" }`, not a bare "stopped". This raises the native relay ABI to 6, so ship a new native build with this release.
- 83ecc46: A React Native relay whose database refuses its upstream (for example, it cannot mirror the server's catalogue) now stops that connection and reports the refusal as a non-retrying upstream failure, with a warning in the log, instead of keeping a socket open that never syncs and reporting nothing.
- 1a0d90a: React Native devices whose store alpha.60 opened without the app's migrations recover. When the server refuses such a store (the new `store_opened_without_migrations` error code), a store holding no unsynced writes is rebuilt and syncs again from the server, with nothing for the app to do. One that does hold writes is kept, and `db.deleteClientStorage()` (which before did nothing on React Native) now deletes the account's native store, keeping the account, so the app can reset it; that drops those writes. See "Upgrading from alpha.60" in the docs.

  On React Native, `clearOnSignOut` and `logout({ wipeData: true })` now really delete the signed-out account's local store too (before, they did nothing there), closing every client of that account in the app first. Unsynced writes, and refused writes kept for retry, go with it.

- 4d6f815: Keep a transaction's reads inside its lifetime. Reads run in the order they were started, see the writes staged before them, and share one serializable snapshot. Committing while a read started through the transaction is still pending now fails with a `not_observed` error and ends the transaction without publishing it, instead of publishing before the read answered. Await every transaction read before committing.
  - jazz-rn-ios@2.0.0-alpha.62
  - jazz-rn-android@2.0.0-alpha.62

## 2.0.0-alpha.61

### Patch Changes

- 5be4fd9: Fix React Native clients reading no rows from a server with schema history. The native relay opened its store from the current schema alone, so it stored columns under different names than the server. It now receives the migrations the app ships, as Node and browser clients do. This raises the native relay ABI to 5, so a JavaScript bundle needs a native build from the same release.
- 328c080: The React Native native relay is now built with a size-oriented profile (`opt-level = "s"`, fat LTO, one codegen unit, staticlib only). On iOS arm64 the relay adds about 9.9 MB to an app instead of 16.7 MB (stripped), and the device `.a` shrinks from 69.6 MB to 21.1 MB. Panics still unwind as before. Thanks to @boorad.
  - jazz-rn-ios@2.0.0-alpha.61
  - jazz-rn-android@2.0.0-alpha.61

## 2.0.0-alpha.60

### Major Changes

- 79b143e: Rename exclusive transactions to serializable transactions. `db.exclusiveTransaction(...)` is now `db.serializableTransaction(...)`, `db.beginExclusiveTransaction()` is now `db.beginSerializableTransaction()`, the Effect `jazz.exclusiveTransaction` is now `jazz.serializableTransaction`, `ExclusiveWriteResult` and `ExclusiveWriteHandle` are now `SerializableWriteResult` and `SerializableWriteHandle`, and the transaction kind `"exclusive"` is now `"serializable"`. A conflict rejection now has the code `"serialization_conflict"` instead of `"exclusive_conflict"`. The error for reads that are not supported yet now says "is not supported in serializable transactions yet". In Rust, `Db::exclusive_tx` is now `Db::serializable_tx`, and `ExclusiveTx`, `ExclusiveTxOps` and `ExclusiveTxRef` are now `SerializableTx`, `SerializableTxOps` and `SerializableTxRef`. The core Rust names follow: `TxKind::Exclusive` is now `TxKind::Serializable`, `RejectionReason::ExclusiveConflict` is now `RejectionReason::SerializationConflict`, `node::Error::UnsupportedExclusiveRead` is now `node::Error::UnsupportedSerializableRead`, `open_exclusive*` and `commit_exclusive*` are now `open_serializable*` and `commit_serializable*`, `Snapshot::exclusive_base` is now `Snapshot::serializable_base`, and `ViewCompleteExclusiveCoverage` is now `ViewCompleteSerializableCoverage`. The raw `writeState` JSON from the napi, wasm and React Native bindings now spells the conflict fate `SerializationConflict` instead of `ExclusiveConflict`. Only the names changed: validation, conflicts, and stored and synced data are the same, and the old names have no alias.

### Patch Changes

- 0aec19c: Fix live combined aggregate subscriptions missing changes when the first aggregate value stays unchanged. Complete summaries now identify replacements without changing group occurrences or limiting large multi-output summaries.
- fbbb242: Core errors from the WASM binding, NAPI promise rejections and React Native now carry their stable reason name as `error.reason` (for example `authorization_denied`), and rejected writes are classified by it instead of by their message. The `causality_violation` and `cascade_rejected` rejection codes, which the SQLite engine never produces, are gone, and the Better Auth adapter no longer retries `cascade_rejected`.
- ab06af5: Add `$deletedAt` and `$deletedBy` as optional projection, filter, and sort columns after `.includeDeleted()`. They return the winning deletion's authored timestamp as a `Date` and author as a `RowAuthor`, or `null` for live and restored rows. History versions share the logical row's current deletion timestamp and author.

  Filters on `isNull`, `eq: null` and `ne: null` now become null checks for every column type before any type-specific handling. Author columns such as `$createdBy` previously threw on `eq: null`; they now match rows whose author is null.

  `$deletedAt` and `$deletedBy` are rejected in flat joins: `.includeDeleted()` widens only the root query, so joined sources have no deletion metadata.

  The query compiler now treats `$createdAt` and `$updatedAt` like other ordered scalar columns: a join on either can drive an indexed lookup, and an equality on either against a query parameter becomes a binding-route key instead of a residual predicate. Results are unchanged. Structured author columns stay predicates.

- ff0d69b: Fix grouped aggregate results using opaque identities for default ordering and explicit-order ties. Reads and live subscriptions now use logical group keys before pagination, while preserving group occurrence identities and explicit sort directions.
- d09159b: Query and subscribe to previous row versions through `app.$history.table` in TypeScript or `history.tableName` in Rust.

  History queries support filtering, projection, ordering, pagination, and local-first and remote reads and subscriptions, fetching missing history from upstream when needed. Historical content is transformed through schema lenses to the requested schema.

  Each version exposes `$acceptedAt` as a JavaScript `Date` and `$transactionId` as a `TxId`, alongside the usual opt-in attribution columns (`$createdAt`, `$createdBy`, `$updatedAt`, `$updatedBy`). Server authorization follows access to the current row.

  **Upgrade note:** history is available for every table, with no opt-in. Anyone who can currently read a row in a branch can read all retained versions of that row in that branch, including versions written while they had no read access. Review your read policies before upgrading if earlier versions of a row may hold content its current readers should not see.

  History tables are read-only and require a bounded `id` constraint. They cannot be filtered by `$transactionId` or used on either side of `.include()`, `.hopTo()`, or `.gather()`. Only accepted content versions appear: pending writes and deletion-only events do not add history rows. Deleted rows' content history can be queried with `.includeDeleted()`. History is currently indexed only by row ID.

  **Breaking wire change:** `SupportingRow` now includes an `is_current` flag, allowing queries to combine a current table and its history without treating historical versions as current rows.

  **Breaking wire and binding change:** `ResultKey` moves to version 2. A v2 key starts with byte `2`. After its union-arm records it carries one transaction flag byte per source occurrence: first the root, then each joined source in order. A flag of `1` is followed by that source version's `tx_time` (u64, big-endian) and `tx_node` (16 bytes); a flag of `0` marks a current source. Version 1 keys are rejected. `OutputOccurrenceId` now serializes as its `ResultKey` bytes instead of the old postcard `(root, joined)` pair, and the typed-row result member variant is removed. These keys are not persisted.

- 70f3002: Support `.includeDeleted()` in subscriptions for regular and history tables, with both local-first and remote reads. Subscriptions retain deleted rows' content, receive restoration updates, and fetch deleted content from upstream when needed. Server read permissions still apply.

  **Breaking wire change:** subscription registrations now carry `ReadViewSpec.include_deleted`. Include-deleted subscriptions have separate coverage keys; existing coverage keys and stored row/version formats are unchanged.

  Include-deleted subscriptions of regular tables accept the current source and snapshots (which transaction reads use), and reject branch views with a "not supported yet" error. Plain one-shot include-deleted reads still accept only the current source. History queries are unaffected.

  **Breaking Rust API change:** `ReadOpts.include_deleted` is removed. Rust callers now set the flag on the read view: `ReadOpts { read_view: ReadViewSpec { include_deleted: true, ..Default::default() }, ..Default::default() }`.

- 74f37fc: Install on Expo SDK 55+ (React Native 0.82+), including today's `create-expo-app` default (SDK 58): `pod install` no longer rejects the app as legacy-architecture, and the config plugin no longer adds a `newArchEnabled` field that fails `expo-doctor`. The iOS relay is now built for iOS 15.1, so apps targeting older iOS than the build machine's SDK no longer link objects marked for a newer iOS. Release-candidate tarballs can find their native payload again.
- 3ca94c7: A column a migration adds with a default now gets that default on a local-first device that heard about the new version from the server while an older tab was still open. Before, the device's existing rows read `null` for that column even after reloading onto the new version, while a fresh device read the default. The sync server now sends each added column's default with the catalogue. A device that already added such a column as `null` keeps it that way until its local store is cleared.
- 7e56c40: React Native: when the server refuses a session's bearer (for example one that expired while the app was in the background), the native socket no longer retries it forever. It stops and reports the refusal, which reaches the app as an auth failure. The account then renews its token and reconnects, instead of the app crashing on an uncaught tick error. Other connection refusals (TLS, a proxy refusing the upgrade) are still retried.

  An account now renews on any refused credential, including one the server calls invalid, as after a signing key rotation. It does not renew when the app has that auth mode disabled, since a new token cannot fix that. A renewal that hands back an already-expired token renews again, under the usual backoff.

- f2a817e: React Native: a refused credential no longer fails the native foreground's ticks. The native socket stops and reports the refusal as connection state, the same way the web socket path does, so local reads and writes keep working while the app renews its token, and the renewed token reconnects.
- 6f292f9: Explain local foreground processing failures in the native tick exception using the existing lifecycle status, without changing the native artifact ABI.
- d54110a: React Native: a native upstream connection that fails for a reason other than the credential (TLS behind a captive portal, a proxy refusing the upgrade) no longer crashes release builds. The failure is reported as connection state, as on the web: the remote link shows as unavailable, local reads and writes keep working, and the link recovers when the connection does.
- e432352: Fix nullable group keys having different public representations in aggregate reads and subscriptions. One-shot reads now preserve NULL group keys and use the same logical result types as snapshots and live updates.

  Fix live aggregates losing nullable groups when their source rows change. Updates now find the indexed group using the same nullable key encoding, preserving counts, sums and group identity.

- c510fd6: Expose `db.captureSnapshot()` and typed, versioned snapshot references for frozen branch bases. Add persistence helpers, explicit unavailable-history checks, native foreground capture support, and a maintained example covering restoration and draft edits.

  Existing compatible React Native builds continue to support existing operations. Only snapshot capture requires an updated native build; older builds report `UnsupportedSnapshotError` when capture is called.

  Open frozen subscriptions retain their captured base values through later writes and transaction fate changes. Reopening still requires the captured history to remain available.

  The TypeScript `snapshot` option now requires a `SnapshotRef` string. Replace hand-built snapshot field objects with `db.captureSnapshot()` or restore a saved string with `parseSnapshotRef()`; field objects passed to this option now throw `SnapshotRefFormatError`.

- 68dd540: Jazz stores and queries data in SQLite on every platform: a SQLite file on Node, React Native and the server, and a SQLite file on IndexedDB in the browser. Local stores written by alpha.59 and earlier (native, native relay, Core and browser) are refused with `UnsupportedStorageCodecs` (`unsupported_storage_codecs`) and left unchanged. See the upgrading-from-alpha-59 changeset for what to do with them.
- c584159: Subscriptions can deliver a `string` or `bytes` column's changes as splices: name the columns in the `splices` query option and read them from `onUpdate(rows, { splices })`. On a range select the splices are relative to the selected window. Columns of other types, transformed columns, and range-selected columns that a computed field reads are refused as not supported yet. The React Native native relay ABI is now 4, so the JavaScript bundle and native build must match.
- 144326e: ### Upgrading from alpha.59

  alpha.60 is a breaking release for every app. Upgrade in this order:
  1. **Sync server first.** An alpha.60 sync server (core) speaks wire protocol v6 and admits only its app's backend, so alpha.59 clients can't reach it, and `createJazz` refuses to start against an older core. Plan one cutover per app, and ship the new backend and clients with it, because both change. Clients can't sync from the moment the alpha.59 server stops until the new backend and client bundle are live, so plan for that downtime, and let clients finish syncing first: writes still queued in alpha.59 browsers are not carried over. On Jazz Cloud you start the upgrade yourself, when your new backend and clients are ready to ship; we don't move your app for you. On your own server, stop it, move its data dir with the alpha59-alpha60 importer, and drop the JWT flags and their `JAZZ_*` variables (see core-admits-only-its-backend). Download the `jazz-alpha59-alpha60-export` and `jazz-alpha59-alpha60-import` binaries for your platform from this release's GitHub assets, export the old data dir with the exporter, then run `jazz-alpha59-alpha60-import import` and `jazz-alpha59-alpha60-import verify` before your backend first starts on the new dir (see alpha59-migration and the Backups & Upgrades docs page). The import checks the result against what alpha.59 reads, and links each user's old identity to `(jazz:backend, sub)`, so returning users keep their accounts when your `authenticate` answers with their old `sub`. Later releases keep the importer, so you can upgrade from alpha.59 whenever you're ready.
  2. **Your backend.** Add `createJazz` with `remote({ url, appId, secret })` (or `sqlite({ path })` on the imported data dir) and an `authenticate` that reads your app's own session (an app that signed users in with a JWT can use `fromJWT()`, which answers with the token's `sub`), and mount it at `/jazz` (see client-signs-in-through-backend). If your app has local-first guests, return `acceptLocalFirstAuth(request)` from `authenticate` for them, or the backend refuses them (see local-first-guests-opt-in). Other machines that used `createJazzSession` call `createJazzClient` now. Your backend now publishes the schema, permissions and migrations when it starts. `jazz-tools deploy` and the dev plugins are gone, and so are the starters: `npm create jazz` now prints a brief for your coding agent instead of scaffolding an app.
  3. **Your schema.** Import `schema as s` and `migration as m` instead of `col`, add `import { migrations } from "./migrations"` and pass them to `s.defineApp(schema, { migrations })`, then run `jazz-tools validate`, which writes `migrations/index.ts`. `defineApp` throws without migrations, so add the import before running the CLI.
  4. **Your clients.** Drop `appId` from browser clients (pass `scope` with the old value to keep existing local-first identities). Point `serverUrl` at your backend (`"/jazz"` by default) and remove JWT auth. Browser and React Native apps, the Node and WASM bindings and the React Native native build all move together: the native relay ABI is now 4, so a JavaScript bundle and a native build from different releases refuse each other.

  Local data on devices doesn't carry over. Opening a local store written by alpha.59 throws `UnsupportedStorageCodecsError` (match it with `isUnsupportedStorageCodecsError`) and leaves the store unchanged. Delete it, and the client resyncs from the server. Writes that were still waiting to upload when a device upgraded are lost, so let clients sync before you cut over.

- Updated dependencies [0aec19c]
- Updated dependencies [ff0d69b]
- Updated dependencies [6f292f9]
- Updated dependencies [e432352]
  - jazz-rn-ios@2.0.0-alpha.60
  - jazz-rn-android@2.0.0-alpha.60

## 2.0.0-alpha.59

### Patch Changes

- d06d76d: Name the table, row and versions that disagreed when a subscription's supporting-set transition is rejected, so an out-of-sync live query can be traced to the row that caused it.
- c93763e: Keep React Native foreground ticks running when an established native upstream connection drops and the socket worker reconnects, instead of throwing a recurring `Jazz native foreground runtime failed during tick` on a healthy `Db`. The outage is reported as disconnected by `nativeConnectionStatus()` until the next successful connection. A server that rejects the session, or a socket worker that gives up, still fails the tick, and `reconnectNativeUpstream()` now replaces a worker that gave up instead of leaving its failure latched.
- 36d7f66: Keep local writes visible while a query waits for large-value bytes from the server. A write no longer waits for another query's first result that is stuck on an attachment fetch, for example while offline. That query restarts after the write and keeps its pending fetch.
- 6eebb2c: Surface a fixed, sanitized diagnostic when a remote HTTP account-session endpoint is rejected; local development endpoints remain supported.
  - jazz-rn-ios@2.0.0-alpha.59
  - jazz-rn-android@2.0.0-alpha.59

## 2.0.0-alpha.58

### Patch Changes

- 6318998: Android builds no longer fail at `mergeConsumerProguardFiles`: jazz-rn now ships the `proguard-rules.pro` its build.gradle names.
- 94e3090: Keep native client relay ticks recoverable after temporary socket closure and HTTP connection failures (408, 425, 429, 500, 502, 503, 504), as well as hostname-resolution failures or TLS EOF without `close_notify`. Authentication, malformed protocol, certificate, and unclassified I/O failures remain terminal.
- Updated dependencies [94e3090]
  - jazz-rn-ios@2.0.0-alpha.58
  - jazz-rn-android@2.0.0-alpha.58

## 2.0.0-alpha.57

### Patch Changes

- 1f58c62: Use declared composite indexes for Global-tier reads. With an `(owner, rank)` index, a page filtered by `owner` and ordered by `rank` with a `limit` reads only about `limit + 1` index entries and checks deletions for those candidates only, falling back to the ordinary read when it cannot prove the page complete. A first result that filters both columns of a two-column composite reads that index prefix. Results are unchanged, Local-tier reads keep their current path, and schemas without a composite index take none of the new paths.
- 163799e: Declare ordered multi-column indexes with `s.table({...}).compositeIndex(["owner", "rank"])` in TypeScript or `TableSchemaBuilder::composite_index` in Rust. An index needs at least two distinct declared columns, and `bytea` columns cannot be indexed. The index is stored and backfilled when the schema is published through the usual migration. Declaring a composite index changes the schema id and needs a server on this version: `deploy` and `pushSchema` stop with `SchemaHashMismatchError` against an older server, which will already have stored the schema without the index; no migrations or permissions are published. A store that uses composite indexes cannot go back to an earlier version. Schemas without a composite index keep byte-identical ids and hashes.
- 163799e: Answer first one-shot reads with fewer row loads: point joins by an exact id use the junction's foreign-key index, root and join equality filters are pushed into indexes, and two equality filters intersect their index keys before rows are loaded. Live subscriptions are unchanged.
- d3d8ba5: Remove server edges. Clients connect to Core, which authorizes reads and writes and confirms global durability. Browser workers and native local persistence relays remain supported, as do local queries and optimistic writes.

  For applications using the old durability options:
  - Replace write waits using `{ tier: "edge" }` with `{ tier: "global" }` when the write must reach the server. Keep `{ tier: "local" }` for local persistence.
  - Use `ReadTier.Remote` for reads that need Core confirmation, or `ReadTier.LocalFirst` for immediate local reads.
  - Remove server upstream/edge configuration. A server now runs Core; a connection gateway may still route traffic without running a Jazz database.

  Existing globally confirmed data keeps its storage encoding. Legacy edge durability is interpreted as local persistence. Locally authored edits accepted only by an old edge remain eligible for normal resubmission to Core, with their original authorship and transaction identity; that old acceptance does not bypass current write permissions. Do not clear local databases to migrate.

  For a historical semantic-edge store, an edit can remain pending if Core lacks a parent created by another author or by the old edge. That parent must arrive through an authorized recovery path; reconnecting as the child’s author does not grant authority to upload someone else’s work. Keep the local store intact. Recovery for such shared-edge histories is tracked in [#3242](https://github.com/garden-co/jazz/issues/3242).

- 163799e: Make writes cheaper when many subscriptions share one query shape with different parameters. A write now only evaluates the subscriptions whose parameters it touches, writes to tables no subscription reads no longer scale with the number of live subscriptions, and one-shot queries and unsubscribes no longer walk the whole subscription graph. Attaching another subscription to a query shape that is already live is incremental, so it costs about the same as the first instead of growing with the number of subscriptions already open.
- b554916: The SQLite storage that React Native uses through its native relay compiles each statement once per connection instead of on every call, and scans read 256-row pages lazily instead of loading the whole range into memory. Write batches bind their keys and values without copying them first. The on-disk format and results are unchanged.
  - jazz-rn-ios@2.0.0-alpha.57
  - jazz-rn-android@2.0.0-alpha.57

## 2.0.0-alpha.56

### Patch Changes

- 9462d65: Resolve the Android Kotlin Gradle plugin and standard library through the consuming React Native project's Kotlin version, with the package's configured version as the fallback.
  - jazz-rn-ios@2.0.0-alpha.56
  - jazz-rn-android@2.0.0-alpha.56

## 2.0.0-alpha.55

### Patch Changes

- 5245e76: Move subscription result buffering into the Rust core and remove redundant TypeScript query preparation and row caches. Preserve scoped relation reads and wait for complete recursive query results before publishing them. Direct binding consumers must adapt to removal of the Rust–TypeScript `prepareQuery` API.

  [PR #2717](https://github.com/garden-co/jazz/pull/2717).

- 060083d: Split React Native binaries into exact-version iOS and Android payload dependencies, retaining all supported architectures while keeping each npm upload below the package budget.
- Updated dependencies [060083d]
  - jazz-rn-ios@2.0.0-alpha.55
  - jazz-rn-android@2.0.0-alpha.55

## 2.0.0-alpha.54

### Patch Changes

- bc70549: Jazz now runs on Groove, a new low-level database engine built around incremental view maintenance. It shares work between similar query subscriptions and provides a foundation for improving correctness and performance. This release remains an alpha: please report anything that breaks or feels slow.

  ### Breaking storage change

  Alpha.54 changes the storage format without automatic migration from alpha.53. If you have existing production data, contact us for migration help before upgrading. For a fresh start, create a new Jazz Cloud app, or use the alpha.54 CLI with fresh server storage when self-hosting. Future storage-format changes will include automatic migration.

  ### Four major changes

  **Simpler read tiers.** Choose `local-first` for immediate local results with background sync; `remote-if-possible` for remotely confirmed state when online, local fallback when offline, and immediate local writes; or `remote` for remotely confirmed state only, including confirmation of local writes before they appear.

  **Clearer account lifecycle.** Configure your account and client once with `createJazzSession`. The session manages auth transitions, graceful shutdown and client replacement. Framework adapters expose that state without app-owned lifecycle plumbing. External authentication can use `loginOrRegisterJWT`; explicit linking remains available for attaching a fresh external identity to an existing account. Backends use the same session API with `initial: { backendSecret }` or `becomeBackend({ backendSecret })`.

  **Build-your-own branching.** Define branched table views with `branchBy` columns and explicit head/base view options. Branch identifiers can be strings or references to your own branch table, letting your app define branch metadata, workflows and row-level permissions.

  **Large values are ordinary columns.** Store files and streams in `s.bytes()`, large JSON documents in `s.json()`, and text in `s.string()`. Read complete values or stream supported ranges and JSON subpaths; update them with byte patches, appends, JSON edits or string splices. Content-based chunking in a Prolly Tree makes these operations efficient while retaining ordinary column permissions and query semantics. JSON and rich-text merge strategies are planned for a later release.

  ### API migration checklist
  - Replace `Db.subscribeAll` with `Db.subscribe` for complete current results.
  - React and React Native `useAll` now return `{ data, isLoading, error }`; `useAllSuspense` still returns rows.
  - Replace `localUpdates` and `propagation` options with read-tier selection.
  - Move account/client lifecycle handling to `createJazzSession` and the framework adapters. Linking happens outside contexts; the session gracefully shuts down the old client and syncs pending writes before replacing it.
  - `$createdBy` and `$updatedBy` are non-null `{ account, identity: { issuer, subject } }` values. Use `.account` for ownership comparisons. SYSTEM authorship uses a reserved account and issuer, with the originating node as subject.
  - Direct `jazz-wasm` consumers must await `acceptSubscriber` and `acceptSubscriberWithSelfSignedProof`.

  ### Reliability improvements

  This release also improves browser and React Native persistence, offline recovery and reconnect behavior; query correctness across relations, aggregates, branches and permissions; and authentication and framework lifecycles. It includes fixes for large JSON relation values, native runtime packaging and generated starter apps.

## 2.0.0-alpha.53

### Patch Changes

- bc74a87: Stop the React Native scheduler from spawning a thread per tick. Every accepted tick or mutation-error delivery used to spawn a fresh thread that blocked until the JS thread serviced its callback, and because the debounce flag was released before the callback returned, a heavy burst of bare inserts minted roughly one blocked thread per millisecond. Jobs are now queued to a single lazily-spawned worker thread, so the scheduler's thread count stays constant and callbacks are serialized. Shutdown from `close()` drops the worker's channel instead of joining it (a worker blocked on a JS callback would deadlock the JS thread running `close()`), the tick callback is no longer invoked while holding the mutex that `close()` needs, a failed worker spawn no longer poisons the scheduler, and a panicking mutation-error callback can no longer kill the worker.

## 2.0.0-alpha.52

## 2.0.0-alpha.51

### Patch Changes

- 1670073: Fix a `batched_tick` deadlock that left React Native apps spinning at 100% CPU when a server-side query subscription was deferred waiting for its catalogue/schema.
  - `jazz-tools`: `RuntimeCore::batched_tick` now drains parked sync messages before deciding whether to reschedule, so a `CatalogueEntryUpdated` that arrives while a subscription is parked actually unblocks it instead of sitting in the parked queue forever. Progressless ticks no longer re-arm the scheduler, breaking the reschedule hot loop.
  - `jazz-rn`: `request_batched_tick` is now deferred off the JS thread (mirroring `schedule_mutation_error_delivery` and `NapiScheduler`) so the JS callback can't synchronously re-enter `batched_tick` and starve `setInterval` / rendering.

- 6f7a83f: Fix an owner `db.update` of a backend-created row hard-deleting the row instead of updating it on persistent-storage clients. A client write can no longer downgrade a batch the server has already accepted, so the row survives and the update applies.
- 5c76bfc: Add soft-deleted row restoration with `db.restore(...)`.

## 2.0.0-alpha.50

### Patch Changes

- e49cf4c: Moves write error propagation fully into the Rust runtime

## 2.0.0-alpha.49

## 2.0.0-alpha.48

## 2.0.0-alpha.47

### Patch Changes

- fa3b607: Fix loss of reactivity for query subscriptions with deeply-nested includes. Subscriptions built from depth-2+ `via` include chains (`org.include({ todoViaOrg: { user_checkViaTodo: true } })`) now correctly receive deltas when a row at the bottom of the chain is inserted, updated, or deleted. Previously only the immediate child table was tracked as a dependency of the outer subscription, so mutations further down the chain were silently missed.
- 5752bde: Add React Native anonymous auth support. `jazz-tools` now mints anonymous JWTs through the React Native runtime module when no auth credentials are provided, and `jazz-rn` exposes the matching native `mintAnonymousToken` binding.
- 15b347d: `jazz-rn`: `query` is now `async` and no longer blocks the React Native JS thread on one-shot reads.

  The native uniffi export used `block_on` on the JS thread, so any `db.all(...)` that needed a later `batched_tick` to settle (e.g. queries that wait on server-sourced data or parked sync messages) could deadlock — the JS thread was blocked, so the `batched_tick` callback could never fire to fulfil the query future. The export is now `async fn` and uniffi-bindgen-react-native generates a Promise-returning JSI call, polled off the JS thread. `JazzRnRuntimeAdapter.query` now `await`s the binding before parsing.

## 2.0.0-alpha.46

## 2.0.0-alpha.45

### Patch Changes

- 2ee98be: Add sync protocol version checks to the WebSocket handshake so incompatible clients and servers fail with an explicit update prompt.

## 2.0.0-alpha.44

## 2.0.0-alpha.43

## 2.0.0-alpha.42

## 2.0.0-alpha.41

## 2.0.0-alpha.40

## 2.0.0-alpha.39

## 2.0.0-alpha.38

## 2.0.0-alpha.37

## 2.0.0-alpha.36

### Patch Changes

- Cache runtime schema lookups across query paths and surface unhandled rejected mutations through targeted batch-id queues instead of rescanning every retained local batch record. This also brings the React Native runtime onto the same rejected-batch helper surface as WASM and N-API.
- 8bb9fbc: Allow caller-supplied row ids to use any valid UUID and rely on explicit row metadata for created-at semantics.

## 2.0.0-alpha.35

### Patch Changes

- 4d67804: Promote `authMode` to a first-class typed field, add anonymous auth, and overhaul the React provider.
  - **`Session.authMode`** is now `"external" | "local-first" | "anonymous"`, derived from the JWT `iss` claim instead of an opaque string in `claims`. The permissions DSL exposes `session.authMode` and supports `session.where({ authMode })`.
  - **Anonymous auth**: when `DbConfig` has neither `secret` nor `jwtToken`, the client mints an ephemeral token. Anonymous sessions can read but are structurally denied writes (checked before policy evaluation); failures surface as `AnonymousWriteDeniedError` on the client.
  - **`DbConfig` flattened**: `auth: { localFirstSecret }` → `secret`.
  - **`AuthState` flattened**: `{ authMode, session, error? }` — no more `status` / `transport` union.
  - **React provider**: `JazzProvider` uses Suspense + `React.use()`; new `onJWTExpired` prop serializes refresh calls and replaces custom sync-wrapper components. New `useAuthState()` and `useLocalFirstAuth()` hooks. Context carries only `{ client }`.
  - **Identity module**: `mint_local_first_token` / `verify_local_first_identity_proof` → `mint_jazz_self_signed_token` / `verify_jazz_self_signed_proof`, taking an explicit issuer.

- 75756a6: Standardize BatchId JSON values on hex strings across Jazz write-context bindings.

  Batch write contexts now accept the TypeScript wire shape used by current clients, including lowercase `batch_mode` values and string `batch_id` values, and reject the old array-style BatchId JSON representation.

- caad318: Modify write APIs to return a `WriteHandle`, which allows callers to wait for a given durability tier to acknowledge the write or reject it. Also introduces a global `onMutationError` handler to receive errors that aren't explicitly handled with `WriteHandle.wait`.

## 2.0.0-alpha.34

## 2.0.0-alpha.33

## 2.0.0-alpha.32

## 2.0.0-alpha.31

### Patch Changes

- 09e16b4: Support recursive gather seeds built from composed same-table relations, including hop-based and unioned permission closures.

## 2.0.0-alpha.30

### Patch Changes

- 848e94d: Replace HTTP `/sync` + SSE `/events` with a single Rust-owned WebSocket `/ws` transport, and wire JWT rotation and server-side auth rejection end-to-end across all bindings.

  **Transport rewrite**
  - New `TransportManager` in `jazz-tools` owns the WebSocket connection, framing (4-byte big-endian length + JSON), reconnect with exponential backoff, periodic heartbeat, and a `TransportControl` channel that observes `Shutdown` / `UpdateAuth` in every phase (connect, backoff, handshake, connected). Dropped `TransportHandle` triggers an implicit shutdown.
  - New `install_transport` helper on `RuntimeCore` centralises the boilerplate (create manager → seed catalogue state hash → register handle → spawn) so all four bindings converge on one code path.
  - `NativeWsStream` (tokio-tungstenite + rustls) and `WasmWsStream` (ws_stream_wasm) implement the shared `StreamAdapter` trait.

  **Auth refresh**
  - `JazzClient.updateAuthToken(jwt)` now pushes the refreshed credentials into the live transport via `runtime.updateAuth`, which routes through `TransportControl::UpdateAuth` and triggers a reconnect with the new auth. Previously the call only mutated local context.
  - `ConnectSyncRuntimeOptions.onAuthFailure` is now wired to `runtime.onAuthFailure` and fires whenever the server rejects the WS handshake with `Unauthorized`. NAPI exposes a `ThreadsafeFunction`-based callback; React Native exposes a UniFFI `callback_interface`; WASM keeps its existing `Function` callback.
  - The worker posts `auth-failed` back to the main thread when `runtime.updateAuth` throws, and supports `update-auth` / `disconnect-upstream` / `reconnect-upstream` messages from the bridge.

  **Bindings**
  - WASM, NAPI, and React Native all now expose `connect`, `disconnect`, `update_auth`, `on_auth_failure`.
  - React Native: `JazzRnRuntimeAdapter` forwards `updateAuth` and `onAuthFailure` to the UniFFI binding (previously missing — auth refresh was a silent no-op on RN).
  - `JazzClient.updateAuthToken` carries `admin_secret` and `backend_secret` forward from context (previously the serialised payload only included `jwt_token`, silently erasing privileged credentials on every refresh).

  **Breaking changes**
  - `POST /sync` and `GET /events` HTTP routes are deleted; external callers receive 404. Use the WebSocket `/ws` route via `runtime.connect(url, authJson)`.
  - `RuntimeCore<S, Sch, Sy>` is now `RuntimeCore<S, Sch>` — the `SyncSender` generic parameter has been removed.
  - `NapiSyncSender` and `RnSyncSender` are removed; bindings use `runtime.connect` instead.
  - `TokioRuntime::new` no longer takes the trailing `SyncSender` argument.
  - Cargo features `transport` and `transport-http` are removed; transport types are default-on, and `transport-websocket` enables the WS implementation.

  **Tests**
  - Inline `TransportManager` tests cover shutdown and `update_auth` in every phase (connect / backoff / handshake / connected).
  - Re-enabled two previously-`#[ignore]`d sync-reliability tests after fixing a debounce-flag race in `TokioScheduler::schedule_batched_tick`.
  - Integration tests migrated from `/events` to `/ws`.
  - New TS coverage: `client.test.ts` (onAuthFailure wiring + secret preservation), `napi.auth-failure.test.ts` (E2E against real NAPI + server), `db.transport.test.ts`, `url.test.ts`, `db.auth-refresh.worker.test.ts` (browser worker round-trip), RN adapter tests for `updateAuth` / `onAuthFailure`.

- e346057: Fix React Native cold-start on offline and unblock initial subscriptions when the transport can't reach the server.
  - `jazz-rn` now regenerates its UniFFI bindings for the `insert` / `insert_with_session` signatures introduced with the caller-supplied UUIDv7 APIs, so the native library and JS adapter agree at startup and Jazz initializes in the app.
  - `jazz-rn` now calls `rehydrate_schema_manager_from_catalogue` after opening SQLite, matching the WASM runtime, so offline cold-starts can decode previously-persisted rows against their original schema/permissions history.
  - `jazz-tools` bounds the "hold remote query frontier while transport connects" wait so a never-completing transport no longer stalls first subscription delivery forever. Pending servers now clear on a new `TransportInbound::ConnectFailed` event (fired from the connect/handshake error paths in both tokio and wasm run loops), with a 2s safety-net timeout for hung connects. The frontier hold also re-evaluates live at settle time so offline or hung first-connect cases release immediately once pending clears.

  No public-API break. RowPolicyMode selection, persisted-row wire format, and transport handshake semantics are unchanged.

## 2.0.0-alpha.29

### Patch Changes

- 58ace62: Add external UUIDv7 create APIs and id-based upsert APIs across the Rust and TypeScript client surfaces.

## 2.0.0-alpha.28

### Patch Changes

- 9b45ec5: Adopt the new row-permission strategy across client and server runtimes. Local clients that only have a structural schema stay permissive for offline reads and writes, while runtimes with current permissions and sync servers enforce deny-by-default row access for session-scoped reads, inserts, updates, and deletes.

## 2.0.0-alpha.27

### Patch Changes

- 463098a: Ship the new unified row-history storage engine across Jazz runtimes.

  Relational rows, query visibility, and sync replay now go through the same storage-backed path instead of mixing durable state with older in-memory cache layers. In practice this makes local persistence and sync behavior more consistent across browser, Node, and native runtimes, especially around cold start, reconnect, and large local datasets.

## 2.0.0-alpha.26

### Patch Changes

- a1cb9d5: Switch the mobile persistent storage engine to SQLite.

  **WARNING:** Existing local data stored with the previous Fjall-based engine is not compatible with SQLite. On-device data will be lost on upgrade — users will need to re-sync from the server.

## 2.0.0-alpha.25

## 2.0.0-alpha.24

## 2.0.0-alpha.23

## 2.0.0-alpha.22

### Patch Changes

- dedab8f: Add authorship-based edit metadata for row writes across the runtime and bindings.

  Rows now expose `$createdBy`, `$createdAt`, `$updatedBy`, and `$updatedAt` magic columns in queries and permissions, and backend contexts can override stamped authorship with `withAttribution(...)`, `withAttributionForSession(...)`, and `withAttributionForRequest(...)`.

- fd7ecd0: Schema authoring no longer has a build/codegen step. Apps now define their schema directly in TypeScript with the namespaced API (`import { schema as s } from "jazz-tools"`), and `jazz-tools validate` is just an optional local preflight check.

  Current `permissions.ts` is now separate from the structural schema and migration lifecycle, instead of being versioned as part of schema identity.

  Runtime permission enforcement now follows the latest published permissions head independently of client schema hashes, with learned schemas, migration lenses, and permissions rehydrated from the local catalogue on restart.

## 2.0.0-alpha.21

### Patch Changes

- 477c43c: Remove Nitro Modules code (`crates/jazz-nitro`, `examples/rn-jazz-nitro`) in favor of uniffi for the React Native bridge.

## 2.0.0-alpha.20

### Patch Changes

- 9f4d4d9: Bound oversized index keys by keeping as much real value prefix as fits in the durable key and appending a length plus hash overflow trailer.

  This keeps large indexed string and JSON equality lookups working without exceeding storage key limits, while preserving prefix-based ordering instead of collapsing oversized values to a pure hash ordering. Large `array(ref(...))` values also continue to support exact array equality and per-member reference indexing.

## 2.0.0-alpha.19

## 2.0.0-alpha.18

### Patch Changes

- 33bc53f: Fail indexed writes cleanly when an indexed value would exceed the storage key limit instead of panicking in native storage.

  Oversized indexed inserts and updates now return a normal mutation error to JS callers, and local updates can recover rows that were previously left in a partial index state by older panic-driven failures.

## 2.0.0-alpha.17

### Patch Changes

- bb10f1c: Add a shared `jazz-tools/expo/polyfills` entrypoint for Expo apps and ensure published `jazz-rn` packages include the generated C++ bindings required for native builds.

## 2.0.0-alpha.16

## 2.0.0-alpha.15

### Patch Changes

- 4871b02: Switch the native persistent storage engine from SurrealKV to Fjall for the CLI, NAPI bindings, and React Native bindings.

  Native local data now lives in Fjall-backed stores and uses `.fjall` database paths by default.

- bb39e15: Modify inserts to return the inserted row instead of just the id

## 2.0.0-alpha.14

### Patch Changes

- 2f5ccba: Add an in-memory storage driver across the Jazz JS, WASM, NAPI, and React Native runtimes.

  Backend contexts can now opt into memory-backed runtimes without local persistence, and runtime driver-mode coverage was expanded to exercise the new in-memory path.

## 2.0.0-alpha.13

## 2.0.0-alpha.12

## 2.0.0-alpha.11

## 2.0.0-alpha.10

## 2.0.0-alpha.9

## 2.0.0-alpha.8

## 2.0.0-alpha.7

## 0.20.9

## 0.20.8

## 0.20.7

## 0.20.6

## 0.20.5

## 0.20.4

## 0.20.3

## 0.20.2

## 0.20.1

## 0.20.0

### Minor Changes

- 8934d8a: ## Full native crypto (0.20.0)

  With this release we complete the migration to a pure Rust toolchain and remove the JavaScript crypto compatibility layer. The native Rust core now runs everywhere: React Native, Edge runtimes, all server-side environments, and the web.

  ## 💥 Breaking changes

  ### Crypto providers / fallback behavior
  - **Removed `PureJSCrypto`** from `cojson` (including the `cojson/crypto/PureJSCrypto` export).
  - **Removed `RNQuickCrypto`** from `jazz-tools`.
  - **No more fallback to JavaScript crypto**: if crypto fails to initialize, Jazz now throws an error instead of falling back silently.
  - **React Native + Expo**: **`RNCrypto` (via `cojson-core-rn`) is now the default**.

  Full migration guide: `https://jazz.tools/docs/upgrade/0-20-0`

### Patch Changes

- 89332d5: Moved stable JSON serialization from JavaScript to Rust in SessionLog operations

  ### Changes
  - **`tryAdd`**: Stable serialization now happens in Rust. The Rust layer parses each transaction and re-serializes it to ensure a stable JSON representation for signature verification. JavaScript side now uses `JSON.stringify` instead of `stableStringify`.

  - **`addNewPrivateTransaction`** and **`addNewTrustingTransaction`**: Removed `stableStringify` usage since the data is either encrypted (private) or already in string format (trusting), making stable serialization unnecessary on the JS side.

## 0.19.22

## 0.19.19

## 0.19.18

## 0.19.17

## 0.19.16

## 0.19.15

## 0.19.14

### Patch Changes

- 41d4c52: Enabled flexible page-size support for Android builds, enabling support for 16KB page sizes to ensure compatibility with upcoming Android hardware and Google Play requirements for cojson-core-rn.

## 0.19.13

## 0.19.12

## 0.19.11

## 0.19.10

### Patch Changes

- 4f5a5e7: Version bump to align the fixed version

## 0.1.1

### Patch Changes

- d901caa: Added cojson-core-rn that improves ReactNative crypto performance
