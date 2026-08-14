# React Native binding rewrite — design

Status: implemented (M1–M5 landed 2026-08-10). Owner: RN surface owner.
Amended by `dev/RN_BINDING_CORE_REALIGN_DESIGN.md` (2026-08-12): the core
engine-swap branch moved subscription carriers, transactions, and permissions
underneath this binding; that document records the realignment.
Scope: `crates/groove` SQLite storage backend, `crates/jazz-rn` rewrite,
`packages/jazz-tools/src/react-native` wiring, revived Expo example E2E.

Implementation receipts: host Rust/TypeScript tests, iOS device + simulator
XCFramework, all four Android ABIs, CocoaPods/Xcode simulator build, npm dry-run
artifact inspection, and a clean iOS simulator run proving offline create,
process reopen, pending upload, and a remote subscription update.

## 1. Context

Before this rewrite, `crates/jazz-rn/rust` targeted the pre-swap runtime
(`jazz::tools::{runtime_core, schema_manager, sync_manager, binding_support,
storage, transport_manager, ws_stream}`), none of which existed after the core
engine swap. The crate was outside the workspace, its generated bindings were
stale, and the TS RN scaffold was compile-level only. The implementation now
routes Hermes through the native actor and Rust-side SQLite backend.

The v2 runtime already has two bindings driven by the same TypeScript adapter
(`packages/jazz-tools/src/runtime/native-runtime/native-runtime-adapter.ts`),
which speaks a structural `NativeDb` contract: postcard-encoded rows and
schemas, JSON option bags, `setTickScheduler`/`tick()`, byte-queue transports
pumped by a JS-owned WebSocket (`WebSocketCarrier`), non-blocking
`Write.wait(tier): Promise<void>`, and
`Subscription.readAll()` polling. Spec `crates/jazz/SPEC/13_db_api.md` §13.13
requires that bindings not fork query/transaction/sync semantics; the RN
binding therefore implements this same contract rather than resurrecting the
old JSON `RnRuntime` surface.

### Compatibility target

The target is the **full `NativeDb` contract and the SPEC 13.7.5 capability
matrix**, not "whatever `jazz-napi` exposes today". The matrix marks exclusive
transactions, dry-run permission probes, and transaction reads as required
(`Y`) for both WASM and NAPI ABIs, and the adapter's public API depends on
them (`readPlainRows` throws "Native runtime does not support transaction
reads" when `allInTransaction` is missing). `jazz-wasm` implements all of
them; `jazz-napi` currently implements none of them and is itself behind the
matrix. Where the two disagree, **`jazz-wasm` is the behavioral reference**
and the napi gap is recorded, not copied.

### Recorded decisions

1. **Storage: SQLite ordered-KV backend** (RN owner, 2026-08-07). A new groove
   `OrderedKvStorage` backend over bundled rusqlite. This supersedes the
   ordering in SPEC 17.6 (RocksDB first, SQLite only if unworkable): we go
   directly to SQLite. The spec already classifies the SQLite backend as "pure
   tooling — a clean additional `OrderedKvStorage` backend behind the existing
   storage contract, with no design decisions attached", so no semantic
   decision is being reversed; we are skipping the RocksDB mobile
   cross-compile spike entirely. This also resolves the root `Cargo.toml`
   pause comment and the SPEC 13 open question on the RN storage route: the
   route is the native module with Rust-side SQLite, not `op-sqlite` /
   `expo-sqlite` TS drivers.
2. **Threading: actor core** (approach A, RN owner, 2026-08-07). `Db` is
   thread-affine (`Rc`/`RefCell` by design); uniffi objects must be
   `Send + Sync + 'static`. A dedicated core thread owns the `Db`; handle
   objects marshal jobs over channels. The napi-style direct-ownership
   pattern (`ThreadBound` + `unsafe impl Send`) was rejected: uniffi futures
   poll from foreign threads and Hermes finalizers/dev-reload can drop handles
   off-thread, so the single-thread bet is unsound.
3. **Scope: full E2E** (RN owner, 2026-08-07). Done means: host-green Rust and
   TS suites, regenerated ubrn bindings, `ubrn build ios`/`android`
   artifacts, and the revived `examples/todo-client-localfirst-expo`
   validating persistence + sync against a local server on the iOS simulator.
   Android on-device validation is best-effort, not gating.

## 2. Architecture

```
examples/todo-client-localfirst-expo        E2E app (revived)
        │  jazz-tools public API
packages/jazz-tools/src/react-native        ReactNativeRuntimeSource
        │                                   └─ NativeRuntimeAdapter (reused, unchanged)
        │                                      └─ native-db.ts shim (new, ~150 lines)
crates/jazz-rn (ubrn / uniffi 0.30)         Send+Sync handles ── mpsc jobs ──► core thread
        │                                                            owns Db<MemoryStorage>
        │                                                                 Db<SqliteStorage>
crates/groove storage/sqlite.rs             OrderedKvStorage + ReopenableStorage (new)
```

One rule governs the whole stack: **jazz-rn implements the full `NativeDb`
contract with the same postcard payloads, options JSON, and error-code
markers the adapter already consumes** — so every TS layer above the shim is
reused unmodified. `jazz-wasm` is the reference implementation for behavior;
`jazz-napi` is the reference for the actor-external method shapes it does
implement.

## 3. Groove SQLite backend

New `crates/groove/src/storage/sqlite.rs`, exported as `SqliteStorage` behind
a new groove feature `sqlite = ["dep:rusqlite"]` (rusqlite `bundled`; the
amalgamation cross-compiles cleanly for aarch64-apple-ios and Android NDK
targets). Jazz's existing vestigial `sqlite = ["dep:rusqlite"]` feature is
re-pointed to `["groove/sqlite"]` and its direct rusqlite dependency dropped.

### Schema and format identification

```sql
CREATE TABLE meta (
  key   TEXT PRIMARY KEY,
  value BLOB NOT NULL
);                                   -- 'format' → 'jazz-groove-kv'
                                     -- 'format_version' → 1
                                     -- 'boundary_seq' → u64 (see Durability)
CREATE TABLE column_families (
  id   INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);
CREATE TABLE kv (
  cf INTEGER NOT NULL,
  k  BLOB    NOT NULL,
  v  BLOB    NOT NULL,
  PRIMARY KEY (cf, k)
) WITHOUT ROWID;
```

`open(path, column_families)`:

- Fresh file ⇒ create the three tables and stamp `format`/`format_version`.
- Existing file ⇒ **validate before use**: `meta.format`/`format_version`
  must match and the table shapes must be exactly the expected ones
  (`pragma table_info`). Any mismatch ⇒ `Error::InvalidStorageLayout` with a
  message naming what diverged. `CREATE TABLE IF NOT EXISTS` alone is not an
  acceptance test — an unrelated pre-existing SQLite file must be rejected,
  not adopted.
- CF names intern to ids; `reopen(self, cfs)` re-validates and may add new
  CFs (matching `ReopenableStorage` semantics used on schema evolution).

### Contract implementation

The composite PK B-tree provides the ordered contract:

- `get`/`set`/`delete` — point ops on `(cf, k)`.
- `scan_range(cf, start, end)` — `WHERE cf=? AND k>=? AND k<?` ordered by
  `k`, streamed through the `ScanVisitor` callback (no materialization).
- `scan_prefix` family — lower bound `k >= prefix` plus a **prefix predicate
  with early termination**, not a computed upper bound alone. When the prefix
  has a rightmost non-`0xFF` byte, the incremented exclusive upper bound is
  used as an index-range optimization; a prefix consisting entirely of `0xFF`
  bytes has **no finite upper bound**, and the scan must run upper-unbounded
  and stop on the first non-`starts_with` key (forward) or skip the
  non-matching tail (reverse). The existing conformance fixtures already
  place `[0xff, …]` keys; the suite adds all-`0xFF` prefixes explicitly.
- `write_many(ops)` — one SQLite transaction over the batch. Must support all
  three `WriteOperation` variants, including **`Delta`**: deltas apply in
  operation order with read-your-own-writes semantics inside the batch (a
  `Delta` on a key written earlier in the same batch reads that staged value,
  matching the memory/RocksDB backends' merge behavior), decoding via the
  shared `StorageDelta` machinery, and erroring the whole transaction on an
  invalid delta (`InvalidStorageDelta`), leaving no partial state.
- `close()` — the trait closes through `&self` while
  `rusqlite::Connection::close` consumes the connection, so the backend holds
  `RefCell<Option<Connection>>`; `close()` takes the connection out,
  checkpoints (see Durability), and closes it. Post-close calls surface a
  closed-storage error, and reopen after close succeeds (mobile
  kill/restore is a first-class path).
- `column_family_names()` — from the intern table.
- `approximate_class_bytes` — `Ok(None)` (the contract's "unsupported"
  answer) unless a cheap page-count heuristic proves useful later.

### Durability

`Durability` (`FullSync` / `WalNoSync`) is today defined inside the
RocksDB-gated module and exported only under `feature = "rocksdb"`. **M1
lifts it into `storage/mod.rs` as a backend-neutral type** (RocksDB re-export
kept for compatibility) so the SQLite backend can accept it without dragging
RocksDB in.

Mapping (`journal_mode=WAL` always):

- `FullSync` ⇒ `synchronous=FULL`: every commit syncs the WAL. Durable
  across power loss per SQLite's documented semantics.
- `WalNoSync` ⇒ `synchronous=NORMAL`: WAL atomicity, commits are durable
  across process death but **not** across power loss until the next durable
  boundary. This is the tier's contract — RocksDB's `WalNoSync` doc defines
  itself as "like SQLite WAL/NORMAL" — so no stronger claim is made.
- `flush_write_boundary()` ⇒ forces a real durable boundary, defined as: a
  bump of `meta.boundary_seq` committed with WAL sync forced for that commit
  (per-connection `PRAGMA synchronous=FULL` around the marker commit, then
  restored). This is verifiable — the commit either returns an error or the
  boundary is on disk — and does not depend on checkpointing. It resets the
  `set_write_flush_cadence` pending counter, mirroring RocksDB's
  `flush_wal(true)` path.
- Checkpoints are WAL-size management, not the durability mechanism.
  SQLite's default PASSIVE auto-checkpoint (~1000 WAL pages) **stays
  enabled** — it is what bounds WAL growth in a long-lived mobile app, it
  never blocks readers, and disabling it would require inventing a
  background checkpoint policy for no gain. `wal_checkpoint(TRUNCATE)` runs
  at `close()` under a `busy_timeout`, and its result is checked — `busy`
  or a partial checkpoint at close is a distinct error surfaced to the
  caller, not silently ignored.
- Apple targets: `fullfsync=ON` and `checkpoint_fullfsync=ON` are set at
  open (no-ops elsewhere). Ordinary `fsync` on Apple hardware may leave
  data in drive caches; `F_FULLFSYNC` is what backs the power-loss claim
  there. Elsewhere the claim is qualified by the platform's fsync
  semantics.

### Error mapping

`groove::storage::Error` has no shared corruption variant today (RocksDB and
OPFS each carry a `#[from]` transparent variant). M1 adds, gated on the
feature:

```rust
#[cfg(feature = "sqlite")]
#[error(transparent)]
Sqlite(#[from] rusqlite::Error),
```

plus a backend-neutral `Error::StorageClosed` (use-after-close is a runtime
state, not a layout problem) and a feature-gated
`Error::SqliteCheckpointIncomplete { busy, log, checkpointed }` for blocked
or partial close-time checkpoints. `InvalidStorageLayout` is reserved for
open-time format/shape validation only. Corruption (`SQLITE_CORRUPT`,
`SQLITE_NOTADB`) surfaces through the `Sqlite` variant with the SQLite
result code preserved; the conformance suite asserts that a
truncated/garbage file produces it at open or first read rather than a
panic or silent empty database. Open-time validation enumerates the entire
`sqlite_master` (excluding SQLite-internal objects) and rejects any
unexpected table, index, view, or trigger — a trigger on `kv` would alter
semantics while a tables-only check passed.

### Threading note

`OrderedKvStorage` is used thread-affinely by `Db`; the backend keeps its
single connection in the `RefCell<Option<Connection>>` and is `!Sync` like
its siblings. No connection pool, no async.

**Single-owner assumption (recorded).** Exactly one owning `SqliteStorage`
opens a given file at a time — the same assumption the sibling backends
make. The boundary-counter bump nevertheless runs inside an `IMMEDIATE`
transaction so the read-increment-write cannot interleave even if the
assumption is ever violated by a second connection.

## 4. `crates/jazz-rn` (Rust)

UniFFI proc-macro surface (no UDL), uniffi 0.30, generated for RN by
`uniffi-bindgen-react-native` exactly as the existing scaffold configures
(`ubrn.config.yaml`, `cpp/`, `android/`, `ios/`, podspec unchanged in shape).

Crate features: `jazz = { default-features = false, features = ["sqlite",
"transport-compression-zstd"] }`. The compression feature is not optional
polish: the shared `WebSocketCarrier` advertises `FEATURE_PAYLOAD_ZSTD` in
the wire `Hello` unconditionally, so a native client without the zstd decode
path can be handed frames it cannot decode (§11 records the pre-existing
napi instance of this bug). `zstd-sys` is plain C and cross-compiles for
mobile targets without the RocksDB-class pain.

### 4.1 Core thread (actor) and its lifecycle

- `RnDb.open_memory(schema, config)` / `open_persistent(data_path, schema,
config)` spawn one `jazz-rn-core` thread per database. The thread decodes
  the same postcard `(schema, config)` payload napi's `decode_core_open_args`
  consumes, opens `MemoryStorage::new(&cfs)` or `SqliteStorage::open(path,
&cfs)`, builds the `Db` via the shared open path, then loops on
  `mpsc::Receiver<Job>`.
- `Job = Box<dyn FnOnce(&mut CoreState) + Send>`. `CoreState` owns the `Db`
  plus id-keyed registries: prepared queries, query attachments, open
  transactions, writes (`TxId`s), pending write waits, subscriptions
  (`SubscriptionStream`s), transports (`PeerConnection` + wire queues). Ids
  mint from an `AtomicU64`.
- Handle objects (`RnDb`, `RnTx`, `RnWrite`,
  `RnTransport`, `RnSubscription`, `RnPreparedQuery`, `RnQueryAttachment`)
  are uniffi Objects holding `{actor: Arc<ActorHandle>, id: u64}` —
  trivially `Send + Sync`. Sync methods marshal a job plus reply channel and
  block for the round-trip (µs-scale; the same order as the JSI hop itself).
  `Drop` on a handle enqueues a best-effort release job (open transactions
  abandon, napi `Tx::drop` parity).

**Lifecycle state machine.** `ActorHandle` carries an explicit shared state:

```
Open ──close()──► Closing ──drained──► Closed
  │
  └──job panic──► Poisoned(reason)
```

- **Job submission** checks the state under the same lock that guards the
  sender: in `Closing`/`Closed`/`Poisoned`, submission fails fast with a
  `Closed`/`Poisoned` error instead of enqueueing — a cloned handle can never
  enqueue after the close barrier and block on a dead actor.
- **Panic containment**: a per-FFI-call `catch_unwind` cannot catch a panic
  that happens later on the core thread, so the **actor loop itself wraps
  every job in `catch_unwind`**. Reply channels are completed on the panic
  path (the job's reply sender is owned by the wrapper, which sends
  `Err(Internal)` if the closure unwound), so no caller is left blocking. A
  panicking job transitions the state to `Poisoned(reason)`; the loop then
  drains remaining queued jobs by failing them, and every subsequent
  submission errors without enqueueing.
- **Close protocol**: `close()` moves `Open → Closing` (new submissions now
  fail), enqueues the terminal job (teardown transports, clear the tick
  scheduler, cancel all registered write-state waiters with a `Closed`
  error, flush + close storage), then **joins the core thread** before
  returning. Joining is safe precisely because the core thread never calls
  into JS (next section) — the old crate's never-join rule applied to the
  notifier, and still does; the DB actor is not the notifier.
- The FFI-entry panic boundaries (`with_panic_boundary`) still port from the
  old crate — they cover panics in argument parsing and channel plumbing on
  the caller's thread.

### 4.2 Queries and write waits

Queries execute on the core thread with the crate-shared `block_on` (noop
waker, immediate ticks), exactly as napi runs `core_block_on` on the JS
thread.

The realigned core owns the complete wait operation through
`Db::wait_for_transaction_with`: it checks the current fate, registers the
completion callback atomically when the requested tier is still pending, and
completes with rejection or success. `RnWrite.wait(tier)` is therefore an
**async** UniFFI method. Its actor job installs the core callback before
returning the oneshot receiver to the future, so immediate completion and
subsequent state changes cannot be lost. Actor close/poison completes every
pending wait with a stable error, and dropping or aborting the foreign future
releases its actor-side waiter registration.

The TypeScript adapter awaits `write.wait(tier)` directly. There is no
binding-level waiter object or `nextWriteStateChange` handshake after the core
realignment.

### 4.3 Tick scheduling and the notifier thread

The core thread must never block on Hermes, and Hermes must never wait on a
busy core thread — the old crate's deadlock lesson (`64b033b19`). The old
`RnScheduler` ports nearly verbatim as the **notifier**:

- `Db.set_tick_scheduler` installs a scheduler forwarding
  `schedule_tick(urgency)` to the notifier: a detached worker thread with
  per-job debounce flags, a never-joined shutdown protocol, and coalescing of
  bursts (its two existing regression tests port with it).
- The notifier invokes the uniffi callback interface
  `TickSchedulerCallback.on_tick_needed(urgency: String)` (`"immediate"` /
  `"deferred"`, napi's vocabulary). JS responds by calling `rnDb.tick()`,
  which enqueues a tick job. Debounce lives Rust-side; deferred-tick pacing
  stays in the TS adapter as today.
- The notifier remains detached and is never joined; only the DB actor joins
  on close (§4.1).

### 4.4 Transport

`RnDb.connect_upstream() -> RnTransport` mirrors napi's queue pair:

- `send_wire_frame(bytes)` / `send_wire_frames(frames)` — enqueue inbound
  frames (job to core thread; frames land in the connection's inbound queue).
- `recv_wire_frames() -> Vec<Vec<u8>>` — drain the outbound queue.
- `tick() -> u32` — pump the `PeerConnection`.
- `close()` — detach from the `Db`.

JS owns the WebSocket via the untouched `WebSocketCarrier`; React Native's
built-in `WebSocket` (binary `arraybuffer` mode) is sufficient.

### 4.5 Exported surface (full `NativeDb` contract)

| Object                                 | Methods                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `RnDb`                                 | `open_memory`, `open_persistent` (constructors); `set_tick_scheduler`, `on_mutation_error`, `tick`, `close`; `prepare_query`; `all`, `all_for_identity`; `all_relation_query(_for_identity)`, `all_relation_snapshot(_for_identity)`; `all_in_transaction(_for_identity)`; `local_current_row`; `set_identity_claims`; `can_insert_encoded(_for_identity)`, `can_read_for_identity`, `can_update_encoded_for_identity`, `can_delete_for_identity`; `attach_query(_for_identity)`, `query_attachment_is_covered`, `detach_query`; `subscribe(_for_identity)`, `subscribe_relation_query(_for_identity)`; `insert_with_id_encoded(_for_identity)`, `update_encoded(_for_identity)`, `upsert_encoded(_for_identity)`, `delete(_for_identity)`, `restore_encoded(_for_identity)`; `mergeable_tx(_for_identity)`, `exclusive_tx`; `connect_upstream` |
| `RnTx`                                 | `insert_with_id_encoded`, `update_encoded`, `upsert_encoded`, `delete`, `restore_encoded`, `commit -> RnWrite`, `rollback`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `RnWrite`                              | `payload`, `wait(tier)` (async), `write_state -> String(JSON)`, `close`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `RnTransport`                          | `send_wire_frame(s)`, `recv_wire_frames`, `tick`, `close`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `RnSubscription`                       | `read_all -> Vec<String(JSON)>`, `drain`, `close`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `RnPreparedQuery`, `RnQueryAttachment` | opaque handles                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| module fns                             | `mint_local_first_token(secret, audience, ttl_seconds, now_seconds)`, `mint_anonymous_token(secret, audience, ttl_seconds, now_seconds)`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |

The permission probes, `exclusive_tx`, and `all_in_transaction*` are
implemented against the core `Db` facade (`can_insert`/`can_read`/
`can_update`/`can_delete` per `INV-API-28`, `exclusive_tx()` per
`INV-API-27`, open-transaction reads via the engine's
`OverlayRef::OpenTransaction` path) with `jazz-wasm`'s implementations as
the behavioral reference, since napi has not implemented them yet.

The token functions take `now_seconds: u64` — `RuntimeTokenOptions` carries
`nowSeconds` and the shim must pass it through, not synthesize time.

Explicitly out: `JazzServer`, `TestJwtIssuer` (Node test fixtures),
`verify_local_first_identity_proof` (backend-only; used by
`backend/request-auth.ts`, never on-device).

Payload conventions: rows/schemas/queries/cells are postcard `Vec<u8>`; read
options and subscription events are JSON strings (`serde_json::Value` has no
uniffi mapping; the adapter already `JSON.parse`s equivalent payloads on the
wasm path).

**Shared binding helpers (amended exception).** The original M2 plan was to
extract the payload codecs and open path from the per-crate `jazz-napi` copy
(`decode_core_open_args`, `open_core_db`, `decode_core_cells`,
`encode_core_rows`, wait-state checking, …) into a shared `jazz`
binding-support module and switch both jazz-rn and `jazz-napi` in the same
change. The core realignment implemented the shared module and jazz-rn
adoption, but deliberately deferred the N-API switch to avoid mis-encoding its
changed carrier. Therefore the "no third copy" rule is not currently a
structural invariant: `jazz-napi` still has its own copy, with drift guarded by
the cross-binding contract test until the follow-up. See the realignment
design's §2 decision 2 and §12.1 for the live exception and options.

### 4.6 Error contract

`JazzRnError` (uniffi error enum, `thiserror`) with variants mirroring the
old crate (`InvalidPayload`, `Schema`, `Runtime`, `Internal`, `Closed`,
`Poisoned`). Two rules:

1. **Wait-state errors carry binding-neutral core error codes.** The adapter
   string-matches classes of failure, and the markers are the contract:
   rejection recognition requires the **`WriteRejected`** marker
   (`rejectedWaitError` matches nothing else), pending recognition matches
   `NotObserved` / `"has not been accepted at requested tier"` / `"has not
reached requested tier"`, and coverage-pending matches `NotCovered`. The
   shared wait-state helper (§4.5) returns the core `ErrorCode` and renders
   messages as `"{code}: {detail}"` — e.g. `WriteRejected: …` — rather than
   preserving napi's current `"transaction was rejected: …"` text, which the
   adapter does **not** recognize as a rejection. napi adopting the shared
   helper in M2 fixes that stale text as a side effect; the TS suites that
   encode these markers gate the change.
2. **Panics never cross the FFI.** Entry boundaries as in the old crate, and
   the actor-loop containment of §4.1. A panic on the core thread is
   terminal for that `RnDb` (`Poisoned`): every subsequent call returns a
   clear error rather than aborting the app.

## 5. TypeScript layer

- **New** `packages/jazz-tools/src/react-native/native-db.ts`: structural shim
  from the ubrn-generated classes to the adapter's `NativeDbConstructor` /
  `NativeDb` contract — camelCase mapping, `ArrayBuffer`↔`Uint8Array`
  conversion at the boundary, `setTickScheduler(cb)` registering the uniffi
  callback interfaces, async `wait(tier)` forwarding (§4.2), detached
  mutation-error delivery, and `readAll()` parsing operations plus one-time
  terminal-layout definitions from the JSON event strings.
- **Persistent identity is deterministic, or `INV-API-30` breaks.** On
  reopen, locally originated pending transactions are rescheduled only when
  `TxId.node == DbIdentity.node` and `made_by == DbIdentity.author` — a
  random node id per launch silently orphans the outbox.
  `DefaultRuntimeSource` already derives deterministic node/author bytes for
  the persistent-browser path (`persistentIdentitySeed`,
  `deterministicBytes`, `authorBytesForSubject`); M3 extracts those helpers
  into a shared module and `ReactNativeRuntimeSource` reuses them keyed on
  `(appId, env, userBranch, subject, dbName)`. A Rust-side reopen test plus a
  TS test pin the derivation.
- **Data directory is an explicit contract.** Public config supplies a
  logical `dbName`, not a path, and React Native has no built-in filesystem
  path API. `ReactNativeDbConfig` gains `dataDirectory: string` (absolute
  path), required for persistent drivers — the RN provider errors clearly
  without it. `jazz-tools/expo` supplies the convenience default from
  `expo-file-system`'s document directory. The final path is
  `${dataDirectory}/${sanitize(resolveDefaultPersistentDbName(config))}.db`;
  sanitization is filename-safe (the old crate's alphanumeric/-/\_ rule), and
  the Rust side runs `create_dir_all` on the parent at open. iOS backup
  policy (excluding the DB from iCloud backup) is recorded as a non-gating
  M5 follow-up.
- **Rewired** `runtime-source.ts`: `ReactNativeRuntimeSource.load()` imports
  `jazz-rn` (optional peer dependency, same loading discipline as `jazz-wasm`
  on other platforms); `createClient()` builds
  `NativeRuntimeAdapter(RnDbShim, schema, node, author, …)` for both memory
  and persistent drivers; `mintLocalFirstToken`/`mintAnonymousToken` route to
  the native module, passing `nowSeconds` through. No wasm load on RN at all.
- **Deleted**: `storage.ts` (`ReactNativeSqliteStorageDriver`,
  `UnimplementedSqliteStorageDriver`) and the `sqliteStorage` config hook —
  storage is native-side. `react-native/README.md` rewritten to describe the
  decided architecture; SPEC 13/17 open-question entries updated to record the
  decision.
- `crates/jazz-rn` npm package: regenerate `src/generated/*` via ubrn from the
  new lib.rs; `index.tsx` remains the installer/re-export shell; `jest`
  placeholder test replaced by real shim unit tests (generated module mocked).

## 6. Data flow walkthroughs

**Write.** App mutation → adapter encodes cells (postcard) →
`rnDb.insertWithIdEncoded(...)` → JSI → uniffi → job to core thread →
`db.mergeable_tx…insert_with_id` → `RnWrite` handle returned with settled
payload → adapter `pumpSubscriptions()`.

**Durability wait.** `waitForTransaction(tier)` awaits `write.wait(tier)`.
The actor delegates to the core-owned atomic wait and completes the UniFFI
future when that transaction reaches the requested tier or is rejected. The
JS thread remains free to pump frames while the future is pending.

**Sync.** Server frame arrives on RN WebSocket → `WebSocketCarrier` →
`transport.sendWireFrame(bytes)` → core thread ingests → tick scheduler wants
work → notifier thread → `TickSchedulerCallback.on_tick_needed("immediate")` →
JS `rnDb.tick()` → core tick produces outbound frames + subscription events →
adapter's debounced pump calls `transport.recvWireFrames()` → carrier sends;
`subscription.readAll()` drains deltas → hooks re-render.

**Cold start offline.** `open_persistent` reopens SQLite storage with the
deterministic identity (§5); pending local transactions reschedule per
`INV-API-30`; schema catalogue and rows serve locally; queries at
`tier: "local"` resolve without a transport.

## 7. Error handling and lifecycle

- Every FFI entry point returns `Result<_, JazzRnError>`; panic containment
  and the actor state machine as §4.1/§4.6.
- Core-thread poisoning is terminal per `RnDb`; the app-visible failure mode
  is a clear error, recoverable by reconstructing the client (RN dev-reload
  friendly). The installer-side "load exactly once" flags in `index.tsx`
  already handle metro reloads of the JS module itself.
- App backgrounding: no special handling in v1 beyond durable-by-default
  writes and clean `close()`; iOS jetsam after suspension is exercised by the
  abrupt-termination storage tests (§8) rather than assumed equivalent to a
  clean close.
- Transport loss: carrier reconnection and auth-failure routing stay in TS
  (`WebSocketCarrier`, `wireAuthFailureReason`). Note the carrier currently
  has **no retry** — one socket, closure reported, waiters failed (§9 orders
  the E2E around this; richer reconnect is the existing SPEC 13 "RN runtime
  reuse" open question plus the ledger's disconnect/reconnect NEEDS-PORT row,
  out of scope here).

## 8. Testing

Per `crates/jazz/TESTING_GUIDELINES.md` (read in full before writing any Rust
test): black-box integration tests through public APIs, no JSON-literal
schema/query construction — schemas and queries built with the public
builders, then encoded with the same helpers the bindings use.

1. **Groove conformance** (`crates/groove`): shared, backend-parametrized
   conformance functions run over both `MemoryStorage` (oracle) and
   `SqliteStorage` — point ops, range/prefix scoping incl. all-`0xFF`
   prefixes, reverse scans and `last_with_prefix*` parity against forward
   scans, empty keys, same-batch delta visibility, reopen with added CFs —
   plus sqlite-local tests for multi-MB values, format/shape rejection of
   alien files (incl. unexpected triggers/views/indexes), corruption
   surfacing, and mid-batch invalid-delta rollback. The rollback guarantee
   is documented as **stronger than MemoryStorage's current behavior**
   (memory applies ops in place and is not atomic under delta-application
   failure) — that divergence is flagged for an owner decision, not patched
   unilaterally.
2. **Abrupt-termination recovery** (`crates/groove`): subprocess writer is
   `SIGKILL`ed at controlled points (mid-initialization; after commit under
   `FullSync`; after `flush_write_boundary` under `WalNoSync`; mid-batch);
   the parent reopens and asserts WAL recovery yields whole committed state
   only — never a torn batch, never a bricked store. Scope honestly stated:
   SIGKILL exercises app-crash recovery; WAL survives app crashes regardless
   of `synchronous`, so **power-loss durability is derived from SQLite's
   documented `synchronous`/`fullfsync` semantics and guarded by
   pragma-state assertions**, not simulated by these tests. Clean
   close → reopen tests cannot stand in for jetsam.
3. **Node harness over SQLite** (`crates/jazz`): run the existing node
   test-harness storage matrix (the slot `NativeBtreeStorage` occupies) with
   `SqliteStorage` under `--features sqlite,test`.
4. **jazz-rn crate tests** (host target, in-workspace): actor lifecycle
   (open/close/reopen, close-joins-actor, submission-after-close errors
   without hanging, drop-abandons-tx), poisoning (induced job panic →
   in-flight reply completes with error, queued jobs fail, subsequent calls
   error, no abort), the §4.2 async write-wait boundary and cancellation matrix,
   write→wait→pending→settle across tiers with the `WriteRejected`-marker
   rendering, exclusive-tx + transaction reads + permission probes asserted
   behaviorally against `jazz-wasm`'s semantics (shared fixtures), tick
   scheduler regression tests (ported), transport frame round-trip against an
   in-process `jazz` server over the wire codec including a
   zstd-compressed-payload exchange, and deterministic-identity reopen
   rescheduling (`INV-API-30`).
5. **TS unit** (`jazz-tools` vitest): shim contract tests over a mocked
   generated module (tick and mutation-error registration, byte conversions,
   async wait forwarding, terminal-layout parsing, error-marker mapping,
   `nowSeconds` passthrough), plus the
   extracted identity-derivation helpers pinned against the current
   persistent-browser values.
6. **E2E** (§9): the only tier that exercises Hermes + JSI + a simulator or
   real device. The landed receipt covers iOS simulator; Android
   device/emulator execution remains best-effort while all Android artifacts
   build.

**Gates.** `crates/jazz-rn/rust` re-enters `workspace.members` (the pause
comment is resolved by this design). Canonical set additions: `cargo test -p
groove --features sqlite`, `cargo test -p jazz-rn`; `cargo test -p jazz
--no-default-features --features test` grows the sqlite-backend harness runs.
This change touches storage, so `dev/benchmarks/smoke.sh` runs at landing
tier; SQLite is not added to perf lanes (mobile backend, not a perf target).
CI keeps `--filter=!jazz-rn` for the pnpm test lane (needs RN toolchain), but
the Rust suite now gates via the workspace. Born-red rule: workspace re-entry
and the test targets land in the same PR as the code they cover.

## 9. E2E slice

- Revived `examples/todo-client-localfirst-expo`: removed its pnpm-workspace
  exclusion (and the stress-test app's, only if free), port `App.tsx` /
  `schema.ts` / `permissions.ts` to the current jazz-tools API, point it at
  the RN provider from `jazz-tools/react-native` with the Expo
  `dataDirectory` default.
- Build loop: `pnpm ubrn:ios` / `pnpm ubrn:android` in `crates/jazz-rn`
  produce the xcframework + jniLibs and regenerate bindings; `expo run:ios`
  on the simulator.
- Gating scenario (iOS simulator), ordered around the carrier's
  no-retry behavior — the app must not be asked to connect before its server
  exists:
  1. Launch with no `serverUrl` (pure local): create todos → kill app →
     relaunch → rows served from SQLite (persistence + reopen).
  2. Start the schema-initialized local E2E server **first**, then relaunch
     the app configured with `serverUrl`: pending local writes upload
     (`INV-API-30`), a second client observes them, inserts a write that
     reaches `edge`, and the mobile subscription re-renders live. The landed
     verifier prints
     `{"observedOfflineTitle":"offline-seed","insertedRemoteTitle":"remote-seed","rowCount":2}`.
     Automatic reconnect after a failed first connect is explicitly not part of
     the scenario (no retry exists today; see §7).
- The local E2E server uses the NAPI test harness with the schema supplied at
  startup. NAPI does not compile a compression codec, so the simulator launch
  sets `JAZZ_TRANSPORT_COMPRESSION=none`; production CLI/mobile pairs both use
  zstd and need no override.
- Android: `ubrn build android` artifact must build; emulator validation
  best-effort.

## 10. Milestones

Each lands green and independently valuable:

1. ✅ **M1 — groove SQLite backend**: `Durability` lift, backend, conformance +
   abrupt-termination tests, jazz feature re-point. No jazz-rn changes.
2. ✅ **M2 — shared binding helpers + jazz-rn Rust rewrite**: helper extraction
   (napi adopts; wait-state helper renders `ErrorCode` markers), actor +
   full surface + crate tests, workspace re-entry. Host target only.
3. ✅ **M3 — bindings + TS**: ubrn regeneration, `native-db.ts` shim, identity
   helper extraction, `ReactNativeRuntimeSource` rewire (incl.
   `dataDirectory`), scaffold deletions, shim unit tests.
4. ✅ **M4 — mobile artifacts**: `ubrn build ios` / `android` verified
   (including cross-compiled `zstd-sys` + bundled SQLite); podspec/gradle
   fixes as needed.
5. ✅ **M5 — E2E**: revived Expo example, gating scenario on iOS simulator,
   spec/README/ledger updates recording the outcome.

## 11. Risks and open questions

- ✅ **ubrn 0.30.0-1 ↔ uniffi 0.30 fidelity.** Generated sync and async
  methods, callback interfaces, and the live tick callback build and
  execute through Hermes on the iOS simulator.
- **Carrier feature advertisement is capability-blind (pre-existing).**
  `WebSocketCarrier` and the Rust byte adapter do not share the negotiated
  codec. `jazz-napi` builds without any `transport-compression-*` feature while
  jazz-rn compiles zstd, so mixed-build local harnesses must explicitly select
  `none`. The clean fix — carrier features and the Rust adapter both derived
  from one negotiated native capability — remains a shared-helper follow-up.
- **Actor round-trip cost on bulk paths.** Initial sync pumps
  (`sendWireFrames`/`recvWireFrames`) are already batched; if per-call hops
  show up in E2E profiling, widen batching at the shim (frames per job), not
  the contract.
- **napi capability gap.** Probes/exclusive-tx/transaction-reads land in
  jazz-rn per the SPEC matrix while napi remains behind; the matrix row stays
  accurate only if napi's gap is tracked. Out of scope here beyond recording
  it.
- ✅ **Expo 54 / RN 0.81 new-architecture drift.** CocoaPods autolinking,
  codegen, local signing, Hermes/JSI, and the iOS 15.1 deployment target are
  verified. The Android project prebuilds and all four native ABIs package.
- **`rusqlite` 0.34 pin.** Single-crate dependency; no workspace conflict
  today. If groove later wants a shared pin, lift to
  `workspace.dependencies`.

## 12. Out of scope

- Expo/RN framework adapter polish beyond what the E2E app needs (hooks API
  churn, auth secret-store UX).
- A public storage-driver plug-in API for RN (explicitly removed; the native
  backend is the route).
- RocksDB-on-mobile packaging, per recorded decision 1.
- WebSocket reconnect/retry (existing SPEC 13 open question + ledger
  NEEDS-PORT row).
- Old-runtime ledger rows marked NEEDS-PORT that concern the deleted broker /
  connection-manager surfaces.
