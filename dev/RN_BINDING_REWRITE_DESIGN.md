# React Native binding rewrite — design

Status: proposed (2026-08-07). Owner: RN surface owner.
Scope: `crates/groove` SQLite storage backend, `crates/jazz-rn` rewrite,
`packages/jazz-tools/src/react-native` wiring, revived Expo example E2E.

## 1. Context

`crates/jazz-rn/rust` still targets the pre-swap runtime
(`jazz::tools::{runtime_core, schema_manager, sync_manager, binding_support,
storage, transport_manager, ws_stream}`), none of which exist after the core
engine swap. The crate is commented out of the workspace and fails to resolve
either standalone or as a member. Its generated TS bindings are stale, and the
TS RN scaffold (`packages/jazz-tools/src/react-native`) is compile-level only:
persistent configs throw `UnimplementedSqliteStorageDriver`, and the fallback
path loads the WASM runtime, which Hermes cannot execute.

The v2 runtime already has two proven bindings over one shared contract:

- `jazz-napi` — Node, holds `Rc<Db<S>>` directly on the JS thread.
- `jazz-wasm` — browser, single-threaded by construction.

Both are driven by the same TypeScript adapter
(`packages/jazz-tools/src/runtime/native-runtime/native-runtime-adapter.ts`),
which speaks a structural `NativeDb` contract: postcard-encoded rows and
schemas, JSON option bags, `setTickScheduler`/`tick()`, byte-queue transports
pumped by a JS-owned WebSocket (`WebSocketCarrier`), non-blocking
`Write.wait(tier)` plus `nextWriteStateChange(): Promise`, and
`Subscription.readAll()` polling. Spec `crates/jazz/SPEC/13_db_api.md` §13.13
requires that bindings not fork query/transaction/sync semantics; the RN
binding therefore implements this same contract rather than resurrecting the
old JSON `RnRuntime` surface.

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

One rule governs the whole stack: **jazz-rn exposes byte-for-byte the same
contract as `NapiDb`** — same postcard payloads, same options JSON, same error
message substrings — so every TS layer above the shim is reused unmodified.

## 3. Groove SQLite backend

New `crates/groove/src/storage/sqlite.rs`, exported as `SqliteStorage` behind
a new groove feature `sqlite = ["dep:rusqlite"]` (rusqlite `bundled`; the
amalgamation cross-compiles cleanly for aarch64-apple-ios and Android NDK
targets). Jazz's existing vestigial `sqlite = ["dep:rusqlite"]` feature is
re-pointed to `["groove/sqlite"]` and its direct rusqlite dependency dropped.

### Schema

```sql
CREATE TABLE IF NOT EXISTS column_families (
  id   INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS kv (
  cf INTEGER NOT NULL,
  k  BLOB    NOT NULL,
  v  BLOB    NOT NULL,
  PRIMARY KEY (cf, k)
) WITHOUT ROWID;
```

`open(path, column_families)` interns CF names to ids; `reopen` re-interns and
may add new CFs (matching `ReopenableStorage` semantics used on schema
evolution). The composite PK B-tree provides the ordered contract directly:

- `get`/`set`/`delete` — point ops on `(cf, k)`.
- `scan_range(cf, start, end)` — `WHERE cf=? AND k>=? AND k<?` ordered by `k`,
  streamed through the `ScanVisitor` callback (no materialization).
- `scan_prefix` / `scan_prefix_reverse` / `last_with_prefix` /
  `last_with_prefix_before_or_at` — prefix upper bound computed by
  rightmost-byte increment (empty prefix ⇒ full-CF scan); reverse variants use
  `ORDER BY k DESC`, `last_*` use `LIMIT 1`.
- `write_many(ops)` — one SQLite transaction over the batch: atomicity parity
  with the RocksDB `WriteBatch`.
- `column_family_names()` — from the intern table.
- `approximate_class_bytes` — `SELECT SUM(LENGTH(k)+LENGTH(v))` per CF is
  O(n); return `Ok(None)` (the contract's "unsupported" answer) unless a cheap
  page-count heuristic proves useful later.

### Durability

Mirrors the RocksDB backend's `Durability` enum, whose `WalNoSync` doc already
reads "like SQLite WAL/NORMAL":

- `journal_mode=WAL`, `foreign_keys` off, prepared-statement cache on.
- `FullSync` ⇒ `synchronous=FULL` (every commit fsyncs).
- `WalNoSync` ⇒ `synchronous=NORMAL` (WAL atomicity, no per-commit fsync).
- `flush_write_boundary()` ⇒ forced WAL sync (`wal_checkpoint(PASSIVE)` after
  an fsync-forcing commit), and resets the pending counter of
  `set_write_flush_cadence`, exactly like RocksDB's `flush_wal(true)` path.
- `close()` ⇒ `wal_checkpoint(TRUNCATE)` + connection close, releasing file
  locks. Mobile lifecycle (kill/restore, background suspension) makes clean
  close/reopen a first-class path, not a shutdown nicety.

### Error mapping

`SQLITE_CORRUPT`/`SQLITE_NOTADB` and open-time shape mismatches map onto the
same `storage::Error` variants the OPFS/RocksDB backends use for corruption
and migration reporting, satisfying the SPEC 13 gate ("migration reporting,
corruption behavior, durability tests") before `OpenStorage` advertises the
backend.

### Threading note

`OrderedKvStorage` is used thread-affinely by `Db`; the backend keeps a single
`rusqlite::Connection` in a `RefCell` and is `!Sync` like its siblings. No
connection pool, no async.

## 4. `crates/jazz-rn` (Rust)

UniFFI proc-macro surface (no UDL), uniffi 0.30, generated for RN by
`uniffi-bindgen-react-native` exactly as the existing scaffold configures
(`ubrn.config.yaml`, `cpp/`, `android/`, `ios/`, podspec unchanged in shape).

### 4.1 Core thread (actor)

- `RnDb.open_memory(schema, config)` / `open_persistent(data_path, schema,
config)` spawn one `jazz-rn-core` thread per database. The thread decodes
  the same postcard `(schema, config)` payload napi's `decode_core_open_args`
  consumes, opens `MemoryStorage::new(&cfs)` or `SqliteStorage::open(path,
&cfs)`, builds the `Db` via the shared `open_core_db` path, then loops on
  `mpsc::Receiver<Job>`.
- `Job = Box<dyn FnOnce(&mut CoreState) + Send>`. `CoreState` owns the `Db`
  plus id-keyed registries: prepared queries, query attachments, open
  transactions, writes (`TxId`s), subscriptions (`SubscriptionStream`s),
  transports (`PeerConnection` + wire queues). Ids mint from an `AtomicU64`.
- Handle objects (`RnDb`, `RnTx`, `RnWrite`, `RnTransport`, `RnSubscription`,
  `RnPreparedQuery`, `RnQueryAttachment`) are `#[derive(uniffi::Object)]`
  structs holding `{jobs: Sender<Job>, id: u64}` — trivially `Send + Sync`.
  Sync methods marshal a job plus `mpsc` reply channel and block for the
  round-trip (µs-scale; the same order as the JSI hop itself). `Drop` on a
  handle enqueues a best-effort release job (open transactions abandon, napi
  `Tx::drop` parity).
- Open/close: `RnDb.close()` drains the queue, tears down transports and the
  scheduler callback, flushes and closes storage, and terminates the thread.
  A second `open_persistent` on the same path after `close()` must succeed
  (lock-release test).

### 4.2 Queries and futures

Queries execute on the core thread with the crate-shared `block_on` (noop
waker, immediate ticks), exactly as napi runs `core_block_on` on the JS
thread. The one true-async method is
`RnWrite.next_write_state_change() -> ()` — an async uniffi export backed by a
`oneshot` whose sender fires from the core-thread
`on_next_write_state_change` callback. No tokio; uniffi futures are polled by
the generated foreign code (same pattern the old crate's async exports used).

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

### 4.4 Transport

`RnDb.connect_upstream() -> RnTransport` mirrors napi's queue pair:

- `send_wire_frame(bytes)` / `send_wire_frames(frames)` — enqueue inbound
  frames (job to core thread; frames land in the connection's inbound queue).
- `recv_wire_frames() -> Vec<Vec<u8>>` — drain the outbound queue.
- `tick() -> u32` — pump the `PeerConnection`.
- `close()` — detach from the `Db`.

JS owns the WebSocket via the untouched `WebSocketCarrier`; React Native's
built-in `WebSocket` (binary `arraybuffer` mode) is sufficient.

### 4.5 Exported surface (napi parity)

| Object                                 | Methods                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `RnDb`                                 | `open_memory`, `open_persistent` (constructors); `set_tick_scheduler`, `tick`, `close`; `prepare_query`; `all`, `all_for_identity`; `all_relation_query(_for_identity)`, `all_relation_snapshot(_for_identity)`; `local_current_row`; `set_identity_claims`; `attach_query(_for_identity)`, `query_attachment_is_covered`, `detach_query`; `subscribe(_for_identity)`, `subscribe_relation_query(_for_identity)`; `insert_with_id_encoded(_for_identity)`, `update_encoded(_for_identity)`, `upsert_encoded(_for_identity)`, `delete(_for_identity)`, `restore_encoded(_for_identity)`; `mergeable_tx(_for_identity)`; `connect_upstream` |
| `RnTx`                                 | `insert_with_id_encoded`, `update_encoded`, `upsert_encoded`, `delete`, `restore_encoded`, `commit -> RnWrite`, `rollback`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `RnWrite`                              | `payload`, `wait(tier)`, `write_state -> String(JSON)`, `next_write_state_change` (async), `close`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `RnTransport`                          | `send_wire_frame(s)`, `recv_wire_frames`, `tick`, `close`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `RnSubscription`                       | `read_all -> Vec<String(JSON)>`, `drain`, `close`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `RnPreparedQuery`, `RnQueryAttachment` | opaque handles                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| module fns                             | `mint_local_first_token`, `mint_anonymous_token`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |

Explicitly out: `JazzServer`, `TestJwtIssuer` (Node test fixtures),
`verify_local_first_identity_proof` (backend-only; used by
`backend/request-auth.ts`, never on-device).

Payload conventions: rows/schemas/queries/cells are postcard `Vec<u8>`; read
options and subscription events are JSON strings (`serde_json::Value` has no
uniffi mapping; the adapter already `JSON.parse`s equivalent payloads on the
wasm path).

**Shared binding helpers.** The payload codecs and open path today live as
per-crate copies in `jazz-napi` (`decode_core_open_args`, `open_core_db`,
`decode_core_cells`, `encode_core_rows`, `core_wait_for_tx`, …) with twins in
`jazz-wasm`. jazz-rn does not become copy three: M2 extracts these into a
shared `jazz` binding-support module (move, not redesign), `jazz-napi`
switches to it in the same change (its existing suite verifies the move), and
`jazz-wasm` adoption is a recorded follow-up. This is what makes the §4.6
"cannot drift" rule structural rather than aspirational.

### 4.6 Error contract

`JazzRnError` (uniffi error enum, `thiserror`) with variants mirroring the old
crate (`InvalidPayload`, `Schema`, `Runtime`, `Internal`, `Closed`). Two rules:

1. **Wait-state messages are wire contract.** The adapter string-matches
   pending/rejected states. The `wait(tier)` implementation must reuse napi's
   `core_wait_for_tx` message text verbatim: `"transaction was rejected:
{reason:?}"`, `"transaction has not been accepted at requested tier
{tier:?}"`, `"transaction has not reached requested tier {tier:?}"`
   (and coverage errors keep `NotObserved`/`NotCovered` markers). A shared
   helper in `jazz` (extracted from jazz-napi) is preferred over copied
   strings so the three bindings cannot drift.
2. **Panics never cross the FFI.** `with_panic_boundary` /
   `with_async_panic_boundary` port from the old crate around every export. A
   panic on the core thread marks the actor poisoned; every subsequent call
   returns `Internal("core thread died: …")` rather than aborting the app.

## 5. TypeScript layer

- **New** `packages/jazz-tools/src/react-native/native-db.ts`: structural shim
  from the ubrn-generated classes to the adapter's `NativeDbConstructor` /
  `NativeDb` contract — camelCase mapping, `ArrayBuffer`↔`Uint8Array`
  conversion at the boundary, `setTickScheduler(cb)` registering the uniffi
  callback interface, `nextWriteStateChange()` passing the uniffi Promise
  through, `readAll()` parsing the JSON event strings.
- **Rewired** `runtime-source.ts`: `ReactNativeRuntimeSource.load()` imports
  `jazz-rn` (optional peer dependency, same loading discipline as `jazz-wasm`
  on other platforms); `createClient()` builds
  `NativeRuntimeAdapter(RnDbShim, schema, node, author, …)` for both memory
  and persistent drivers (persistent resolves `data_path` under the app's
  documents directory); `mintLocalFirstToken`/`mintAnonymousToken` route to
  the native module. No wasm load on RN at all.
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

**Durability wait.** `waitForTransaction(tier)` loop (adapter, unchanged):
`write.wait(tier)` round-trips the actor and either returns or throws a
pending-marker error → adapter awaits `write.nextWriteStateChange()` (uniffi
future; oneshot fired by core thread on state change) → retry. The JS thread
is never blocked while frames still need pumping — same non-blocking contract
napi relies on.

**Sync.** Server frame arrives on RN WebSocket → `WebSocketCarrier` →
`transport.sendWireFrame(bytes)` → core thread ingests → tick scheduler wants
work → notifier thread → `TickSchedulerCallback.on_tick_needed("immediate")` →
JS `rnDb.tick()` → core tick produces outbound frames + subscription events →
adapter's debounced pump calls `transport.recvWireFrames()` → carrier sends;
`subscription.readAll()` drains deltas → hooks re-render.

**Cold start offline.** `open_persistent` reopens SQLite storage; schema
catalogue and rows serve locally; queries at `tier: "local"` resolve without
a transport, matching the local-first contract of the other bindings.

## 7. Error handling and lifecycle

- Every FFI entry point returns `Result<_, JazzRnError>`; panic boundaries as
  §4.6. JS exceptions inside the tick callback are caught by the notifier
  (ported behavior) and never unwind into Rust.
- Core-thread poisoning is terminal per `RnDb`; the app-visible failure mode
  is a clear error, recoverable by reconstructing the client (RN dev-reload
  friendly). The installer-side "load exactly once" flags in `index.tsx`
  already handle metro reloads of the JS module itself.
- App backgrounding: no special handling in v1 beyond durable-by-default
  writes and clean `close()`; iOS jetsam after suspension is equivalent to
  the kill/restore path the reopen tests cover.
- Transport loss: carrier reconnection and auth-failure routing stay in TS
  (`WebSocketCarrier`, `wireAuthFailureReason`) — no Rust-side reconnect
  logic, per the "RN runtime reuse" open question's direction (deterministic
  connect/disconnect, no per-call executors).

## 8. Testing

Per `crates/jazz/TESTING_GUIDELINES.md` (read in full before writing any Rust
test): black-box integration tests through public APIs, no JSON-literal
schema/query construction — schemas and queries built with the public
builders, then encoded with the same helpers the bindings use.

1. **Groove conformance** (`crates/groove`): backend-parametrized suite
   running the ordered-KV contract over `SqliteStorage` and asserting
   behavioral equality with `MemoryStorage` — point ops, range/prefix/reverse
   scans, `last_with_prefix*`, atomic `write_many` (including a mid-batch
   failure leaving no partial state), reopen with added CFs, close → reopen
   durability at both `Durability` levels, corruption reporting on a
   truncated/garbage file.
2. **Node harness over SQLite** (`crates/jazz`): run the existing node
   test-harness storage matrix (the slot `NativeBtreeStorage` occupies) with
   `SqliteStorage` under `--features sqlite,test`.
3. **jazz-rn crate tests** (host target, in-workspace): actor lifecycle
   (open/close/reopen, drop-abandons-tx), napi-parity behavior for
   write→wait→pending→settle across tiers, subscription delta delivery via a
   registered tick callback, transport frame round-trip against an in-process
   `jazz` server over the wire codec, the two ported scheduler regression
   tests, and a poisoned-actor test (induced panic → subsequent calls error,
   no abort).
4. **TS unit** (`jazz-tools` vitest): shim contract tests over a mocked
   generated module (tick registration, byte conversions, error mapping,
   pending-wait recognition). The adapter itself is already covered by the
   napi/wasm suites; no RN-specific adapter fork exists to test.
5. **E2E** (§9): the only tier that exercises Hermes + JSI + real devices.

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

- Revive `examples/todo-client-localfirst-expo`: remove its pnpm-workspace
  exclusion (and the stress-test app's, only if free), port `App.tsx` /
  `schema.ts` / `permissions.ts` to the current jazz-tools API, point it at
  the RN provider from `jazz-tools/react-native`.
- Build loop: `pnpm ubrn:ios` / `pnpm ubrn:android` in `crates/jazz-rn`
  produce the xcframework + jniLibs and regenerate bindings; `expo run:ios`
  on the simulator.
- Gating scenario (iOS simulator): create todos offline → kill app → relaunch
  → rows served from SQLite → start local server (`cargo run -p jazz` CLI) →
  connect → writes reach `edge` tier → second client (Node script or web)
  observes them; subscription deltas re-render live.
- Android: `ubrn build android` artifact must build; emulator validation
  best-effort.

## 10. Milestones

Each lands green and independently valuable:

1. **M1 — groove SQLite backend** + conformance/durability tests (+ jazz
   feature re-point). No jazz-rn changes.
2. **M2 — jazz-rn Rust rewrite** + workspace re-entry + crate tests, host
   target only.
3. **M3 — bindings + TS**: ubrn regeneration, `native-db.ts` shim,
   `ReactNativeRuntimeSource` rewire, scaffold deletions, shim unit tests.
4. **M4 — mobile artifacts**: `ubrn build ios` / `android` verified;
   podspec/gradle fixes as needed.
5. **M5 — E2E**: revived Expo example, gating scenario on iOS simulator,
   spec/README/ledger updates recording the outcome.

## 11. Risks and open questions

- **ubrn 0.30.0-1 ↔ uniffi 0.30 async/callback fidelity.** The old crate
  proved sync methods + callback interfaces + async exports on this pair, but
  not uniffi futures resolved from a non-JS thread. M3 starts with a spike
  binding (`next_write_state_change` against a ticking core) before the full
  surface is generated. Fallback if foreign-polled futures misbehave: a
  callback-interface completion (`WriteStateWaiter.on_change()`) behind the
  same shim Promise — contract unchanged.
- **Actor round-trip cost on bulk paths.** Initial sync pumps
  (`sendWireFrames`/`recvWireFrames`) are already batched; if per-call hops
  show up in E2E profiling, widen batching at the shim (frames per job), not
  the contract.
- **SQLite blob-key edge cases.** Ordered-KV keys are arbitrary blobs; the
  conformance suite includes empty keys, 0xFF-run prefixes (upper-bound
  increment edge), and multi-MB values (large blob rows) before trusting the
  backend under real fixtures.
- **Expo 54 / RN 0.81 new-architecture drift.** The scaffold predates the
  swap; podspec/codegen may need version bumps discovered only in M4. Treat
  as mechanical, timebox, and record fixes in the crate README.
- **`rusqlite` 0.34 pin.** Single-crate dependency; no workspace conflict
  today. If groove later wants a shared pin, lift to
  `workspace.dependencies`.

## 12. Out of scope

- Expo/RN framework adapter polish beyond what the E2E app needs (hooks API
  churn, auth secret-store UX).
- A public storage-driver plug-in API for RN (explicitly removed; the native
  backend is the route).
- RocksDB-on-mobile packaging, per recorded decision 1.
- Old-runtime ledger rows marked NEEDS-PORT that concern the deleted broker /
  connection-manager surfaces.
