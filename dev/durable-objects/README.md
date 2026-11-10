# Jazz on Durable Object SQLite

Jazz's authority uses a Durable Object's own `ctx.storage.sql`, without a page
VFS. The production host and `durableObjects()` provider live in the main crates
and packages; this directory contains integration tests, probes and historical
spike evidence. Transaction overlays and durable large-value storage remain
unsupported. Remaining work is in [PLAN.md](PLAN.md).

## Maintained integration suites

Run from any directory using the path to `test.mjs`:

```sh
node dev/durable-objects/test.mjs          # production-host suite
node dev/durable-objects/test.mjs storage # lower-level SQL/engine conformance
node dev/durable-objects/test.mjs all     # both local suites; never deploys
```

The runner builds the selected WASM artifacts (fast local builds by default)
and the storage suite's native sync client, then runs tests sequentially with
inherited output and a nonzero exit on failure. Host tests require the repository's
built NAPI artifacts; local tests need permission to bind loopback sockets. The
crash suite requires POSIX process groups. Use `--no-build` only when artifacts
match current source. Set `RUSTC_WRAPPER= CC=cc CXX=c++` if local compiler wrappers
are unavailable. This runner is not the repository's CI-equivalent gate.

| Suite        | Retained coverage                                                                                                                                                                                           |
| ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`       | Provider unit tests; production `createJazz` publication/admission/read/write, accounts and revocation, reconnect/restart, and abrupt process-crash recovery.                                               |
| `storage`    | Catalogue validation/rollback with generated fixtures, nested SQL scopes, rejected sync batches, independent storage owners, exact SQL values and native replica sync, plus shared-core scheduling/cleanup. |
| `cloudflare` | Opt-in disposable deployment, lost uploads/acknowledgements, deferred upgrades, fresh-client persistence and verified resource cleanup.                                                                     |

```sh
CLOUDFLARE_ACCOUNT_ID=... CLOUDFLARE_API_TOKEN=... \
  node dev/durable-objects/test.mjs cloudflare
```

Cloudflare is never included in `all` and requires explicit credentials. Its
build uses the normal release settings unless overridden. No test identities,
fault controls or probe routes are added to the production host.

Focused scripts remain in `native-sqlite/harness/` for debugging distinct
contracts; they are not separate server implementations. The older
`test-cloudflare.mjs` provisioning experiment remains a manual diagnostic with
its own state/cleanup instructions below, not part of the disposable suite.
Performance probes and historical SQL replay are also outside the regression
runner. On 2026-11-09, both local suites passed with freshly built artifacts:
11 provider unit tests and all eight integration scripts. Detailed evidence and
limitations follow.

## Compatibility baseline (2026-11-04)

The refreshed probes run on **Miniflare 4.20260730.0 / workerd 1.20260730.1**,
with compatibility date `2026-07-01`. This is the version currently available
from the npm registry used here; it is not a verification of November production
workerd. Versions are pinned in `package-lock.json` and printed by the runner.

| Area                         | Observed behavior / current Jazz implications                                                                                                                                                                                                                                  |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Transactions                 | SQL BEGIN, SAVEPOINT, RELEASE, COMMIT and ROLLBACK are refused. Nested `transactionSync` rolls back an inner throw independently. Async `transaction` rolls back across await; SQL issued by an awaited helper participates, on both success and failure.                      |
| Columns                      | 100 per table. Current rows have 8 structural columns (`rid`, `id`, `_branch`, `_clock`, `_writer`, `_state`, `_base`, `_touched`) plus 4 provenance columns: **88 physical app columns**. Retained migration columns count. History tables omit `rid`.                        |
| Bound parameters             | 100 accepted, 101 refused. Long eligible IN lists still use one JSON parameter; mixed-type shapes may retain placeholders.                                                                                                                                                     |
| SQL length                   | Exactly 100,000 ASCII bytes accepted; 100,001 refused. The limit applies to the whole `exec` input. The schema DDL helper splits at complete-statement boundaries, including trigger bodies.                                                                                   |
| Expression / compound limits | Flat AND chain of 101 refused (depth limit 100); 5 compound SELECT terms accepted, 6 refused. Jazz balances long AND/OR chains, but nested policy/query shapes can still exceed limits.                                                                                        |
| Function arguments           | This workerd accepts 127 and refuses 128. Cloudflare documents **32**; native `durable-object-limits` now uses the conservative documented value.                                                                                                                              |
| Values                       | Large values remain chunked. The probe accepts a BLOB just below 2 MiB and rejects one just below 3 MiB; the SQLite length limit also covers a whole row.                                                                                                                      |
| Integers                     | Reading `9007199254740993` as a JS number loses precision; BigInt bindings are refused. Decimal text bound to a current affinity-free app column stays TEXT and changes equality. `CAST(? AS INTEGER)` plus `CAST(value AS TEXT)` round-trips signed 64-bit endpoints exactly. |
| Storage classes              | Current app columns have no type affinity. Inline text/JSON is TEXT, bytes are BLOB, and chunked cells are INTEGER epoch markers. Integral REAL values retain REAL in SQL even though JS cannot distinguish them from integers.                                                |
| Internal tables              | KV use creates `_cf_KV` in this workerd, which current fresh-store checks ignore. Creating `_cf_x` is refused.                                                                                                                                                                 |
| PRAGMAs                      | journal mode, synchronous, cache size, user version, temp store and page count are refused. Foreign keys, recursive triggers, table info, optimize, data version and quick check work.                                                                                         |
| SQL features                 | Triggers, WITHOUT ROWID, STRICT, partial/expression indexes, FTS5, ANALYZE, JSON functions and unhex work. TEMP, ATTACH, rtree and sqlite_version are refused. LIKE patterns accept 50 bytes and reject 51.                                                                    |

The column diagnostics are enabled by the native
`durable-object-limits` feature. Schema admission checks the physical union
before DDL, including retained columns, and names the table and limit in a
`Schema` error. Other SQLite failures retain their existing error classification.
This feature is a compatibility test profile, not the production storage interface.

References: [Cloudflare SQL limits](https://developers.cloudflare.com/durable-objects/platform/limits/),
[SQL storage and transaction APIs](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/).
The documented function-argument limit and measured runtime limit differ; do
not silently treat the more permissive local result as the production contract.

The pinned [workerd security profile](https://github.com/cloudflare/workerd/blob/v1.20260730.1/src/workerd/util/sqlite.c%2B%2B#L1353)
also limits compiled VM instruction allocation to 25,000, trigger depth to 10,
attached databases to zero, and worker threads to zero. These are now included
in the native profile. Its length limit is 2,200,000 bytes; the native profile
retains the more conservative 2 MiB limit. Instruction allocation grows in
chunks, so fewer than 25,000 emitted instructions can still exhaust its budget.
The probe accepts 5,000 literal IN members and rejects 8,000 with SQLITE_NOMEM.

## Run the probes and replay

From the repository root:

```sh
npm ci --prefix dev/durable-objects
node dev/durable-objects/run-probe.mjs

# Use a new trace path for each run: the tracer appends.
JAZZ_SQL_TRACE=/tmp/jazz-do-trace.jsonl cargo test --workspace \
  --features jazz/durable-object-limits,jazz/sql-trace --no-fail-fast
python3 dev/durable-objects/replay-prep.py /tmp/jazz-do-trace.jsonl /tmp/jazz-do-jobs.json
node dev/durable-objects/run-replay.mjs /tmp/jazz-do-jobs.json /tmp/jazz-do-failures.json

# Native boundary regression tests.
cargo test -p jazz --features durable-object-limits --test integration durable_object_limits
```

The probe runner asserts transaction outcomes, exact casts, and key limit
boundaries, and exits nonzero on a mismatch. `--bench` separately enables the
old exec timing probe; ordinary compatibility runs do not measure performance.

Replay executes DDL and prepares other statements with EXPLAIN QUERY PLAN and NULL
bindings. It skips transaction statements and PRAGMAs; probes cover those
separately. It does not execute the traced writes, prove result correctness,
or reconstruct a reopened file's earlier connections. Full EXPLAIN cursor
enumeration hung after schema changes in the pinned runtime; QUERY PLAN avoids
bytecode rendering while still preparing SQL and draining each cursor. The
runner saves progress, bounds each job to 30 seconds, records incomplete jobs,
and exits nonzero for any failure. `START`/`END` select a job range and
`DEBUG_JOB` prints its operations. Missing-table failures
must be inspected rather than counted as platform incompatibility. Failed
native prepares do not enter the SQL trace. Native limits do not emulate the
DO authorizer, JS value bridge, or callback transaction lifetime.

## Exact SQL value probe

Run `node dev/durable-objects/run-value-probe.mjs` from the repository root
(after `npm ci --prefix dev/durable-objects`). No Rust/WASM rebuild is needed.
This standalone platform proof passed on 2026-11-05 with Miniflare
4.20260730.0, workerd 1.20260730.1, and compatibility date 2026-07-01.

- JavaScript numbers bind as SQL REAL, including `1`. All Rust INTEGER
  bindings need explicit SQL conversion, not only integers wider than 2^53.
- Decimal text with `+CAST(? AS INTEGER)` preserves signed 64-bit values and
  numeric storage. The unary `+` matters: plain `CAST` adds expression affinity,
  so an affinity-free TEXT cell containing `"10"` incorrectly matches an integer
  parameter expressed as `CAST('10' AS INTEGER)`. The unary form matches native
  affinity-free numeric-parameter comparison behavior in this fixture.
- Integer results projected as text retain exact digits, but transport casts
  must not change query semantics. An unqualified ORDER BY matching the cast's
  alias orders text, not integers. This probe orders by the original column.
- SQL storage-class information distinguishes INTEGER, integral REAL, and
  numeric TEXT. The prototype uses `typeof` plus a conditional integer-to-text
  projection; this is not yet a chosen production projection representation.

Assertions cover i64 endpoints and neighbors around ±2^53, equality, numeric
ordering/LIMIT, MIN/MAX/SUM, UPDATE RETURNING, rollback, REAL 1.0 and 1e100,
NULL, numeric TEXT, UTF-8 text containing NUL, and binary bytes. Only fixture
rows are read. No performance, full engine compatibility, or deployed Cloudflare
claim is made. Persisted encodings are unchanged. For actual adapter coverage, use the
Rust/WASM checks below; the standalone probe remains independent of the adapter.

## Shared server runtime compile check

`jazz-server` now has a default `native` feature for process startup, file
storage, threads, Axum, and native network clients. With default features
omitted, `runtime::ServerCore` accepts an already-open `Db` and a host waker;
it contains the same sync sessions, async thin requests, and catalogue mutation
logic used by the native host. Its clock uses `web-time`. Tokio is used only
for channel/semaphore primitives in this build, without its native executor.

```sh
cargo check --locked \
  --manifest-path dev/durable-objects/native-sqlite/Cargo.toml \
  -p jazz-do-server-core-check --target wasm32-unknown-unknown
```

This compile-only consumer uses the DO workspace's declarations-only SQLite
patch. It does not include Axum, reqwest, Jazz's native runtime, or the native
SQLite execution adapter. Existing native imports remain available through
re-exports and the default feature.

This is not a DO server deployment. The core accepts identities admitted by
its host; HTTP authentication, protocol handshake, account-registry access,
and production object startup still need the host integration described in the
plan. The local harness now exercises event scheduling and WebSocket output
through this core, as described below.

## Experimental WASM build and WebSocket harness

`native-sqlite/` ports the isolated build from
[PR #4579](https://github.com/garden-co/jazz/pull/4579), source head
`838e7fa8b927292a29853b66c4c05c80cb81f0e6`, to the current tree. Its workspace
patches `sqlite-wasm-rs` to declarations only so reuse of rusqlite's conversion
and error types does not bundle SQLite C. This build omits the optional
`sqlite` adapter; native shells and the browser feature select it. `durable_object.rs` now implements Jazz's typed storage
backend directly over DO SQL; it defines no `sqlite3_*` shim functions. Browser
stores retain the native SQLite adapter by default; the isolated patch does
not apply to browser builds.

```sh
# Requires wasm-pack and the wasm32-unknown-unknown Rust target.
# FAST skips LTO and wasm-opt; use it for compatibility checks.
FAST=1 dev/durable-objects/native-sqlite/build.sh
node dev/durable-objects/native-sqlite/harness/serve.mjs

# In another terminal: a small sync smoke workload, not a performance claim.
cargo build -p jazz-example-doc-shelf-benchmark --bin doc-shelf-do-ceiling-ws
SMOKE=1 STORE=20 PROJECT=20 SESSIONS=2 GROUP=2 ROWS=2 WRITES=2 \
  target/debug/doc-shelf-do-ceiling-ws ws://127.0.0.1:8787
```

The default optimized build additionally needs wasm-opt. `PERSIST=<directory>`
retains DO storage, `TRACE=1` counts SQL statements, and `INSPECTOR_PORT=9229`
enables the optional CPU profiler. The harness accepts fixture identities from
its caller; bind it only to local loopback. It is not production authentication.

The sync authority uses the production `WasmServerCore` host in
`crates/jazz-wasm/src/server.rs` (the optional `server` feature). The dev-only
`FixtureServer` in `native-sqlite/jazz-do/src/fixture.rs` opens a DO-backed `Db`,
hands ownership to that host, and publishes the fixture schema and permissions
bundle through its shared deployment path. Scheduling, session transport, and backend-session handshake have one Rust
implementation; fixture setup and raw test controls are not production APIs.
Catalogue validation and staging live in `jazz-server::runtime::{catalogue,
deployment}` and are also used by native HTTP handlers. The WASM host exposes
`publishCatalogue`, `catalogueSnapshot`, and `catalogueSchema`; its caller must
provide admin authentication, request bounds, and serialized scheduling. These
are host APIs, not public HTTP routes. The production catalogue adapter below
provides those boundaries; the raw sync harness still uses fixture startup.
Socket frames enter session queues; one async tick advances and flushes the
core before the host sends output. Microtask wakeups avoid re-entering Rust;
disconnects close session input. `WasmDb` remains only for standalone value probes.
The `reopen` control operation supplies only fixture identity/configuration,
not schema or permissions. `StoreFile::from_storage` reads the catalogue before
handing that same storage to `Db::open_file`; shared `catalogue_schema` selects
the activated revision or admitted genesis. Reopen publishes nothing and refuses
an empty catalogue. The harness requires exactly one stored bundle; production
identity recovery and authenticated bundle selection are still separate work.

```sh
# After the scope-tests build and native client build below:
node dev/durable-objects/native-sqlite/harness/test-server-core.mjs
# Policy-filtered subscriptions and rejected writes, then catalogue-only restart:
node dev/durable-objects/native-sqlite/harness/test-scopes.mjs --catalogue
```

### Production DO HTTP and WebSocket adapter

`crates/jazz-wasm/durable-object-server.js` owns catalogue HTTP routing and startup.
The build entry supplies the generated WASM exports to `createJazzDurableObject`
and exports the returned class. `routeJazzRequest` forwards requests through the
`JAZZ` SQLite DO namespace, using `idFromName(JAZZ_APP_ID)`. Configure a canonical
UUID `JAZZ_APP_ID` and a nonempty `JAZZ_ADMIN_SECRET`; the app's object and the
outer Worker both check the requested app. There is no new durable encoding:
startup uses the existing catalogue and native authority node convention.

Supported routes are `GET /apps/<app>/schemas`, `GET /apps/<app>/schema/<hash>`,
`GET /apps/<app>/admin/migrations/graph`, and `GET|POST /apps/<app>/admin/bundles`.
Every route requires `X-Jazz-Backend-Credential` matching the configured secret
and `X-Jazz-Backend-Access: admin`, as sent by `remote()`. Direct admin-secret
headers and user/session access do not grant catalogue access. Body and queue
limits are enforced before entering Rust. Jobs and ticks share one owner queue.

First creation and publication run inside a short
[DO storage transaction](https://developers.cloudflare.com/durable-objects/api/legacy-kv-storage-api/#transaction).
The callback performs local initialization only, with no network waits or admitted
sessions. A rejected first publication drops the new host and rolls back schema
creation; a later valid publication can start cleanly. Existing authorities use
the shared publication path. Reconstruction reads only the persisted catalogue.
The adapter keeps accepted jobs alive independently of the request with `waitUntil`.

After building with `--features scope-tests`, run:

```sh
node dev/durable-objects/native-sqlite/harness/test-catalogue.mjs
```

The test-only entry `catalogue-worker.mjs` supplies WASM build wiring and generated
fixtures; HTTP behavior comes from the production adapter. Coverage includes the
real Node remote provider, missing/wrong credentials, access separation, oversized
bodies, failed first startup, concurrent publication retries, catalogue reads, and
workerd reconstruction. `POST /apps/<app>/backend/admit` also verifies service authority without creating
an account; it requires verified `backend` or `admin` access. `/apps/<app>/ws`
verifies the upgrade credential, then uses the normal prelude/Hello handshake.
Prelude secrets cannot override the upgrade's access. Backend/admin service links
are trusted as on native hosting, while backend-stated user sessions remain
policy-scoped. Session-only upgrades do not grant service or stated-user access;
guest proofs use the account admission described below. Ordinary thin sessions
are supported as described below; relay modes remain explicitly unsupported.

The adapter serializes admission with catalogue operations and ticks. Established
socket input uses `SessionInput` directly, even while a tick awaits progress.
Frames are capped at 2 MiB, pending input at 8 MiB per socket and 16 MiB per
object, including retained preludes. It permits 64 sockets with a two-second
handshake deadline. Output is flushed after ticks and catalogue publication;
close/error paths release the transport and queued bytes. Hibernation is not used.

```sh
node dev/durable-objects/native-sqlite/harness/test-create-jazz.mjs
```

This uses the real Node `createJazz()` and `remote()` provider: readiness publishes
the catalogue and admits its backend client, writes settle globally, and reads
recover persisted values. The test restarts workerd with the same Node backend
still alive and verifies another globally settled write after reconnect, then
opens a fresh backend against the retained SQLite. It also checks user-session
handshakes and access/identity/bundle refusals. The schema denies ordinary users;
service writes exercise native-equivalent trusted-backend authority. Existing
`test-scopes.mjs` checks policy-filtered user sync and rollback through the shared
engine. Account routes and guest proofs are covered by `test-accounts.mjs` below;
packaging/provisioning and thin hosting are documented separately below.

Validated locally: numeric sync and atomic rejection on two objects, disconnected
session cleanup, and fresh replicas reading persisted values after workerd restart
without republishing schema/permissions. The numeric history probe failed once
with “history at a snapshot” during validation on 2026-11-09 and passed on rerun;
its assertion is unchanged and the cause is unresolved.
`test-scopes.mjs` also passes: Rust callbacks can run without an open SQL scope;
reads requiring a persistent overlay fail explicitly and clean up their batch.
It additionally validates nested scopes, rejected writes, and persisted restart.

Synchronous callback scopes now use the owning `ctx.storage.transactionSync`.
`WasmDb.openDurableObject` takes `ctx.storage`, not just `ctx.storage.sql`.
`Db::open_storage` receives one owner for SQL execution and transactions, with
no connection registry or global storage slot. Native execution uses rusqlite
cached statements and borrowed rows directly; managed execution uses the DO
backend and a forward-only cursor. The `sqlite` feature includes the native
implementation and is omitted from DO builds. Rust retains scope outcomes and
undo logs, and the JS bridge throws a private marker for deliberate rollback. Nested
scopes roll back independently. SQLite commit/rollback hooks run at the
outermost scope boundary; Cloudflare output gates govern external durability.
Raw SQL transaction statements are refused. Rust callback transactions now
use the same staged batches as TypeScript; their persistent transaction overlay
still returns a typed unsupported error on DO storage. Supporting that shared
overlay is the next transaction task, not a separate async SQL callback bridge.

Run the actual WASM/workerd scope and restart checks with:

```sh
FAST=1 dev/durable-objects/native-sqlite/build.sh --features scope-tests
cargo build -p jazz-example-doc-shelf-benchmark --bin doc-shelf-do-ceiling-ws
node dev/durable-objects/native-sqlite/harness/test-scopes.mjs
```

The optional test build includes database-layer conformance probes. The runner
checks nested rollback, rollback with a returned value, enum-tag undo and cache
rollback tracking. A stale replica's two-row insert fails on its second row;
the first leaves no data, history or subscriber update. Subsequent successful
writes are verified after restarting workerd with the same persisted SQLite
and connecting fresh replicas. Two objects in the same process run the same
fixture independently. This remains a local harness, not deployment or
hibernation validation.

### Exact values in the DO adapter

After building with `FAST=1 ... --features scope-tests`, run:

```sh
# Actual Rust -> JS -> SQLite -> JS -> Rust, plus public Jazz API checks.
node dev/durable-objects/native-sqlite/harness/test-values.mjs
# Also check native/DO numeric sync, atomic rejection, and persisted restart.
RUSTC_WRAPPER= cargo build -p jazz-example-doc-shelf-benchmark --bin doc-shelf-do-ceiling-ws
node dev/durable-objects/native-sqlite/harness/test-values.mjs --sync
```

The `--sync` mode runs a correctness fixture through the existing WebSocket
transport (`VALUE_TEST=1`), separate from the ceiling performance workload.
Alice's native SQLite replica uploads safe-range boundary integers and REAL
values (`1.0`, `1.5`, ±`1e100`); Bob's fresh native replica checks exact typed
results. Four batches pairing a valid row with an unsupported integer must fail
without publishing subscriber updates. The existing test-only storage assertion
checks physical row/history absence and pending change markers, in addition to
public history reads. These backend errors use the current wire error mapping:
`Protocol` with the safe-integer-range message, not a policy `WriteRejected`.
The runner then recreates workerd from persisted SQLite and verifies values and
rejected-row absence using fresh replicas. Both phases pass locally (2026-11-06).

The TypeScript history suite also passes all 84 tests with fresh native/WASM
artifacts. The artifact producer previously overflowed its Git output buffer
when macOS emitted a sandbox warning; bounded stderr headroom fixes that blocker.

The DO backend rejects integer bindings and integer results outside
±9,007,199,254,740,991. Safe integer bindings cross JS as numbers and use
`+CAST(? AS INTEGER)` to retain INTEGER without changing comparison affinity.
REAL values, including `1.0` and `1e100`, remain REAL and are not range-limited.

Compiled query projections, stored-column types for sync/subscriptions, pending
cell encodings, and adapter counters now supply result descriptions. Known
positional results execute directly; named results use cached metadata to align
types with columns. Unknown physical cells still use SQL transport projections,
including raw defaults and unannotated internal queries. Only unknown cells are
tagged; known cells in a mixed result pass through directly. Native SQLite does
not build result descriptions and retains its original decoding path.

Metadata/projection caches remain bounded to 64 entries and 1 MiB of estimated
string data per owner, invalidated by schema operations and rollback. Results
remain streamed. Named bindings and data-modifying RETURNING reads fail
explicitly; non-UTF-8 text bindings remain refused. Extra projection SQL counts
toward platform compilation limits.

History's `$acceptedAt` is projected from the acceptance clock into Unix
milliseconds before SQL filtering and result decoding. It fits the safe range;
`_clock` preserves acceptance ordering within a millisecond. Workerd checks
cover history timestamps, safe integer
boundaries, rejected wide binds and aggregates, REAL values, mixed decoding,
permission boundaries, cache invalidation, 100-column results, and sync smoke.

The following measurement describes the earlier full-wrapping implementation,
not the subsequent schema-directed decoder.

Local A/B measurement (2026-11-05, macOS arm64): baseline `746da3b616f4`
versus the exact-value adapter, both built with `FAST=1 --features scope-tests`.
The same native benchmark client drove fresh workerd instances, with four runs
per build and workload in alternating order: 400 stored rows, 200 rows/project,
10 sessions, groups of 5, and 150 writes per run. Median authority tick time per
write rose from 0.76 to 0.83 ms for one-row writes and from 3.45 to 4.40 ms for
ten-row writes. Median client p50 latency rose from 3.70 to 3.79 ms and from
9.28 to 10.30 ms respectively. SQL execs per ten-row write were almost unchanged
(327.06 versus 327.10). One-row timing ranges overlap; ten-row ranges do not.
These are local timings with a 1-ms workerd clock, not production CPU/capacity
measurements or CodSpeed results. They do not isolate individual conversion costs
or cover large values. WASM linear memory was unchanged; JS heap was not measured.

The subsequent schema-directed decoder was compared with full wrapping on the
same rebased source (`3bf228e5c027`), using the same build settings and workloads
above, with four alternating samples per variant/workload. Median tick time per
one-row write was 1.03 ms for both variants; ten-row writes were 4.49 ms with full
wrapping versus 4.58 ms with schema-directed decoding. Median client p50 latency
was 4.27 versus 4.29 ms and 10.83 versus 10.97 ms respectively. Tick timing ranges
overlap, and SQL calls remain effectively unchanged (327.10 versus 327.09 per
ten-row write). This does not demonstrate a performance improvement. These
measurements compare the two implementations on safe-valued workloads; their
behavior for unsupported wide integers differs. The local raw results, source
patches, runner, and binary hashes are retained in `/tmp/jazz-do-typed-perf/`.

The `do-value-bridge-v2` framing is private to the matching WASM/JS module, never
persisted or sent to peers: byte tags 0=NULL, 1=i64 LE, 2=f64 LE, 3=text, 4=blob;
text/blob carry a u32 LE byte length, and result rows start with a u32 LE cell
count. Tests pin integer/REAL binding bytes and exercise the full bridge. No
SQLite storage or sync encoding changes, migration, or mixed-version protocol
changes are introduced.

The standalone platform probe demonstrates full-width integer transport; the
production adapter instead enforces the agreed safe-integer limit. Chunk caching
still uses the pathless configuration; bounded memory and durable chunk reads
remain section 7. Numeric sync and persisted restart are covered above; full lifecycle and
production deployment validation remain in section 11.

## Validation of this refresh

- Probe assertions pass on the pinned runtime, including async helper commit
  and rollback, exact integer casts, storage classes, and limit boundaries.
- The final workspace suite with all 12 native limits reports 2,644 passed,
  2 failed, and 11 ignored.
  Both failures are `durable_file` memory tests that read `/proc/self/status`,
  which is unavailable on the macOS host. Their expectations were not changed.
- A subsequent Jazz integration run, including the new fresh-schema,
  schema-evolution, and retained-column boundary tests, reports 908 passed,
  0 failed, and 4 ignored.
- The isolated DO WASM build succeeds, and the normal browser WASM target
  passes `cargo check -p jazz-wasm --target wasm32-unknown-unknown`.
- The WebSocket smoke checks 20 seeded rows, two subscribers, and two writes
  of two rows each. Both writes reach global acknowledgement and both
  subscribers' contents match the expected result; the authority reports no
  errors. This exercises successful writes, not rollback or production auth.
- The new trace contains 5,376,911 statements, 14,360 distinct SQL texts, and
  5,717 connections. This capture preceded adding the VM/trigger limits.
  Replay preparation produces 2,142 jobs / 223,444 operations.
  No traced binding exceeds 100 parameters or the scanned 2 MiB value bound.
  Wide integer literals/bindings remain present and require the typed bridge.
- All 2,142 replay jobs complete with 978 errors: 977 missing-table errors
  (coverage gaps in per-connection schema reconstruction) and one reproducible
  `SQLITE_NOMEM` on the S1 SaaS policy query. Missing-table errors are not passes.
  There are no other error categories or timed-out jobs in the QUERY PLAN run.

The S1 failure combines history-cut unions with nested membership/inheritance
policy joins. Its full EXPLAIN contains 32,106 instructions on SQLite 3.51.3
and 31,459 on Jazz's bundled SQLite 3.53.2. Setting the 25,000 VM allocation
limit reproduces the failure on both versions. The native S1 scenario tests
still pass, so they do not establish that this exact emitted shape fits. This
is a query-shape restriction, not evidence of exhausted isolate heap. The
fixture contains only the existing simulator's synthetic DDL and the failing
query; it fails in a fresh workerd with no application rows:

```sh
node dev/durable-objects/run-replay.mjs \
  dev/durable-objects/fixtures/s1-policy-prepare.json /tmp/s1-policy-failures.json
# Expected today: one SQLITE_NOMEM failure and a nonzero exit.
```

The existing S1 expectations remain unchanged. This baseline does not resolve
policy-query expansion or establish support for the failing shape. Do not turn
an arbitrary SQLITE_NOMEM into an unsupported-query error: real allocation
failures share that code and need to remain distinguishable.

## Historical evidence

The October spike captured 2.4 million statements, 17,000 distinct SQL texts,
and 1,624 connections. It found and fixed long IN bindings, oversized DDL
batches, deep AND/OR chains, engine integer ranges, and fresh-store detection.
Those counts and its reported suite results describe that snapshot, not the
current tree. Its 89/95 app-column figures and large-value TEXT-marker advice
are obsolete. The subsequent #4579 experiment ran sync over WebSockets with
10–500 sessions, but its timing and memory estimates are not measurements of
this refreshed build.

### Authenticated WebSocket harness

The dev harness's `/socket` route accepts the normal JSON prelude, binary Hello,
and batched sync frames. It requires a `BACKEND_SECRET` Worker binding and a
backend-stated user session. Native and DO hosts share credential comparison,
canonical identity validation, and policy-claim conversion; `ServerCore` validates
the bundle and governs the session's reads/writes. The secret is never taken from
`/control` or used to trust a caller-supplied author without checking the session.

After rebuilding the scope-test WASM and `doc-shelf-do-ceiling-ws`, run:

```sh
BACKEND_SECRET=do-admission-test-secret AUTHENTICATED_SESSION=do-admission-test-secret \
  node dev/durable-objects/native-sqlite/harness/test-scopes.mjs --catalogue
BACKEND_SECRET=do-admission-test-secret AUTHENTICATED_SESSION=do-admission-test-secret \
  node dev/durable-objects/native-sqlite/harness/test-server-core.mjs
```

The client checks invalid/missing credentials, mismatched identity, malformed
sessions, missing/unknown bundles, and unsupported admission modes before using
native replicas through the authenticated carrier. The scope suite exercises
permission-filtered reads and rejected writes, then reconstructs two objects from
SQLite and verifies successful data with fresh replicas.

This remains a **development harness**: `/control` and raw `/session` are fixture
interfaces, not authenticated production routes. The new route supports only
ordinary backend-stated user sync sessions. It refuses direct proof-only auth,
admin/backend-only links, relays, catalogue bootstrap, and thin clients. Production
account admission/revocation, HTTP routes and provider wiring are now implemented
outside this fixture harness; broader lifecycle work remains in the plan. No
storage or wire encoding changed.

## Provisioning provider and packaged host

Node backends can now use the same provider architecture as Vercel:

```ts
import { createJazz, durableObjects } from "jazz-tools/backend";

const jazz = createJazz({
  app,
  permissions,
  store: durableObjects({ name: "my-jazz-authority" }),
});
await jazz.ready();
```

Supply `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_API_TOKEN`, and a stable, randomly
generated `JAZZ_CORE_SECRET` (at least 32 characters). Explicit `credentials:
{ accountId, apiToken }` and `secret` options override these. The API token needs
Worker script/settings and workers.dev access for the selected account. Configure
that account's workers.dev subdomain first. API-token rotation does not rotate
core credentials. Changing the name creates another authority; keep the core
secret and name across backend restarts. Secret rotation is not implemented.

Opening uploads the packaged production host when absent, reuses matching bytes,
or upgrades an older recognized version. It preserves the SQLite namespace and
secret binding, refuses downgrades and same-version/different-byte builds, and
checks the live authenticated host before returning the existing remote transport.
Closing releases the backend handle without deleting the shared authority.
The upload uses Cloudflare's migration `old_tag` check to reject stale upgrades;
creation additionally sends `If-None-Match: *`. Actual Cloudflare provisioning and concurrent opens were tested on 2026-11-09;
this does not prove every cross-process upgrade race.
See the [upload API](https://developers.cloudflare.com/api/resources/workers/subresources/scripts/methods/update/).

`pnpm --filter jazz-tools build:durable-objects` builds the production-only
`jazz-do-server` wrapper and bundles Worker JavaScript plus WASM into the package's
`durable-object-worker/` directory. `prepack` runs this build. The fixture crate
and test endpoints are excluded. `FAST=1` skips WASM optimization for iteration;
release packages should use the default optimized build. Generated artifacts are
ignored by Git, included in npm packages, and verified before upload.

Provisioning metadata has explicit versions, separate from database and sync
encodings (which are unchanged):

- App ID: UUIDv5 in the DNS namespace over UTF-8
  `jazz-durable-objects-v1:<accountId>:<workerName>`. Never change this mapping
  for an existing authority.
- `jazz-do-worker-v1` manifest: SHA-256 hex hashes of the exact Worker/WASM bytes;
  artifact digest is SHA-256 of UTF-8 `format`, package version, Worker hash,
  and WASM hash joined with NUL bytes, with no trailing NUL.
- `jazz-do-host-v1` JSON binding records version, artifact digest, app ID and
  SHA-256 of the core secret. Unknown formats fail closed. Upgrades retain
  identity and namespace; incompatible future formats require an explicit
  migration, not regeneration of the app ID or deletion of the namespace.

Validation:

```sh
# Mock Cloudflare HTTP API: create, reuse, upgrade, concurrent opens and refusals.
cd packages/jazz-tools
pnpm exec vitest run src/backend/core-providers/durable-objects.test.ts
# From repository root, test the actual packaged host in local workerd.
JAZZ_DO_PACKAGED=1 node dev/durable-objects/native-sqlite/harness/test-create-jazz.mjs
```

The packaged-host integration covers createJazz readiness, authenticated writes
and reads, reconnect and SQLite-only restart. Provisioning tests do not contact
Cloudflare. Thin-client hosting, transaction overlays and large-value storage
remain limited as recorded in the plan; this provider does not remove those limits.

### Opt-in Cloudflare deployment check

`native-sqlite/harness/test-cloudflare.mjs` deploys the dedicated
`jazz-do-provisioning-test` Worker using the real provider. Set
`CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_API_TOKEN` (a Wrangler OAuth token also works),
and `JAZZ_DO_TEST_STATE` to a private file **outside the repository**. The script
creates that file with mode 0600, retaining the randomly generated core secret
and synthetic test row identity. It never prints the credentials. The account
must already have a workers.dev subdomain.

Run once to provision and write, then in a fresh process to verify reuse and the
previously acknowledged row. Run with `--upgrade` last to install a synthetic
next-alpha artifact on this test Worker, verify namespace/identity/data retention
and new writes, and check that the original provider refuses a downgrade. This
fixture does not publish a package or change the repository's package version.
After the upgrade, ordinary opens with the original package deliberately fail.
The script leaves the authority and its data in place for inspection; it never
deletes the Worker or namespace. This is a deployment test, not a benchmark.

Deployed validation (2026-11-09): provider opening and createJazz readiness passed,
a write received global acknowledgement and was read by a fresh backend process,
and a synthetic next-alpha host retained the namespace, identity, credential and
saved row. New writes after upgrade succeeded and the old version refused to
open. Concurrent opens converged on the same authority. The first deployment
encountered TLS setup delay and the upgrade encountered version propagation delay;
readiness now retries network/HTTP failures within two minutes, while rejecting
invalid credentials immediately. No production-load or forced-eviction claim is
made by these checks.

## Account routes and guest admission

The production host now serves `POST /apps/<app>/admin/accounts/resolve`,
`login-or-register`, `found-local-first`, `link`, and `revoke`. Backend access may
resolve/register/found; linking and revocation require admin access. Requests use
the existing body/queue limits and serialize with admission and ticks.

Account request types, decisions and SQL rows are shared in
`jazz-server::runtime::accounts`. Native HTTP retains its threaded registry owner
and notifications; the DO borrows the owning database through `HostTables::host_sql`
for short callback scopes. The two host tables, UUID blobs, principal strings,
link nonces and integer encodings are unchanged. No storage or wire format changed.

Guest admission verifies the existing Jazz self-signed proof (signature, audience,
expiry and canonical identity), founds a local-first account when necessary, and
checks its persisted assignment. Only verified backend access can override the
proof audience. External provider JWT verification remains the Node backend's
responsibility, matching native core behavior. Backend-stated sessions retain
native semantics; they are not reclassified as guest-proof sessions.

Revocation rechecks active proof-authenticated sessions. Revoked sessions stop
accepting frames and discard queued output immediately, then close. Registry
storage failures fail these checks closed, including for thin sessions. Relay
modes remain unsupported; the persistent transaction overlay is still deferred.

Build the production package, then run:

```sh
node dev/durable-objects/native-sqlite/harness/test-accounts.mjs
```

Local validation (2026-11-09) covers public `createJazz` enrollment/linking,
permission-scoped reads and rejected writes, concurrent first registration,
backend/admin access separation, link nonce reuse, wrong-account revocation,
valid guest admission, forged proofs/identities/audiences, active revocation,
and assignment/revocation persistence after SQLite-only workerd restart. The
existing packaged-host sync/reconnect test and 18 native account/storage/admission
tests also pass. Native and DO WASM Clippy pass. These account changes have not
been deployed to Cloudflare; the earlier deployment test covered provisioning.

## Interrupted transport and lifecycle validation (section 9)

`test-lifecycle.mjs` runs against the packaged production host. Its test provider
drops Alice's outgoing frames before an upload, or drops incoming frames after
sending a write. Bob observes the authority independently: the first write has
not landed; the second is committed while Alice's global receipt remains pending.
After workerd reconstruction with SQLite retained, Alice reconnects and both
receipts settle. Fresh clients verify the row counts and history, including
unchanged acceptance timestamps and transaction IDs for the already accepted
write. A further write proves forward progress. No fault routes or sync bypasses
are added to production code.

```sh
FAST=1 node packages/jazz-tools/scripts/bundle-durable-object-worker.mjs
node dev/durable-objects/native-sqlite/harness/test-lifecycle.mjs
# Opt-in real deployment; credentials stay in the environment:
CLOUDFLARE_ACCOUNT_ID=... CLOUDFLARE_API_TOKEN=... \
  node dev/durable-objects/native-sqlite/harness/test-lifecycle.mjs --cloudflare
```

The Cloudflare mode creates a randomly named `jazz-do-lifecycle-*` Worker with
synthetic data. It disconnects sockets at the lost-upload boundary, applies one
synthetic host upgrade with the committed write still unacknowledged, then checks
fresh clients. The upgrade retains app/namespace identity; it is not an abrupt
process kill. The temporary artifact version is never packaged or published. Cleanup deletes
the dedicated Worker and checks its namespace is gone; it never reuses an
existing Worker. The normal local run needs no credentials.

Local validation passed on 2026-11-09. These are transport-loss and reconstruction
checks, not forced eviction or process termination during SQLite commit.
Hibernation, interruption inside a commit and wider lifecycle coverage remain
unverified. Section 9 is not complete.

The deployed test exposed a readiness mismatch: an active object's upgrade timed
out after two minutes. Cloudflare's [deferred code-update policy](https://developers.cloudflare.com/durable-objects/deployments/durable-objects-code-updates/)
allows old code to serve for five minutes. The provider now allows up to seven
minutes (deferral plus propagation) after an authenticated readiness response
identifies the expected app running different code. Cold-host/network failures
retain the two-minute bound; invalid credentials still fail immediately. Retries
only poll readiness and do not repeat uploads or force active objects to reset.
Provider tests cover delayed adoption and bounded failure without changing the
existing cold-host retry expectations.

Deployed validation passed on 2026-11-09. The active object adopted the synthetic
upgrade within the extended readiness window, retaining its app identity and
namespace. The writer's pending global receipt settled after reconnect; history
retained the same acceptance timestamps and transaction IDs without duplicate
versions. A fresh client read the persisted rows/history, and a subsequent write
succeeded. Lost-upload reconnect recovery also passed. Cleanup deleted the
temporary Worker and verified its namespace was absent.

Earlier attempts exposed the old two-minute readiness timeout and were then
blocked by upload connections closing (`UND_ERR_SOCKET`); the successful run
closes that validation gap. The local lifecycle test and all 11 provider unit
tests also pass. This verifies upgrade-driven reconstruction, not abrupt eviction
or interruption inside SQLite commit; section 9 remains in progress.

### Abrupt local workerd termination

```sh
# Uses the same packaged production host as the lifecycle test above.
node dev/durable-objects/native-sqlite/harness/test-crash.mjs
```

This POSIX-only harness starts Miniflare/workerd in a dedicated process group,
checks that workerd belongs to it, and sends the group `SIGKILL`. Fault runs never
call `dispose()` or a shutdown hook. Each restart retains only the same temporary
SQLite storage; normal cleanup stops the final host and removes that directory.
There are no production-code changes or fault routes.

Alice submits 20-row transactions through the native backend client, so the DO
receives completed replica batches rather than using its deferred transaction
overlay. Cases cover a deliberately undelivered upload, kills at 0/1/5/20 ms after
a transport send, and a kill after Bob independently observes the entire committed
batch while Alice's acknowledgement is withheld. Alice's outgoing traffic stays
blocked after restart until a fresh Bob has inspected persisted rows/history.
Every batch must be entirely absent or entirely present before retry. Then Alice
reconnects, the pending receipt settles, and Bob checks all row values and exactly
one history version per row. Already accepted versions retain their timestamps
and transaction IDs. An acknowledged baseline must survive every crash, and a
final fresh client checks all six batches after both earlier client caches close.

Local validation passed on 2026-11-09: before-delivery and 0/1 ms cases recovered
0/20 rows; 5/20 ms and the known-committed case recovered 20/20. All six settled
successfully after retry without duplicate history; all 120 batch rows plus the
acknowledged baseline survived. Timing outcomes can vary across runs. The test
asserts atomicity for whichever outcome occurs, not a required outcome at each
delay. These timings do **not** establish that the kill landed inside SQLite
commit. Deterministic interruption within commit and deployed abrupt eviction
remain unverified; section 9 stays in progress.

## Thin backend client and scoped requests

`createJazz()` now uses `ThinRuntime` for `jazz.db`, including when its core is
native. It shares one connection across backend and request-scoped facades;
queries run on the authority instead of an in-memory replica. Standalone native
`createJazzClient()` memory/persistent clients retain their existing behavior.
`forReqUser()` carries the user's session and claims; `withAttribution()` retains
SYSTEM authority while recording the supplied author. The authority enforces
bundle policies and checks the connection's authenticated identity before
accepting any backend scope. Subscriptions retain their operation's read scope.

The DO host now admits thin connections through the shared server engine.
Independently borrowable input handles queue requests while ticks suspend;
queued request bytes remain charged until completion. Ordinary reads, writes,
subscriptions and streamed operation messages use this path. Whole requests
are bounded by the existing wire frame and ingress limits; split thin requests
are explicitly refused by this host input. Durable chunks remain deferred.
Backend application transactions report an explicit unsupported error for now.
Managed storage rejects begin before retaining a batch; ordinary native thin
transactions are unchanged.

### Wire compatibility: thin-backend-scope-v1

This is an additive wire-v6 capability, feature bit 12. It appends ThinOp tag 18
without changing tags 0–17 or durable storage bytes. The envelope contains
length-prefixed JSON context bytes and length-prefixed encoded ThinRequest
bytes. The inner request must have the same id; nested envelopes and scoped
schema declarations are rejected. Only authenticated SYSTEM sessions can use
it. This is an authorization assertion from a trusted backend, never a public
client capability. Scope is not inferred from the peer's advertised role.

New backend clients require the feature in Hello negotiation and fail explicitly
against older servers. Ordinary thin clients continue to use the existing
frames. The Rust `backend_scope_v1_bytes` and TypeScript wire tests pin the same
bytes: `04 07 12 02 7b 7d 03 04 07 0d` (request 7, empty backend context, inner
capture-snapshot request). No stored or sync-record encoding changes.

Local packaged-host checks cover backend writes/reads and reconnect/restart;
the account suite additionally checks thin runtime selection, user-policy
refusals, attribution, subscriptions and refusal of a public client's forged
backend scope. The Worker entry and its invocation lifetime are described below.

Validation on 2026-11-09: 102 native createJazz/thin integration tests passed,
plus 46 context/wire unit tests and the Rust envelope byte fixture. TypeScript
checking and native/DO Clippy passed. Packaged workerd createJazz, account and
lost-request/lost-acknowledgement lifecycle checks passed. One existing native
remote-core diagnostic test was excluded from the final pass after reproducing
its missing-URL failure with both the old NAPI client and the thin client; its
expectations are unchanged. No deployed Cloudflare or Worker-entry-point claim
is made by these checks.

## Worker-hosted createJazz

`jazz-tools/backend/worker` exports `createJazz`, `remote`, `schema` and the
shared authentication helpers. Enable Cloudflare's `nodejs_compat` flag; this
entry uses supported Node crypto and Buffer APIs, but no NAPI, local database,
Node listener or Vercel SDK. Import the schema DSL from this entry as well to
avoid pulling browser-runtime initialization from the general package entry.

Provision the core separately with the Node `durableObjects()` provider. Inside
the application Worker, `remote({ url, appId, secret })` connects to that existing
core. Its authenticated socket upgrade uses Worker `fetch()` and the returned
WebSocket. Cloudflare provisioning credentials are not needed in this Worker.
This implementation uses the core's URL, not a namespace/service binding.

Create and close the client within each invocation. Share schema/configuration,
not a live Jazz client or its sockets across requests:

```ts
import { createJazz, remote } from "jazz-tools/backend/worker";
import { app, permissions } from "./app.js";

export default {
  async fetch(request, env, ctx) {
    const jazz = createJazz({
      app,
      permissions,
      store: remote({
        url: env.JAZZ_CORE_URL,
        appId: env.JAZZ_APP_ID,
        secret: env.JAZZ_CORE_SECRET,
      }),
      // Supply your application's authentication callback here.
    });
    let upgraded = false;
    try {
      // Application handlers can also await jazz.db.all(...), forReqUser(...), etc.
      const response = await jazz.handle(request, ctx);
      upgraded = response.status === 101;
      return response;
    } finally {
      if (!upgraded) await jazz.close();
    }
  },
};
```

`handle(request, ctx)` adapts incoming WebSocketPair events into the existing
Jazz admission/protocol path. A successful upgrade retains its client until
socket close/error and schedules cleanup with `ctx.waitUntil`. Ordinary HTTP
requests and custom database handlers await their work and close before returning.
The HTTP-stream fallback needs state across requests and is explicitly refused
(501); WebSocket capability probes do not advertise it. Backend transactions,
DO split thin requests and durable large values retain their existing limitations.
Worker text-anchor helpers are unsupported. Node attachment APIs remain available
only from the Node entry.

Publication, authentication, account routes, user scopes and thin request
handling are shared with Node. Node retains local native validation; the Worker
relies on the authority's atomic schema/migration/permission validation during
publication. It does not run a second schema validator or database engine.
No durable or wire encoding changes are introduced by this host extraction.

```sh
# Requires built jazz-tools JS and the current packaged DO host; included in test.mjs host/all.
node dev/durable-objects/native-sqlite/harness/test-worker-backend.mjs
```

Validated in local workerd on 2026-11-09: four concurrent request-owned clients,
fresh reads of committed writes, account authentication, permission-scoped reads,
attributed writes, valid and forged incoming WebSocket admission, and explicit
stream refusal. The test bundles the real Worker entry and checks that native
engines and Node/Vercel socket dependencies are absent. Node regression coverage:
102 createJazz/thin integration tests and 46 context/wire tests passed; the known
remote-core diagnostic test remains excluded with its expectations unchanged.
These checks do not establish deployed application-Worker behavior or performance.
