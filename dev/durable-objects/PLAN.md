# Durable Objects implementation plan

Run Jazz's authority on `ctx.storage.sql`, preserving its data model,
permissions, transaction guarantees, and sync protocol. Use the
[feasibility spike](README.md) and [DO experiment #4579](https://github.com/garden-co/jazz/pull/4579)
as starting points. The experiment demonstrates working sync in local workerd;
its counted savepoints and unsupported rollback are not production semantics.

## 1. Refresh the compatibility baseline — completed

Adapt the experiment's build and harness to current code and rerun the SQL
probes and limits tests. Current tables have 12 engine columns, leaving 88
physical app columns under the 100-column limit; retained migration columns
also count. Check current platform limits, internal table names, generated
DDL, and value encodings. Reconcile the native function-argument limit of 127
with Cloudflare's documented 32. Reject unsupported schemas and query shapes
with typed errors naming the exact limitation.

Baseline refreshed on 2026-11-04; see the README for measured results. The
refresh also found a 25,000-instruction compilation allocation limit missing
from the old profile. A captured S1 policy/history query exceeds it; its
reproduction is kept in `fixtures/s1-policy-prepare.json`. The baseline is not
a blanket compatibility pass.

## 2. Introduce the SQL storage boundary — completed

Add a small internal interface in `jazz-db` for typed parameters/results,
statement execution, affected-row counts, bounded result consumption, and
transaction scopes. Implement it with rusqlite for existing hosts and DO SQL
for Workers. Keep query compilation, permissions, history, and sync above this
boundary. Use the experiment's SQLite C shim as a reference; avoid making its
partial SQLite emulation the permanent contract. Express backend differences
as storage capabilities rather than scattered provider checks.

`Db::open_storage` takes one owned `Storage` for SQL and transaction scopes.
Storage selects its native or managed implementation with an enum. Native
operations use rusqlite cached statements and borrowed rows directly; the DO
backend implements typed execution, a forward-only cursor, and callback scopes.
The positive `sqlite` feature includes native execution; native hosts and browser
builds enable it, while the DO build omits it. There is no separate scope driver
or connection registry. Rusqlite value/error conversion types are still reused.
No storage or wire encoding changed; sections 6 and 7 remain.

The uniform dynamic-dispatch and owned-result trials were discarded after
native regressions. The original execution paths have been restored with
`sqlite` replacing `managed-sql-only`.

## 3. Make synchronous transactions callback-owned — completed

Replace separate savepoint begin/end calls with a scope callback: native
SAVEPOINT/RELEASE/ROLLBACK TO and DO `transactionSync`. Support both committing
a result and deliberately rolling back while returning a result, as overlays
and advisory reads require. Preserve nested rollback, enum-tag undo logs,
cache invalidation, chunk pins, and speculative engine-state cleanup. Separate
scope completion from durable publication: no acknowledgement or sync update
may escape before its required persistence boundary.

### Native callback scopes — completed

Converted synchronous write, commit, catalogue, sync, and speculative-read
scopes to callbacks with explicit commit/rollback outcomes. Existing native
savepoint bookkeeping remains in use. Local subscription notifications now
run after the outer upload scope completes. Validated with 905 integration
tests and 69 database-layer tests (2026-11-04).

### DO callback scopes — completed

The owned DO SQL backend runs callbacks through `transactionSync`, with nested
rollback, Rust undo bookkeeping, and outer-scope hooks. The workerd
harness checks rollback with a returned value, enum tags/cache invalidation,
rejected two-row sync writes (no residual data, history or subscriber update),
and successful writes after workerd restart with persisted SQLite. Two objects
in the same workerd process are checked for independent storage ownership. Run
`native-sqlite/harness/test-scopes.mjs` as documented in the README.

This completes synchronous callback scopes, not all transaction APIs. Raw SQL
savepoints are refused by the DO adapter. Rust and TypeScript now share staged
transactions (section 4); their persistent overlay still needs section 5.
Single-connection chunk storage and bounded caching remain in section 7.

## 4. Unify Rust and TypeScript transaction implementations — completed

Rust callback transactions and `MergeableTxHandle::commit` now use the same
staged batches, overlay validation, and commit path as the TypeScript bindings.
Removed `transaction_as`, its callback-lifetime SQL savepoint, and its separate
upload capture buffer. The callback facade retains owner/view write routing
and early write receipts; it does not implement another SQL transaction engine.
Errors and cancellation abandon the batch, including its retained upload leases.

This replaces the proposed async DO transaction bridge: an async application
callback does not itself require a SQL transaction spanning the callback.
The existing staged overlay still retains a savepoint between calls and can
therefore span awaits; replacing that representation remains section 5. Do not
claim full DO transaction support until that work is complete.

Native regression coverage includes callback suspension/resumption,
cancellation, caught errors, read-your-writes, schema-view writes, commit
receipts, upload-record failures, and large-value edits and streaming.

## 5. Replace the persistent transaction overlay — deferred

Client DBs and servers serving thin-client transactions use the same staged
transaction engine: begin a batch, stage writes, read through its private view,
then validate and commit or abandon it. For a full client DB this runs locally;
for a thin client it runs on the server across separate protocol messages.
Application transactions executed directly on an authority use it too.
An authority receiving completed replica sync batches does not need this
persistent overlay; that path already uses short synchronous scopes.

Today pending writes become queryable by applying them to ordinary tables
inside a cached SQLite savepoint. It stays open between operations and is
rolled back before unrelated access. Replace this representation so speculative
state survives between calls without holding a SQL transaction open.

An async DO `storage.transaction()` cannot provide that lifetime: our local
[workerd probe](README.md#async-overlay-lifetime-probe) blocks incoming HTTP and
WebSocket handlers until the transaction exits. This prevents thin clients'
next operation or commit from running, and prevents a client DB from receiving
sync responses needed by remote reads. This has not been tested on deployed
Cloudflare. Keep `transactionSync` for short rollback scopes and atomic commit.

Build on existing snapshot and branch-view machinery rather than a separate
query engine. `Cut` already selects a snapshot plus a transaction's own versions;
`View` composes a branch head over a live or frozen base. Evaluate an incremental
private transaction layer that makes staged versions available to those queries
without exposing them to ordinary reads, history, subscriptions, or sync. Reusing
view composition does not require turning transactions into public branches.
The storage representation is not yet chosen.

Preserve mergeable and serializable semantics, read-your-writes, read-prefix
behavior across awaits, permission checks, conflict validation, and chunk/cache
ownership. Commit through the existing validation and write path in a synchronous
scope; abort or session loss must discard speculative state. Bound memory and
cleanup by the transaction's touched data. Do not replay the whole batch on every
operation or load whole tables; unsupported shapes must fail explicitly.

First prove staged inserts, updates, and deletions with private transactional
reads, ordinary-reader isolation, intervening committed writes, and existing
conflict behavior. Then cover joins, permissions, read prefixes, large values,
and thin-session disconnects through the shared engine. Section 5 remains required
for full DO transaction support, even when the DO is only used as a server.

## 6. Preserve integer precision and SQL value types in the DO bridge — completed

**Problem.** SQLite stores signed 64-bit integers, but Cloudflare passes numeric
results through JavaScript `Number`. Integers outside ±(2^53−1) can be rounded.
Also, numeric JS bindings become SQL REAL, and JavaScript cannot distinguish
INTEGER `1` from REAL `1.0`. REAL already uses the same floating-point format
as JS; TEXT, BLOB, and NULL have no numeric precision problem.

**Agreed behavior.** The DO backend supports integers only between
−9,007,199,254,740,991 and +9,007,199,254,740,991. Reject values outside this
range before binding and when decoding integer results, including SQL aggregates.
Do not round them or limit REAL values to this range. Preserve SQL INTEGER on
writes with `+CAST(? AS INTEGER)`, which avoids adding comparison affinity.
Native/browser integer support and durable storage/sync encodings stay unchanged.

**Use existing type information.** Compiled queries already describe their
outputs, including joins, history and aggregates. Sync and subscriptions can
use the stored-column type catalogue; pending cells have a known physical
encoding. Pass these descriptions to the managed backend so it can decode JS
numbers without wrapping known result columns in SQL. Native SQLite continues
using rusqlite's types and does not build these descriptions. Keep runtime SQL
type handling for remaining raw-cell reads, including heterogeneous defaults
and internal queries that do not yet supply descriptions. Do not guess numeric
types from `Number.isInteger`.

**Current implementation and validation.** The schema-directed paths are in
place, with streamed rows and a bounded per-owner cache for remaining metadata
and runtime projections. Workerd checks cover safe boundaries, rejected unsafe
bindings/results, large REAL values and `1.0`, mixed known/unknown result types,
permissions, aggregates, selected history fields, and replica sync. The private
`do-value-bridge-v2` framing is transient, not a durable encoding.
Native integration validation: 948 passed, 4 ignored. The same-base local
comparison with full wrapping shows no demonstrated speedup (ten-row tick time
4.49 versus 4.58 ms, overlapping ranges); see the README for methodology.

History's `$acceptedAt` now projects the acceptance clock as Unix milliseconds
inside `history.rs`, so filtering and results stay within the safe range.
The full `_clock` still breaks ordering ties within a millisecond. No special
DO conversion is needed; all backends use the same timestamp semantics.
Validated on 2026-11-06 with 950 native integration tests (4 ignored), including
same-millisecond history filters/order, and the workerd value bridge checks.
Additional validation on 2026-11-06: native replicas upload safe-boundary integers
and REAL values (`1.0`, `1.5`, and ±`1e100`) through the DO, and fresh replicas
read them back after a workerd restart with only SQLite retained. Batches with
a valid row followed by an unsupported integer (`i64::MIN`, `i64::MAX`, or either
safe-range boundary plus one) fail without subscriber updates, physical rows,
history residue, or pending change markers. Backend failures currently surface
through the existing protocol as `Protocol` errors containing the range message.
Run `test-values.mjs --sync` as documented in the README.

The artifact-provenance buffer issue is fixed; fresh native/WASM artifacts build
and all 84 TypeScript history tests pass, including timestamp predicates.
Raw-cell paths retain runtime type projections as the supported fallback.
Named bindings and data-modifying RETURNING reads remain explicitly unsupported.
No production deployment or full transaction compatibility is claimed; section 5
remains deferred, section 7 owns durable chunk access, and section 11 owns wider
lifecycle and deployment validation.

## 7. Use one connection with bounded chunk storage — deferred

Separate durability and cache budgeting from file paths. Read chunks and
epochs through the owning DO connection, without cache locks held across SQL
calls or unsafe statement re-entry. Bound cached, pinned, and in-flight memory;
preserve atomic updates of rows, epochs, chunks, and reference counts. Replace
the server catalogue's separate `HostReader` connection too. Keep existing
S3 and Vercel Blob behavior intact through the shared blob-store abstractions.

## 8. Adapt the server core to the DO runtime

Start from the existing `JazzCore` contract, not a broad server rewrite. Map
`appId`, `handle`, `openSocket`, `signLinkChallenge`, and `close` to a DO host;
extract only the shared server logic those operations need. Unlike Vercel's
Linux Sandbox, a DO hosts the engine inside the Workers runtime, so the native
server's operating-system dependencies need adapters.

### 8.1 Database scheduling and threads — in progress

The native core owns its database on a dedicated OS thread and runs chunk I/O
on additional threads; Workers cannot run those threads. Drive the engine from
the DO event loop with explicit storage ownership and ordered ticks. Preserve
serialization, error propagation, and persistence-before-publication behavior.
Call `set_authority()` before admitting sessions.

The first extraction separates native startup/execution (`NativeOwner`) from
session state (`Owner`). The latter accepts an already-open `Db` and exposes an
awaitable advance followed by output draining. Native execution preserves the
existing tick → chunk-I/O scheduling → output order, thread, and panic recovery.
Chunk runners now belong to the native adapter. This is not yet a Workers host:
Tokio session output, clock access, and DO event scheduling still need
adaptation before DO integration.
Validated with `cargo test -p jazz-server --features test`: 294 tests passed,
2 ignored, including direct owner driving without StoreLoop and native socket,
restart, thin-client, and network chunk-store coverage.

Session input now has a cloneable, bounded queue independent of the owner borrow.
Handlers can enqueue ordered frames or disconnect during a suspended tick; a
coalesced request flag retains the need for another pass. The tick exclusively
borrows the owner, and disconnects discard queued output before publication.
A real chunk-I/O suspension test covers arrivals, ordering, disconnects, and
post-tick draining. The host still schedules the initial tick on input; wiring
that scheduling to DO events remains.
Validation after the input split: 296 server tests passed, 2 ignored.

Thin requests now run as one owned async future per session, with FIFO requests
and ingress permits retained until completion or cancellation. The owner polls
pending work on engine passes and explicit I/O wakeups; other sessions and ticks
can progress while a request waits. Completed replies drain independently of a
later pending request. Disconnect drops the future, abandons open transactions,
and schedules upload-lease cleanup through the existing drop path. No thin
handler uses `block_on`; native startup/tick execution still does.
Validated with 297 server tests passed, 2 ignored, including a delayed offloaded
read alongside another session and sync input, ordered completion, and cancellation.
This does not implement the deferred transaction overlay or a DO event adapter.

### 8.2 Catalogue reads and the separate connection

The core opens a separate `HostReader` for catalogue access. Route these reads
through the owning storage connection without unsafe re-entry. This overlaps
with deferred section 7: identify the minimum catalogue work needed for admission
explicitly before implementation; do not silently pull chunk storage into scope.

### 8.3 HTTP and WebSocket hosting

Native standalone startup binds TCP listeners and serves Axum requests. A DO
receives requests and WebSockets through Cloudflare handlers. Adapt requests,
responses, and ordered frames at that boundary; share route/protocol behavior.
Porting the standalone listener is unnecessary for the `JazzCore` contract.

### 8.4 Outbound HTTP and background I/O

Authentication fetches keys through an HTTP client; the S3 adapter also owns a
multithreaded Tokio runtime. Use Worker-compatible HTTP execution and scheduling
where needed. HTTP itself is supported. Preserve credential checks, timeouts,
cancellation, and retry behavior; leave native S3 and Vercel Blob paths intact.

### 8.5 Persistent state and native startup

Native startup creates data directories, locks files, and uses OS-specific
resource monitoring. Keep these in the native host. The DO host uses managed
storage and explicit memory budgets, with no persistent filesystem dependency.
Persist app identity and signing material so reconstruction does not create a
new authority; lifecycle verification belongs to section 9.

### 8.6 Production admission and shared server behavior

The harness accepts a caller-supplied author; it is not production authentication.
Share hello/prelude handling, authenticated session admission, bundle validation,
accounts, and catalogue operations with the native core. Keep `createJazz()`'s
application authentication and publication responsibilities intact. Verify valid
sessions and rejection of invalid credentials/bundles through the normal paths.

## 9. Implement the DO lifecycle and routing

Route each app consistently to its authority object. Reopen persisted identity,
schema, catalogue, and sync state before admission; drive ticks and ordered
WebSocket frames through the host adapter. Handle disconnect, eviction, restart,
and idempotent commit replay. Verify multiple objects sharing an isolate cannot
share storage handles or mutable authority state. Establish reconnect/resubscribe
correctness first; add WebSocket hibernation with explicit session reconstruction
as a separate lifecycle step.

## 10. Connect the provider and assess Workers compatibility

Implement `durableObjects(...)` as a `JazzCoreProvider` returning the existing
`JazzCore` interface: request handling, sockets, stable link-challenge signing,
and close. Preserve socket ordering, authentication, bundle pinning, and the
meaning of close for a shared core. Add no provider-specific branch to
`createJazz()` and keep the deferred interface rename out of scope. The target
API is `createJazz({ store: durableObjects(env.JAZZ) })`: the provider connects to
the DO host from sections 8–9; it does not itself replace that host. Provider
credentials remain private, and local routing markers are not DO credentials.

A supported backend can initially reach the DO core through the remote provider.
Running `createJazz()` itself in a Worker requires a separate dependency audit
and replacements for its native backend-client/runtime dependencies; hosting
the core in a DO does not solve that automatically.

## 11. Validate correctness, then measure deployment behavior

Read the Rust testing guidelines before adding tests. Run native conformance
tests and actual workerd tests for nested rollback, rejected writes, async
transactions, overlay isolation, exact values, large values, reconnect, and
restart. Add CI coverage for both native DO limits and the workerd adapter;
native limits alone cannot validate bridge or lifecycle behavior. Preserve
existing test expectations unless a semantic change is explicitly approved.

After local correctness, validate the harness in Cloudflare for durability,
CPU, total isolate memory, and bundle size. Before performance trials, read
the rejected experiments and search related PRs. Measure statement counts and
consider bounded per-tick caching/batching without bypassing permissions or
sync. Use same-base CodSpeed comparisons for end-to-end performance claims.

## Delivery order

The refreshed harness, SQL storage boundary, callback scopes, unified transaction
implementation, and exact SQL value handling are in place. Sections 5 and 7 are
deferred. Next map the provider contract to the DO host (section 8), identify
its catalogue dependency on section 7, and tackle the runtime adaptations one
at a time. Then complete lifecycle/routing and provider integration. Do not claim
full transaction or large-value support while sections 5 and 7 remain deferred.
Workers-hosted `createJazz()` and hibernation have separate acceptance criteria.

The first correctness milestone is a rejected multi-row write that leaves no
partial rows, history, speculative cache state, or outgoing sync publication,
paired with a successful write that survives object reconstruction.
