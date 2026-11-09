# Durable Objects implementation plan

Run Jazz's authority on `ctx.storage.sql`, preserving its data model,
permissions, transaction guarantees, and sync protocol. Use the
[feasibility spike](README.md) and [DO experiment #4579](https://github.com/garden-co/jazz/pull/4579)
as background. This plan lists remaining work; completed implementation and
validation are documented in the [README](README.md). Section numbers are
retained so existing references stay meaningful.

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

## 7. Use one connection with bounded chunk storage — deferred

Separate durability and cache budgeting from file paths. Read chunks and
epochs through the owning DO connection, without cache locks held across SQL
calls or unsafe statement re-entry. Bound cached, pinned, and in-flight memory;
preserve atomic updates of rows, epochs, chunks, and reference counts. Native
hosts can retain separate readers; DO reads use the owning connection. Keep
existing S3 and Vercel Blob behavior intact through the shared blob-store
abstractions.

## 8. Adapt the server core to the DO runtime

Start from the existing `JazzCore` contract, not a broad server rewrite. Map
`appId`, `handle`, `openSocket`, `signLinkChallenge`, and `close` to a DO host;
extract only the shared server logic those operations need. Unlike Vercel's
Linux Sandbox, a DO hosts the engine inside the Workers runtime, so the native
server's operating-system dependencies need adapters.

### 8.1 Database scheduling and threads — production integration remaining

Integrate the harness's serialized `ServerCore` ticks, bounded input queues,
async request handling, and microtask wakeups into the production DO host.
Preserve ordering, cancellation, error propagation, and persistence before
publication. Connect background I/O completion to host wakeups (section 8.4)
and validate scheduling across production lifecycle events (section 9).

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

Sections 5 and 7 remain deferred. Next implement production HTTP/WebSocket
hosting (8.3), then the remaining runtime adaptations in section 8, lifecycle
and routing, and provider integration. Do not claim full transaction or
large-value support while sections 5 and 7 remain deferred. Workers-hosted
`createJazz()` and WebSocket hibernation have separate acceptance criteria.
