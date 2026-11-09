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

Work top-down from the existing Node backend and remote-core contract:

```text
createJazz({ store: durableObjects(options) })
  -> provider provisions/reuses the Jazz Worker and DO namespace
  -> existing remote-core HTTP/WebSocket transport
  -> Worker routes to the app's DO
  -> WASM ServerCore on ctx.storage.sql
```

This follows the Vercel provider: `createJazz()` retains authentication,
catalogue publication, and its existing NAPI backend client. Running
`createJazz()` itself inside a Worker or DO is outside this implementation.
No second SQLite-in-WASM client is needed for this architecture.

Build the production DO core host first and connect a Node `createJazz()` through
`remote(...)` in local workerd. Its readiness must perform real schema/permissions
publication and backend admission. Then verify an authenticated client read/write
and reconstruction from persisted SQLite before adding automatic provisioning.
Move reusable hosting into production modules; retain fixture setup, raw probe
routes, and assertions only in the harness. Do not maintain two server engines.

### 8.1 Database scheduling and threads — sync host integrated

WASM core ownership, async ticks, microtask wakeups, session transport, and
backend-session handshake now live in `crates/jazz-wasm/src/server.rs`, behind
its optional `server` feature. The harness's `FixtureServer` supplies test setup
and delegates to that host. The production adapter serializes catalogue jobs,
socket admission and microtask ticks. Established sockets deliver through
`SessionInput` without borrowing the owner, so a suspended tick cannot block
input it needs. Bounded handshake/frame queues and disconnect cleanup are now
in the production adapter.
Preserve ordering, cancellation, error propagation, and persistence before
publication. Connect background I/O completion to host wakeups (section 8.4)
and validate scheduling across production lifecycle events (section 9).

### 8.3 HTTP and WebSocket hosting — in progress

Native standalone startup binds TCP listeners and serves Axum requests. A DO
receives requests and WebSockets through Cloudflare handlers. Adapt requests,
responses, and ordered frames at that boundary; share route/protocol behavior.
Porting the standalone listener is unnecessary for the `JazzCore` contract.

Prelude parsing, Hello decoding, foreign-version refusals, and bounded frame
batching now live in `jazz-server::runtime::websocket`, compiled for native and
WASM hosts. The native route uses these shared helpers. The harness now has a
`/socket` endpoint for the normal prelude/Hello and batched sync carrier, with
bounded admission queues and a handshake deadline. It verifies a host-configured
backend credential and opens a bundle-pinned user session.

Catalogue request validation, migration lowering, permission validation, artifact
staging, and read response shapes now live in the shared runtime. Native HTTP
handlers use them; `ServerCore::deploy` applies publication through the owner's
connection and supersedes sessions only for new bundles. `WasmServerCore` exposes
catalogue reads and publication, and fixture startup now uses that publication
path. Native deployment tests and an owner restart/idempotency test cover the
extraction. The workerd catalogue suite exercises publication on DO SQLite.

The production adapter in `crates/jazz-wasm/durable-object-server.js` now routes
catalogue requests to the configured app's stable object, verifies the remote
provider's credential plus admin access, and serializes catalogue jobs and ticks.
Bodies are limited to 8 MiB, pending input to 16 MiB, and pending requests to 16.
Accepted operations are retained with `waitUntil`. First database creation and
publication share a short storage transaction; errors discard the new host and
roll back its schema. This transaction performs no network I/O and is not an
application transaction overlay. Recovery opens the persisted catalogue without
republishing or accepting a client-supplied recovery schema.

`test-catalogue.mjs` validates authentication, size limits, rejected first
publication (permissions and SQLite errors), concurrent retries, catalogue reads,
and workerd restart. It also calls the real Node `remote()` provider over HTTP.
The production adapter also serves `POST /backend/admit` and `/ws`. Upgrade
credentials and access override prelude secrets: service links have the same
trusted access as native backends; backend-stated user sessions remain scoped.
Session-only upgrades cannot elevate themselves. Guest proofs and thin sessions
are refused explicitly. Socket frames are limited to 2 MiB, pending frames to
8 MiB per socket and 16 MiB per object (including retained preludes), with 64
sockets and a two-second handshake deadline.

`test-create-jazz.mjs` now exercises Node `createJazz({ store: remote(...) })`
through readiness, globally settled backend writes and reads, live reconnect
while Node stays running across workerd restart, and a fresh backend reading the
persisted data. It also checks stated-user admission and credential/access,
identity and bundle refusals. Automatic provisioning remains section 10.

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

The harness's raw `/session` endpoint still accepts a fixture author. Its new
`/socket` endpoint verifies backend-stated user sessions, sharing credential
comparison, canonical identity checks, and policy-claim conversion with native
hosting. Bundle validation uses the existing core admission path. Local workerd
checks cover credential/identity/bundle refusals, policy-scoped sync, and restart.
Trusted backend/admin links and backend-stated sessions now work through the
production host. Direct guest proofs and account revocation, thin-client
hosting, and shared HTTP account routes remain unconnected; unsupported
socket modes fail explicitly. Keep `createJazz()`'s
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

Implement `durableObjects(options)` as a `JazzCoreProvider` with the same lifecycle
responsibility as `vercel()`. Given Cloudflare account credentials and a stable
resource name, find or deploy a versioned Jazz Worker, ensure its SQLite DO
namespace exists, contact the app's object, and return the existing remote-core
transport. Users should not have to write or manually deploy the DO class.
The precise options shape is not yet a public API.

Keep provisioning credentials separate from credentials used to access the core.
Preserve app identity and the namespace across restarts and upgrades. Coordinate
concurrent opens and check version compatibility before reusing/upgrading a host;
an older backend must not overwrite a newer deployment. Closing a provider releases
its connections without deleting storage or stopping a shared authority.

The resulting core implements the existing `appId`, `handle`, `openSocket`,
`signLinkChallenge`, and `close` contract. Keep provider branches out of
`createJazz()` and the deferred interface rename out of scope. Test provisioning
with an injected Cloudflare API client before deploying to a real account.

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

Sections 5 and 7 remain deferred. The reusable host is in production modules,
and Node `createJazz({ store: remote(...) })` now reaches readiness against it in
workerd. Backend read/write, live reconnect and persisted restart are validated.
Add the `durableObjects()` provisioning provider around this host while keeping
the unsupported account/guest/thin modes explicit (section 8.6). Workers-hosted
`createJazz()` is not a prerequisite. Do not claim full transaction or large-value
support while sections 5 and 7 remain deferred; hibernation remains separate.
