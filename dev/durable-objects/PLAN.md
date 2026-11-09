# Durable Objects implementation plan

Run Jazz's authority on `ctx.storage.sql`, preserving its data model,
permissions and sync semantics. This plan lists remaining work; completed
implementation, validation and known limits are in the [README](README.md).
Section numbers are retained for existing references.

## 5. Replace the persistent transaction overlay — deferred

Client DBs, thin-client servers and application transactions on an authority
share the staged transaction engine. Its private queryable view currently holds
a SQLite savepoint between operations. Replace that representation without
holding a SQL transaction open across application awaits or protocol messages.
Completed replica sync batches already use short synchronous scopes.

An async DO transaction does not solve this: the local workerd probe blocks
incoming handlers until it exits, preventing the next thin-client operation or
a remote-read response. Keep `transactionSync` for atomic commit and short scopes.

Evaluate an incremental private layer using existing `Cut` snapshot and `View`
branch composition. Preserve read-your-writes, ordinary-reader isolation,
mergeable/serializable conflict validation, read prefixes, permissions and
chunk ownership. Bound work and cleanup by touched data; do not replay the whole
batch per operation or load entire tables. The representation is not yet chosen.

First prove inserts, updates and deletes with intervening committed writes;
then cover joins, policies, large values and session loss. This remains required
for full transaction support, including thin-client transactions on a DO server.

## 7. Durable chunk storage on one connection — deferred

Separate durability and cache budgets from file paths. Read chunks and epochs
through the owning DO connection, without locks across SQL calls or unsafe
statement re-entry. Bound cached, pinned and in-flight memory; preserve atomic
updates of rows, epochs, chunks and reference counts. Native hosts may retain
separate readers. Keep S3 and Vercel Blob behavior intact through shared
blob-store abstractions; include Worker-compatible blob I/O where needed.

## 8.6 Thin clients for Node and Worker backends — in progress

Node `createJazz()` now uses one shared thin runtime for its backend database;
there is no NAPI in-memory replica behind `jazz.db`. Native authorities still
use NAPI. The production DO host serves ordinary thin reads, writes and
subscriptions through the shared server engine. Backend requests carry an
explicit authenticated permission/attribution context; public clients cannot
use it. See the README for the versioned wire addition and validation.

Backend application transactions fail explicitly for now. Existing ordinary
thin-client transactions on native authorities retain their implementation;
managed storage refuses transaction begin until section 5 is implemented.
DO host input currently accepts whole thin requests only; split large requests
are refused explicitly. Durable large-value storage remains section 7.

Reuse this same thin runtime in a Worker-compatible `createJazz()` entry point.
Separate native imports and Node server attachment from shared publication,
authentication and routes; adapt authenticated WebSocket upgrades and
request-scoped connection lifetimes. Provision the DO host through deployment
tooling and connect through an existing namespace binding inside the Worker.
Validate concurrent requests without sharing request-owned I/O. The Worker
entry point is not implemented by switching the backend client alone.

## 9. Additional lifecycle work — deferred

Retain the existing restart, lost-upload/acknowledgement, deployed upgrade and
local SIGKILL tests as regression coverage. Stop expanding crash scenarios for
now. Their evidence and limits are recorded in the README; timed kills do not
prove interruption inside SQLite commit.

Remaining coverage includes deployed abrupt eviction, deterministic interruption
within commit and broader deployment races. WebSocket hibernation is a separate
feature requiring explicit session reconstruction. None is claimed complete.

## 11. Maintain regression coverage and assess deployment limits

Use `node dev/durable-objects/test.mjs all` for local integration coverage;
Cloudflare deployment tests remain explicit and opt-in. Add the local suites and
native DO-limit checks to CI. Extend existing tests as sections 5 and 7 land,
rather than growing a separate server implementation. Read the Rust testing
guidelines before adding Rust tests; preserve existing expectations unless a
semantic change is approved.

Measure deployed CPU, total isolate memory and bundle size under representative
load. Before performance trials, read rejected experiments and search related
PRs. Consider bounded batching/caching without bypassing permissions or sync;
use same-base CodSpeed comparisons for native end-to-end claims.

## Delivery order

Node backend thin clients and ordinary DO thin hosting are implemented. Next
reuse that client in Worker-hosted `createJazz()` (section 8.6).
Transaction overlays (section 5) and durable chunks (section 7) remain deferred.
Keep current integration tests as regression coverage; further lifecycle
expansion is deferred. Do not claim full DO transaction or large-value support
until those implementations are complete.
