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

## 2. Introduce the SQL storage boundary

Add a small internal interface in `jazz-db` for typed parameters/results,
statement execution, affected-row counts, bounded result consumption, and
transaction scopes. Implement it with rusqlite for existing hosts and DO SQL
for Workers. Keep query compilation, permissions, history, and sync above this
boundary. Use the experiment's SQLite C shim as a reference; avoid making its
partial SQLite emulation the permanent contract. Express backend differences
as storage capabilities rather than scattered provider checks.

## 3. Make synchronous transactions callback-owned — native scopes completed

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

### DO callback scopes — pending

Implement the same scope contract through DO `transactionSync` and validate
rollback and publication behavior in workerd. The persistent overlay and Rust
transaction that spans `await` retain their existing lifetimes; those are
covered by sections 5 and 4 respectively. Section 3 is not yet complete for DOs.

## 4. Support transactions across await explicitly

First prove a bridge where DO `storage.transaction(async () => ...)` owns and
awaits the complete Rust transaction future. Pass an explicit transaction
context through Jazz operations, translating errors into callback rejection.
Prevent unrelated requests and ticks from joining Cloudflare's implicit active
transaction. Test nested scopes, cancellation, and awaits that need engine
progress so admission control cannot deadlock. DO supports async transactions;
write staging and failing on the first suspended poll are alternatives to
evaluate, not approved defaults.

## 5. Adapt staged transactions and overlays

The current overlay retains a SQLite savepoint between calls, which cannot
escape a `transactionSync` callback. Design a bounded replacement for this
cached speculative view while preserving read-your-writes, isolation from
ordinary readers, read-prefix behavior, and serializable snapshot/conflict
validation. Evaluate an incremental overlay against the existing query and
policy machinery before choosing its representation. Do not replay the whole
batch for every operation or load whole tables. Buffering writes alone does
not establish isolation; any initially unsupported operation must fail explicitly.

## 6. Preserve exact SQL values through JavaScript

Current app columns have no SQLite affinity. Transport wide integers as
decimal text, but explicitly cast bound integer expressions to INTEGER so
storage and comparisons remain numeric. Project exact integer digits on reads
and reconstruct typed cells before decoding. Cover policies, history, ordering,
aggregates, and internal queries as well as app reads. Preserve the existing
rejection of `U64` above `i64::MAX`, REAL values, NULL, and binary values.
Current large values use INTEGER epoch markers; the experiment's TEXT-marker
workaround is obsolete. Preserve durable bytes; any necessary encoding change
requires a named version, byte fixtures, and a compatibility write-up.

## 7. Use one connection with bounded chunk storage

Separate durability and cache budgeting from file paths. Read chunks and
epochs through the owning DO connection, without cache locks held across SQL
calls or unsafe statement re-entry. Bound cached, pinned, and in-flight memory;
preserve atomic updates of rows, epochs, chunks, and reference counts. Replace
the server catalogue's separate `HostReader` connection too. Keep existing
S3 and Vercel Blob behavior intact through the shared blob-store abstractions.

## 8. Extract a host-neutral server core

Share protocol hello/prelude handling, authenticated session admission,
accounts, and catalogue operations between native and DO hosts. Move thread
management, network clients, and transport scheduling into host adapters.
Provide a single-threaded WASM host with per-object storage ownership and call
`set_authority()` before serving sessions. Use existing authentication and
permission checks; the experiment's caller-supplied author URL is only a harness.

## 9. Implement the DO lifecycle and routing

Route each app consistently to its authority object. Reopen persisted identity,
schema, catalogue, and sync state before admission; drive ticks and ordered
WebSocket frames through the host adapter. Handle disconnect, eviction, restart,
and idempotent commit replay. Verify multiple objects sharing an isolate cannot
share storage handles or mutable authority state. Establish reconnect/resubscribe
correctness first; add WebSocket hibernation with explicit session reconstruction
as a separate lifecycle step.

## 10. Connect the provider and assess Workers compatibility

Implement `durableObject(...)` as a `JazzCoreProvider` returning the existing
`JazzCore` interface: request handling, sockets, stable link-challenge signing,
and close. Preserve socket ordering, authentication, bundle pinning, and the
meaning of close for a shared core. Add no provider-specific branch to
`createJazz()` and keep the deferred interface rename out of scope.

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

Start with the refreshed harness, native scope semantics, and the async bridge
proof. Then implement typed DO storage and single-connection chunk access,
followed by the shared core, lifecycle, and provider. Resolve overlay support
before claiming full transaction compatibility. Workers-hosted `createJazz()`
and hibernation have separate acceptance criteria.

The first correctness milestone is a rejected multi-row write that leaves no
partial rows, history, speculative cache state, or outgoing sync publication,
paired with a successful write that survives object reconstruction.
