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

### 8.6 Thin-client hosting — deferred

Account routes, guest proof admission and active-session revocation are implemented
and validated in local workerd; see the README. Add thin-client hosting once the
shared transaction overlay is supported (section 5). Until then, unsupported
socket modes must fail explicitly. Keep fixture identities and probe routes
confined to the development harness.

## 9. Complete DO lifecycle validation — in progress

The packaged-host lifecycle test now covers a lost upload and a committed write
whose acknowledgement is dropped. An independent client observes authority
state; after workerd reconstruction, the writer settles and a fresh client sees
the same accepted history without duplicates. Deployed testing exposed the
provider's readiness timeout being shorter than Cloudflare's deferred-update
window; the wait is now extended for an authenticated app running older code.
Live validation of that fix was blocked by upload connection failures. See the
README for commands and validation limits.

Extend existing restart/reconnect coverage to deployed eviction, interrupted
commits and idempotent replay. Verify reconstruction retains app identity,
signing material, catalogue and sync state before admission, and that multiple
objects in one isolate cannot share storage handles or mutable authority state.
Preserve frame ordering, cancellation, error propagation and persistence before
publication across lifecycle events. Validate the existing microtask wakeups and
request/tick retention without blocking input needed by suspended work.

Add WebSocket hibernation with explicit session reconstruction as a separate
lifecycle step. Broaden concurrent deployment/upgrade race coverage beyond the
validated concurrent opens and sequential synthetic upgrade.

## 11. Validate correctness, then measure deployment behavior

Read the Rust testing guidelines before adding tests. Extend native conformance
and workerd coverage as the remaining account, overlay, chunk-storage and
lifecycle features land. Add CI coverage for both native DO limits and the workerd
adapter; native limits alone cannot validate bridge or lifecycle behavior.
Preserve existing test expectations unless a semantic change is explicitly
approved. Completed checks and their limits are recorded in the README.

Measure deployed CPU, total isolate memory and bundle size under representative
load. Before performance trials, read the rejected experiments and search related
PRs. Measure statement counts and consider bounded per-tick caching/batching
without bypassing permissions or sync. Use same-base CodSpeed comparisons for
end-to-end performance claims.

## Delivery order

The outbound I/O audit (section 8.4) found no additional HTTP adapter needed for
the current host; see the README. Next broaden lifecycle validation (section 9). Thin-client hosting (section 8.6) remains
dependent on the deferred transaction overlay.
Sections 5 and 7 remain deferred; resolve them before claiming full transaction
or large-value support. Workers-hosted `createJazz()` remains outside this
implementation, and hibernation has separate acceptance criteria.
