# Stale client cache when objects leave query scope

## What

When a row is deleted (or otherwise exits a query's result set) while a client has no active server-side subscription for that query, the client's local object manager retains stale data indefinitely. Subsequent one-shot `query()` calls with `tier: "edge"` return the stale row because the server never sends the deletion to the client — it considers the object "out of scope" and skips it.

This is the root cause of the flaky `"syncs queries and mutations between two TS clients via cloud-server"` integration test, which fails ~1/9 runs.

## Where

The bug spans the sync protocol boundary between server-side scope management and client-side cache invalidation:

- **Server scope diffing** — `SyncManager::set_client_query_scope()` at `crates/jazz-tools/src/sync_manager/mod.rs:333-375`. Only computes `newly_visible` (objects entering scope). Objects leaving scope are silently dropped — no notification sent.
- **Server forwarding gate** — `forward_update_to_clients_except()` at `crates/jazz-tools/src/sync_manager/forwarding.rs:47-61`. Line 58: `client.is_in_scope(object_id, &branch_name)` — only forwards `ObjectUpdated` to clients whose query scopes include the object. If a client has no active subscription at the moment of the delete, the object is not in scope, and the delete is never forwarded.
- **Client-side graph settlement** — `QueryManager::process()` at `crates/jazz-tools/src/query_manager/manager.rs` (function starts at line 651, row_loader at lines 721-745). The `row_loader` (line 722) reads from the local object manager via `om.get_or_load()`, which still has the stale pre-delete version. `is_soft_deleted` (line 736) returns false because the client never received the delete commit. The delivery gate (lines 757-770) holds results until `tier_satisfied`, but once `QuerySettled` arrives, it delivers whatever the local graph settled to — with no comparison against server-side results.
- **Client-side index scan** — `IndexScanNode::scan()` at `crates/jazz-tools/src/query_manager/graph_nodes/index_scan.rs:71-102`. Scans the client's local storage index, which still contains the stale row's entry.

## Steps to reproduce

The test `"syncs queries and mutations between two TS clients via cloud-server"` at `packages/jazz-tools/src/runtime/cloud-server.integration.test.ts:1247` (currently `it.skip`) reproduces this ~11% of the time. The race window is during the delete step (lines 1304-1305).

Deterministic reproduction requires this exact sequence:

```
 clientA                    server                    clientB
    |                         |                         |
    |--- createDurable ------>|--- ObjectUpdated ------->|  (row enters B's cache)
    |                         |--- QuerySettled -------->|  (B's query() resolves)
    |                         |                         |
    |                         |  B's subscription destroyed (one-shot query done)
    |                         |  B sleeps 150ms before next poll
    |                         |                         |
    |--- deleteDurable ------>|                         |
    |                         |  forward_update_to_clients_except:
    |                         |    B.is_in_scope(row) = false  (no active subscription)
    |                         |    => delete NOT forwarded to B
    |<-- PersistenceAck ------|                         |
    |                         |                         |
    |                         |  B wakes, sends QuerySubscription
    |                         |  server settles: row deleted => scope = empty
    |                         |  set_client_query_scope: newly_visible = empty
    |                         |    => no ObjectUpdated sent
    |                         |--- QuerySettled -------->|
    |                         |                         |
    |                         |  B settles client-side graph:
    |                         |    IndexScan finds row in local index
    |                         |    row_loader returns stale cached data
    |                         |    is_soft_deleted = false (never got delete commit)
    |                         |    => query returns stale row
```

This repeats every 150ms for the full 20s timeout. Every `query()` call creates a fresh subscription, but the server never sends the delete because the deleted row is never "in scope."

## Expected

`waitForRows(clientB, queryAllTodos, (rows) => !rows.some(...))` should resolve within a few hundred milliseconds after client A's `deleteDurable()` completes.

## Actual

Client B's `query()` returns `[{"id":"...","values":[{"type":"Text","value":"shared-item"},{"type":"Boolean","value":true}]}]` for the full 20s timeout, then the test fails with:

```
Error: timed out waiting for predicate; lastRows=[...], lastError=none
```

## Priority

high

## Notes

### Key evidence from CI logs

Failing runs on `fix/sync-reconnect-on-transport-failure`:

- https://github.com/garden-co/jazz2/actions/runs/23639547938 (most recent, with debug logging)
- https://github.com/garden-co/jazz2/actions/runs/23637118294
- https://github.com/garden-co/jazz2/actions/runs/23624850103

- Server processes ALL sync POSTs in 1-5ms, all 200 OK — zero failures
- No "Sync POST timeout" errors anywhere in the run
- No "events stream lagged" warnings (broadcast channel capacity 256 is never hit)
- Client B's SSE stream connects once (`connection_id=2`) and never reconnects
- Client A's `deleteDurable()` resolves (server acknowledged via `PersistenceAck`)
- Both clients keep sending sync POSTs for the full 60s test duration
- The failure is specifically on the DELETE step (line 1305) — create and update propagate fine

### Why create and update work but delete doesn't

For create/update, the server sends `ObjectUpdated` to client B (either via reactive forwarding when B has an active subscription, or via `queue_initial_sync_to_client` when B creates a new subscription and the object enters scope). The client receives fresh data and its cache is correct.

For delete, the object LEAVES scope. `set_client_query_scope` only handles `newly_visible` (entering scope), not objects exiting scope. And `forward_update_to_clients_except` only forwards to clients with the object in scope — which excludes client B during the 150ms polling gap.

### Possible fix directions

1. **Include result IDs in QuerySettled** — Extend `QuerySettled` (currently just `query_id + tier + through_seq`, defined at types.rs:257-263) to carry the set of object IDs in the server's result. Client compares its local graph settlement against the server's result set and prunes stale rows not present in the server's list. Protocol change, but directly addresses the root cause: the client has no way to know which objects the server considers valid.

2. **~~Server sends removed-scope objects~~** — ~~In `set_client_query_scope`, compute `old_scope.difference(&new_scope)` and send `ObjectUpdated` for objects that left scope.~~

   **Does not fix this bug.** The one-shot polling pattern destroys the subscription (and its scope) via `drop_client_query_subscription()` (mod.rs:380-391) BEFORE the delete happens. When the next poll creates a new subscription, `old_scope` is already empty — `old_scope.difference(new_scope)` produces nothing. Applying the diff at `drop_client_query_subscription` also doesn't help: at that point the row hasn't been deleted yet, so sending its current state would just re-send the non-deleted version.

   Direction 2 would only help persistent subscriptions whose scope shrinks due to a data or policy change while the subscription is alive. But persistent subscriptions already handle deletes correctly via the forwarding gate (`forward_update_to_clients_except` at forwarding.rs:58 — `is_in_scope` returns true because the subscription is still active).

Direction 1 is the most correct fix. The fundamental problem is that `QuerySettled` tells the client "your query has settled" without telling it WHAT the result should be. The client then trusts its local cache, which may contain objects the server no longer considers in scope. Adding result IDs to `QuerySettled` closes this information gap cleanly.

### Why the client can't self-correct

The client runs two independent settlement paths that never reconcile:

- **Server-side settlement** (`server_queries.rs:541`): correctly produces an empty result set and computes `scope = empty`. Calls `set_client_query_scope()` with the empty scope. Emits `QuerySettled`.
- **Client-side settlement** (`manager.rs:747`): `subscription.graph.settle(storage_ref, row_loader)` runs against the **local** storage index (`IndexScanNode::scan` at index_scan.rs:71-102) and local object manager (`om.get_or_load` at manager.rs:722). Finds the stale row. `is_soft_deleted` check (line 736) passes because the delete commit was never received.

The delivery gate (`manager.rs:757-770`) waits for `tier_satisfied` (i.e., `QuerySettled` received), then delivers whatever the local graph settled to. There is no mechanism to compare the local result against the server's result. The client trusts its local state unconditionally once the tier is satisfied.

This means even an infinite number of polls will never converge — each poll creates a fresh subscription, the server correctly says "empty", but that information (the emptiness of the result) is never communicated to the client.

### Why persistent subscriptions don't have this bug

With `subscribe()`, client B keeps an active subscription. The row stays in B's scope (`is_in_scope` returns true at forwarding.rs:58). When A deletes, `forward_update_to_clients_except` forwards the delete commit to B. B's local object manager receives the delete, `is_soft_deleted` returns true, and the row is filtered out on the next settlement. The reactive forwarding path works correctly because the subscription is alive when the delete happens.

### Related: `sendSyncMessage` silent drop

`client.ts:1437-1439` silently discards outbox payloads when `getServerUrl()` returns null (only happens during `stop()`, not during reconnect). This is a separate issue — not the cause here since `getServerUrl()` returns the URL even during reconnect. But it means payloads generated after `stop()` are lost without error.
