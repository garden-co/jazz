# Stale client cache when objects leave query scope

## What

When a row is deleted (or otherwise exits a query's result set) while a client has no active server-side subscription for that query, the client's local object manager retains stale data indefinitely. Subsequent one-shot `query()` calls with `tier: "edge"` return the stale row because the server never sends the deletion to the client — it considers the object "out of scope" and skips it.

This is the root cause of the flaky `"syncs queries and mutations between two TS clients via cloud-server"` integration test, which fails ~1/9 runs.

## Where

The bug spans the sync protocol boundary between server-side scope management and client-side cache invalidation:

- **Server scope diffing** — `SyncManager::set_client_query_scope()` at `crates/jazz-tools/src/sync_manager/mod.rs:333-375`. Only computes `newly_visible` (objects entering scope). Objects leaving scope are silently dropped — no notification sent.
- **Server forwarding gate** — `forward_update_to_clients_except()` at `crates/jazz-tools/src/sync_manager/forwarding.rs:47-61`. Line 58: `client.is_in_scope(object_id, &branch_name)` — only forwards `ObjectUpdated` to clients whose query scopes include the object. If a client has no active subscription at the moment of the delete, the object is not in scope, and the delete is never forwarded.
- **Client-side graph settlement** — `QueryManager::process()` at `crates/jazz-tools/src/query_manager/manager.rs:715-747`. The `row_loader` (line 721) reads from the local object manager, which still has the stale pre-delete version. `is_soft_deleted` returns false because the client never received the delete commit.
- **Client-side index scan** — `IndexScanNode::scan()` at `crates/jazz-tools/src/query_manager/graph_nodes/index_scan.rs:71-102`. Scans the client's local storage index, which still contains the stale row's entry.

## Steps to reproduce

The test `"syncs queries and mutations between two TS clients via cloud-server"` at `packages/jazz-tools/src/runtime/cloud-server.integration.test.ts:1253` reproduces this ~11% of the time. The race window is during the delete step (line 1310-1311).

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
- The failure is specifically on the DELETE step (line 1311) — create and update propagate fine

### Why create and update work but delete doesn't

For create/update, the server sends `ObjectUpdated` to client B (either via reactive forwarding when B has an active subscription, or via `queue_initial_sync_to_client` when B creates a new subscription and the object enters scope). The client receives fresh data and its cache is correct.

For delete, the object LEAVES scope. `set_client_query_scope` only handles `newly_visible` (entering scope), not objects exiting scope. And `forward_update_to_clients_except` only forwards to clients with the object in scope — which excludes client B during the 150ms polling gap.

### Possible fix directions

1. **Server sends removed-scope objects** — In `set_client_query_scope`, compute `old_scope.difference(&new_scope)` and send `ObjectUpdated` (with current state including delete commit) for objects that left scope. Minimal server change, client automatically picks up the delete.

2. **Include result IDs in QuerySettled** — Extend `QuerySettled` to carry the set of object IDs in the result. Client prunes its graph to only include those IDs. Protocol change.

3. **Client-side: don't use cached data for fresh subscriptions** — When a one-shot query settles with a server tier, only include objects that were explicitly received via `ObjectUpdated` during this subscription's lifetime. Client-only change but harder to implement correctly.

4. **Use persistent subscriptions in waitForRows** — Change the test helper to use `subscribe()` instead of polling `query()`. This keeps client B's subscription alive, so the reactive path handles the delete. But this only fixes the test, not the underlying protocol gap.

Direction 1 seems simplest and most correct — it's a ~10-line change in `set_client_query_scope` and fixes the protocol gap for all consumers.

### Related: `sendSyncMessage` silent drop

`client.ts:1437-1439` silently discards outbox payloads when `getServerUrl()` returns null (only happens during `stop()`, not during reconnect). This is a separate issue — not the cause here since `getServerUrl()` returns the URL even during reconnect. But it means payloads generated after `stop()` are lost without error.
