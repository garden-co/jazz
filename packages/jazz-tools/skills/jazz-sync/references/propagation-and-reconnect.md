# Propagation, settlement, and reconnect

## Read controls

Query and subscription options answer separate questions:

- `tier` controls the minimum source for the first result: `local`, `edge`, or `global`.
- `localUpdates: "immediate"` lets local writes appear while that result is pending.
- `localUpdates: "deferred"` holds local writes behind the requested initial result.
- `propagation: "full"` allows the read to request and receive upstream data.
- `propagation: "local-only"` confines the read to local state.

High-level `Db` reads currently default local updates to `deferred`; inspect the installed types and
defaults before depending on that detail.

When a UI must distinguish optimistic from confirmed state, use a deliberate product model. One
option is an immediate local subscription for the rendered value plus an edge/deferred subscription
or tracked write handles for the last confirmed value. Tracking write handles is more reliable when
two equal values could have different settlement states.

An `edge` result means the nearest sync server, not necessarily the global core. A subscription tier
gates its first delivery; later updates arrive as they propagate.

## Write settlement and rejection

```ts
const write = db.update(app.documents, id, patch);

void write.wait({ tier: "edge" }).then(
  () => markConfirmed(),
  (error) => markRejected(error),
);
```

The local mutation has already happened. While offline, edge/global waits remain pending until
reconnection and propagation. If authority rejects the write, Jazz rebuilds visible state without
the rejected history entry, so previously rendered optimistic data may disappear.

Await the write when the caller owns its failure. Register `db.onMutationError(...)` for un-awaited
and restart-surviving rejection events. Dispose the listener with its owner.

## Disconnect and reconnect

Use `db.disconnect()` and `db.reconnect()` only on the owning unscoped database. Scoped backend
handles do not support this lifecycle.

Verify these product behaviors separately:

1. local reads and writes continue while disconnected;
2. peers cannot see the offline write yet;
3. edge/global waits remain pending;
4. reconnect uploads offline writes;
5. reconnect retrieves remote writes missed while disconnected;
6. persistent clients recover pending settlement after restart when supported.

Do not promise background mobile sync unless the native platform lifecycle explicitly implements
it.

## Replay limitation

Inspect the installed transport and open status-quo issues before claiming exactly-once delivery. In
versions without outbox deduplication, a server may accept a payload but lose the acknowledgement;
the client can replay that payload after reconnect. Idempotent snapshot writes may converge anyway,
while non-idempotent counter intent can be applied twice.

Use stable operation IDs and an idempotent operation-row model when duplicate intent is unacceptable.

## Deterministic conflict tests

- Use two independent clients and a real local server.
- Confirm a baseline row at edge and make both clients observe it before conflict work begins.
- Disconnect both clients or use the server's message-blocking test support so the second writer
  cannot receive the first update before writing.
- Run both propagation orders and assert the same converged result.
- Use edge waits and retrying assertions, never fixed sleeps.
- Connect a fresh third client as an authority-visible oracle.
- Give persistent clients distinct `dbName` values and storage keys.
- Add rejection, restart, duplicate-intent, and mixed-schema cases when they affect the feature.
