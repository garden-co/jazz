# Query/Sync Integration — Status Quo

This is the seam where "live relational query" turns into "scoped sync stream".

Neither side can do the whole job alone:

- `QueryManager` knows how to evaluate queries
- `SyncManager` knows who is connected and what can be sent where

The integration layer joins those two facts together.

## The Core Story

When a client says:

> "Subscribe me to unfinished todos"

the runtime does four things:

1. record that desired query subscription
2. compile and settle the query using the client's schema/session context
3. send the row batch entries needed for the initial result
4. keep the subscription live so later row changes can add, update, or remove rows

That is the entire shape of query-scoped sync in Jazz.

On enforcing runtimes, that server-side graph is compiled against the active
authorization schema. Missing explicit read policies filter rows out, and the
same deny-by-default behavior shapes the current sync scope. Runtimes without a
loaded policy bundle may stay permissive locally, but upstream query scope is
still computed by the enforcing side.

## Initial Subscription Flow

```text
client sends QuerySubscription
  -> SyncManager records pending subscription
  -> QueryManager compiles a server-side graph
  -> graph settles against current visible rows
  -> matching rows are sent as RowBatchNeeded
  -> matching rows' BatchFate records are sent after row entries
  -> QuerySettled marks the first snapshot as ready at a given tier
```

The point of `RowBatchNeeded` is straightforward: a newly subscribed client may need rows that already existed before the subscription was created.
The point of `BatchFate` is separate: a newly subscribed client may need the whole-batch
durability or rejection outcome for rows it already received. Batch fate does not carry the query
result set; that is `QuerySettled.scope`.

## Live Update Flow

After the initial fill, the server-side query graph stays alive.

When a row changes:

1. row history / visible state updates locally
2. the subscription graph is marked dirty
3. the graph settles incrementally
4. the runtime figures out which rows entered, changed inside, or left the result
5. sync payloads are sent only for the affected rows
6. batch fate carries the durability/rejection outcome that lets receivers decide when those known
   rows satisfy tiered delivery

That is how Jazz avoids treating every remote subscription update as a full snapshot.

## Why QuerySettled Exists

`QuerySettled` answers a different question from row delivery.

Row delivery says:

> "Here are the rows you need."

`QuerySettled` says:

> "The query result has now settled at tier T."

That distinction matters because the runtime may have already received some rows but still be waiting for the requested durability tier before it should publish the first callback to application code.

In the browser stack, for example:

```text
main thread subscribes
  -> worker settles locally
  -> worker may also forward upstream
  -> worker sends row batch entries + QuerySettled(local)
  -> main thread publishes once requested tier is satisfied
```

The implementation also threads per-connection sequence information through these signals so a `QuerySettled` message is not treated as valid before earlier row payloads on the same stream have been applied.

Because batch fate is the active durability signal, server-side subscriptions may emit a new
`QuerySettled` when their settled graph was dirtied even if the object-id scope did not change. That
lets downstream runtimes release updated rows whose `batch_id` changed but whose query membership
stayed the same.

## Reconnect and Replay

Subscriptions are treated as desired state, not as one-off network events.

So on reconnect:

- active forwarded subscriptions are replayed upstream
- the upstream side rebuilds scope
- any rows the client still needs are re-sent

This is why a reconnect can restore live query behavior without the application code reissuing every subscription manually.

## The Boundary Between Managers

The division of labor is deliberate:

### SyncManager owns

- connection state
- query registration state
- role/session tracking
- inbox/outbox queues
- relay bookkeeping

### QueryManager owns

- query compilation
- row filtering/sorting/policy evaluation
- current scope computation
- incremental resettlement

That keeps the transport side from growing a shadow SQL engine.

## Key Files

| File                                                    | Purpose                                     |
| ------------------------------------------------------- | ------------------------------------------- |
| `crates/jazz-tools/src/query_manager/server_queries.rs` | Server-side query subscription execution    |
| `crates/jazz-tools/src/query_manager/subscriptions.rs`  | Local/forwarded subscription lifecycle      |
| `crates/jazz-tools/src/sync_manager/mod.rs`             | Query registration and relay state          |
| `crates/jazz-tools/src/sync_manager/inbox.rs`           | Inbound query/sync payload handling         |
| `crates/jazz-tools/src/runtime_core/ticks.rs`           | Settled-signal release during runtime ticks |
