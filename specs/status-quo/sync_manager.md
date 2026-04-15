# Sync Manager — Status Quo

The Sync Manager is the runtime's traffic controller.

It does not execute SQL and it does not own storage. Its job is to answer questions like:

- which peers are connected?
- what role does each peer have?
- which queries has each peer subscribed to?
- which row-batch or catalogue payloads still need to be sent?
- when can we tell a downstream runtime that a query has settled at a given durability tier?

## The Core Asymmetry

Sync is intentionally different in the two directions:

### Upward, toward trusted servers

Jazz forwards row batch entries, explicit transactional seals, replayable batch fate, and catalogue updates so the server can build the same relational view and answer forwarded queries.

### Downward, toward clients

Jazz sends only the data that matches the client's active query subscriptions.

That is the heart of the current design:

- servers need enough data to do their job
- clients only receive the rows their active subscriptions are entitled to see

## Roles

Each client connection has a role:

- `User`
- `Admin`
- `Peer`

Those roles decide how incoming writes are handled.

### User

User writes may require a session and can be queued for permission evaluation
before they are applied.

Those pending checks carry the client session plus row metadata/content so
`QueryManager` can evaluate them against the active authorization schema. On an
enforcing runtime, missing explicit policies are rejected instead of treated as
implicit grants. If the relevant schema or permissions head is not ready yet,
the check can remain pending and be retried after catalogue state catches up.

### Admin

Admin writes can take the direct server-side path.

### Peer

Peer is used for trusted runtime-to-runtime links such as browser main-thread to worker or server-to-server communication.

## What Moves Over Sync

The sync payloads now speak in row-history and query terms:

- `CatalogueEntryUpdated`
- `RowBatchCreated`
- `RowBatchNeeded`
- `RowBatchStateChanged`
- `SealBatch`
- `BatchSettlement`
- `BatchSettlementNeeded`
- `QuerySubscription`
- `QueryUnsubscription`
- `QueryScopeSnapshot`
- `QuerySettled`
- `SchemaWarning`
- `Error`

That payload set matches the table-first runtime model:

- new row batch entries travel as row batch entries
- initial query fill can explicitly ask for needed row batch entries
- transactional batches are explicitly sealed upstream
- replayable whole-batch fate travels separately from concrete row entries
- row-state and durability progression travel as row-state changes
- schemas and lenses travel as catalogue entries

## A Typical Flow

### Direct local write

1. A runtime appends a new row batch entry locally.
2. Storage updates the flat visible row and indices.
3. Query subscriptions settle locally.
4. The Sync Manager queues `RowBatchCreated` (and later state changes if needed) for peers and servers.
5. Replayable durability eventually converges through `BatchSettlement::DurableDirect`.

### Transactional write

1. A runtime upserts staging row batch entries locally.
2. Ordinary readers ignore those `StagingPending` rows.
3. The writer explicitly seals the batch, sending `SealBatch` upstream.
4. The authority decides replayable batch fate.
5. Accepted output becomes visible and is replayable as `BatchSettlement::AcceptedTransaction`.
6. Rejection or missing authority truth is replayable as `Rejected` or `Missing`.

### New query subscription

1. A client sends `QuerySubscription`.
2. The Sync Manager records that desired state.
3. The Query Manager compiles and settles the server-side query.
4. Matching rows are sent down as `RowBatchNeeded`.
5. A `QuerySettled` signal tells the downstream runtime when the first snapshot is safe to deliver for a requested durability tier.

### Later visibility/state change

When a row already known to a peer changes durability or state, the runtime can send `RowBatchStateChanged` without pretending the row is brand new.

The row-level identity for those changes is `RowBatchKey { row_id, branch_name, batch_id }`.

## Query-Scoped Delivery

The Sync Manager never tries to be a second query engine.

Instead it tracks:

- which queries each client cares about
- which clients originated which forwarded queries
- which query-settled signals still need to be relayed

The actual relational work stays inside `QueryManager`. That separation is what lets sync remain a state machine rather than a parallel SQL implementation.

## QuerySettled

`QuerySettled` is the runtime's read-delivery signal.

It means:

> "For this query id, this tier now has a settled answer."

That matters because the subscription engine can accumulate row changes before the requested durability tier is ready to publish them as the first callback.

Typical browser example:

```text
main thread subscribes
  -> worker settles query locally
  -> worker relays upstream if propagation is full
  -> worker emits QuerySettled(worker)
  -> main thread can deliver once requested tier is satisfied
```

The key point is that row delivery and "safe to publish first snapshot" are related, but not identical.

## Reconnect Behavior

Active query subscriptions are treated as desired state.

When an upstream server is re-added:

- the Sync Manager records the new link
- the Query Manager replays active forwarded subscriptions
- the server rebuilds scope and resends the rows the client still needs

This is what makes reconnect feel reliable without every app having to remember which queries to resubscribe manually.

## Key Files

| File                                                    | Purpose                                    |
| ------------------------------------------------------- | ------------------------------------------ |
| `crates/jazz-tools/src/sync_manager/mod.rs`             | Core state machine and queues              |
| `crates/jazz-tools/src/sync_manager/inbox.rs`           | Applying inbound sync payloads             |
| `crates/jazz-tools/src/sync_manager/types.rs`           | Payloads, ids, roles, and durability tiers |
| `crates/jazz-tools/src/sync_manager/permissions.rs`     | Permission-check routing                   |
| `crates/jazz-tools/src/query_manager/server_queries.rs` | Server-side query subscription handling    |
| `specs/status-quo/batches.md`                           | Batch lifecycle and settlement model       |
