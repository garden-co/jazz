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

Jazz forwards row batch entries, explicit batch seals, replayable batch fate, and
query subscriptions so the server can build the same relational view and answer
forwarded queries.

Catalogue updates use the same sync payload lane, but publication authority is
core-only in edge deployments. Edges receive schema, migration, and permissions
catalogue entries from core; they do not accept local admin catalogue writes and
proxy them upstream.

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

Peer is used for trusted runtime-to-runtime links such as browser main-thread to
worker or server-to-server communication. Server-to-server WebSocket links use
the peer-secret handshake and are registered as `ClientRole::Peer`, preserving
the normal sync state for the connection while allowing trusted catalogue and
row sync payloads.

## What Moves Over Sync

The sync payloads now speak in row-history and query terms:

- `CatalogueEntryUpdated`
- `RowBatchCreated`
- `RowBatchNeeded`
- `SealBatch`
- `BatchFate`
- `BatchFateNeeded`
- `QuerySubscription`
- `QueryUnsubscription`
- `QueryScopeSnapshot`
- `QuerySettled`
- `SchemaWarning`
- `Error`

That payload set matches the table-first runtime model:

- new row batch entries travel as row batch entries
- initial query fill can explicitly ask for needed row batch entries
- direct and transactional batches are explicitly sealed upstream
- replayable whole-batch fate travels separately from concrete row entries
- durability and rejection travel as `BatchFate`
- row/query visibility is derived from row delivery, query scopes, and local or sealed batch
  membership, not from fate payload members
- schemas and lenses travel as catalogue entries
- permissions bundles and permissions heads travel as catalogue entries

## Server Tiers

Server durability identity is configured by topology:

- a server without an upstream URL is the core/global node and owns
  `GlobalServer`
- a server with an upstream URL is an edge node and owns `EdgeServer`

Because `GlobalServer` is higher than `EdgeServer`, the core can satisfy both
global-tier and edge-tier durability. An edge can satisfy edge-tier work locally
after it has durable edge state, but global-tier writes and global-tier query
settlement continue upstream to the core.

Edges connect to core as peer clients over the existing WebSocket transport.
That means reconnect/backoff, active subscription replay, batch-settlement
replay, and catalogue-digest optimization all stay inside the same transport
and SyncManager paths used by other runtime links.

## A Typical Flow

### Direct local write

1. A runtime appends a new row batch entry locally.
2. Storage updates the flat visible row and indices.
3. Query subscriptions settle locally.
4. The writer seals the direct batch, sending `SealBatch` upstream. Simple write APIs do this immediately.
5. The Sync Manager queues `RowBatchCreated` for peers and servers.
6. Replayable durability eventually converges through `BatchFate::DurableDirect`.
7. If authority rejects any member write in that direct batch, the whole batch resolves as
   `BatchFate::Rejected`; independent write fate requires independent batches.

### Transactional write

1. A runtime upserts staging row batch entries locally.
2. Ordinary readers ignore those `StagingPending` rows.
3. The writer explicitly seals the batch, sending `SealBatch` upstream.
4. The authority decides replayable batch fate.
5. Accepted output becomes visible and is replayable as `BatchFate::AcceptedTransaction`.
6. Rejection or missing authority truth is replayable as `Rejected` or `Missing`.

### New query subscription

1. A client sends `QuerySubscription`.
2. The Sync Manager records that desired state.
3. The Query Manager compiles and settles the server-side query.
4. For each matching object/branch, initial replay sends the current visible row as `RowBatchNeeded`.
5. If that current row has replayable batch fate, the current `BatchFate` is replayed too.
6. A `QuerySettled` signal tells the downstream runtime when the first snapshot is safe to deliver for a requested durability tier.

### Later visibility/fate change

When a row already known to a peer becomes durable, accepted, or rejected, the runtime sends the
batch fate that proves the new whole-batch outcome. Successful fate applies to every known row with
that `batch_id`; receivers use their own delivered row batches, local batch records, sealed
submissions, and `QuerySettled.scope` to decide which concrete rows or subscriptions are affected.

The legacy `visible_members` field is deprecated. It may be read for compatibility
with old storage or old sync peers, but new logic should not need to decode or scan it on a
per-row read path.

The row-level identity for local interest remains `RowBatchKey { row_id, branch_name, batch_id }`,
but active sync does not send a row-state-change payload for it.

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
  -> worker emits QuerySettled(local)
  -> main thread can deliver once requested tier is satisfied
```

The key point is that row delivery and "safe to publish first snapshot" are related, but not identical.

For an edge client requesting `GlobalServer`, the first snapshot is not released
just because the edge has a local answer. The edge forwards the subscription
upstream and relays `QuerySettled(GlobalServer)` only after the core settles the
query at the global tier.

## Reconnect Behavior

Active query subscriptions are treated as desired state.

When an upstream server is re-added:

- the Sync Manager records the new link
- the Query Manager replays active forwarded subscriptions
- the server rebuilds scope and resends the rows the client still needs
- pending batch-settlement requests and catalogue state are replayed or skipped
  according to the upstream connection state and catalogue digest

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
