# mini-jazz-sqlite - Subscription Reconciliation Protocol

**Date:** 2026-06-03
**Status:** MVP implemented
**Scope:** Defines the subscription download reconciliation path for current row
state. History repair, conflict signaling, and complete policy dependency sync
are deferred follow-up designs.

## Problem

The current subscription path sends a full query-scoped bundle on subscription.
That bundle can include current rows, history rows, transaction metadata, reads,
policy dependency history, branch metadata, and repair rows.

That is correct but too expensive. A reconnecting client may already have almost
all current rows for a large subscription. Sending every row and all related
history again makes subscribe/reconnect cost scale with total result size
instead of with the actual difference between client and server state.

We need subscription reconciliation:

- the client tells the server which local row versions it currently has for the
  subscription query
- the server anchors reconciliation to a server snapshot cursor
- the server sends readable current row data only for missing or stale row heads
- if a client-mentioned row advanced to an unreadable version, the server sends
  an obfuscated row-head advance so the client stops showing stale readable data
- ordinary subscription data does not include full history
- the first implementation uses an exact row-head list as the reconciliation
  sketch; rateless symbols remain a follow-up

## Non-Goals

This design does not define:

- history reconciliation
- multi-head conflict detection/signaling
- a final rateless algorithm parameter set
- complete edge/server policy dependency synchronization
- cache eviction for rows outside active subscriptions

Those are follow-up designs.

## Design Summary

Subscription reconciliation is about row state, not subscription result
membership.

The client computes the rows that match the query locally, then sends a
reconciliation sketch over their current visible row heads. The server computes
authoritative row heads at a snapshot cursor and uses set reconciliation to find
which row versions differ.

If the client is missing a readable row version, the server sends row data. If
the client mentioned a row whose authoritative head is now unreadable, the
server sends an obfuscated head advance containing only the row identity, new
tx id, and parent commit link.

The client applies data/obfuscated updates to local storage and reruns the query
locally. There is no protocol-level "remove this row from the subscription
result" message in the MVP.

```text
Client                                  Server
  |                                       |
  | Subscribe(query, row_head_sketch) --->|
  |                                       | choose snapshot cursor S
  |                                       | compute authoritative row heads
  |                                       | decode row-head difference
  |                                       |
  |<------ Data(cursor S, updates) -------|
  |                                       |
  | apply updates                         |
  | rerun query locally                   |
  |                                       |
  | Ack(message_id, cursor S) ----------->|
```

## Row Head Items

A row-head reconciliation item identifies one visible row version:

```rust
struct RowHeadItem {
    branch_id: BranchId,
    table: TableName,
    row_id: RowId,
    head_tx_id: TxId,
}
```

Meaning:

```text
For this branch/table/row, the visible head version is head_tx_id.
```

Examples:

```text
main.todos.todo-1@tx-9
main.todos.todo-2@tx-12
draft.todos.todo-1@tx-14
```

The item does not include a content hash. The `head_tx_id` is already tied to
the row version. The reconciliation algorithm may hash the canonical encoded
item internally, but that hash is not a semantic content version.

The item includes branch/scope and kind information in the canonical encoding so
that different branch views and different future reconciliation sets cannot
collide.

## Client Sketch Construction

For a normal client subscription, the client:

1. runs the query locally against its current cache
2. collects the current visible head tx id for each resulting row
3. encodes those `RowHeadItem`s into a reconciliation sketch
4. sends the sketch on `Subscribe` or `Replay`

Example:

```text
client local open todos:
  todo-1@tx-8
  todo-3@tx-5

client sketch:
  main.todos.todo-1@tx-8
  main.todos.todo-3@tx-5
```

The visible tx id is internal sync metadata. App-facing query APIs do not need
to expose it.

## Server Snapshot Rule

Every subscription reconciliation round is anchored to a server snapshot cursor.

The server must not mix rows from different logical moments in one
reconciliation result. Live changes after snapshot `S` are sent later with newer
cursors, in cursor order.

```text
time 10: server scope = todo-1, todo-2
time 11: todo-2 changes, todo-3 is created

Subscribe arrives
  -> server chooses S = 10 or S = 11
  -> reconciliation uses exactly that snapshot
  -> later updates are sent after S
```

## Server Reconciliation

The server computes authoritative row heads from the snapshot.

For readable rows that match the authoritative query at snapshot `S`, the server
includes row-head items in its server set:

```text
server readable query result at S:
  main.todos.todo-1@tx-9
  main.todos.todo-2@tx-12
```

Given:

```text
client sketch:
  main.todos.todo-1@tx-8
  main.todos.todo-3@tx-5

server set:
  main.todos.todo-1@tx-9
  main.todos.todo-2@tx-12
```

Decoded difference:

```text
server-only:
  main.todos.todo-1@tx-9
  main.todos.todo-2@tx-12

client-only:
  main.todos.todo-1@tx-8
  main.todos.todo-3@tx-5
```

Server handling:

- `todo-1@tx-9`: send readable row data for the newer version
- `todo-2@tx-12`: send readable row data for the missing row
- `todo-3@tx-5`: inspect the authoritative current head for that row identity

For client-only row identities, the server may need to send a state advance even
when the row no longer matches the authoritative query. This is how the client
learns that a stale local row should stop matching after it reruns the query.

Example:

```text
client has:
  todo-3 done=false @tx-5

server current readable head:
  todo-3 done=true @tx-9

server sends:
  RowDataUpdate(todo-3@tx-9, values: { done: true, ... })
```

The client applies the update, reruns `done = false`, and hides `todo-3`.

## Unreadable Advances

If a client-mentioned row has advanced to a version the client is no longer
allowed to read, the server must not send values. It can send an obfuscated
row-head advance:

```rust
struct ObfuscatedRowAdvance {
    branch_id: BranchId,
    table: TableName,
    row_id: RowId,
    tx_id: TxId,
    parent_tx_id: TxId,
}
```

Meaning:

```text
Your old readable head is superseded by tx_id.
You are not allowed to see the values for tx_id.
```

Example:

```text
client sketch:
  main.notes.secret-note@tx-5

server current head:
  main.notes.secret-note@tx-9 unreadable, parent tx-5

server sends:
  ObfuscatedRowAdvance(secret-note, tx_id=tx-9, parent_tx_id=tx-5)
```

The client applies enough metadata to stop treating `tx-5` as the visible
readable head. Local query/rendering should then hide the stale value. The MVP
stores this locally as a deleted current marker at the new tx id so an older
readable bundle cannot resurrect the stale value later.

Obfuscated advances are sent only for rows the client sketch proves the client
already knew. If the client did not mention an unreadable row, the server sends
nothing about it.

```text
client mentioned old readable row -> server may send obfuscated advance
client did not mention row        -> server sends nothing
```

This avoids turning subscriptions into an existence leak for unreadable rows.

## Data Payloads

The exact Rust names are provisional. The protocol-level shape is:

```rust
ServerMessage::Data {
    message_id: MessageId,
    subscription_id: Option<SubscriptionId>,
    cursor: ReplayCursor,
    bundle: SubscriptionDataBundle,
}

struct SubscriptionDataBundle {
    rows: Vec<RowDataUpdate>,
    obfuscated: Vec<ObfuscatedRowAdvance>,
    txs: Vec<TxRecord>,
    branches: Vec<BranchRecord>,
}
```

Readable row updates contain authoritative current row data:

```rust
struct RowDataUpdate {
    branch_id: BranchId,
    table: TableName,
    row_id: RowId,
    tx_id: TxId,
    op: DataOp,
    values: BTreeMap<String, JsonValue>,
    created_at: i64,
    updated_at: i64,
    created_by: UserId,
    updated_by: UserId,
}
```

`RowDataUpdate` is current state, not full history. A delete is represented as a
row update at a new tx id with delete semantics and no readable values.

The bundle contains only metadata required to apply these updates. It does not
send all history for the subscription.

## Client ACK

`Ack` keeps its current MVP shape:

```rust
ClientMessage::Ack {
    message_id: MessageId,
    cursor: Option<ReplayCursor>,
}
```

The ACK means:

```text
The client received and applied Data(message_id, cursor).
```

It does not carry a history sketch in this design.

## Subscribe And Replay

`Subscribe` carries reconciliation directly:

```rust
ClientMessage::Subscribe {
    subscription_id: SubscriptionId,
    query: BuiltQuery,
    requested_tier: SettlementTier,
    reconciliation: Option<ReconciliationSketch>,
}
```

`ReplaySubscription` carries the same reconciliation information:

```rust
struct ReplaySubscription {
    subscription_id: SubscriptionId,
    query: BuiltQuery,
    requested_tier: SettlementTier,
    last_applied_cursor: Option<ReplayCursor>,
    reconciliation: Option<ReconciliationSketch>,
}
```

`last_applied_cursor` and `reconciliation` answer different questions:

```text
last_applied_cursor = which server Data cursor did I already apply?
reconciliation      = which row heads do I currently have for this query?
```

Both are kept on replay.

## Reconciliation Sketches

The implementation supports exact sorted row-head lists for focused/debug paths
and uses rateless row-head symbols as the default subscription sketch.

Implemented shape:

```rust
struct ReconciliationSketch {
    set: ReconcileSet,
    algorithm: ReconcileAlgorithm,
    parameters: Option<ReconcileParameters>,
    symbols: Vec<ReconcileSymbol>,
    row_heads: Vec<RowHeadItem>,
}

enum ReconcileSet {
    RowHeads,
    PolicyDeps,
}

enum ReconcileAlgorithm {
    Exact,
    Rateless,
}
```

The rateless layer answers only:

```text
Which item ids are present on one side but not the other?
```

It does not carry row data, history data, or policy payloads. Once the server
identifies the set difference, normal `Data` messages carry the content.

`RowHeads` is the normal client subscription set.

`PolicyDeps` is role-gated and only for trusted edge/server peers that
explicitly request policy dependency sync. Ordinary clients do not receive
policy dependencies by default.

History is intentionally absent from `ReconcileSet` for this design.

## Bounded Decode

Rateless reconciliation is best when subscriptions are large and differences
are small. It is not a promise of unbounded server CPU.

The server should bound decode work:

```text
Subscribe carries initial symbols.
Server tries to decode.
If decode succeeds, send exact readable updates and safe obfuscated advances.
If decode fails, ask for more symbols up to a limit.
If still undecodable, send a scoped retryable error.
```

More-symbol messages are implemented:

```rust
ServerMessage::ReconcileMore {
    subscription_id: SubscriptionId,
    set: ReconcileSet,
    parameters: ReconcileParameters,
    next_symbol_index: u32,
    requested_symbols: u32,
}

ClientMessage::ReconcileSymbols {
    subscription_id: SubscriptionId,
    set: ReconcileSet,
    parameters: ReconcileParameters,
    symbols: Vec<ReconcileSymbol>,
}
```

## Fallback

If the row-head sketch cannot be decoded within the bounded attempt, the server
sends a retryable `reconciliation_decode_failed` error scoped to the
subscription. It does not fall back to a full query export.

The server must not send obfuscated advances for unknown unreadable rows when it
cannot decode which rows the client mentioned. Doing so would leak row
existence.

Tradeoff: fallback may not repair every stale client-only row that no longer
matches the authoritative query, because the server did not decode the
client-only identities. Those stale rows can be repaired by a later successful
reconciliation, explicit cache eviction, or a future stronger fallback.

The stronger fallback can be designed later, for example:

- legacy full query-scoped repair bundle
- fixed hash buckets
- adaptive chunked reconciliation

## Live Updates

MVP reconciliation is for initial `Subscribe` and reconnect `Replay`.

Live subscription updates continue to send changed row data normally. Later, the
same reconciliation machinery can be used for periodic repair or for live
refresh paths that lost precise per-subscription change tracking.

## Policy Dependencies

Permissions are server-side by default. Ordinary clients should not receive
policy dependency rows unless a separate role/request explicitly allows it.

Edge/server peers may request policy dependency reconciliation when they need
that data for local authorization or onward sync. The server may deny
`PolicyDeps` for connection roles that are not allowed to receive policy data.

## Invariants

- Subscription reconciliation is anchored to a server snapshot cursor.
- Live updates after the snapshot are delivered after snapshot data, in cursor
  order.
- Reconciliation uses row-head items, not subscription membership items.
- Row-head versions use `head_tx_id`, not content hashes.
- Current readable rows are sent as row data, not as full history.
- There is no subscription-scope removal message in the MVP.
- Obfuscated advances are sent only for client-mentioned rows.
- Ordinary clients do not receive policy dependency rows by default.
- History reconciliation and conflict detection are deferred.
- `Ack` remains message/cursor acknowledgement only.
- `ReplaySubscription` carries reconciliation as well as `last_applied_cursor`.

## Open Questions

- Exact rateless algorithm and symbol encoding.
- Default initial symbol budget and maximum decode effort.
- Final fallback strategy for undecodable sketches.
- Exact storage representation for obfuscated row-head advances.
- Whether `Data` should reuse the existing `Bundle` type with new sections or
  introduce `SubscriptionDataBundle`.
