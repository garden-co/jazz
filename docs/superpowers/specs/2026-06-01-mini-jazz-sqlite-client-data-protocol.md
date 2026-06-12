# mini-jazz-sqlite - Client Transaction Upload Protocol

**Date:** 2026-06-01
**Status:** Design checkpoint
**Scope:** Defines the client-to-server data upload path, reconnect reconciliation
from the client upload queue, upload acknowledgement semantics, transaction status
messages, and targeted unsubscribe behavior.

## Problem

The current sync protocol is effectively download/subscription-oriented. Clients can
subscribe, replay subscriptions, acknowledge server data, and close, but they do not
have a clear protocol for asking an upstream peer to process local transactions.

This creates three gaps:

- clients have no dedicated message for sending local transaction data
- reconnect has no client-to-server reconciliation path for locally committed
  transactions that were not filed before shutdown
- unsubscribe is modeled as `Replay` with a reduced subscription set, forcing the
  server to diff interests and often resend remaining subscriptions unnecessarily

## Design Summary

Client upload is transaction-scoped.

One `ClientMessage::UploadTx` uploads one transaction. A transaction may contain multiple
row mutations across multiple tables. Transport-level batching may coalesce many
protocol messages into one WebSocket frame or `postMessage` delivery, but the protocol
message remains one transaction.

The implementation also changes locally generated public `tx_id` values from
`tx-{node_id}-{local_epoch}` strings to UUIDv7 strings. The protocol treats `tx_id` as
opaque and must not derive writer, epoch, ordering, or authority state from the string.

The client keeps a durable upload queue. Every normal committed local transaction is
inserted into that queue atomically with the transaction commit. After connection
handshake, the client sends queued transactions ordered by `(created_at, sync_seq)`,
with up to `max_in_flight_uploads` unacknowledged uploads. The initial default is
`1000`; negotiation can be added later if servers need to advertise lower capacity.

Upload ACK is flow control only:

```text
Server -> UploadAck { tx_id }
```

It means the server received the upload message on this connection. It does not mean
the transaction was durably stored, accepted, edge-filed, globally accepted, or
rejected.

Transaction fate/progress is reported separately:

```text
Server -> TxStatus { tx_id, status }
```

`TxStatus` is non-cumulative. `GlobalAccepted` implies edge satisfaction for retry and
wait checks, but storage does not need to materialize an edge receipt when only a global
receipt exists.

## Protocol Messages

### Client Upload

```rust
ClientMessage::UploadTx {
    tx: ClientTx,
    data: Vec<ClientDataRecord>,
    reads: Vec<ReadRecord>,
}
```

`data` must be non-empty.

`reads` uses the existing `ReadRecord` shape for now. The read-set model is expected to
change soon, so this design does not introduce a new read-set protocol type.

### Client Transaction Header

```rust
struct ClientTx {
    tx_id: String,
    branch_id: Option<String>,
    conflict_mode: TxConflictMode,
    created_at: i64,
    author: Option<String>,
}
```

`branch_id` semantics:

- `Some(branch_id)` applies the transaction to that branch.
- `None` applies the transaction to the connection/session default branch.
- If no default branch exists, the transaction is invalid for that session.

`author` semantics:

- for untrusted client connections, the receiver ignores `author` and uses the
  authenticated session user
- for trusted peer connections, `author` is required and may be preserved as
  authoritative provenance

`created_at` is the transaction timestamp used for client-side upload ordering. The
server processes messages in transport receive order. WebSocket and `postMessage`
provide ordered delivery for the intended transports; unordered transports must provide
an ordered stream abstraction below this protocol.

There is no `target_tier` in the message. Upload retry completion is independent of a
requested tier and is driven by authoritative local transaction fate.

### Client Data Record

```rust
struct ClientDataRecord {
    table: String,
    row_id: String,
    op: DataOp,
    values: BTreeMap<String, JsonValue>,
}
```

`values` is row-image-shaped:

- `Insert`: values contain the effective fields for the new row
- `Update`: values contain the effective fields after the update
- `Delete`: values must be empty

System fields such as `j_created_at`, `j_updated_at`, `j_created_by`, and
`j_updated_by` are not valid inside `values`. They are derived from `ClientTx` and the
connection trust context.

Row ids and reference field values use public ids on the wire. The receiver hydrates
them into local physical ids before storage.

One transaction must not contain multiple `ClientDataRecord`s for the same
`(table, row_id)`. The client normalizes repeated mutations before upload.

### Data Operations

```rust
enum DataOp {
    Insert,
    Update,
    Delete,
}
```

`Insert` against an existing row is a transaction rejection.

`Update` against a row missing from the local server should fetch/wait for
authoritative state when the row might exist upstream. It is rejected only after
authoritative absence is known.

`Delete` with non-empty `values` is rejected for MVP.

### Conflict Mode

```rust
enum TxConflictMode {
    Mergeable,
    Exclusive,
}
```

Mergeable transactions may omit reads by sending an empty `reads` vector.

Exclusive or conflict-sensitive transactions require the read facts needed for
validation. Missing or incomplete reads are a transaction rejection, not a fatal
protocol error.

### Server Upload ACK

```rust
ServerMessage::UploadAck {
    tx_id: String,
}
```

`UploadAck` means the server received the upload message on this connection and the
client may free one in-flight upload slot.

`UploadAck` is ignored if the client has no matching in-flight upload.

`UploadAck` for an already completed transaction is also ignored except for in-flight
bookkeeping.

### Server Transaction Status

```rust
ServerMessage::TxStatus {
    tx_id: String,
    status: TxStatusKind,
}

enum TxStatusKind {
    EdgeAccepted,
    GlobalAccepted { global_epoch: i64 },
    Rejected {
        code: String,
        detail: Option<JsonValue>,
    },
}
```

The server only sends `TxStatus` when the transaction reaches edge, reaches global, or
is rejected. It does not send pending or awaiting-dependency statuses.

`TxStatus` is non-cumulative:

- `EdgeAccepted` means edge filing/acceptance happened
- `GlobalAccepted` means global filing/acceptance happened and satisfies edge-level
  retry/wait checks
- `Rejected` is terminal for upload retry

Unknown `TxStatus` messages are ignored by the client for MVP.

Applying `TxStatus` updates normal local transaction fate tables first. Upload queue
completion is then derived from local tx fate, not from the status message path itself.

## Client Upload Queue

Every normal committed local transaction enters the upload queue atomically with the
transaction commit. Registry insertion failure fails the commit.

True local-only writes, if needed later, should be an explicit transaction mode that
skips upload queue insertion.

MVP schema:

```sql
CREATE TABLE jazz_tx_upload_queue (
  sync_seq INTEGER PRIMARY KEY AUTOINCREMENT,
  tx_num INTEGER NOT NULL UNIQUE,
  status INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  branch_id TEXT,
  author TEXT,
  completed_at INTEGER,
  last_upload_attempt_at INTEGER,
  last_ack_at INTEGER,
  attempt_count INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX jazz_tx_upload_queue_active_idx
ON jazz_tx_upload_queue(status, created_at, sync_seq)
WHERE status = 1;

```

Status values:

```text
1 = active
2 = completed
```

`sync_seq` is a stable local queue sequence assigned atomically with commit. It is used
as a tie-breaker for upload order:

```sql
ORDER BY created_at, sync_seq
```

`sync_seq` is local-only and is not sent to the server.

Upload data is reconstructed from transaction write metadata and committed history
rows. Cleanup removes completed queue rows only; it never deletes transaction records,
history, receipts, or rejection details.

Upload queue completion rule for mergeable transactions:

```text
complete when local tx fate has:
  edge receipt
  OR global receipt
  OR rejected outcome
```

Upload queue completion rule for exclusive transactions:

```text
complete when local tx fate has:
  global receipt
  OR rejected outcome
```

Exclusive transactions require global final fate. If an edge-only status or receipt is
observed for an exclusive transaction, the client ignores it for upload queue completion.

For retry/completion purposes, global acceptance satisfies edge. Existing receipt APIs can
keep reporting literal stored receipts; higher-level satisfaction checks should treat
global as satisfying edge.

The queue is not completed by `UploadAck`. `UploadAck` is only in-flight flow
control.

Queue completion can be triggered by any authoritative local tx fate update:

- `TxStatus`
- subscription/query sync that applies a receipt or rejection
- any other trusted sync path that enriches the local transaction record

## Client Send Loop

After `ServerHello`, the client starts upload reconciliation.

The client:

1. scans active queue rows ordered by `(created_at, sync_seq)`
2. sends `ClientMessage::UploadTx` while `in_flight_uploads < max_in_flight_uploads`
3. records `last_upload_attempt_at` and increments `attempt_count`
4. tracks in-flight uploads by `tx_id`
5. removes an in-flight tx when `UploadAck { tx_id }` arrives
6. sends more queued transactions as upload ACKs free slots

The default `max_in_flight_uploads` is `1000`.

The client does not retry upload-acked transactions on the same healthy connection. If
the connection drops, in-flight state is cleared and reconnect reconciliation replays
active queue rows that are still not completed.

If a queued transaction completes before its upload ACK arrives, the queue row is marked
completed and any matching in-flight entry may be dropped.

## Server Handling

On `ClientMessage::UploadTx`, the server:

1. verifies handshake/session is established
2. verifies the connection is authenticated
3. validates protocol shape
4. sends `UploadAck { tx_id }` after parsing/accepting the message into the connection flow
5. validates/applies the proposed transaction according to trust role and conflict mode
6. emits `TxStatus` only when the tx reaches edge, reaches global, or is rejected

For untrusted client connections:

- policy validation uses the authenticated session user
- `ClientTx.author` is ignored
- row provenance is derived from the authenticated session and transaction timestamp
- the client cannot forge edge/global acceptance, rejection, receipt state, catalogue
  publication, or system fields

For trusted peer connections:

- `ClientTx.author` is required
- trusted provenance may be preserved
- the receiver may treat peer-provided transaction data as coming from a trusted sync
  role, subject to the connection's authority level

The server processes upload messages in receive order. Ordering is a transport
requirement, not an application-level buffering/reordering protocol.

If the server receives duplicate `UploadTx` for a tx it already knows, it still sends
`UploadAck { tx_id }`. If the tx is already edge-accepted, globally accepted, or
rejected, the server should also send the current `TxStatus` so the client can complete
its queue quickly after reconnect.

## Validation And Rejection

Envelope, protocol-shape, and auth failures are fatal and close the session.

Examples:

- `UploadTx` before handshake
- missing transaction header
- invalid enum tag
- unauthenticated upload on a connection that requires auth

Semantically invalid transactions produce `TxStatus::Rejected` and do not close the
session.

Examples:

- `branch_id = None` when the session has no default branch
- empty `data`
- delete record with non-empty values
- system fields in `values`
- duplicate `(table, row_id)` data records
- insert missing required fields
- insert row that already exists
- policy denied
- stale read set
- exclusive transaction missing required reads

If an edge/server lacks authoritative state needed to validate an update/delete, it
should fetch or wait for trusted upstream state when possible. Missing local state is not
automatically a rejection.

## Reconnect Reconciliation

Reconnect has no special upload replay message. After handshake, the normal send loop
scans `jazz_tx_upload_queue` and sends active transactions.

This means clients still upload local transactions even when no subscription is active.

Server-side implicit interest in uploaded txs is live-session state only. On disconnect,
the server may forget it. The client upload queue is the durable source of truth and
rebuilds interest by replaying still-active transactions after reconnect.

## Unsubscribe

Unsubscribe is a dedicated client message:

```rust
ClientMessage::Unsubscribe {
    subscription_id: SubscriptionId,
}
```

It is fire-and-forget.

Server handling:

- remove `active_subscriptions[subscription_id]`
- remove pending download messages for that subscription
- remove last acknowledged cursor for that subscription
- do not touch upload queue state or uploaded-tx implicit interest
- send no reply

`Replay` remains subscription-only and is used for reconnect/full subscription
reconciliation, not single-subscription drops.

## Capability And Versioning

This is a protocol-breaking change. Bump `SUPPORTED_PROTOCOL_VERSION` from `1` to `2`.

Add a required server capability:

```rust
ProtocolCapabilities {
    replay: bool,
    acknowledgements: bool,
    query_settlement: bool,
    tx_upload: bool,
}
```

Default `tx_upload` is `true`.

Clients reject `ServerHello` if `tx_upload` is false.

## Cleanup

Upload queue cleanup only removes completed retry metadata. It never deletes transaction
records, row data, history, receipts, or rejection details.

Cleanup never deletes active rows, regardless of age.

Default MVP cleanup policy:

```text
retention_age = 7 days
max_completed_rows = 10_000
delete_batch_size = 500
min_cleanup_interval = 60 seconds
```

Run cleanup:

- on runtime open/startup
- after marking transactions completed, rate-limited
- through an explicit maintenance API for tests/tools

Accepted and rejected completed upload queue rows use the same retention policy. Durable
rejection detail lives in transaction fate tables, not in the upload queue.

## Out Of Scope

- negotiated `max_in_flight_uploads`
- unordered transport recovery
- compact/delta optimization for `TxStatus`
- final read-set protocol shape
- true local-only transaction mode
- richer receipt metadata such as authority identity, receipt timestamp, signatures, or
  audit payload
- server-durable client interest/inbox for uploaded txs
