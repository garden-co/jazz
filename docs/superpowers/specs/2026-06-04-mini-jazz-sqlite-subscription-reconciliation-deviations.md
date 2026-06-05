# mini-jazz-sqlite - Subscription Reconciliation Spec Deviations

**Date:** 2026-06-04
**Last updated:** 2026-06-05
**Compared spec:** `docs/superpowers/specs/2026-06-03-mini-jazz-sqlite-subscription-reconciliation-protocol.md`
**Compared implementation:** current `mini-jazz-sqlite` subscription protocol implementation

This document records where the implementation differs from the written
subscription reconciliation spec. It only covers subscription download sync, not
client upload sync. Deviations are about the `mini-jazz-sqlite` core unless a
section explicitly calls out the `mini-sqlite-todo-yew` example server.

## Material Deviations

### 1. `Data` reuses the legacy `Bundle`

The spec describes a subscription-shaped data bundle:

```rust
struct SubscriptionDataBundle {
    rows: Vec<RowDataUpdate>,
    obfuscated: Vec<ObfuscatedRowAdvance>,
    txs: Vec<TxRecord>,
    branches: Vec<BranchRecord>,
}
```

The implementation reuses the existing `Bundle` type and adds `rows` and
`obfuscated` fields to it. That means `Data` can still physically carry
`history`, `reads`, and `query_reads`.

On the exact reconciliation path, the implementation currently emits empty
`history`, `reads`, and `query_reads`. Missing or unsupported reconciliation is
rejected before any subscription `Data` is emitted.

### 2. Missing or unsupported reconciliation is rejected

The spec says subscription data should not send all history. It also describes a
snapshot-oriented repair path for reconciliation gaps.

The implementation rejects malformed or unsupported subscription reconciliation
requests instead of repairing them with `export_query(query)`:

- `reconciliation: Some(RowHeads/Exact)` sends only `rows`, `obfuscated`, `txs`,
  and `branches`
- `reconciliation: None` returns a retryable scoped `ServerMessage::Error` with
  code `missing_reconciliation`
- any set or algorithm other than `RowHeads/Exact` returns a retryable scoped
  `ServerMessage::Error` with code `unsupported_reconciliation`

Rejected subscription requests do not emit `Data` or `Settled`, and they do not
activate the subscription. General hydration/export APIs still use
`export_query(query)`, but the subscription download path does not fall back to a
legacy full query export.

### 3. `PolicyDeps` is type-only and rejected

The spec says `PolicyDeps` is role-gated and only for trusted edge/server peers.

The implementation defines `ReconcileSet::PolicyDeps`, but there is no
role-gated policy dependency reconciliation path. A non-`RowHeads` sketch is
rejected with `unsupported_reconciliation` instead of being handled as a
dedicated policy-dependency request.

### 4. The server cursor is not a real storage snapshot cursor

The spec says every reconciliation round is anchored to a server snapshot cursor,
and all rows in a `Data` message must come from that logical snapshot.

The implementation uses a per-session monotonic `ReplayCursor` assigned after
exporting the bundle. It is a protocol message cursor, not a persisted storage
snapshot/version cursor.

The current runtime export is synchronous, so this is probably fine for the
prototype, but the code does not enforce the spec's stronger snapshot rule with
a storage transaction or database snapshot identifier.

### 5. ACK and cursor semantics are split across several states

The spec keeps both:

```text
last_applied_cursor = which server Data cursor did I already apply?
reconciliation      = which row heads do I currently have for this query?
```

The implementation now keeps downstream and upstream subscription state
separate. Downstream state carries `last_applied_cursor`, which advances after a
`Data` bundle is applied locally and is sent back in `ReplaySubscription`.
Upstream state carries `last_sent_cursor`, which advances when the server sends
subscription `Data`.

ACK tracking is a third state: `pending_messages` maps `message_id` to the sent
cursor, and `last_acknowledged` advances only when an ACK arrives.

The server does not trust or validate the cursor field sent by the client ACK;
it uses the cursor remembered for the acknowledged `message_id`. Local
`Settled` is emitted immediately after `Data` for `SettlementTier::Local`, so it
is not gated by the ACK.

The remaining deviation is that upstream replay repair is still driven by the
reconciliation sketch, not by the client-provided `last_applied_cursor`.

### 6. Live refresh uses reconciliation machinery

The spec says MVP reconciliation is for initial `Subscribe` and reconnect
`Replay`; live subscription updates continue to send changed row data normally.

The implementation also uses `export_subscription_reconciliation` from
`refresh_active_subscriptions`. For refreshes, the server uses a server-stored
row-head sketch from the previous send, not a fresh client-authored sketch.

This is useful because it lets the server send delete/current-state advances for
rows it previously sent, but it is stronger and different from the spec's stated
MVP live-update model.

### 7. Row data is persisted as row-version history locally

The spec says `RowDataUpdate` is current state, not full history.

On the wire, the exact reconciliation path does not send `bundle.history`.
However, the receiver applies each `RowDataUpdate` by converting it into a
`HistoryRecord` and storing it in the local history table.

This is intentional in the current implementation: without a durable row-version
record, later projection rebuilds can make synced rows disappear.

### 8. Insert/update semantics are collapsed for row data

The spec models `RowDataUpdate.op` as a data operation.

The implementation's readable row data uses numeric `op = 2` for current row
state, including rows that are missing locally and are effectively inserts for
the receiver. Deletes use `op = 3` with empty values.

So for subscription download, `op` is effectively:

```text
2 = upsert readable current row data
3 = delete/tombstone row data
```

It is not preserving an insert-vs-update distinction for readable row updates.

### 9. Obfuscated advances are minimal current markers

The spec says an obfuscated advance contains a parent commit link and enough
metadata to stop showing stale readable data.

The implementation:

- writes a deleted current marker with `is_deleted = 1`
- records a tx write for the obfuscated tx
- does not create a full history row for the obfuscated advance
- uses synthetic metadata locally (`created_at = 0`, `updated_at = 0`,
  user `"unknown"`)
- sets `parent_tx_id` to the client-mentioned stale head, but does not validate
  that it is the direct parent of the current head

This matches the practical stale-value hiding behavior, but it is a thinner
representation than the spec wording suggests.

### 10. Only `Local` subscription settlement is supported

The spec includes `requested_tier` on `Subscribe` and `ReplaySubscription`.

The implementation rejects `Edge` and `Global` subscription requests with
`unsupported_settlement_tier`. Only `SettlementTier::Local` is supported for
subscription settlement right now.

### 11. Reconciliation construction errors surface locally

The downstream connection manager must compute local reconciliation before
sending `Subscribe` or `Replay`.

If local sketch construction fails, the client-side subscription or replay call
returns that error instead of sending `reconciliation: None`. The spec does not
define this local error path, but surfacing the error preserves the "do not send
all history on subscription" invariant.

### 12. No canonical item encoding exists yet

The spec says row-head items should be canonically encoded for reconciliation,
with enough set/kind information to avoid collisions between future
reconciliation sets.

The MVP implementation sends an exact sorted `Vec<RowHeadItem>` and does not
define a reusable canonical item encoding or hash input. The enclosing sketch
does carry `set = RowHeads`, so this is not a current correctness problem, but
the future rateless layer still needs an explicit encoding.

### 13. Branch handling is effectively single-active-branch

`RowHeadItem` carries `branch_id`, as specified.

The implementation compares client heads against the runtime's current branch
and returns no repair for heads from other branches. It does not implement a
multi-branch authoritative reconciliation pass.

This is consistent with the current runtime shape, but weaker than the
branch-aware protocol shape in the spec.

### 14. The example server adds a debounced live-refresh broadcast path

The protocol spec describes subscription reconciliation at the core
connection/session layer. The `mini-sqlite-todo-yew` example server adds extra
live fan-out behavior around that core:

- upload-originating changes are broadcast to other WebSocket connections
- the broadcast path sleeps for `SUBSCRIPTION_REFRESH_DEBOUNCE`, currently 50 ms,
  to coalesce nearby changes
- when the uploaded transaction can be exported, the example server can push the
  exported upload bundle directly to peers
- it refreshes active subscriptions when the upload bundle is incomplete or when
  no direct push bundle is available

This is useful for two-browser demo sync, but it is not specified by the
subscription reconciliation protocol. It can create multiple server messages for
one local write and makes observed live-update latency depend partly on the
example-server debounce and broadcast plan.

## Provisional Spec Items Not Implemented

These appear in the spec as future or provisional machinery and are not in the
current implementation:

- rateless reconciliation symbols
- bounded decode effort
- `ServerMessage::ReconcileMore`
- `ClientMessage::ReconcileSymbols`
- stronger fallback after failed rateless decode
- periodic repair driven by fresh client sketches
- complete edge/server policy dependency sync

## Matches Worth Keeping

These parts do match the spec closely:

- `Subscribe` carries optional reconciliation.
- `ReplaySubscription` carries optional reconciliation.
- MVP reconciliation uses exact row-head lists.
- Row-head versions use `head_tx_id`, not content hashes.
- Exact reconciliation sends missing/stale readable rows as row data.
- Client-mentioned deleted rows are repaired with delete row data.
- Obfuscated advances are only generated for client-mentioned rows.
- ACK is message/cursor-shaped and is sent after successful bundle apply.
- History repair and conflict signaling remain deferred.
