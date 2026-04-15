# WriteHandle `wait({ tier })` Implementation — TODO (MVP)

This doc decides how unified CRUD handles reach replayable settlement without keeping
separate `*Durable` and `*Persisted` APIs.

It assumes the first CRUD migration step has already landed:

- `insert(...)` returns a `WriteHandle<{ value }>` instead of returning the row directly

## Related

- [Unified CRUD API](unified_crud_api.md)
- [Batches — Status Quo](../../status-quo/batches.md)
- [TypeScript Client — Status Quo](../../status-quo/ts_client.md)
- [Opt-In Transactions, Replayable Reconciliation, and Strict Visibility](opt_in_transactions_replayable_reconciliation.md)

## Current constraint

The replayable waiting path already exists, but only behind the persisted-write APIs:

- TypeScript already has `waitForPersistedBatch(batchId, tier)`
- Rust already persists `LocalBatchRecord`s and reconciles them after reconnect
- batch settlement is already the replayable source of truth for resolve vs reject

The missing link is ordinary direct CRUD:

- plain `insert` / `update` / `delete` do not give the caller a `batchId`
- the current `*Persisted` path chooses the requested tier at write time
- `wait({ tier })` chooses the tier later, on the handle
- `LocalBatchRecord.requested_tier` drives reconnect reconciliation, so a later `wait({ tier })`
  cannot be implemented purely in TypeScript unless the core can persist that upgraded interest

So `wait({ tier })` is not a wrapper around the current `insertDurable(...)` or
`insertPersisted(...)` path. It needs explicit support in the Rust core and runtime bindings.

## Decision

### 1. Ordinary CRUD writes become tracked direct batches

Every user-facing direct `insert` / `update` / `delete` should create or retain one replayable
direct-batch record immediately.

That record should exist even if the caller never calls `wait(...)`.

Why:

- the handle needs a stable `batchId`
- later `wait({ tier })` needs persisted state it can upgrade
- restart-safe late-error reporting will need the same batch identity and stored settlement history

This does **not** change visibility semantics:

- direct writes are still visible immediately
- transactions remain the only path for confirm-gated visibility

### 2. Plain CRUD returns batch-backed results

The Rust core should return `batch_id` for ordinary direct writes.

Conceptually:

- `insert(...) -> { row, batch_id }`
- `update(...) -> { batch_id }`
- `delete(...) -> { batch_id }`

The exact Rust struct names can be chosen during implementation, but the JS bindings need this
shape so `WriteHandle` can retain hidden batch metadata without exposing a second public API.

### 3. `wait({ tier })` upgrades retained settlement on the existing batch

`wait({ tier })` should work by:

1. taking the already-created `batchId`
2. telling the runtime that this batch now needs replayable settlement through at least tier `T`
3. waiting on replayable batch settlement for that same batch

It must **not**:

- reissue the write
- synthesize a second logical batch
- route through `insertPersisted(..., tier)` with a fresh write

This keeps one mutation equal to one logical batch.

### 4. Tier escalation is monotonic and batch-wide

If the same batch is waited on more than once, the effective requested tier is the max of all
requested tiers seen so far.

That rule applies equally to:

- one direct write handle with repeated `.wait(...)` calls
- multiple handles created inside one explicit direct batch
- transactional writes and the transaction commit handle, which all point at the same batch

Examples:

```ts
const { value: todo, wait } = db.insert(app.todos, { title: "Ship", done: false });

await wait({ tier: "edge" });
await wait({ tier: "global" }); // upgrades the same batch; does not write again
```

```ts
const batch = db.beginDirectBatch();
const { wait: waitA } = batch.insert(app.todos, { title: "A", done: false });
const { wait: waitB } = batch.insert(app.todos, { title: "B", done: false });

await waitA({ tier: "edge" });
await waitB({ tier: "global" }); // shared batch now effectively waits for global
```

## Rust/core changes

### Return batch ids from ordinary writes

`RuntimeCore` should stop discarding the direct batch id for plain CRUD calls.

Today:

- `insert(...)` returns the row only
- `update(...)` and `delete(...)` return `()`

Needed:

- `insert(...)` returns row data plus `batch_id`
- `update(...)` and `delete(...)` return `batch_id`

That change then propagates through:

- `runtime_core`
- `runtime_tokio`
- Rust client wrappers
- NAPI/WASM bindings
- TypeScript `Runtime` interface

### Retain a local batch record for ordinary direct writes

Plain direct writes should persist a `LocalBatchRecord` just like the replayable persisted path
does today.

For ordinary direct writes, initialize the record as:

- `mode = Direct`
- `sealed = true`
- `requested_tier = max_local_durability_tier()`
- `latest_settlement = DurableDirect(max_local_durability_tier())`

That means:

- `wait()` with no tier can resolve immediately in TypeScript without touching the runtime
- `wait({ tier: "worker" })` also resolves immediately when worker is the local tier
- a later request for `edge` or `global` can upgrade the same retained record

### Add a batch-tier retention upgrade primitive

The core needs one new operation along the lines of:

- `retain_batch_until_tier(batch_id, tier)`

Behavior:

- load the existing local batch record
- if `requested_tier >= tier`, no-op
- otherwise set `requested_tier = tier`
- if the current settlement already satisfies `tier`, stop there
- if not, request replayable settlement for that batch from connected upstream servers and persist
  the upgraded record before returning

Why this is necessary:

- reconnect reconciliation only asks for batches whose retained `requested_tier` is still unmet
- without persisting the upgrade, `wait({ tier: "global" })` would be forgotten on restart
- without nudging reconciliation for live connections, a late tier upgrade could wait much longer
  than necessary

This primitive should be idempotent and safe to call repeatedly.

## TypeScript/runtime changes

### `WriteHandle` stores hidden batch metadata

Public insert shape stays simple:

```ts
type WriteHandle<T> = {
  value: T;
  wait: (options: { tier: DurabilityTier }) => Promise<void>;
};
```

Internally, the handle also needs:

- `batchId`
- a reference back to `JazzClient`

`update` and `delete` handles will use the same internal batch-backed implementation, just without
public `value`.

### `wait({ tier })` behavior

`wait()` or `wait({})`:

- resolves immediately

`wait({ tier })`:

1. call the new runtime/client retention-upgrade API for that batch
2. wait on replayable settlement for the same batch and tier
3. resolve once `latestSettlement.confirmedTier >= tier`
4. reject if replayable settlement becomes `Rejected`

Initially, TypeScript can keep reusing the existing `waitForPersistedBatch(...)` helper internally.
Renaming it to something batch-generic can happen in the same change or immediately after.

## Why not just reuse `*Persisted` internally?

Because the timing is wrong.

`insertPersisted(table, values, { tier })` means:

- choose the requested tier up front, while writing

`insert(...).wait({ tier })` means:

- perform the write now
- decide later whether this particular caller cares about edge/global confirmation
- possibly decide that more than once, with a higher tier later

Those are different contracts. The unified API needs the second one.

## Interaction with restart-safe mutation errors

This spec does not define the global `onMutationError(...)` surface in detail.

It does make that follow-up possible:

- every ordinary direct write now has a retained batch identity
- retained batch records survive restart
- later rejections can be surfaced from replayable settlement instead of live promise state

The notification policy can be specified separately, but this batch-tracking change is the
necessary substrate for it.

## Required tasks

- [ ] Change ordinary direct insert/update/delete in Rust core to return `batch_id` alongside their current result data.
- [ ] Make ordinary direct writes always retain a `LocalBatchRecord` with direct-batch metadata and initial local settlement.
- [ ] Add a core/runtime binding method to raise `requested_tier` for an existing batch and trigger reconciliation when needed.
- [ ] Thread the new direct-write result shape through NAPI/WASM bindings, `Runtime`, and `JazzClient`.
- [ ] Extend `WriteHandle` so it stores hidden batch metadata and implements `wait({ tier })` on top of retained batch settlement.
- [ ] Reuse or rename the existing TypeScript batch waiter so direct handles, persisted handles, direct batches, and transactions all wait on the same replayable settlement path.
- [ ] Add integration tests for direct-write tier upgrades, offline restart + reconnect settlement, and rejection after a late `wait({ tier })`.
