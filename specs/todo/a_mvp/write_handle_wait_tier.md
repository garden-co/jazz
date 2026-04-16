# WriteHandle `wait({ tier })` vs Authoritative Retention — TODO (MVP)

This spec narrows the implementation of `WriteHandle.wait({ tier })` so it does one thing well:

- `wait({ tier })` is a caller-facing readiness threshold

It must not also decide:

- how long the runtime retains a batch
- whether the runtime keeps reconciling that batch after an earlier threshold is satisfied
- whether a later authoritative rejection can still surface through `onMutationError`

## Related

- [Unified CRUD API & Error Handling](unified_crud_api.md)
- [Opt-In Transactions, Replayable Reconciliation, and Strict Visibility](opt_in_transactions_replayable_reconciliation.md)
- [Batches — Status Quo](../../status-quo/batches.md)

## Problem

Today the local batch record's `requested_tier` is overloaded.

It currently mixes two different concerns:

- waiter semantics: "what tier is this caller waiting for?"
- retention semantics: "how long should the runtime keep chasing this batch?"

That coupling creates the wrong behavior for the unified CRUD API:

- `wait({ tier: "edge" })` can accidentally become "stop caring after edge"
- a batch may stop being retained once an earlier tier is reached
- a later authoritative rejection may be lost to the original write site
- `onMutationError` becomes harder to define, because the runtime may have already dropped the batch record that should have produced the error

## Desired semantics

For `insert`, `update`, and `delete`:

- the local mutation applies immediately
- `wait({ tier: "edge" })` resolves as soon as the batch is confirmed at edge
- `wait({ tier: "global" })` resolves only once the batch is confirmed at global
- if the batch is rejected before the requested tier is reached, the wait rejects
- regardless of whether anyone calls `wait`, the runtime keeps tracking the batch until it reaches a terminal authoritative fate
- if that later authoritative fate is rejection, `onMutationError` can still fire

This means an earlier successful wait does not imply the batch is done being tracked.

Example:

1. `db.insert(...)` applies locally
2. caller awaits `handle.wait({ tier: "edge" })`
3. edge confirms, so that promise resolves
4. runtime keeps the batch retained and keeps reconciling it
5. later the authoritative outcome is rejection
6. `onMutationError` still fires unless that rejection was already handled by an unresolved awaited path

## Core model

We need two separate mechanisms.

### 1. Wait threshold

This is per handle / per `wait()` call.

Properties:

- in-memory only
- not persisted across restart
- keyed by `batchId`
- each waiter has its own requested tier
- settles from the current replayable batch settlement

Resolution rule:

- resolve when `latestSettlement.confirmedTier >= requested tier`
- reject on terminal rejection before that happens

### 2. Authoritative retention

This is per tracked batch.

Properties:

- persisted in local batch records
- independent of whether any caller is currently waiting
- survives reconnect and restart
- drives retransmit / reconciliation / late error delivery

Retention rule:

- retain every mutation batch until terminal authoritative fate

Terminal authoritative fate means:

- `Rejected` is terminal
- accepted settlement is terminal only when it reaches the topology's highest authoritative tier

In practice:

- local-only runtimes may treat `worker` as terminal acceptance
- cloud-backed runtimes typically treat `global` as terminal acceptance
- `edge` is not terminal if a higher authoritative tier still exists

## Data-model changes

The current `requested_tier` field on `LocalBatchRecord` should stop representing caller wait intent.

Preferred direction:

- remove `requested_tier` from `LocalBatchRecord`
- stop exposing `requestedTier` on the TypeScript `LocalBatchRecord` surface
- keep wait thresholds in memory with the live handle / client waiter tables

The retained record should instead answer only questions like:

- what batch is this?
- what mode is it?
- what is its latest replayable settlement?
- is it still awaiting terminal authoritative fate?

We do not need to persist per-caller wait thresholds, because promises do not survive restart anyway.

## Runtime-core changes

### Reconciliation predicate

`pending_batch_ids_needing_reconciliation()` should no longer use `record.requested_tier`.

Instead it should keep a batch active while:

- there is no settlement yet
- the latest settlement is `Missing`
- the latest settlement is accepted but still below terminal authoritative acceptance

It should stop actively reconciling once:

- the batch is authoritatively rejected
- the batch is accepted at terminal authoritative tier

### Settlement application

`apply_received_batch_settlement(...)` should keep doing two separate things:

- update the persisted local batch record
- resolve or reject any in-memory waiters whose threshold is now satisfied

Important:

- resolving `wait({ tier: "edge" })` must not prune the local batch record
- reaching a non-terminal accepted tier must not stop reconciliation

### Watchers

The existing in-memory ack watcher concept is still useful, but it should be understood as waiter state only.

It should not control record retention.

No new "tier upgrade" primitive is required for this model. Calling `wait({ tier })` later should attach a new waiter against an already-tracked batch, not mutate the batch's retention policy.

## Client/runtime API changes

### `WriteHandle.wait({ tier })`

`wait({ tier })` should:

- read current replayable batch state first
- resolve immediately if that state already satisfies the threshold
- reject immediately if the batch is already rejected
- otherwise register an in-memory waiter for that `batchId` and tier

Calling `wait({ tier: "global" })` after a prior `wait({ tier: "edge" })` should be valid. The second wait is a stricter readiness threshold, not a retention upgrade.

### `onMutationError`

`onMutationError` is the restart-safe fallback for mutation failures that were not fully handled by an awaited path.

Delivery rule:

- if a rejection arrives while there is an unresolved waiter for that batch, reject the waiter(s)
- if a rejection exists and no unresolved waiter remains to observe it, surface it through `onMutationError`

Important consequence:

- a previously resolved `wait({ tier: "edge" })` does not count as handling a later terminal rejection

That is the whole point of separating readiness from authoritative retention.

### Reuse rejected-batch acknowledgement

The existing rejected-batch acknowledgement path should be reused as the suppression / pruning mechanism after `onMutationError` delivery.

That keeps one source of truth for:

- "this rejected batch should still be surfaced after restart"
- "this rejected batch has already been handled and can be pruned"

## Rust/core impact

This design does not require a new sync protocol primitive just to support `wait({ tier })`.

The main Rust-side change is semantic:

- direct and transactional mutation batches must remain retained until terminal authoritative fate
- waiter thresholds must stop being encoded as batch-retention policy

The protocol already carries tiered settlements and authoritative rejections. The missing piece is keeping batch retention tied to final fate instead of the earliest awaited tier.

## Testing

We should add or update tests at three levels.

### Runtime-core

- `wait(edge)` resolves when settlement reaches edge
- the batch record remains retained after edge confirmation if final fate is still pending
- a later rejection still survives restart until acknowledged
- terminal accepted settlement stops reconciliation

### TypeScript integration

- `insert`, `update`, and `delete` handles all support `wait({ tier })`
- multiple waits on the same handle can observe different thresholds
- late rejection after an earlier edge wait surfaces through `onMutationError`

### Cloud-server integration

- with an upstream server that exposes edge and global tiers, `wait({ tier: "edge" })` resolves before global
- `wait({ tier: "global" })` resolves only at global
- batches are still tracked after edge confirmation until final authoritative settlement

## Required tasks

- [x] Remove the coupling between `requested_tier` and batch retention. `pending_batch_ids_needing_reconciliation()` should reconcile until terminal authoritative fate, not until an arbitrary requested tier is reached.
- [x] Replace persisted `requested_tier` usage in `LocalBatchRecord` with a retained-batch model that is independent of per-caller wait thresholds.
- [x] Keep direct `insert` / `update` / `delete` batches retained until terminal authoritative settlement, even if an earlier wait threshold has already resolved.
- [x] Make `WriteHandle.wait({ tier })` a pure waiter attachment API that never upgrades or mutates batch retention policy.
- [ ] Reuse rejected-batch acknowledgement as the durable suppression mechanism for `onMutationError` after a rejected batch has been surfaced.
- [x] Add runtime-core tests covering edge readiness vs final authoritative fate.
- [ ] Add TypeScript integration tests covering multi-threshold waits and late rejection delivery.
- [ ] Add cloud-server integration tests covering edge/global wait behavior against a real upstream server.
