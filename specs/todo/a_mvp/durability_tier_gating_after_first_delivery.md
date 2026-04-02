# Durability Tier Gating After First Delivery — TODO (MVP)

Read durability tier only gates the first delivery. After the initial `QuerySettled` unblocks a subscription, all subsequent incremental updates bypass the tier check — breaking the consistency guarantee that `tier: "global"` implies.

## The problem

`achieved_tiers` is a `HashSet<DurabilityTier>` on each subscription. It accumulates tiers from `QuerySettled` notifications and is never cleared. `QuerySettled` is emitted exactly once per subscription (guarded by `if !sub.settled_once`). Once the required tier is present in `achieved_tiers`, the gate at the top of the settlement loop is permanently open:

```rust
let tier_satisfied = match &subscription.durability_tier {
    None => true,
    Some(required) => subscription.achieved_tiers.iter().any(|t| t >= required),
};
```

This means: after the first globally-confirmed delivery, `tier_satisfied` is always `true`. New writes that have only reached the local edge are delivered immediately, even though they haven't propagated to the required tier.

> `query_manager/manager.rs:1048-1062` (tier gate)
> `query_manager/manager.rs:879-886` (achieved_tiers accumulation)
> `query_manager/server_queries.rs:1068-1072` (one-shot QuerySettled emission)

## Scenario

Three clients — Alice and Bob on Edge-1, Carol on Edge-2. All three subscribe with `tier: "global"`.

1. Alice subscribes on Edge-1. Her subscription is correctly gated — she waits for `QuerySettled` from Global before seeing any data. Her `achieved_tiers` now contains `{Global}`.
2. Carol subscribes on Edge-2. Same behaviour — correctly gated, waits for `QuerySettled` from Global.
3. Bob writes X which propagates to Edge-1. X is available on Edge-1 but has not yet propagated to Global.
4. Alice sees X. Because her `achieved_tiers` already contains `Global` from step 1, the tier gate is permanently open. She receives Bob's write as soon as it settles on Edge-1 — before Global has confirmed it.
5. X eventually propagates from Edge-1 to Global, then from Global to Edge-2.
6. Carol finally sees X.

Result: Alice and Carol both requested `tier: "global"`, but Alice saw X before it reached Global while Carol correctly waited. The tier contract held for the first delivery but was violated on every subsequent update.

Note: the bug is on the read side, as soon as Bob's update hits `Edge-1`, it is visible to Alice, even if it never reaches `Global`, or represents an interim state which never exists as a `Global` state due to conflict resolution with clients from other edges. This is indepenent of the durability Bob requests for his write.

## What the gate *should* mean

`tier: "global"` on a subscription is a read-consistency contract: "don't show me data that hasn't been confirmed at the global tier." This should hold for every delivery, not just the first.

The current design treats `achieved_tiers` as a high-water mark ("has this subscription *ever* been confirmed at this tier?"). The semantic it needs to express is per-delivery: "is the data *being delivered right now* confirmed at this tier?"

## Related mechanisms

**`allow_local_while_waiting`** — There's an intentional carve-out (manager.rs:1053-1057) that delivers local write-driven updates before tier satisfaction, gated by `settled_once && local_updates == Immediate && has_pending_local_updates`. This is by design (sync_manager.md:181). Whatever fix is applied to the tier gate should preserve this exception.

**`PersistenceAck`** — The write-side durability signal already works per-commit: each commit tracks which tiers have acknowledged it individually. The read side has no equivalent per-update mechanism — `QuerySettled` is one-shot and `achieved_tiers` is monotonic.

**Forwarding** — `SyncManager::forward_update_to_clients()` (forwarding.rs:38-94) filters by scope membership only, not durability requirements. This is by design — forwarding answers *who* gets an update, tier gating answers *when* a subscription delivers it. These are separate concerns.

## Secondary concern: potential double-delivery

If `propagation: "full"` is set, Edge-1 delivers Bob's write locally, then may receive it back from Global after propagation. Commit deduplication in the object layer likely prevents duplicate storage, but there's no guarantee at the subscription delivery level that the same row update isn't emitted twice.

## Open questions

- What granularity should per-update tier tracking use — per-commit, per-object, or per-settlement-batch?
- How does this interact with the `allow_local_while_waiting` exception — should we offer an `allow_edge_while_waiting`? I propose no, because the purpose of `allow_local_while_waiting` is primarily to enable optimistic UI updates, not to bypass the durability guarantee requested as part of the subscription set-up.

> Related: [Sync Manager — QuerySettled](../../status-quo/sync_manager.md#querysettled-read-durability-signal)
> Related: [Confirmed Tier Reconciliation and Replayable Query Settlement](./confirmed_tier_reconciliation.md)
> Related: [Globally Consistent Transactions](../b_launch/globally_consistent_transactions.md)
