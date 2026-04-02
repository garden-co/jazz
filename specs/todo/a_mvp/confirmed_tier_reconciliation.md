# Confirmed Tier Reconciliation and Replayable Query Settlement — TODO (MVP)

Two current durability bugs share the same root problem: we treat tier confirmation as an edge-triggered event instead of replayable state. On the read side, [Durability Tier Gating After First Delivery](./durability_tier_gating_after_first_delivery.md) shows that query durability is tracked as a subscription-wide high-water mark, so later updates bypass the requested tier. On the write side, a missed live `PersistenceAck` can leave a durable write pending forever even after the row is visible at `tier: "global"` to a fresh client. In both cases, we are missing one durable fact: for the commits a client knows about, what is the highest confirmed tier?

## Problem

The current status quo splits related state across mechanisms that do not reconcile together:

- Query scope tracks reachability only: `(object_id, branch_name)` pairs.
- `sent_tips` tracks what the sender believes it has already sent to a client.
- `QuerySettled` acts as a query-wide durability gate for first delivery.
- `PersistenceAck` acts as a commit-level durability signal for writes.

That leaves two holes:

1. **Read durability is too coarse.** `achieved_tiers` is stored on the subscription, not on the visible versions being delivered. Once a subscription has ever seen `GlobalServer`, later incremental updates can slip through before their commits reach that tier.
2. **Write durability is too fragile.** Durable completion depends on observing a live `PersistenceAck` event. If the event is dropped during connect, reconnect, or stream lag, the write can remain pending with no idempotent recovery path.

The correction to the status quo is important: we already keep per-client `sent_tips`, but that is sender bookkeeping, not client-confirmed known state. It helps incremental sync, but it does not tell us what the client definitely knows, and it does not carry confirmed-tier provenance.

## Solution

Promote `confirmed_tier` to authoritative per-commit state, and make stream resume/replay first-class.

### State model

Keep two kinds of replicated state distinct:

- **Frontier state**: for each `(object_id, branch_name)`, which commit tips define the client's current synced frontier?
- **Provenance state**: for each `commit_id`, what is the highest `confirmed_tier` known so far?

Recommended invariants:

- `confirmed_tier` is monotonic: `Worker < EdgeServer < GlobalServer`.
- Merge rule is `max(local, remote)`.
- Query durability checks read the visible commits' `confirmed_tier`, not a subscription-wide high-water mark.
- Durable write completion reads the written commit's `confirmed_tier`, not just a transient ack watcher.

### Query settlement semantics

Keep a query completeness signal, but narrow its meaning.

Today `QuerySettled { query_id, tier, through_seq }` mixes two concerns:

- "the upstream query has settled through this stream frontier"
- "the whole query has achieved tier T"

The spec should separate them:

- **Query frontier settled** means: all sync updates for this query through sequence `N` have been emitted.
- **Commit confirmed tier** means: each specific commit has advanced to a tier.

That lets first delivery mean:

1. the query frontier is settled through `N`
2. the client has processed all relevant object updates through `N`
3. every visible non-local commit in that settled result satisfies the requested tier

After first delivery, later deliveries use the same per-commit rule. There is no permanent "query already reached global once" bypass.

`local_updates = Immediate` stays as the existing carve-out for optimistic local updates. It remains an explicit exception, not a side effect of stale tier state.

### Protocol changes

Use the existing per-client stream sequence machinery as the backbone for reconciliation.

The cloud server already emits ordered per-client sync sequences. Formalize that into resume semantics:

```text
client connects
  -> server sends Connected { client_id, next_sync_seq, catalogue_state_hash }

client replays active QuerySubscription desired state
  -> existing reconnect behavior rebuilds server-side query scopes

client resumes
  -> client sends ResumeSync {
       last_seen_seq,
       pending_commit_ids
     }

server responds
  -> replay missed sync events since last_seen_seq
     OR
  -> send compact snapshots for current query frontier + pending commit tiers
```

Recommended message roles:

- `ObjectUpdated`: unchanged meaning; still carries object/branch commit frontier deltas
- `PersistenceAck`: commit-level `confirmed_tier` advancement; no longer the only source of truth for durable completion
- `QuerySettled`: query frontier completeness only; no query-wide durability watermark
- `ResumeSync`: client anti-entropy request after connect or reconnect

`ResumeSync` should include:

- `last_seen_seq`: highest stream sequence the client has fully processed
- `pending_commit_ids`: durable writes still waiting for confirmation

The existing active-query replay on reconnect stays in place. `ResumeSync` complements it by reconciling missed stream state after the desired subscriptions have been re-established.

### Replay and snapshot strategy

Prefer replay. Fall back to snapshots.

#### Replay path

If the server still has an outbound replay window covering `last_seen_seq + 1 .. current`, it replays missed:

- `ObjectUpdated`
- commit confirmed-tier updates
- query frontier-settled notifications

This is the fast path and keeps reconnect cheap.

#### Snapshot fallback

If the replay window is gone, the server sends compact current truth:

- For each currently active query scope, the current `(object_id, branch_name, tip_commit_ids...)`
- For the frontier commits of those objects, the current `confirmed_tier`
- For `pending_commit_ids`, the current `confirmed_tier` even if those commits are no longer frontier tips

This avoids replaying whole histories and still gives the client enough state to converge.

### Why not a global hash?

Not in the MVP.

A single hash over "everything the client knows" is good for equality checks, but bad for repair:

- a mismatch does not explain what changed
- commit-tier advancements can change without changing object tips
- the server still needs replay or snapshot machinery to heal divergence

If we want a fast skip later, we can add a frontier digest as an optimization on top of replay/snapshot. It should not be the only reconciliation mechanism.

## Flow Sketches

### Read-side tiered query

```text
Alice subscribes with tier=global
  -> Edge forwards query upstream
  -> Global evaluates query and emits:
       ObjectUpdated(...)
       CommitConfirmedTier(commit=R1, tier=global)
       QueryFrontierSettled(query=Q, through_seq=41)
  -> Edge replays those to Alice in order
  -> Alice delivers Q only after:
       frontier settled through 41
       visible commit R1 is known
       R1.confirmed_tier >= global
```

### Write-side durable insert with missed live ack

```text
Alice insertDurable(..., tier=global)
  -> local runtime records pending_commit_id = C9
  -> live PersistenceAck(C9, global) is dropped
  -> stream reconnects
  -> Alice replays active QuerySubscription messages
  -> Alice sends ResumeSync(last_seen_seq=88, pending_commit_ids=[C9])
  -> server cannot replay old event, so it snapshots:
       CommitTierSnapshot(C9 -> global)
  -> Alice resolves the durable write from reconciled state
```

## Rabbit Holes

- Do not collapse query completeness and durability back into one signal. We still need a completeness marker for "have I seen the whole settled frontier yet?"
- Be explicit about which commit ids gate row delivery. Auth filtering, lenses, and row materialization can obscure the row-version identity if we only speak in terms of "rows."
- Replay windows need bounded retention. The fallback snapshot path must work even when the replay buffer has already rolled over.
- Client identity persistence matters. `last_seen_seq` is only useful if reconnects reuse the same logical client id where appropriate.
- The optimistic local-updates exception must remain narrow. We should not accidentally create an `allow_edge_while_waiting` loophole.

## No-gos

- No ack-the-ack handshake.
- No Merkle tree or whole-state hash as the primary correctness mechanism.
- No exact-once transport guarantee in the MVP.
- No broad transport rewrite to WebSockets or HTTP/2 just to support this design.
- No subscription-wide `achieved_tiers` replacement with another coarse high-water mark.

## Testing Strategy

Prefer integration tests that read like real sync usage:

- `alice` writes with `insertDurable(..., { tier: "global" })`, the live ack is dropped, reconnect happens, and the write resolves from replay or snapshot.
- `alice` and `carol` both subscribe with `tier: "global"` on different edges, `bob` writes on one edge, and neither reader sees the update until the visible commit reaches global.
- reconnect within the replay window replays missed `ObjectUpdated`, commit-tier updates, and query-settled notifications without a full snapshot.
- reconnect after the replay window expires falls back to a frontier snapshot and still converges.
- `local_updates = Immediate` still shows Alice her own optimistic local change before global, while Bob's remote edge-only update remains gated.

## Related

- [Durability Tier Gating After First Delivery](./durability_tier_gating_after_first_delivery.md)
- [Sync Protocol Reliability & Unification](../../../todo/ideas/1_mvp/sync-protocol-reliability.md)
- [Query/Sync Integration — Status Quo](../../status-quo/query_sync_integration.md)
