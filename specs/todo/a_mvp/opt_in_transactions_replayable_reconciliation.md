# Opt-In Transactions, Replayable Reconciliation, and Strict Visibility — TODO (MVP)

This spec is written for readers who know the status quo on `main`.

Today Jazz has one optimistic sync pipeline:

- writes become ordinary object-branch commits
- `PersistenceAck` tracks durability
- `QuerySettled` gates only the first tiered delivery
- reconnect replays active query subscriptions, but does not reconcile pending writes or strict visibility from one coherent source of truth

That leaves three correctness gaps:

1. durable writes can hang forever after a missed live ack
2. `tier: "global"` only reliably gates first delivery
3. there is no clean opt-in transaction model for authority-decided multi-object fate

This MVP assumes the batch-branch substrate from [Batch Branches and Prefix-Indexed Storage](../b_launch/batch_branches_and_prefix_storage.md) has landed first. On top of that substrate, it adds one coherent model for:

- opt-in transactions
- replayable write reconciliation
- persisted accepted/rejected settlement states
- strict query visibility after reconnect and offline restart

## Related

- [Sync Manager — Status Quo](../../status-quo/sync_manager.md)
- [Query/Sync Integration — Status Quo](../../status-quo/query_sync_integration.md)
- [Object Manager — Status Quo](../../status-quo/object_manager.md)
- [Batch Branches and Prefix-Indexed Storage](../b_launch/batch_branches_and_prefix_storage.md)
- [Globally Consistent Transactions](../b_launch/globally_consistent_transactions.md)

## Why this exists

The current status quo on `main` splits related truth across mechanisms that do not reconcile together:

- object history is branch/tip based
- `PersistenceAck` answers write durability only if the live event arrives
- `QuerySettled` acts as a subscription-wide durability high-water mark
- reconnect rebuilds active query subscriptions, but not pending write state
- rejected optimistic writes are not a replayable, persisted terminal state

That creates four user-visible failure modes:

1. **Missed live acks strand durable writes.** If a commit was accepted upstream but the live ack was dropped, the write can remain pending forever.
2. **Tier-gated reads are too coarse.** Once a subscription has ever seen `QuerySettled(Global)`, later remote updates can bypass the requested tier.
3. **Rejection is not a first-class outcome.** Permission denial, session denial, backpressure loss, and authority rejection do not all converge through one persisted replayable path.
4. **There is no opt-in strict write model.** Multi-object correctness, rollback, and restart-safe visibility are sketched separately from the main sync path instead of using one shared batch/history model.

The intent of this spec is not to make every write transactional. It is to keep the current default local-first path for ordinary writes, while making strict correctness available when an app explicitly asks for it.

## Before / After

### Before (`main`)

| Topic                     | Status quo                                                 |
| ------------------------- | ---------------------------------------------------------- |
| History unit              | one branch may have many concurrent tips                   |
| Default write model       | optimistic direct write to visible branch state            |
| Transactional write model | none                                                       |
| Durable completion        | live `PersistenceAck` watcher                              |
| Write rejection           | partial / non-replayable                                   |
| Tier-gated reads          | first delivery only                                        |
| Reconnect                 | replay active queries only                                 |
| Offline restart           | no single persisted source of truth for pending write fate |

### After (`#415` substrate + this spec)

| Topic                     | Proposed MVP                                                                            |
| ------------------------- | --------------------------------------------------------------------------------------- |
| History unit              | explicit batches under a shared prefix                                                  |
| Default write model       | direct visible batches remain default                                                   |
| Transactional write model | explicit opt-in, authority-decided                                                      |
| Durable completion        | replayable batch settlement                                                             |
| Write rejection           | one replayable batch-settlement model                                                   |
| Tier-gated reads          | per visible batch, not per subscription high-water mark                                 |
| Reconnect                 | replay first, snapshot fallback, pending-batch reconciliation                           |
| Offline restart           | persisted local batch records + replayable accepted transactional visible batch members |

## Terms used here

- **replayable**: a fact is replayable if the client can recover it after a dropped live event, reconnect, or restart from durable protocol state such as ordered replay, snapshot fallback, or persisted local records. In other words, it is not "only true if you happened to catch the live callback"
- **logical batch**: one user-visible write unit identified by one `BatchId`. A logical batch may touch multiple objects/branches, so it may materialize as multiple per-object batch members
- **visible batch member**: one per-object `(object_id, branch_name, batch_id)` materialization of a logical batch on the visible prefix
- **visible prefix**: the full user-meaningful branch-prefix namespace introduced by the batch-branch substrate. Concretely, within an `(object_id, branch_name)` lineage, this is the prefix normal reads use, and where direct visible batch members and accepted transactional visible batch members are stored under their shared `BatchId`
- **staging prefix**: a sibling branch-prefix namespace used only for staging transactional batches before acceptance. Ordinary reads ignore it
- **authority**: the first durable upstream node allowed to turn a local batch into replayable truth; this is a responsibility of the existing upstream owner path, not a new server tier introduced by this spec. For transactional batches that same durable upstream node also validates the batch and emits the accepted visible-batch-member set
- **remote visibility**: whether a change is allowed to affect what another runtime, or any non-local subscription result, can see over sync
- **strict transaction visibility**: an opt-in query mode that waits for accepted transactional results to be complete for the query's current local scope before showing them

One master invariant runs through the whole design:

- only visible-prefix batches may affect remote visibility
- staging batches are staging state and optional local overlay state only

## Goals

- Keep ordinary local-first writes as the default write path.
- Add an explicit opt-in transaction path for writes that need authority-decided fate.
- Reuse batch-branch history as the only write-history substrate.
- Separate two questions cleanly:
  - what replayable settlement has this batch reached?
  - if this is an accepted transaction, is it complete for this query's current local scope?
- Make reconnect and offline restart converge from replayable protocol state, not just live event timing.
- Make `tier: "global"` mean the same thing for later deliveries that it means for first delivery.
- Keep optimistic local UX for the writer without letting remote edge-only data bypass strict visibility.

## Non-goals

- No change to the default local-first behavior for writes that do not opt into transactions.
- No distributed or multi-owner transaction authority in the MVP.
- No exact global query completeness.
- No secrecy of row existence or transaction touch sets beyond existing content permissions.
- No ack-the-ack protocol.
- No whole-state hash or Merkle tree as the primary repair mechanism.
- No second history substrate beside batch branches.

## Core decisions

### 1. Batch branches are the prerequisite substrate

This spec assumes [Batch Branches and Prefix-Indexed Storage](../b_launch/batch_branches_and_prefix_storage.md) already exists.

That prerequisite does two important things:

1. it turns "a write" into an explicit `BatchId`
2. it gives staging and visible accepted history the same physical shape

This spec does not add a separate transaction log format or second object-history model on top of that.

### 2. Transactionality is opt-in

Jazz keeps two write modes:

1. **direct visible batches** — default
2. **transactional batches** — explicit opt-in

Direct visible batches preserve the current local-first shape:

- they write directly to the visible prefix
- they do not require authority-decided multi-object acceptance
- they still benefit from replayable reconciliation and fixed tier gating

Transactional batches are for writes that need a stricter contract:

- one logical decision for the whole write
- authoritative accepted or rejected fate
- restart-safe visibility
- rollback on rejection

The exact opt-in API shape is not the important part of this spec. It can be:

- an explicit `transaction(...)` or `runTransaction(...)` API
- an explicit write option selecting transactional mode

What matters is that the stricter mode is chosen deliberately rather than becoming the default for every write.

The MVP should not make applications pay transaction latency, authority dependency, or stricter visibility costs unless they opt in explicitly.

### 3. `BatchId` is the transaction id for transactional writes

For every write, `BatchId` remains the batch identifier from the batch-branch substrate.

The only extra rule is:

- for **direct visible batches**, `BatchId` is just the batch id
- for **transactional batches**, that same `BatchId` also acts as the transaction id

So for a transactional write, there is no second semantic id beside the batch id.

- `BatchId` is the logical transaction id
- the same `BatchId` is reused across every touched prefix/object

This means:

- every transaction is exactly one logical batch id
- one transaction may still materialize as multiple per-prefix batch branches

Example:

- Alice starts transactional batch `B7`
- she touches `todo/1` and `project/9`
- those become separate physical branches that both carry `BatchId = B7`

### 4. One batch-settlement model folds fate and durability together

The status quo spreads write truth across separate concepts. The MVP should collapse that into one replayable settlement model, while still keeping transaction completeness separate.

Every pending local batch, whether direct or transactional, should reconcile through one replayable settlement type:

```text
BatchSettlement =
  Missing { batch_id }
  Rejected { batch_id, code, reason }
  DurableDirect {
    batch_id,
    confirmed_tier,
    visible_members: Vec<VisibleBatchMember>,
  }
  AcceptedTransaction {
    batch_id,
    confirmed_tier,
    visible_members: Vec<VisibleBatchMember>,
  }
```

`VisibleBatchMember` identifies one per-object visible-prefix member of a logical batch:

```text
VisibleBatchMember {
  object_id,
  branch_name,
  batch_id,
}
```

`confirmed_tier` is a property of the whole batch settlement, not of individual commits. For any settled visible batch, its `confirmed_tier` is the minimum confirmed tier reached by its `visible_members`.

This means:

- a single-ref batch behaves exactly like the old intuitive model
- a multi-member batch only reaches tier `T` when every visible batch member in that batch reaches `T`
- if an application wants independent visibility, it should emit independent batches

`Rejected` is terminal. `Missing` is a replayable absence result that tells the client to retransmit the original submission if it still cares about that batch. `DurableDirect` and `AcceptedTransaction` are monotonic replayable states whose `confirmed_tier` may advance over time.

This one `BatchSettlement` model replaces three separate ideas:

- special status for ordinary direct writes
- a separate transactional fate type
- a separate per-commit confirmed-tier stream

### 5. Batch settlement becomes the read-side durability truth

`QuerySettled` should no longer mean "this query permanently achieved tier T".

Instead:

- `QueryFrontierSettled` means "all query updates through sequence N have been emitted"
- `BatchSettlement` means "this visible batch currently exists at confirmed tier T, or was rejected, or is missing"

Read delivery should check the currently visible batches, not a subscription-wide high-water mark.

In plain terms:

- `BatchSettlement.DurableDirect` answers "this direct visible batch exists durably at tier T"
- `BatchSettlement.AcceptedTransaction` answers "this transaction was accepted and its published visible batch members currently sit at tier T"

### 6. Strict transaction visibility is opt-in and has one optional local overlay

Queries and subscriptions keep ordinary behavior by default.

A caller may opt into strict transaction visibility. In that mode:

- only accepted visible transactional results may affect the visible query result
- a transaction is visible only when it is complete for the query's current local scope
- any requested durability tier must be satisfied by the visible accepted batches

Queries that do not opt into this mode keep ordinary visible-prefix behavior. Accepted transactional results become atomic only for queries that explicitly ask for strict transaction visibility.

Strict mode may additionally enable one optional local overlay:

- the current runtime may also show its own pending transactional state locally

That optional overlay is the narrow replacement for today's broad "local updates while waiting" loophole:

- only the author's own local pending transaction may bypass acceptance
- remote edge-only updates must not bypass strict visibility

### 7. Replay first, snapshot fallback

Reconnect should converge from ordered replay when possible, and from compact current truth when replay history is gone.

The protocol should be designed around:

- `last_seen_seq`
- active query replay as desired state
- reconciliation of still-pending writes

Replay remains the fast path. Snapshot fallback remains the correctness path.

### 8. Rejected outcomes survive restart until acknowledged

For both direct visible batches and transactional batches:

- rejected outcomes must survive restart
- they must be queryable after long offline periods
- acknowledged rejections may be compacted and pruned later

This is required for correctness and debuggability. A rejection should not exist only as an ephemeral live callback.

## Write modes

### Direct visible batches (default)

This is the default write mode for today's insert/update/delete APIs.

Behavior:

1. the client creates a new `BatchId` for the visible write
2. writes append directly to the visible prefix
3. local optimistic UX behaves as today
4. the batch remains pending until reconciliation yields one replayable `BatchSettlement`

For direct visible batches, the relevant settled states are:

- `Missing`
- `Rejected`
- `DurableDirect`

`DurableDirect` means:

- this direct visible batch exists durably on the visible prefix
- its current `confirmed_tier` applies to the whole batch
- the write is no longer in the "maybe dropped before publication" state

Durable completion for a direct visible batch requires:

1. `BatchSettlement.DurableDirect`
2. `confirmed_tier >= requested_tier`

If a direct visible batch spans multiple visible batch members, its batch `confirmed_tier` is the minimum across those members. Apps that want one row to become visible independently of another should issue separate batches.

### Transactional batches (explicit opt-in)

This is the stricter write mode.

Behavior:

1. the client explicitly starts a transactional batch
2. all staged row changes land on staging prefixes
3. ordinary readers do not include staging prefixes
4. the authority validates the batch against its captured frontier
5. the authority emits one replayable `BatchSettlement`
6. if accepted, the authority creates accepted visible batch members
7. if rejected, the staging batches never become visible and local pending state rolls back

Because transactionality is opt-in:

- ordinary writes keep current latency/availability semantics
- the authority is only on the path for writes that asked for transactional guarantees

### Transactional batch lifecycle at a glance

For a successful transactional batch, the end-to-end shape is:

1. create one `BatchId`
2. stage changes on staging prefixes carrying that `BatchId`
3. ask the authority to validate and decide that batch
4. receive `BatchSettlement.AcceptedTransaction { batch_id, confirmed_tier, visible_members }`
5. wait for the accepted visible batch members in `visible_members` to become locally present and for `confirmed_tier` to satisfy any requested tier

For a rejected transactional batch, the shape is shorter:

1. create one `BatchId`
2. stage local changes on the staging prefix
3. receive `BatchSettlement.Rejected`
4. roll back the local pending view and retain the rejection across restart until acknowledged

## Batch settlement semantics

Semantics for the unified `BatchSettlement` model:

- `Missing`: the authority has no durable record of this batch; the client must retransmit the original direct or transactional submission
- `Rejected`: the batch was refused before or during authoritative apply
- `DurableDirect`: a direct visible batch exists durably on the visible prefix and currently has batch `confirmed_tier = T`
- `AcceptedTransaction`: the authority accepted a transactional batch, published the authoritative visible batch members, and those visible batch members currently have batch `confirmed_tier = T`

`Rejected` covers cases such as:

- permission denied
- session required
- catalogue write denied

### Accepted Visible Batch Member Metadata

Every accepted transactional visible batch member should carry enough metadata to mark it as an accepted transaction output rather than an ordinary direct visible batch member.

Minimum metadata:

- `tx_role=accepted_transaction_output`

This is needed for two reasons:

1. after restart, the runtime must be able to map a visible batch member back to accepted-transaction semantics rather than treating it like an ordinary direct visible batch member
2. `BatchSettlement.AcceptedTransaction { visible_members }` and the visible-prefix history must agree about which visible batch members belong to the accepted transaction

`AcceptedTransaction { visible_members }` remains the authoritative replayable settlement. The accepted visible-batch-member metadata exists so the visible-prefix history itself still carries transaction attribution after persistence and reload.

## Local persisted records

Each runtime with durable local storage should persist one record for each still-relevant local batch:

```text
LocalBatchRecord {
  batch_id,
  mode: Direct | Transactional,
  requested_tier,
  latest_settlement,
}
```

High-level state machine:

```text
Pending
  -> Missing
  -> Rejected
  -> DurableDirect(tier) + waiting for requested tier
  -> AcceptedTransaction(tier) + waiting for requested tier / completeness
```

Persisted local records exist to support:

- reconnect reconciliation
- restart-safe rejection handling
- rollback of rejected optimistic state
- user-facing outcome inspection and acknowledgement

## Query visibility

### Ordinary queries

Ordinary queries keep today's overall shape:

- they read visible prefixes
- they ignore staging prefixes
- they may still request a durability tier

The important fix is that durability is checked per visible batch, not per subscription high-water mark.

A later remote update must not become visible until every visible batch for that delivery satisfies the requested tier.

This is intentionally batch-wide. If a query sees any part of batch `B`, it gates on batch `B`'s current `confirmed_tier`, not just the specific visible ref.

Ordinary queries do **not** get transactional completeness guarantees. If an accepted transactional batch reaches the visible prefix and its batch `confirmed_tier` satisfies any requested tier, ordinary queries may observe it like any other visible update.

### Strict transaction visibility (opt-in)

Strict transaction visibility adds one more rule on top of ordinary public visibility:

- accepted transaction results are only visible when complete for the query's current local scope

The MVP completeness rule stays the one from the earlier transaction work:

- `complete_for_current_local_scope`

Definition:

1. compute the query's current local contributing scope
2. intersect that scope with the batch's accepted `visible_members`
3. the transaction is complete for that query only when every intersecting accepted visible batch member is locally present

This is intentionally weaker than exact global query completeness.

It is still strong enough to guarantee:

- no partial accepted transaction visibility inside the query's current local scope
- restart-safe re-derivation from persisted accepted visible batch members

### Optional local pending overlay

If the caller opts into the local pending overlay, the runtime may overlay its own local pending transactional state before authority acceptance.

That exception is intentionally narrow:

- it applies only to the current runtime's own pending transactional writes
- it does not make remote pending state visible
- it does not let remote edge-only updates bypass durability or transaction completeness

## Read-side provenance requirements

To make strict visibility concrete, the query layer needs two pieces of shadow state for visible output:

1. which visible `BatchId`s currently contribute to this row/tuple
2. which objects/branches are in the current contributing scope

This replaces per-visible-commit tracking with per-visible-batch tracking.

Single-table queries can often derive visible batch ids directly from the row version. Joins, array subqueries, and other derived outputs must union the visible batch ids of every contributing tuple element.

This shadow provenance is internal delivery state. It does not need to become a public row shape in the MVP.

## Reconnect and reconciliation

Reconnect has two separate responsibilities:

1. rebuild desired query state upstream
2. repair missed write settlement and strict-visibility state

Those should stay separate.

### Step 1: replay active queries as desired state

This remains as it works today:

- active query subscriptions are replayed on reconnect
- the server rebuilds current scope and query execution state

### Step 2: reconcile still-pending local batches

After query replay, the client sends:

```text
ResumeSync {
  last_seen_seq,
  pending_batches: Vec<PendingBatchRef>,
}

PendingBatchRef {
  batch_id,
  mode: Direct | Transactional,
}
```

The server responds in one of two modes.

### Replay path

If the replay window still covers `last_seen_seq + 1 .. current`, the server replays:

- `ObjectUpdated`
- `QueryFrontierSettled`
- `BatchSettlement`

### Snapshot fallback

If the replay window is gone, the server sends compact current truth:

- the current query frontier for active queries
- current `BatchSettlement` for the visible batches in that frontier
- `BatchSettlement` for each `pending_batches` entry not already covered by the frontier state

The client then:

- resolves direct batches waiting only on tier
- resolves accepted transactional batches waiting on completeness and tier
- retransmits `Missing` batches
- fails `Rejected` batches
- re-checks strict query visibility using the now-current frontier and accepted visible batch members

## Before / After flow sketches

### Scenario 1: ordinary durable write, live ack dropped

**Today on `main`**

```text
Alice writes visible batch B1 with tier=global
  -> visible write sent upstream
  -> durable waiter watches for live PersistenceAck

PersistenceAck(B1, global) is dropped

Reconnect:
  -> active queries replay
  -> nothing reconciles B1
  -> waiter hangs forever
```

**After this spec**

```text
Alice writes visible batch B1 with tier=global
  -> LocalBatchRecord(B1, mode=Direct, Pending)

Live BatchSettlement.DurableDirect(B1, confirmed_tier=global, visible_members=[...]) is dropped

Reconnect:
  -> replay active queries
  -> ResumeSync(last_seen_seq=N, pending_batches=[{B1, Direct}])

Server replies:
  -> BatchSettlement.DurableDirect(B1, confirmed_tier=global, visible_members=[...]) via replay or snapshot

Alice:
  -> marks B1 durably published
  -> sees B1 settled at global
  -> resolves waiter
```

### Scenario 2: tier-gated query delivery

**Today on `main`**

```text
Alice subscribes with tier=global

QuerySettled(Global) arrives once
  -> achieved_tiers = { Global }

Later Bob writes C2 on Alice's edge only
  -> delivery gate sees achieved_tiers already satisfied
  -> Alice sees C2 before Global confirmation
```

**After this spec**

```text
Alice subscribes with tier=global

QueryFrontierSettled(through_seq=41) arrives
BatchSettlement.DurableDirect(B1, confirmed_tier=global, visible_members=[...]) arrives
  -> first delivery allowed

Later Bob writes batch B2 on Alice's edge only
  -> visible batches now include B2
  -> delivery gate checks settlement(B2).confirmed_tier
  -> B2 held back until confirmed at global
```

### Scenario 3: opt-in transaction accepted

**Today on `main`**

```text
Alice wants one strict write touching todo/1 and project/9

Only direct visible optimistic writes exist
  -> partial visibility and rollback semantics are ad hoc
  -> reconnect cannot re-derive one authority-owned fate record
```

**After this spec**

```text
Alice starts transactional batch B7
  -> writes stage on staging prefixes for todo/1 and project/9
  -> strict mode + local pending overlay lets Alice see her own pending state

Authority validates B7
  -> BatchSettlement.AcceptedTransaction(
       B7,
       confirmed_tier=edge,
       visible_members=[todo member, project member],
     )

Bob runs a strict transaction-visible query
  -> Bob sees nothing until:
     - AcceptedTransaction is present
     - accepted visible batch members relevant to Bob's local query scope are present
     - the batch's confirmed_tier satisfies any requested tier
```

### Scenario 4: opt-in transaction rejected after optimistic local work

**Today on `main`**

```text
Alice makes an optimistic multi-object change
  -> no first-class persisted rejected tx outcome
  -> restart may lose the rejection surface
  -> rollback is not one coherent replayable rule
```

**After this spec**

```text
Alice starts transactional batch B8
  -> local pending view visible only to Alice via the optional local pending overlay

Authority rejects B8
  -> BatchSettlement.Rejected(B8, code=permission_denied)

Alice runtime:
  -> rolls back pending staging view
  -> persists rejected outcome
  -> any dependent local pending txs may be superseded
  -> rejection survives restart until acknowledged
```

## Restart semantics

After restart, a durable runtime should be able to reconstruct:

- still-pending direct visible batches
- still-pending transactional batches
- visible batch settlements and their current confirmed tiers
- accepted transaction visible batch members
- rejected outcomes awaiting acknowledgement

What it should **not** need:

- a live connection
- replay of every historical event
- a second hidden transaction store unrelated to batch-branch history

This is why the MVP needs:

- persisted `LocalBatchRecord`s
- replayable `BatchSettlement`s

## Rabbit Holes

- Exact naming and encoding of staging prefixes on top of the batch-branch substrate.
- Efficiently computing `complete_for_current_local_scope` for joins, arrays, and recursive query shapes.
- Re-triggering delivery when tier state changes without row bytes changing.
- Deciding whether batch confirmed tier should always be emitted live or can sometimes be synthesized only from replay/snapshot state.
- Garbage collection of rejected staging branches without losing debuggability too early.
- Single durable owner semantics when a browser app has both a persistent worker runtime and a memory main-thread mirror.

## No-gos

- No change that makes all writes transactional by default.
- No second transaction id beside `BatchId` for transactional writes.
- No subscription-wide durability watermark as the read-side source of truth.
- No exact global atomic visibility guarantees for every query shape.
- No distributed authority placement, leases, or multi-owner consensus in the MVP.
- No transport rewrite just to support this design.

## Testing Strategy

Prefer RuntimeCore and SchemaManager integration tests with realistic actors and explicit flow sketches.

- `alice` writes a direct visible batch with `tier: "global"`, the live tier update is dropped, reconnect happens, and the write resolves from replay or snapshot rather than hanging.
- `alice` and `carol` subscribe with `tier: "global"` on different edges, `bob` writes one direct visible batch on one edge, and neither sees that batch until its batch settlement reaches global.
- `alice` starts transactional batch `B7` touching two objects, the authority accepts it, and a strict transaction-visible subscription only sees the accepted result after `complete_for_current_local_scope` is satisfied.
- `alice` starts transactional batch `B8`, the authority rejects it, the local pending view rolls back, and the rejected outcome survives restart until acknowledged.
- a reconnect within the replay window replays missed `ObjectUpdated`, `QueryFrontierSettled`, and `BatchSettlement` events without needing a full snapshot.
- a reconnect after the replay window expires falls back to a frontier snapshot plus pending-batch reconciliation and still converges.
- the optional local pending overlay shows Alice her own pending transactional edits locally, while Bob never sees those pending edits and still waits for accepted visible batch members.

## Planning summary

The MVP should be shaped as one design with two write modes over one shared batch-history substrate:

1. **direct visible batches remain the default**
   - local-first
   - replayable `BatchSettlement`
   - fixed per-batch tier gating

2. **transactional batches are explicit opt-in**
   - `BatchId` also acts as tx id
   - authority emits `AcceptedTransaction` or `Rejected` inside `BatchSettlement`
   - accepted visible batch members drive strict query visibility
   - rejected outcomes roll back and survive restart

This keeps the everyday local-first path simple while giving applications one coherent stricter path when they explicitly need it.
