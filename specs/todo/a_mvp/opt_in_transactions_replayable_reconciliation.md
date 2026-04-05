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
- persisted accepted/rejected outcomes
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
| Default write model       | optimistic direct write to public branch                   |
| Transactional write model | none                                                       |
| Durable completion        | live `PersistenceAck` watcher                              |
| Write rejection           | partial / non-replayable                                   |
| Tier-gated reads          | first delivery only                                        |
| Reconnect                 | replay active queries only                                 |
| Offline restart           | no single persisted source of truth for pending write fate |

### After (`#415` substrate + this spec)

| Topic                     | Proposed MVP                                                                 |
| ------------------------- | ---------------------------------------------------------------------------- |
| History unit              | explicit batches under a shared prefix                                       |
| Default write model       | direct public batches remain default                                         |
| Transactional write model | explicit opt-in, authority-decided                                           |
| Durable completion        | replayable batch outcome + confirmed tier                                    |
| Write rejection           | one replayable batch-outcome model                                           |
| Tier-gated reads          | per visible commit, not per subscription high-water mark                     |
| Reconnect                 | replay first, snapshot fallback, pending-batch reconciliation                |
| Offline restart           | persisted local batch records + replayable accepted transactional merge refs |

## Terms used here

- **public prefix**: the ordinary reader-visible prefix where direct writes and accepted transactional merges live
- **tx-private prefix**: a staging prefix used only for transactional batches before acceptance
- **authority**: the first durable upstream node allowed to turn a local batch into replayable truth; this is a responsibility of the existing upstream owner path, not a new server tier introduced by this spec. For transactional batches that same durable upstream node also validates the batch and emits the accepted merge set
- **strict transaction visibility**: an opt-in query mode that waits for accepted transactional results to be complete for the query's current local scope before showing them

One master invariant runs through the whole design:

- only public commits participate in remote visibility
- tx-private commits are staging state and optional local overlay state only

## Goals

- Keep ordinary local-first writes as the default write path.
- Add an explicit opt-in transaction path for writes that need authority-decided fate.
- Reuse batch-branch history as the only write-history substrate.
- Separate three questions cleanly:
  - was this write accepted?
  - how durable is the accepted result?
  - is this accepted transaction complete for this query's current local scope?
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
2. it gives transaction-private and public accepted history the same physical shape

This spec does not add a separate transaction log format or second object-history model on top of that.

### 2. Transactionality is opt-in

Jazz keeps two write modes:

1. **direct public batches** — default
2. **transactional batches** — explicit opt-in

Direct public batches preserve the current local-first shape:

- they write directly to the ordinary public prefix
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

For a transactional write, there is no second semantic id beside the batch id.

- `BatchId` is the logical transaction id
- the same `BatchId` is reused across every touched prefix/object

This means:

- every transaction is exactly one logical batch id
- one transaction may still materialize as multiple per-prefix batch branches

Example:

- Alice starts transactional batch `B7`
- she touches `todo/1` and `project/9`
- those become separate physical branches that both carry `BatchId = B7`

### 4. One fate model, separate durability and completeness

The status quo mixes or conflates these concepts. The MVP should keep them distinct:

- **fate**: what replayable outcome did the authority report for this batch?
- **durability**: for accepted public commits, what is the highest `confirmed_tier`?
- **completeness**: for an accepted transaction, is it complete for this query's current local scope?

Every pending local batch, whether direct or transactional, should reconcile through one replayable outcome type:

```text
BatchOutcome =
  Missing { batch_id }
  Rejected { batch_id, code, reason }
  AcceptedDirect { batch_id }
  AcceptedTransaction { batch_id, merges: Vec<MergeRef> }
```

`MergeRef` identifies one authoritative accepted public merge commit:

```text
MergeRef {
  object_id,
  branch_name,
  batch_id,
  commit_id,
}
```

These answer different questions:

- a batch can have a replayable accepted outcome before its visible public commits reach `Global`
- a transactional batch can have an accepted outcome before a given query has all relevant accepted merges locally
- a dropped batch can be `Missing` even though the local client believes it was sent

This one `BatchOutcome` model replaces two separate reconciliation languages:

- a special status type for ordinary direct writes
- a separate transactional fate type

### 5. Per-commit confirmed tier becomes the read-side durability truth

`QuerySettled` should no longer mean "this query permanently achieved tier T".

Instead:

- `QueryFrontierSettled` means "all query updates through sequence N have been emitted"
- `CommitConfirmedTier` means "this accepted public commit is confirmed at tier T"

Read delivery should check the currently visible commits, not a subscription-wide high-water mark.

In plain terms:

- `BatchOutcome.Accepted*` answers "did the authority durably accept this batch?"
- `CommitConfirmedTier` answers "how far has each accepted public commit advanced through the durability lattice?"

### 6. Strict transaction visibility is opt-in and has one optional local overlay

Queries and subscriptions keep ordinary behavior by default.

A caller may opt into strict transaction visibility. In that mode:

- only accepted public transactional results may affect the visible query result
- a transaction is visible only when it is complete for the query's current local scope
- any requested durability tier must be satisfied by the visible accepted commits

Queries that do not opt into this mode keep ordinary public-prefix behavior. Accepted transactional results become atomic only for queries that explicitly ask for strict transaction visibility.

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

For both direct public batches and transactional batches:

- rejected outcomes must survive restart
- they must be queryable after long offline periods
- acknowledged rejections may be compacted and pruned later

This is required for correctness and debuggability. A rejection should not exist only as an ephemeral live callback.

## Write modes

### Direct public batches (default)

This is the default write mode for today's insert/update/delete APIs.

Behavior:

1. the client creates a new public `BatchId`
2. writes append directly to the ordinary public prefix
3. local optimistic UX behaves as today
4. the batch remains pending until reconciliation yields one replayable `BatchOutcome`

For direct public batches, the relevant outcomes are:

- `Missing`
- `Rejected`
- `AcceptedDirect`

`AcceptedDirect` does **not** mean "globally durable". It only means:

- the authority durably knows this public batch
- the write is no longer in the "maybe dropped before acceptance" state

Durable completion for a direct public batch requires:

1. `AcceptedDirect`
2. every written public commit reaching the caller's requested `confirmed_tier`

### Transactional batches (explicit opt-in)

This is the stricter write mode.

Behavior:

1. the client explicitly starts a transactional batch
2. all staged row changes land on tx-private prefixes
3. ordinary readers do not include tx-private prefixes
4. the authority validates the batch against its captured frontier
5. the authority emits one terminal `BatchOutcome`
6. if accepted, the authority creates accepted public merge batches
7. if rejected, the tx-private batches never become public and local pending state rolls back

Because transactionality is opt-in:

- ordinary writes keep current latency/availability semantics
- the authority is only on the path for writes that asked for transactional guarantees

### Transactional batch lifecycle at a glance

For a successful transactional batch, the end-to-end shape is:

1. create one `BatchId`
2. stage changes on tx-private prefixes carrying that `BatchId`
3. ask the authority to validate and decide that batch
4. receive `BatchOutcome.AcceptedTransaction { batch_id, merges }`
5. wait for the accepted public merge commits in `merges` to become locally present and reach any requested `confirmed_tier`

For a rejected transactional batch, the shape is shorter:

1. create one `BatchId`
2. stage local tx-private changes
3. receive `BatchOutcome.Rejected`
4. roll back the local pending view and retain the rejection across restart until acknowledged

## Authority outcomes

Semantics for the unified `BatchOutcome` model:

- `Missing`: the authority has no durable record of this batch; the client must retransmit the original direct or transactional submission
- `Rejected`: the batch was refused before or during authoritative apply
- `AcceptedDirect`: the authority durably knows a direct public batch
- `AcceptedTransaction`: the authority accepted a transactional batch and returns the authoritative accepted public merge set

`Rejected` covers cases such as:

- permission denied
- session required
- catalogue write denied

### Accepted merge commit metadata

Every accepted public merge commit should carry enough metadata to point back at the originating transactional batch.

Minimum metadata:

- `tx_batch_id`
- `tx_role=accepted_merge`

This is needed for two reasons:

1. after restart, the runtime must be able to map a visible accepted public commit back to its transaction
2. `BatchOutcome.AcceptedTransaction { merges }` and the public commit graph must agree about which commits belong to which accepted transaction

`AcceptedTransaction { merges }` remains the authoritative fate record. The accepted merge commit metadata exists so the public history itself still carries transaction attribution after persistence and reload.

## Local persisted records

Each runtime with durable local storage should persist one record for each still-relevant local batch:

```text
LocalBatchRecord {
  batch_id,
  mode: Direct | Transactional,
  requested_tier,
  latest_outcome,
}
```

High-level state machine:

```text
Pending
  -> Missing
  -> Rejected
  -> AcceptedDirect + waiting for tier
  -> AcceptedTransaction + waiting for tier / completeness
```

Persisted local records exist to support:

- reconnect reconciliation
- restart-safe rejection handling
- rollback of rejected optimistic state
- user-facing outcome inspection and acknowledgement

## Query visibility

### Ordinary queries

Ordinary queries keep today's overall shape:

- they read public prefixes
- they ignore tx-private prefixes
- they may still request a durability tier

The important fix is that durability is checked per visible commit, not per subscription high-water mark.

A later remote update must not become visible until the visible public commits for that delivery satisfy the requested tier.

Ordinary queries do **not** get transactional completeness guarantees. If an accepted transactional merge reaches a public prefix and satisfies any requested tier, ordinary queries may observe it like any other public update.

### Strict transaction visibility (opt-in)

Strict transaction visibility adds one more rule on top of ordinary public visibility:

- accepted transaction results are only visible when complete for the query's current local scope

The MVP completeness rule stays the one from the earlier transaction work:

- `complete_for_current_local_scope`

Definition:

1. compute the query's current local contributing scope
2. intersect that scope with the batch's accepted `merges`
3. the transaction is complete for that query only when every intersecting accepted merge is locally present

This is intentionally weaker than exact global query completeness.

It is still strong enough to guarantee:

- no partial accepted transaction visibility inside the query's current local scope
- restart-safe re-derivation from persisted accepted merge refs

### Optional local pending overlay

If the caller opts into the local pending overlay, the runtime may overlay its own local pending transactional state before authority acceptance.

That exception is intentionally narrow:

- it applies only to the current runtime's own pending transactional writes
- it does not make remote pending state visible
- it does not let remote edge-only updates bypass durability or transaction completeness

## Read-side provenance requirements

To make strict visibility concrete, the query layer needs two pieces of shadow state for visible output:

1. which accepted public commit ids are currently visible for this row/tuple
2. which objects/branches are in the current contributing scope

This is broader than today's row-level `commit_id` field.

Single-table queries can often derive visible accepted commit ids directly from the row version. Joins, array subqueries, and other derived outputs must union the visible accepted commit ids of every contributing tuple element.

This shadow provenance is internal delivery state. It does not need to become a public row shape in the MVP.

## Reconnect and reconciliation

Reconnect has two separate responsibilities:

1. rebuild desired query state upstream
2. repair missed write outcome, durability, and strict-visibility state

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
- `CommitConfirmedTier`
- `BatchOutcome`

### Snapshot fallback

If the replay window is gone, the server sends compact current truth:

- the current query frontier for active queries
- current confirmed tiers for the relevant frontier commits
- `BatchOutcome` for each `pending_batches` entry

The client then:

- resolves accepted direct batches waiting only on tier
- resolves accepted transactional batches waiting on completeness and tier
- retransmits `Missing` batches
- fails `Rejected` batches
- re-checks strict query visibility using the now-current frontier, confirmed tiers, and accepted merge refs

## Before / After flow sketches

### Scenario 1: ordinary durable write, live ack dropped

**Today on `main`**

```text
Alice writes public batch B1 with tier=global
  -> public commit sent upstream
  -> durable waiter watches for live PersistenceAck

PersistenceAck(B1, global) is dropped

Reconnect:
  -> active queries replay
  -> nothing reconciles B1
  -> waiter hangs forever
```

**After this spec**

```text
Alice writes public batch B1 with tier=global
  -> LocalBatchRecord(B1, mode=Direct, Pending)

Live CommitConfirmedTier(B1, global) is dropped

Reconnect:
  -> replay active queries
  -> ResumeSync(last_seen_seq=N, pending_batches=[{B1, Direct}])

Server replies:
  -> BatchOutcome.AcceptedDirect(B1)
  -> CommitConfirmedTier(B1, global) via replay or snapshot

Alice:
  -> marks B1 accepted
  -> sees B1 confirmed at global
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
CommitConfirmedTier(C1, global) arrives
  -> first delivery allowed

Later Bob writes C2 on Alice's edge only
  -> visible commits now include C2
  -> delivery gate checks C2.confirmed_tier
  -> C2 held back until confirmed at global
```

### Scenario 3: opt-in transaction accepted

**Today on `main`**

```text
Alice wants one strict write touching todo/1 and project/9

Only direct public optimistic writes exist
  -> partial visibility and rollback semantics are ad hoc
  -> reconnect cannot re-derive one authority-owned fate record
```

**After this spec**

```text
Alice starts transactional batch B7
  -> writes stage on tx-private prefixes for todo/1 and project/9
  -> strict mode + local pending overlay lets Alice see her own pending state

Authority validates B7
  -> BatchOutcome.AcceptedTransaction(B7, merges=[todo merge, project merge])

Bob runs a strict transaction-visible query
  -> Bob sees nothing until:
     - AcceptedTransaction is present
     - accepted merges relevant to Bob's local query scope are present
     - visible accepted commits satisfy any requested tier
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
  -> BatchOutcome.Rejected(B8, code=permission_denied)

Alice runtime:
  -> rolls back pending tx-private view
  -> persists rejected outcome
  -> any dependent local pending txs may be superseded
  -> rejection survives restart until acknowledged
```

## Restart semantics

After restart, a durable runtime should be able to reconstruct:

- still-pending direct public batches
- still-pending transactional batches
- accepted public commits and their confirmed tiers
- accepted transaction merge refs
- rejected outcomes awaiting acknowledgement

What it should **not** need:

- a live connection
- replay of every historical event
- a second hidden transaction store unrelated to batch-branch history

This is why the MVP needs:

- persisted `LocalBatchRecord`s
- replayable `BatchOutcome`s
- per-commit confirmed-tier state

## Rabbit Holes

- Exact naming and encoding of tx-private prefixes on top of the batch-branch substrate.
- Efficiently computing `complete_for_current_local_scope` for joins, arrays, and recursive query shapes.
- Re-triggering delivery when tier state changes without row bytes changing.
- Deciding whether direct-batch accepted outcomes should always be emitted live or can sometimes be synthesized only from replay/snapshot state.
- Garbage collection of rejected tx-private branches without losing debuggability too early.
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

- `alice` writes a direct public batch with `tier: "global"`, the live tier update is dropped, reconnect happens, and the write resolves from replay or snapshot rather than hanging.
- `alice` and `carol` subscribe with `tier: "global"` on different edges, `bob` writes on one edge, and neither sees the later update until the visible public commit reaches global.
- `alice` starts transactional batch `B7` touching two objects, the authority accepts it, and a strict transaction-visible subscription only sees the accepted result after `complete_for_current_local_scope` is satisfied.
- `alice` starts transactional batch `B8`, the authority rejects it, the local pending view rolls back, and the rejected outcome survives restart until acknowledged.
- a reconnect within the replay window replays missed `ObjectUpdated`, `QueryFrontierSettled`, `CommitConfirmedTier`, and `BatchOutcome` events without needing a full snapshot.
- a reconnect after the replay window expires falls back to a frontier snapshot plus pending-batch reconciliation and still converges.
- the optional local pending overlay shows Alice her own pending transactional edits locally, while Bob never sees those pending edits and still waits for accepted public merges.

## Planning summary

The MVP should be shaped as one design with two write modes over one shared batch-history substrate:

1. **direct public batches remain the default**
   - local-first
   - replayable `BatchOutcome`
   - fixed per-commit tier gating

2. **transactional batches are explicit opt-in**
   - `BatchId` also acts as tx id
   - authority emits `AcceptedTransaction` or `Rejected`
   - accepted public merges drive strict query visibility
   - rejected outcomes roll back and survive restart

This keeps the everyday local-first path simple while giving applications one coherent stricter path when they explicitly need it.
