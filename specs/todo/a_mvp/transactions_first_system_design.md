# Transactions-first system design (MVP)

Focused MVP design for making Jazz2 transaction-first without first solving the full distributed problem.

Related specs:

- `../a_mvp/permissions_and_optimistic_updates.md`
- `../../status-quo/schema_manager.md`

## Goal

Give Jazz2 one coherent write model now:

1. all writes are transactions
2. tx fate is authority-decided
3. clients still get immediate local optimistic UX
4. strict subscriptions work after crash and offline restart
5. the design reuses current branch/object machinery as much as possible

## Non-goals

The MVP does **not** try to solve:

1. exact distributed query completeness
2. strong secrecy of hidden row existence
3. movable / sharded authorities beyond simple `appId` routing
4. merge-operator tx semantics
5. full query-scoped tx coverage protocols

Those move to the later-stage doc.

## MVP decisions

1. All writes are transaction intents.
2. Each `appId` has one semantically central tx authority.
3. Tx row data uses tx-private branches plus authoritative merge commits onto the general branch.
4. Clients persist one global `TxDecision` per tx.
5. Strict visibility uses `complete_for_current_local_scope`.
6. Permissions protect row content, not row existence or tx touch-set secrecy.
7. Server and client use the same visibility rule.

## Terminology

- **Tx intent**: immutable write intent(s) associated with `tx_id`.
- **Tx branch**: transaction-private branch such as `tx/<tx_id>`.
- **General branch**: the ordinary branch outside-the-tx readers use.
- **TxDecision**: global tx update carrying fate, order, and the full accepted merge set.
- **RowRef**: `(object_id, branch_name)`.
- **MergeRef**: `(object_id, branch_name, commit_id)`.
- **Current local scope**: the query engine's currently materialized contributing frontier for one query instance.
- **Compile epoch**: one compiled interpretation of a query under one schema context.

## Transaction lifecycle

1. `DraftPending`
   - local-only optimistic draft
   - no authority snapshot bound yet
2. `BoundPending`
   - authority snapshot bound
   - ready for validation/commit
3. `Decided`
   - `Accepted` or `Rejected`

Optional durability progression can remain separate from tx fate:

- `Accepted@Worker -> Accepted@Edge -> Accepted@Global`

## Authority model

### Routing

1. route every `appId` to one authority core
2. multiple cores are allowed operationally via hashing, but each app's tx ordering domain stays centralized
3. no multi-owner tx in MVP

### Why this split

This proves the hard semantic core first:

1. tx-private branches
2. authoritative merge commits
3. persisted tx fate
4. strict visibility under restart

without first paying the cost of distributed coordination.

## Branch-native transaction representation

### Data plane

Represent tx writes using existing object branches:

1. inside the tx, writes land on tx-private branches on touched row objects
2. in-tx reads query `general_branch + tx_branch`
3. outside reads query only the general branch
4. acceptance is an authority-created merge commit from tx branch into general branch
5. rejection means the tx branch is never merged and can later be GC'd

### Why not same-branch twigs

Do not put drafts on the same branch as outside readers.

With current semantics that leaks draft state because:

1. reads use LWW over branch tips
2. sync forwards commits reachable from branch tips
3. there is no built-in tx-status-aware visibility filter in the read path

So the viable branch-native shape is tx-private branches, not same-branch twigs.

## Global tx decision record

### Logical shape

```text
TxDecision {
  tx_id: TxId,
  order: TxOrder,
  decision: Rejected | Accepted { merges: Vec<MergeRef> },
}

TxOrder {
  decision_seq: u64,
}

MergeRef {
  object_id: ObjectId,
  branch_name: BranchName,
  commit_id: CommitId,
}
```

Notes:

1. `tx_id` should be UUIDv7 or another k-sortable unique ID.
2. `decision_seq` can be a monotonic per-`appId` sequence assigned by the authority.
3. Durability progression can stay separate from `TxDecision`.

### Accepted merge commit metadata

Accepted merge commits on the general branch should carry tx attribution in commit metadata.

Minimum keys:

1. `tx_id`
2. `tx_role=accepted_merge`

Why commit metadata is the right place:

1. `Commit` already supports metadata
2. it survives persistence and restart
3. it makes `commit_id -> tx_id` indexing straightforward
4. it avoids relying on external side tables after crash recovery

Optional later keys:

1. `tx_decision_seq`
2. `tx_origin_branch`
3. `tx_authority`

## Permission model

### MVP assumption

For the MVP, we explicitly accept a weaker permission model:

1. row content may still be permission-protected
2. row existence is not treated as secret
3. tx touch-set membership is not treated as secret
4. clients may receive full accepted tx merge scopes even for rows whose contents they cannot read

### Consequences

In MVP, a client may learn:

1. that a row exists
2. that a row participated in a transaction
3. branch/schema identity for that row
4. commit IDs for accepted merges

The client should still not learn row content unless separately authorized to receive it.

## Schema and lens interaction

Bound pending txs should pin:

1. `snapshot_token`
2. `execution_schema_hash`
3. `lens_epoch`

This prevents tx meaning from changing if the lens graph changes mid-tx.

Accepted writes land as authoritative merge commits on the execution schema's general branch.

`TxDecision.Accepted { merges }` refers to those authoritative merge commits on their actual execution branches.

### Query compile boundaries

A query's current local scope is only meaningful within one compiled interpretation of that query.

That interpretation depends on:

1. target schema hash
2. lens epoch / catalogue interpretation
3. effective branch set / live schemas used by the query

Current runtime recompiles local subscriptions in place when live schemas change. Semantically, that should be treated as crossing a compile boundary.

MVP simplification:

1. schema context is part of the query instance
2. a material schema-context change resets local scope state
3. explicit `compile_epoch` can stay implicit for now

## Completeness model

MVP has one completeness rule:

1. `complete_for_current_local_scope`
   - all tx rows whose refs lie in the query's current local contributing scope are present

This is the only completeness model in MVP.

## Query visibility rule

For MVP strict visibility, a tx is visible to a query iff:

1. there is a persisted `TxDecision` for that `tx_id`
2. the decision is `Accepted { merges }`
3. for every `MergeRef` in that tx whose `RowRef` lies in the query's current local contributing scope, the referenced merge commit is present locally

Equivalently:

1. compute the query's current local scope
2. intersect it with `TxDecision.Accepted.merges`
3. if any merge in that intersection is missing locally, the tx stays invisible for that query
4. if all merges in that intersection are present locally, the tx is eligible for that query

This rule is the same on server and client.

## What this guarantee means

The guarantee is local-scope transactional consistency, not full global query atomicity.

Concrete examples:

1. **New matching insert**
   - query currently only has row `A` in scope
   - tx updates `A` and inserts new row `B`
   - `B` is not locally loaded yet
   - tx may first become visible through `A`, and later affect the same query again when `B` loads

2. **Aggregate / order / limit**
   - tx updates one visible row and also inserts another row that would change `COUNT`, `SUM`, or top-k ordering
   - if the second row is not yet local, the query may temporarily show a tx effect that is still incomplete from a global perspective

3. **Join / EXISTS**
   - tx changes one side of a join and also inserts or updates the matching side
   - if the second side is not yet local, the query may later re-evaluate the same tx differently

This is intentional in MVP. It should be described as a local-scope guarantee.

## Subscriptions vs in-transaction reads

### In-transaction reads

1. fixed snapshot
2. repeatable
3. includes tx read-your-writes
4. no moving frontier during tx
5. read from `general_branch + tx_branch`

### Subscriptions

1. moving frontier over many tx outcomes
2. incremental graph updates
3. configurable visibility mode:
   - `confirmed_only`
   - `confirmed_plus_local_pending`

Strict transactional subscriptions need continuous gating for tx visibility.

### Integration approach

1. raw intents are not directly subscription-visible
2. row data still arrives through normal `ObjectUpdated`
3. persisted `TxDecision` provides tx fate and full merge scope
4. local query scope determines which subset of a tx matters to the current query
5. accepted tx appears atomically after decision + local-scope completeness checks
6. rejected tx never enters confirmed view

## Query-manager algorithm sketch

Recommended MVP algorithm:

1. persist `TxDecision` records in a local `tx_visibility_index`
2. build a local `commit_id -> tx_id` index from:
   - `TxDecision.Accepted.merges`
   - accepted merge commit metadata (`tx_id`)
3. for each active query, maintain its current local scope frontier using `contributing_object_ids()` or a conservative local approximation
4. for each tx relevant to that query, compute `required_merges(query, tx) = tx.merges ∩ local_scope(query)`
5. mark `tx_complete_for_current_local_scope(query, tx)` true when all merges in `required_merges(query, tx)` are locally present
6. in row loading / visibility resolution for that query:
   - if the winning general-branch commit has no `tx_id`, it is visible normally
   - if it has `tx_id` and `tx_complete_for_current_local_scope(query, tx_id)` is false, fall back to the previous visible commit on that row
   - if it has `tx_id` and `tx_complete_for_current_local_scope(query, tx_id)` is true, it is eligible normally
7. when local scope changes, recompute `required_merges(query, tx)` and re-evaluate affected txs
8. when a tx becomes complete for the current local scope, mark affected rows dirty
9. re-settle subscriptions; the next delta exposes the tx relative to that local scope

If a query shape makes this local-scope fixed point too hard to compute directly, the implementation should conservatively widen the local scope approximation for that query shape rather than switching to a different completeness model.

## Crash and restart

We do not need one giant storage transaction that atomically persists all accepted row merges plus every query's visibility state.

What we do need is crash-safe visibility gating:

1. authoritative tiers must durably persist tx fate before claiming durability at that tier
2. accepted merge commits must remain identifiable as belonging to a `tx_id`
3. a replica must not treat accepted merge commits as visible after restart unless the persisted `TxDecision` and local completeness rule both say they are visible
4. partial persistence after a crash is acceptable as long as partial tx effects remain invisible

This means:

1. persist tx outcomes at authoritative tiers
2. persist merge commits normally
3. persist global `TxDecision` on clients
4. after restart, rebuild each query's local scope and apply the same local-scope intersection rule

This is the main reason for using global `TxDecision` in MVP: it makes offline restart possible without replaying per-query server state.

## Protocol direction

MVP keeps the data plane branch-native:

1. row tx branches and authoritative merge commits flow through normal `ObjectUpdated`
2. tx-branch GC can use normal `ObjectTruncated`
3. `TxDecision` can be a dedicated payload or a normal object encoding

The important thing is the logical record, not the transport choice.

## MVP rollout

### Phase 0: metadata and lifecycle groundwork

1. add tx IDs/state to write path
2. introduce tx-private branch naming/creation
3. define tx control object schema
4. keep existing read semantics outside txs

### Phase 1: centralized authority

1. route every `appId` to one authority core
2. decide accepted/rejected + order there
3. accept via authoritative merge commits onto general branches
4. reject via durable `TxDecision` + unmerged branches

### Phase 2: strict visibility correctness

1. propagate durable `TxDecision`
2. persist it locally
3. tag accepted merge commits with `tx_id`
4. gate tx visibility continuously for strict queries using `complete_for_current_local_scope`
5. enforce the documented MVP permission model

### Phase 3: offline restart hardening

1. verify strict queries can restart offline from persisted row data + persisted `TxDecision`
2. define local GC policy for old tx decisions
3. test crash/restart during partial tx arrival

## MVP open questions

1. What exact schema/object layout should `TxDecision` use?
2. Should `TxDecision` be a dedicated payload, a normal object, or both?
3. Which query shapes need a wider local-scope approximation to make `complete_for_current_local_scope` practical and defensible?
4. How do we want to present rejected reasons safely to clients?

## Decision summary

1. One global persisted `TxDecision` per tx.
2. Accepted merge commits carry `tx_id` in commit metadata.
3. Strict visibility uses `complete_for_current_local_scope`.
4. MVP permissions protect row content, not existence/touch-set secrecy.
5. Offline restart is a first-class requirement.
