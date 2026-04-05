# Transactions-first system design (MVP)

Focused MVP design for making Jazz2 transaction-capable, with per-table transaction enforcement, without first solving the full distributed problem.

Related specs:

- `../a_mvp/permissions_and_optimistic_updates.md`
- `../../status-quo/schema_manager.md`

## Goal

Give Jazz2 one coherent write model now:

1. every table can participate in explicit transactions
2. some tables can require transactions for confirmed writes
3. direct writes on other tables remain simple general-branch batches
4. explicit tx fate is authority-decided
5. clients still get immediate local optimistic UX
6. strict subscriptions work after crash and offline restart
7. the design reuses current branch/object machinery as much as possible

## Non-goals

The MVP does **not** try to solve:

1. forcing every write through the authority path
2. exact distributed query completeness
3. strong secrecy of hidden row existence
4. movable / sharded authorities beyond simple `appId` routing
5. merge-operator tx semantics
6. full query-scoped tx coverage protocols

Those move to the later-stage doc.

## MVP decisions

1. Every table supports explicit transactions.
2. Each table has tx write-admission mode: `Optional` or `Required`.
3. Direct writes on `Optional` tables land as ordinary general-branch batches with no `tx_id`.
4. Explicit tx row data uses tx-private branches plus authoritative merge commits onto the general branch.
5. Clients persist one global `TxDecision` per explicit tx.
6. Strict visibility uses `complete_for_current_local_scope` only for commits tagged with `tx_id`.
7. Permissions protect row content, not row existence or tx touch-set secrecy.
8. Server and client use the same visibility rule.

## Terminology

- **TxMode**: table write-admission mode, either `Optional` or `Required`.
- **Direct write**: confirmed general-branch batch with no `tx_id`.
- **Tx intent**: immutable write intent(s) associated with `tx_id` for one explicit transaction.
- **Tx branch**: transaction-private branch such as `tx/<tx_id>`.
- **General branch**: the ordinary branch outside-the-tx readers use.
- **TxDecision**: global tx update carrying fate, order, and the full accepted merge set.
- **RowRef**: `(object_id, branch_name)`.
- **MergeRef**: `(object_id, branch_name, commit_id)`.
- **Current local scope**: the query engine's currently materialized contributing frontier for one query instance.
- **Compile epoch**: one compiled interpretation of a query under one schema context.

## Explicit transaction lifecycle

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

Direct writes on `Optional` tables do not enter this lifecycle. They become immediately confirmed general-branch commits.

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

## Table transaction mode

Every table declares one tx write-admission mode in schema:

1. `Optional`
   - direct writes allowed
   - explicit tx writes also allowed
2. `Required`
   - direct writes rejected
   - confirmed writes must be part of an explicit tx

Rules:

1. all tables can participate in explicit txs regardless of mode
2. touching any `Required` table makes the overall operation explicit-tx only
3. the mode changes write admission, not the underlying branch/storage representation
4. MVP keeps this knob table-level only

## Branch-native transaction representation

### Data plane

Represent direct and tx-backed writes using existing object branches:

1. direct writes on `Optional` tables land on the general branch as ordinary batches with no `tx_id`
2. inside an explicit tx, writes land on tx-private branches on touched row objects
3. in-tx reads query `general_branch + tx_branch`
4. outside reads query only the general branch
5. acceptance is an authority-created merge commit from tx branch into general branch
6. rejection means the tx branch is never merged and can later be GC'd

## Mixed direct and transactional writes

`Optional` tables deliberately admit both direct writes and explicit tx writes.

Rules:

1. direct writes and accepted tx writes both become general-branch state
2. direct writes have no `tx_id`; accepted tx writes do
3. when an explicit tx binds or validates, the authority snapshot/frontier includes current general-branch state, including direct writes on `Optional` tables
4. direct writes are not a bypass around tx conflict checking; later tx validation must treat them as part of the observed frontier
5. if a product surface needs stronger guarantees for a table, mark that table `Required`

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
4. Direct writes do not produce a `TxDecision`.

### Accepted merge commit metadata

Accepted merge commits on the general branch should carry tx attribution in commit metadata.

Minimum keys:

1. `tx_id`
2. `tx_role=accepted_merge`

Direct writes on the general branch intentionally omit `tx_id`.

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

Bound pending explicit txs should pin:

1. `snapshot_token`
2. `execution_schema_hash`
3. `lens_epoch`

This prevents tx meaning from changing if the lens graph changes mid-tx.

Accepted writes land as authoritative merge commits on the execution schema's general branch.

`TxDecision.Accepted { merges }` refers to those authoritative merge commits on their actual execution branches.

Direct writes on `Optional` tables execute immediately on the current general branch under the current schema context and do not create a `TxDecision`.

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

MVP has one tx completeness rule:

1. `complete_for_current_local_scope`
   - all tx rows whose refs lie in the query's current local contributing scope are present

This is the only tx completeness model in MVP. Direct general-branch commits without `tx_id` remain visible under normal branch rules.

## Query visibility rule

For MVP strict visibility:

1. if the winning general-branch commit has no `tx_id`, it is visible normally
2. if the winning general-branch commit has `tx_id`, that tx is visible to the query iff:
   - there is a persisted `TxDecision` for that `tx_id`
   - the decision is `Accepted { merges }`
   - for every `MergeRef` in that tx whose `RowRef` lies in the query's current local contributing scope, the referenced merge commit is present locally

Equivalently:

1. compute the query's current local scope
2. if the winning commit has no `tx_id`, no tx gating applies
3. if it has `tx_id`, intersect `TxDecision.Accepted.merges` with the local scope
4. if any merge in that intersection is missing locally, the tx stays invisible for that query
5. if all merges in that intersection are present locally, the tx is eligible for that query

This rule is the same on server and client.

## What this guarantee means

The guarantee is local-scope transactional consistency for explicit tx-backed writes, not full global query atomicity for all writes.

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

1. direct writes on `Optional` tables are visible as normal confirmed general-branch updates
2. raw tx intents are not directly subscription-visible
3. row data still arrives through normal `ObjectUpdated`
4. persisted `TxDecision` provides tx fate and full merge scope
5. local query scope determines which subset of a tx matters to the current query
6. accepted tx appears atomically after decision + local-scope completeness checks
7. rejected tx never enters confirmed view

## Query-manager algorithm sketch

Recommended MVP algorithm:

1. persist `TxDecision` records for explicit txs in a local `tx_visibility_index`
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

1. direct writes, row tx branches, and authoritative merge commits all flow through normal `ObjectUpdated`
2. tx-branch GC can use normal `ObjectTruncated`
3. `TxDecision` can be a dedicated payload or a normal object encoding

The important thing is the logical record, not the transport choice.

## MVP rollout

### Phase 0: metadata and lifecycle groundwork

1. add table tx mode metadata with `Optional` as the default
2. add tx IDs/state to the explicit tx path
3. introduce tx-private branch naming/creation
4. define tx control object schema
5. keep direct writes on `Optional` tables using existing general branches

### Phase 1: centralized authority

1. route every `appId` to one authority core
2. enforce that direct writes to `Required` tables are rejected
3. decide accepted/rejected + order there
4. accept via authoritative merge commits onto general branches
5. reject via durable `TxDecision` + unmerged branches

### Phase 2: strict visibility correctness

1. propagate durable `TxDecision`
2. persist it locally
3. tag accepted merge commits with `tx_id`
4. gate tx visibility continuously for strict queries using `complete_for_current_local_scope`
5. keep direct writes with no `tx_id` visible normally
6. enforce the documented MVP permission model

### Phase 3: offline restart hardening

1. verify strict queries can restart offline from persisted row data + persisted `TxDecision`
2. define local GC policy for old tx decisions
3. test crash/restart during partial tx arrival

## MVP open questions

1. What exact schema/object layout should `TxDecision` use?
2. Should `TxDecision` be a dedicated payload, a normal object, or both?
3. How should table-level `TxMode` be declared and exposed in schema APIs?
4. Do we eventually need an object-level override that can tighten `Optional -> Required`?
5. Which query shapes need a wider local-scope approximation to make `complete_for_current_local_scope` practical and defensible?
6. How do we want to present rejected reasons safely to clients?

## Decision summary

1. Every table can participate in explicit txs.
2. Tables choose `Optional` or `Required` write admission.
3. Direct writes on `Optional` tables remain general-branch commits with no `tx_id`.
4. One global persisted `TxDecision` exists per explicit tx.
5. Accepted merge commits carry `tx_id` in commit metadata.
6. Strict visibility uses `complete_for_current_local_scope` only for tx-tagged commits.
7. MVP permissions protect row content, not existence/touch-set secrecy.
8. Offline restart is a first-class requirement.
