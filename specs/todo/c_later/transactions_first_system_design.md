# Transactions-first system design (later stages)

Post-MVP design space for taking the MVP transaction model toward stronger secrecy, stronger correctness, and a fully distributed authority layout.

This doc assumes the MVP design in `../a_mvp/transactions_first_system_design.md` already exists.

Related specs:

- `../b_launch/sharding_design_sketch.md`
- `partial_schema_visibility.md`
- `branching_snapshots.md`

## Starting point

The MVP establishes:

1. branch-native tx data (`tx/<tx_id>` branches + authoritative merge commits)
2. one global persisted `TxDecision` per tx
3. centralized authority per `appId`
4. `complete_for_current_local_scope`
5. weak permission secrecy (content protected, existence/touch-set not secret)

Later work revisits each of those constraints where necessary.

## Main goals after MVP

1. stronger permission secrecy
2. richer tx/query affinity than `appId`
3. exact or stronger query completeness where needed
4. multi-owner tx coordination
5. better tx metadata compression and operational scaling
6. merge-operator tx semantics and undo/redo

## Scope tokens and routing policy

The old "globally consistent transactions" sketch still contains one useful later-stage idea: routing and authority choice can be driven by explicit scope tokens even if the data model stays one unified transaction pipeline.

Possible later inputs to a routing/classification layer:

1. table-level tokens
2. constraint-level conflict keys
3. policy-evaluation-input dependencies
4. range or predicate tokens where necessary

This can support later decisions such as:

1. which authority owns a tx
2. when multi-owner coordination is required
3. whether a scope should fail closed when authority is unavailable
4. where stronger completeness/secrecy guarantees are worth the cost

This should not force a separate write model; it can remain a routing and visibility policy over one transaction pipeline.

## Authority availability modes

The old launch sketch also had a useful framing for unavailable authorities.

Possible later behavior knobs per scope:

1. `strict_online`
   - fail closed if authority unavailable
2. `optimistic_queued`
   - keep local pending intent and resolve later

MVP can stay simple, but later stages may want this to vary by scope or product surface.

## Stronger permission secrecy

MVP intentionally leaks row existence and tx touch-set membership. If that becomes unacceptable, later work needs a different tx metadata model.

Likely options:

1. **projection-scoped manifests**
   - one tx manifest per client/session projection
   - stronger than MVP, still replayable
   - permissions changes become more complicated

2. **query-scoped coverage**
   - coverage computed for one query/scope epoch
   - strongest secrecy
   - much more replay/state complexity

3. **more cryptographic designs**
   - commitments / proofs / PSI-like ideas
   - likely too complex unless secrecy becomes truly mandatory

Open questions:

1. Is projection-scoped metadata enough, or do we eventually need true query-scoped coverage?
2. How do permission changes interact with already persisted tx metadata on clients?
3. Can we ever revoke previously sent existence metadata in a meaningful way?

## Stronger completeness than current local scope

MVP guarantees only `complete_for_current_local_scope`.

Later stages may want a stronger model for some queries:

1. exact `complete_for_query`
2. projection-wide completeness
3. special-case stronger semantics for query classes such as:
   - point lookups
   - bounded known-ID joins
   - limited aggregate families

This likely requires more than a bare global `TxDecision.Accepted { merges }`.

Possible ingredients:

1. query-scoped coverage
2. projection-scoped manifests
3. predicate witnesses
4. explicit query-shape classification in the engine

## Query scope and compile continuity

MVP can treat schema-context changes as resetting local scope state.

Later stages may want explicit continuity across recompiles:

1. make `compile_epoch` explicit
2. make scope tracking first-class across schema/lens changes
3. reason more carefully about tx completeness across schema transitions

This especially matters if stronger completeness semantics are added.

## Affinity and distributed authorities

### Moving beyond `appId -> core`

Later stages can choose a finer-grained affinity key shared by tx routing and query routing.

Selection guidance:

1. capture most invariants
2. appear in most query predicates
3. keep most joins local
4. be immutable after creation
5. have enough cardinality for load distribution

Typical candidates:

1. `tenant_id`
2. `workspace_id`
3. `project_id`

### Edge-local authorities

The old edge-authority sketch is still relevant here:

1. place authorities near the clients most likely to issue a given class of txs
2. let region-local or affinity-local txs complete in one hop
3. escalate cross-region / cross-affinity txs to a coordinator

Open questions that remain valid:

1. should placement be geographic, affinity-key based, or hybrid?
2. what failover model preserves correctness without losing in-flight txs?
3. how much latency reduction is available before multi-owner coordination dominates again?

### One-hop and multi-owner paths

1. one-hop tx when all touched scopes map to one owner
2. deterministic coordinator + 2PC/consensus when scopes span owners
3. explicit fencing/lease epochs to prevent split brain

### Control plane

Later-stage distribution likely needs:

1. fixed logical partitions
2. versioned `PartitionMap`
3. repartitioning by partition movement, not key-by-key movement
4. authority lease handoff and epoch fencing

## Repartitioning and balancing

Avoid key-by-key migration.

Later-stage protocol direction:

1. assign target owner with higher lease epoch
2. snapshot copy
3. log catch-up
4. atomic owner switch
5. source rejects stale-epoch writes

Operational constraints:

1. move partition-by-partition
2. throttle concurrent moves
3. maintain deterministic ownership during migration

## SPOF mitigation

Central authority is fine for MVP, but not a long-term architecture.

Later direction:

1. authority as replicated consensus group(s)
2. partition authority by scope token / affinity key
3. epoch fencing on failover and handoff
4. explicit behavior when authority is unavailable

## Merge-operator transactions

Some data subsets may want multiple concurrent txs to be valid and merged rather than rejected.

Later-stage model:

1. define per-column or per-field merge operators
2. represent writes as operator-applicable intents
3. fold ordered intents deterministically
4. keep tx grouping for undo/redo and intent history

Undo/redo should be compensating txs, not history rewrites.

Open questions:

1. which operators are first-class?
2. how do invertibility and conflict behavior get encoded?
3. how much of this belongs in schema vs runtime?

## Compression and transport evolution

MVP can ship verbose `TxDecision.Accepted { merges }` records.

Later stages may want:

1. compressed `TxDecision`
2. `TxObserved`
3. `TxDurable`
4. dictionary/reference encoding for repeated refs
5. better batch compression around authority sequence numbers

Compression ideas:

1. batch by monotonic sequence (delta encode)
2. tx-id dictionary/reference within batch
3. compress repeated `object_id` / `branch_name` pairs inside merge sets
4. exploit UUIDv7 locality where useful

## Schema and lens evolution follow-ups

The MVP already pins `snapshot_token`, `execution_schema_hash`, and `lens_epoch`.

Later work may need:

1. explicit policy for `execution_schema_hash` selection
2. explicit `compile_epoch` / query-schema epoch continuity
3. better debugging metadata on `TxDecision`
4. stronger handling of partial schema visibility and schema GC

## Likely decision pressure points

The main questions that will probably force a redesign after MVP are:

1. Do we need stronger secrecy than content-only permissions?
2. Do we need exact query completeness beyond current local scope?
3. Do we need lower latency than a central `appId` authority can provide?
4. Do we need merge operators soon enough that they should shape the MVP data model now?

## Later-stage open questions

1. What is the next secrecy step after MVP: projection-scoped manifests or query-scoped coverage?
2. What exact multi-owner coordination protocol should be used?
3. What is the final shared affinity key model for tx routing and distributed queries?
4. Which query families deserve stronger-than-local-scope completeness?
5. How should tx metadata be compressed without complicating recovery?

## Planning summary

After MVP, the design probably branches into three tracks:

1. **stronger correctness/secrecy**
   - projection/query-scoped tx metadata
   - stronger completeness semantics
   - explicit compile epochs

2. **distributed authorities**
   - richer affinity keys
   - partition map
   - movable authorities
   - multi-owner txs

3. **expressive tx semantics**
   - merge operators
   - undo/redo groups
   - better tx metadata and debugging
