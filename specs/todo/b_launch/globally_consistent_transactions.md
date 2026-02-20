# Globally Consistent Transactions — TODO (Launch)

Opt-in globally consistent transactions on top of the existing local-first model.

Default behavior remains local-first eventual consistency. Global consistency is enabled only for selected scopes:

- whole table
- specific constraint
- policy evaluation inputs

## Goals

- Preserve existing fast local-first lane for most app data.
- Add a strict lane for invariants that need linearizable conflict resolution.
- Keep transaction history immutable, including failed transactions.
- Let subscribers choose between:
  - confirmed state only
  - confirmed plus local pending state

## Integrated MVCC Model (Commit-Graph Native)

### 1) Immutable row intents

All writes produce immutable row commits ("intents") in the row object DAG, tagged with transaction identity:

- `tx_id`
- `tx_write_seq`
- `author_client_id`

These commits are never rewritten or deleted because a transaction fails.

### 2) Transaction status as sidecar graph data

Transaction lifecycle is recorded separately (append-only), keyed by `tx_id`:

- `TxBegin { tx_id, scope_tokens, expected_write_count, write_set_hash, ... }`
- `TxDecision { tx_id, outcome: Confirmed|Rejected, lsn, authority_id, ... }`
- optional tier acks

This is the "annotation layer": row commit interpretation depends on tx status events rather than mutating commit payloads.

### 3) Visibility derived at read time

Current state is computed from:

- row intents
- tx decisions
- subscriber visibility mode

Failed txs remain in history but are excluded from normal confirmed views.

## Subscriber Visibility Modes

Subscriptions can opt into one of:

1. `confirmed_only`
2. `confirmed_plus_local_pending`

`confirmed_only`:

- only row commits from `Confirmed` txs are visible

`confirmed_plus_local_pending`:

- confirmed base snapshot
- plus overlay from local pending tx intents (same client/session)
- rejected pending intents disappear automatically when decision arrives

## Opt-In Global Consistency Granularities

### Table-level

Mark table as globally consistent:

- all operations that can affect table invariants route to strict lane

### Constraint-level

Mark specific constraint as globally consistent (for example unique/balance constraints):

- only operations that may violate that constraint route to strict lane

### Policy-evaluation-level

Mark policy as globally consistent:

- promote writes that affect evaluation inputs of that policy (its dependency closure)
- do **not** automatically promote all writes merely governed by the policy

Example: team membership may be global, while regular document content remains eventual even if updates are authorized by membership policy.

## Consistency Classifier

Each mutation is classified before execution:

- `LocalEventual`
- `GlobalStrict(scope_tokens)`

Classifier inputs:

- touched tables/columns/rows
- constraint conflict keys
- policy dependency closure touches

If any touched token is strict, mutation is routed to global lane.

For mixed batches, MVP behavior is "promote whole batch to strict" to preserve atomicity.

## Authority and Linearization

Strict-lane transactions are submitted to a transaction authority.

- authority assigns monotonic `lsn` per scope token
- authority returns `TxDecision(Confirmed|Rejected)`
- replicas apply confirmed transactions in authority order (by `lsn`) within scope

High latency is acceptable for strict lane; this path exists for correctness, not speed.

## Atomic Multi-Row Visibility

A confirmed transaction becomes visible only when:

- `TxDecision = Confirmed`
- all expected row intents are present locally (`expected_write_count` / `write_set_hash` checks)

This prevents partial visibility of multi-row transactions during replication races.

## Failure and Offline Behavior

Per strict scope, choose mode:

- `strict_online`: fail fast if authority unavailable
- `optimistic_queued`: keep local pending overlay, resolve on later decision

MVP should start with `strict_online` for simpler correctness.

## Compatibility With Current Architecture

- Local lane keeps existing behavior.
- Strict lane adds tx metadata + tx decision stream + visibility rules.
- Existing multi-tier ack model can be extended from commit-level to tx-level.

## Open Questions

- Authority placement: dedicated service vs core server role.
- Scope token design: table/constraint/policy token encoding.
- Conflict detection precision: row-key only vs predicate/range keys.
- GC policy for rejected/obsolete intents (retention without correctness impact).

Related: `../c_later/edge_transaction_authorities.md`
