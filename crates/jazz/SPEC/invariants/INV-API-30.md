# INV-API-30

- Status: now
- Coverage: ✓

## Invariant

Reopening persistent storage with the same `DbIdentity` MUST schedule every locally originated transaction that reached `Local` durability and has not reached terminal settlement for upstream delivery. Locally originated means `TxId.node == DbIdentity.node` and `Transaction.made_by == DbIdentity.author`; delivery is at-least-once by `TxId` and relies on idempotent authority handling.

## Enforced by (tests)

`durable_local_write_replay_integration::persistent_restart_replays_pending_write_with_valid_token`

## Implementation

`jazz/src/db.rs::Db::open`; `jazz/src/db.rs::Node::restore_pending_uploads`; `jazz/src/node/mod.rs::NodeState::pending_transaction_ids_for`
