# INV-TX-9

- Status: now
- Coverage: ✓

## Invariant

Originating nodes MUST retain rejected local payloads in retry storage and remove the rejected versions from normal history; non-origin authorities MUST NOT retain foreign rejected retry payloads.

## Enforced by (tests)

`jazz::node::tests::exclusive_transactions::originating_rejected_exclusive_moves_payload_to_retry_store`; `jazz::node::tests::sync::originating_causality_rejection_retains_child_payload`; `jazz::node::tests::sync::originating_cascade_rejection_retains_root_cause`

## Implementation

`jazz/src/node/ingest.rs::NodeState::remove_rejected_local_versions`; `jazz/src/node/ingest.rs::NodeState::apply_fate_update`; `jazz/src/node/ingest.rs::NodeState::ingest_rejected_transaction`
