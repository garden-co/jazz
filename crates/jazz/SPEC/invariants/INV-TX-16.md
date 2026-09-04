# INV-TX-16

- Status: now
- Coverage: ✓

## Invariant

Exclusive authority validation MUST reject when any recorded row read is no longer the globally current visible content/deletion read version. A later deletion invalidates a prior visible-content read even though that content remains its register winner; this read-state check is distinct from written-layer CAS (`INV-TX-20`).

## Enforced by (tests)

`jazz::node::tests::exclusive_transactions::{exclusive_row_read_conflict_rejects_and_client_restores_old_value, exclusive_row_read_conflicts_when_a_later_delete_hides_the_content}`

## Implementation

`jazz/src/node/ingest/fates.rs::NodeState::validate_exclusive_commit_unit`; `jazz/src/node/global_state.rs::NodeState::visible_global_row_tx_id_now`
