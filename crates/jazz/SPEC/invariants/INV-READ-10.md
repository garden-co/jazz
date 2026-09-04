# INV-READ-10

- Status: now
- Coverage: ✓

## Invariant

Current-row visibility MUST be content-layer current rows anti-joined with the current deletion-register winner; content writes alone MUST NOT restore a deleted row, while `DeletionEvent::Restored` reveals current content.

## Enforced by (tests)

`jazz::node::tests::general::deletion_register_hides_and_restore_reveals_current_content`

## Implementation

`jazz/src/node/open_tx.rs::NodeState::snapshot_row`; `jazz/src/node/mod.rs::NodeState::global_current_rows_from_storage`
