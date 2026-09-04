# INV-READ-8

- Status: now
- Coverage: ✓

## Invariant

Global current-row reads MUST use per-layer global-current tables and MUST exclude rows whose global-current deletion-register winner is `DeletionEvent::Deleted`.

## Enforced by (tests)

`jazz::node::tests::general::deletion_register_hides_and_restore_reveals_current_content`; `jazz::node::tests::general::writer_subscription_reads_own_pending_at_local_tier`

## Implementation

`jazz/src/node/mod.rs::NodeState::current_rows`; `jazz/src/node/mod.rs::NodeState::global_current_rows_from_storage`; `jazz/src/node/global_state.rs::NodeState::visible_global_content_tx_id_now`
