# INV-DATA-25

- Status: now
- Coverage: ✓

## Invariant

Global current selection MUST retain immutable history and select content and deletion-register winners independently per physical lineage, branch key, and row UUID. Only accepted transactions with authority global time participate; direct-parent domination wins, otherwise concurrent versions use `(TxTime, NodeUuid)` order. Pending, rejected, incomplete, or malformed receipts MUST NOT become global winners through replay or reopen.

## Enforced by (tests)

`jazz::node::tests::harness::accepted_fates_maintain_global_current_tables`; `jazz::node::tests::harness::deletion_register_hides_and_restore_reveals_current_content`; `jazz::node::tests::catalogue_lenses::physical_deletion_register_spans_renamed_schemas_and_reopens`

## Implementation

`jazz/src/node/codec.rs::version_wins_over_open_winner`; `jazz/src/node/ingest/fates.rs::NodeState::apply_fate_update`; `jazz/src/node/currency.rs::NodeState::query_global_layer_winner_in_branch`
