# INV-BVIEW-5

- Status: now
- Coverage: ✓

## Invariant

Content and deletion histories and current winners MUST be selected independently per `(PhysicalTableId, BranchKey, RowUuid, Layer)`.

## Enforced by (tests)

`jazz::tests::branch_views::sibling_branch_view_subscriptions_isolate_first_writes`; `jazz::tests::branch_views::inherited_delete_is_a_head_register_and_can_be_restored`

## Implementation

`schema.rs::TableSchema::global_current_storage_tables`; `node/source_resolution.rs::NodeState::branch_view_rows_for_schema`
