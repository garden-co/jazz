# INV-BVIEW-10

- Status: now
- Coverage: ✓

## Invariant

A content write MUST NOT imply restoration; an inherited or head-key `Deleted` winner remains effective until an explicit `Restored` winner supersedes it.

## Enforced by (tests)

`jazz::tests::branch_views::inherited_delete_is_a_head_register_and_can_be_restored`

## Implementation

`db/mutations.rs::Db::restore_in_branch`; `node/source_resolution.rs::NodeState::branch_view_rows_for_schema`
