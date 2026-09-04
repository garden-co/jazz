# INV-BVIEW-1

- Status: now
- Coverage: ✓

## Invariant

Every `branchBy` entry MUST name a non-null, key-encodable ordinary column; same-named branch columns across tables MUST have the same type and canonical encoding.

## Enforced by (tests)

`jazz::node::tests::harness::added_branch_column_defaults_old_history_and_survives_column_rename`; `jazz::tests::branch_views::branch_view_join_projects_branch_column_subsets_and_shared_tables` ; `jazz::node::tests::harness::remote_authored_branch_keys_are_validated_atomically_before_storage`

## Implementation

`schema.rs::JazzSchema::validated`; `schema.rs::JazzSchema::project_branch_view_selector`
