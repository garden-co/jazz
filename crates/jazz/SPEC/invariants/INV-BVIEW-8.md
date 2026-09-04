# INV-BVIEW-8

- Status: now
- Coverage: ✓

## Invariant

A read selector MUST use globally named branch-column values and each table MUST project that selector onto its declared subset; equal projected head/base branch keys collapse to one source.

## Enforced by (tests)

`jazz::tests::branch_views::branch_view_join_projects_branch_column_subsets_and_shared_tables`

## Implementation

`schema.rs::JazzSchema::project_branch_view_selector`; `node/query_eval/query_read_sets.rs::query_read_set_for_read_view`
