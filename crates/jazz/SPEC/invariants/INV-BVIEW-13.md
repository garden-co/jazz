# INV-BVIEW-13

- Status: now
- Coverage: ✓

## Invariant

Normal references MUST resolve a `RowUuid` through the current effective branch view; branch-qualified row references are a separate, unsupported capability.

## Enforced by (tests)

`jazz::tests::branch_views::branch_view_join_projects_branch_column_subsets_and_shared_tables`; `jazz::tests::branch_views::branch_column_reference_policy_controls_effective_reads`

## Implementation

`node/query_eval/query_read_sets.rs::query_read_set_for_read_view`; `node/query_eval/read_sources.rs::CurrentQuerySourceResolver::resolve_source`
