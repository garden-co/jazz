# INV-BVIEW-18

- Status: now
- Coverage: ✓

## Invariant

Read and write policy MUST use ordinary branch columns and the same effective branch view as the operation; missing reference/policy evidence fails closed, and Jazz MUST NOT impose a built-in branch-row existence or lifecycle gate.

## Enforced by (tests)

`jazz::tests::branch_views::branch_column_reference_policy_controls_effective_reads`; `jazz::db::tests::mutations::admitted_server_authorizes_branch_write_through_referenced_application_row`

## Implementation

`node/query_eval/read_sources.rs::resolved_current_source_graph`; `node/state/authorization.rs::NodeState::commit_unit_satisfies_write_policies`
