# INV-RLS-21

- Status: now
- Coverage: ✓

## Invariant

A read or write policy subplan MUST read every table source it evaluates as raw policy evidence without recursively applying that source table's own read policy. The complete evaluating policy—including filters, joins, authenticated claims, policy branches, inheritance, and reachability—MUST still be enforced fail-closed, and dependency rows MUST NOT thereby become user-visible.

## Enforced by (tests)

`jazz::tests::prepared_claim_routing::dependency_policies_are_not_recursively_composed_into_outer_policy`; `jazz::tests::prepared_claim_routing::policy_dependency_reads_do_not_expose_dependency_rows`; `jazz::tests::prepared_claim_routing::mutually_referential_dependency_policies_do_not_recurse`

## Implementation

`jazz/src/node/query_engine/lowering.rs::source_authorization_for_source`; `jazz/src/node/query_eval.rs::NodeState::policy_authorization_row_id_graph`
