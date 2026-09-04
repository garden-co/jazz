# INV-RLS-14

- Status: now
- Coverage: ✓

## Invariant

Policy evaluation MUST deny when it cannot determine that a policy predicate is satisfied.

## Enforced by (tests)

`jazz::node::tests::policies_rls::unsupported_policy_predicates_deny_instead_of_allowing`; `jazz::node::tests::policies_rls::unresolved_policy_operands_deny_instead_of_allowing`

## Implementation

With the interpreter removed, policy compilation treats unsupported authorization forms as denial and claim binding treats unresolved operands as denial rather than allowance: `jazz/src/node/query_eval.rs::NodeState::policy_filtered_current_source_graph_via_query_engine`; `jazz/src/node/query_eval.rs::NodeState::program_binding_for_shape_and_policy`; `jazz/src/node/query_eval.rs::prepared_claim_value`
