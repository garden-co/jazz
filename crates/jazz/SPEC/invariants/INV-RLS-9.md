# INV-RLS-9

- Status: now
- Coverage: ✓

## Invariant

Join-based policies MUST require at least one matching global-current joined row that reaches the protected row and whose filters pass for the same authenticated identity.

## Enforced by (tests)

jazz::node::tests::policies_rls::join_policy_authorizes_writes_reads_and_next_emission_revocation; jazz::node::tests::policies_rls::composed_read_policy_grants_and_revokes_incrementally

## Implementation

jazz/src/node/query_eval.rs::normalize_filter_join_chain; jazz/src/node/query_eval.rs::NodeState::policy_composed_shape_binding
