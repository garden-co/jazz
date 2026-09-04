# INV-RLS-6

- Status: now
- Coverage: ✓

## Invariant

Read-policy revocation MUST remove rows from future settled subscription result sets and MUST NOT redact previously delivered local copies from the receiving node.

## Enforced by (tests)

jazz::node::tests::policies_rls::owner_transfer_removes_settled_result_set_without_redacting_local_copy; jazz::node::tests::policies_rls::join_policy_authorizes_writes_reads_and_next_emission_revocation

## Implementation

jazz/src/node/views.rs::NodeState::view_update_for_query; jazz/src/node/policy.rs::NodeState::retain_policy_atomic_rows
