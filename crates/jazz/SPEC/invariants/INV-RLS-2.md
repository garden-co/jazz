# INV-RLS-2

- Status: now
- Coverage: partial

## Invariant

`AuthorSubject::SYSTEM` MUST bypass both read and write policy checks.

## Enforced by (tests)

`jazz::node::tests::policies_rls::system_identity_read_policy_sees_everything`; `jazz::node::tests::exclusive_transactions::exclusive_view_shipping_is_view_atomic_per_recipient`; partial: system write-policy bypass remains uncovered

## Implementation

jazz/src/node/policy.rs::NodeState::write_policy_allows_version_record; jazz/src/node/policy.rs::NodeState::read_policy_allows_version; jazz/src/node/policy.rs::NodeState::read_policy_allows_deletion_version; jazz/src/node/query_eval.rs::NodeState::policy_composed_shape_binding
