# INV-RLS-12

- Status: now
- Coverage: ✓

## Invariant

Exclusive transaction view shipping MUST be policy-atomic per recipient and maintained subscription view: a non-system recipient MUST NOT receive a result member or program fact from an exclusive transaction unless all versions required for that view are readable to that recipient.

## Enforced by (tests)

`jazz::node::tests::exclusive_transactions::exclusive_view_shipping_is_view_atomic_per_recipient`

## Implementation

jazz/src/node/policy.rs::NodeState::retain_policy_atomic_rows; jazz/src/node/views.rs::NodeState::visible_exclusive_tx_result_entries_for_table
