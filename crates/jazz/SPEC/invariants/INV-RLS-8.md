# INV-RLS-8

- Status: now
- Coverage: ✓

## Invariant

A deletion-register version MUST be readable to a non-system identity only when the row has a global content winner and that content winner satisfies the table read policy for that identity.

## Enforced by (tests)

`jazz::node::tests::policies_rls::deletion_read_policy_requires_visible_global_content_winner`

## Implementation

jazz/src/node/policy.rs::NodeState::read_policy_allows_deletion_version
