# INV-RLS-7

- Status: now
- Coverage: ✓

## Invariant

A deletion-register version by a non-system author MUST satisfy the table write policy against the current global content version for that row; if there is no current global content version, the delete MUST be denied.

## Enforced by (tests)

jazz::node::tests::policies_rls::owner_only_delete_requires_current_owner

## Implementation

jazz/src/node/policy.rs::NodeState::write_policy_allows_version_record
