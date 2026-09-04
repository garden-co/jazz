# INV-BVIEW-16

- Status: now
- Coverage: ✓

## Invariant

Transactions MAY atomically contain versions in multiple branch keys, but admission, fate, persistence, and rejection remain all-or-nothing.

## Enforced by (tests)

`jazz::tests::branch_views::one_mergeable_transaction_can_atomically_write_multiple_branches`

## Implementation

`node/state/commit.rs::NodeState::commit_mergeable_many`; `node/ingest/validation.rs::NodeState::stage_transaction_and_versions_with_current_indexes`
