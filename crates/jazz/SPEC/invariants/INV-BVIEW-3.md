# INV-BVIEW-3

- Status: now
- Coverage: ✓

## Invariant

Every content or deletion version on a branch-keyed table MUST carry a complete canonical branch key; its version parents MUST have the same branch key.

## Enforced by (tests)

`jazz::node::tests::harness::version_parents_cannot_cross_branch_keys`; `jazz::node::tests::harness::branched_table_writes_require_an_explicit_exact_selector` ; `jazz::node::tests::harness::remote_authored_branch_keys_are_validated_atomically_before_storage`

## Implementation

`node/state/commit.rs::NodeState::commit_mergeable_many`; `protocol.rs::BranchKey`
