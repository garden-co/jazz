# INV-TX-8

- Status: now
- Coverage: ✓

## Invariant

Rejection MUST cascade to known pending descendants and later arriving children of rejected ancestors as `RejectionReason::Cascade { root }`, preserving the original root transaction id.

## Enforced by (tests)

`jazz::node::tests::sync::authority_rejects_later_child_of_rejected_parent_with_cascade`; `jazz::node::tests::exclusive_transactions::authority_parks_child_until_unknown_exclusive_parent_rejects`; `jazz::node::tests::sync::client_side_rejection_cascades_to_local_mergeable_descendant`

## Implementation

`jazz/src/node/ingest.rs::NodeState::cascade_root_for_versions`; `jazz/src/node/ingest.rs::NodeState::cascade_rejections_from`; `jazz/src/node/ingest.rs::NodeState::local_cascade_descendants`; `jazz/src/node/ingest.rs::NodeState::apply_fate_update`
