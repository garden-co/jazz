# INV-BVIEW-11

- Status: now
- Coverage: ✓

## Invariant

A base source MUST be either live current state or the exact state of the selected branch key at a supplied `SnapshotRef`; the cut applies consistently to every table and policy dependency in the read.

## Enforced by (tests)

`jazz::tests::branch_views::frozen_base_subscription_keeps_the_base_fixed_and_the_head_live`

## Implementation

`protocol.rs::BranchViewBase`; `node/source_resolution.rs::NodeState::branch_view_rows_for_schema`
