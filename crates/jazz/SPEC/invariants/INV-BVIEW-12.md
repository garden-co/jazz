# INV-BVIEW-12

- Status: now
- Coverage: ✓

## Invariant

The canonical read/subscription identity MUST include normalized head and base branch sources, including any snapshot cut.

## Enforced by (tests)

`jazz::tests::branch_views::sibling_branch_view_subscriptions_isolate_first_writes`; `jazz::tests::branch_views::frozen_base_subscription_keeps_the_base_fixed_and_the_head_live`

## Implementation

`protocol.rs::ReadViewSpec::read_view_key`; `node/maintained_subscription_view.rs::MaintainedSubscriptionView`
