# INV-SYNC-22

- Status: now
- Coverage: ✓

## Invariant

An edge's upstream permission-scope subscriptions MUST be deduplicated at the sync level: identical or covering `(policy_shape, writer_claim)` scopes share one upstream subscription whose settled result fans out to every dependent acceptance gate. (Identical-key sharing implemented; covering future.)

## Enforced by (tests)

`jazz::tests::four_tier::edge_deduplicates_scope_subscription_for_repeated_deferred_units`

## Implementation

`jazz/src/peer.rs::PeerState::unsettled_permission_scope_subscriptions`
