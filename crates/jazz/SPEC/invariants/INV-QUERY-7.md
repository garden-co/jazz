# INV-QUERY-7

- Status: now
- Coverage: untested

## Invariant

A reset-result-set `ViewUpdate` MUST replace the subscription result set while retaining per-peer version dedup state.

## Enforced by (tests)

`jazz::peer::tests::incremental_query_result_sets_match_full_rehydrate_after_seeded_commits` (equivalence); direct dedup-survives-rehydrate test NONE-FOUND

## Implementation

`peer.rs::PeerState::rehydrate_query`; `peer.rs::PeerState::forget_subscription`; `peer.rs::view_update_reset_result_set`
