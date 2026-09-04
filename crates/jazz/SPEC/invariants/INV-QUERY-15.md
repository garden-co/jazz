# INV-QUERY-15

- Status: now
- Coverage: ✓

## Invariant

Incremental query result-set updates MUST converge to the same typed result-member and program-fact state as a full rehydrate over the same committed state.

## Enforced by (tests)

`jazz::peer::tests::incremental_query_result_sets_match_full_rehydrate_after_seeded_commits`

## Implementation

`peer.rs::PeerState::query_update_from_deltas`; `peer.rs::PeerState::rehydrate_query`
