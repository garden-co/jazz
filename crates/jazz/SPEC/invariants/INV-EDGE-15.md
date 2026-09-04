# INV-EDGE-15

- Status: now
- Coverage: untested

## Invariant

Edge refetch after eviction MUST use payload-inventory resubscribe rather than assuming the edge has complete history.

## Enforced by (tests)

NONE-FOUND

## Implementation

`jazz/src/peer.rs::PeerState::forget_evicted_versions`; `jazz/src/node/eviction.rs::NodeState::evict_cold`
