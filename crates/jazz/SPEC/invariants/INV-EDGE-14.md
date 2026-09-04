# INV-EDGE-14

- Status: now
- Coverage: untested

## Invariant

An edge cache MUST NOT evict fate-pending units, permission-scope results currently backing edge acceptance, parked commit families, or edge-accepted versions not yet globally durable.

## Enforced by (tests)

NONE-FOUND

## Implementation

`jazz/src/node/eviction.rs::NodeState::classify_row_version_for_eviction`; `jazz/src/node/eviction.rs::NodeState::evict_cold`; `jazz/src/node/eviction.rs::NodeState::enforce_edge_cache_budget`; `jazz/src/peer.rs::PeerState::eviction_pins`
