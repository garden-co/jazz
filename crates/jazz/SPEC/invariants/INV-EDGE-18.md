# INV-EDGE-18

- Status: now
- Coverage: ✓

## Invariant

Overlapping `(policy_shape, writer_claim)` scopes MUST resolve to a single upstream subscription whose settled result satisfies every acceptance gate that depends on it; the upstream subscription MUST be reference-counted by dependent gates and dropped only when the last dependent is gone. (Exact-key sharing implemented; "covering" scope subsumption is future — no covering relation exists yet.)

## Enforced by (tests)

`jazz::tests::four_tier::edge_deduplicates_scope_subscription_for_repeated_deferred_units`; `jazz::tests::four_tier::edge_releases_scope_subscription_after_last_deferred_unit_resolves`

## Implementation

`jazz/src/peer.rs::PeerState::retain_edge_scope_subscription`; `jazz/src/peer.rs::PeerState::release_edge_scope_subscription`; `jazz/src/peer.rs::PeerState::edge_scope_subscription_count`
