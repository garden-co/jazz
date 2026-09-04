# INV-EDGE-8

- Status: now
- Coverage: ✓

## Invariant

Edge acceptance of a mergeable transaction MUST be a final authorization outcome; core MUST NOT re-evaluate or reject it solely because policy changed concurrently after the edge's settled permission basis.

## Enforced by (tests)

`jazz::tests::four_tier::edge_accepted_mergeable_is_final_at_core_after_policy_revocation`

## Implementation

`node/ingest.rs::NodeState::finalize_edge_accepted_mergeable_commit_unit_once`; `node/views.rs::NodeState::ingest_view_bundle`; `peer.rs::PeerState::ingest_edge_mergeable_commit_unit`
