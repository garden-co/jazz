# INV-TX-23

- Status: now
- Coverage: ✓

## Invariant

Fate authority MUST be structurally wired by the host. Applying a bare unfated commit unit on a non-authority sync path MUST stage or park it pending remote fate; it MUST NOT accept, assign global timestamp, or create merge versions from that payload.

## Enforced by (tests)

`jazz::tests::four_tier::edge_peer_terminates_client_identity_and_relays_upstream`; `jazz::node::tests::harness::merge_heads_match_history_for_relay_pending_then_edge_fate`; `jazz::node::tests::harness::edge_current_rows_include_edge_accepted_ahead_versions`

## Implementation

`jazz/src/node/mod.rs::NodeState::apply_sync_message`; `jazz/src/node/ingest.rs::NodeState::ingest_commit_unit_once`; `jazz/src/peer.rs::PeerState::ingest_edge_mergeable_commit_unit`
