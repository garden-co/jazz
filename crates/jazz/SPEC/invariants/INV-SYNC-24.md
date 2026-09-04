# INV-SYNC-24

- Status: now
- Coverage: ✓

## Invariant

Known-state payload dedup MUST omit only version bodies, never result membership, program facts, or inventory refs; a body may be omitted only under the skip rule — believed membership plus (for fast declarations) settle position at or before the declared `p`, or exact row-version membership for slow declarations; not-yet-fated versions always ship under fast declarations (§8.11).

## Enforced by (tests)

`jazz::node::tests::harness::known_state_rehydrate_skips_known_bodies_and_repairs_missing_payload`; `jazz::node::tests::harness::known_state_declaration_never_skips_unfated_edge_members`; `jazz::node::tests::harness::slow_known_state_declaration_skips_exact_local_versions_only`; `jazz::node::tests::harness::over_cap_slow_known_state_declaration_degrades_to_full_ship`

## Implementation

`protocol.rs::KnownStateDeclaration`; `protocol.rs::SyncMessage::Subscribe`; `protocol.rs::SyncMessage::ViewUpdate`; `node/views.rs::view_update_for_maintained_result_members`; `peer.rs::PeerState::declare_known_state`; `db.rs::PeerConnection::tick`
