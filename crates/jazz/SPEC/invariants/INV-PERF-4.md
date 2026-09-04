# INV-PERF-4

- Status: now
- Coverage: untested

## Invariant

INV-PERF-4 steady-state peer view updates must preserve per-peer complete-tx payload dedup and result-set deltas. Identifiers: PeerState::shipped_complete_tx_payloads, PeerMetrics::{version_bundles_out, duplicate_version_bundles_out, complete_tx_payload_refs_out, result_adds_out, result_removes_out}, SyncMessage::ViewUpdate::{version_bundles, peer_payload_inventory.complete_tx_payloads, result_member_adds, result_member_removes, program_fact_adds, program_fact_removes}. Tests: peer_state_dedups_version_payloads_across_subscription_views.

## Enforced by (tests)

NONE-FOUND

## Implementation
