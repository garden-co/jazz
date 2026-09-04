# INV-DISC-11

- Status: prov
- Coverage: untested

## Invariant

peer-level complete-tx payload inventory and deterministic counters are implementation artifacts, not semantic state. PeerState owns shipped_complete_tx_payloads, per-subscription state, deferred edge fates, and PeerMetrics; outgoing view update metadata records bundles/refs/result add/remove counts; four-tier tests assert version_bundles_out == shipped_complete_tx_payloads().len() plus duplicate complete payload bundles per link. This is a guidance/process anchor, not runtime conformance.

## Enforced by (tests)

NONE-FOUND

## Implementation
