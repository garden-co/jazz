# INV-DISC-3

- Status: prov
- Coverage: untested

## Invariant

relays and peers are roles over the same node/message vocabulary, not separate semantic implementations. Node::ingest_relay_commit_unit stores pending local units without assigning fate (ingest.rs lines 299-367); PeerRole::{Relay, ClientLink} controls link identity/read narrowing (peer.rs lines 34-58); four-tier tests use Node and PeerState across UI/worker/edge/core (four_tier.rs lines 156-180; threaded_four_tier.rs lines 352-391). This is a guidance/process anchor, not runtime conformance.

## Enforced by (tests)

NONE-FOUND

## Implementation
