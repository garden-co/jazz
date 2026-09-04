# INV-EDGE-5

- Status: now
- Coverage: untested

## Invariant

Edge-local fate assignment MUST support only `TxKind::Mergeable`; an edge MUST NOT use the edge mergeable path to assign fate for `TxKind::Exclusive`.

## Enforced by (tests)

NONE-FOUND

## Implementation

`peer.rs::PeerState::ingest_edge_mergeable_commit_unit`
