# INV-SYNC-16

- Status: now
- Coverage: ✓

## Invariant

A mergeable transaction MAY be delivered and applied partially; each visible mergeable version can contribute without waiting for `tx.n_total_writes`.

## Enforced by (tests)

`jazz::node::tests::sync::receiver_tracks_partial_mergeable_payload_coverage`

## Implementation

`node/views.rs::ingest_view_bundle`; `node/ingest.rs::ingest_known_transaction`
