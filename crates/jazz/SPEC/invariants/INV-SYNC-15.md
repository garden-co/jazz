# INV-SYNC-15

- Status: now
- Coverage: ✓

## Invariant

Exclusive transaction payloads MAY be delivered, stored, and participate partially at the transaction level; receiver-visible subscription state MUST expose them only when complete for the maintained subscription view being served, and partial fragments MUST NOT update whole-database current indexes.

## Enforced by (tests)

`jazz::node::tests::exclusive_transactions::receiver_tracks_partial_exclusive_payload_coverage_per_view`; `jazz::peer::tests::all_exclusive_never_gated_stays_incremental`

## Implementation

`node/views.rs::ingest_view_bundle`; `node/views.rs::retain_policy_atomic_rows`; `node/ingest.rs::ingest_transaction_fragment_without_current_indexes`; `peer.rs::incremental_delta_misses_exclusive_sibling`
