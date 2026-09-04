# INV-TX-22

- Status: now
- Coverage: ✓

## Invariant

Downstream incomplete exclusive bundles MUST be stored but remain invisible for subscription views whose required exclusive payload is incomplete; they MAY become visible for a maintained subscription view once that view's required exclusive versions are present, even before all `n_total_writes` versions are known.

## Enforced by (tests)

`jazz::node::tests::exclusive_transactions::receiver_tracks_partial_exclusive_payload_coverage_per_view`

## Implementation

`jazz/src/node/views.rs::NodeState::apply_view_update`; `jazz/src/node/ingest.rs::NodeState::ingest_transaction_fragment_without_current_indexes`; `jazz/src/node/mod.rs::NodeState::subscription_current_rows`
