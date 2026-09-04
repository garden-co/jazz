# INV-QUERY-19

- Status: now
- Coverage: ✓

## Invariant

Exclusive transaction view shipping MUST be view-atomic, not transport-atomic: a visible exclusive result for a maintained subscription view MUST include every exclusive version required by that view, but the `VersionBundle` MAY omit transaction versions outside that view.

## Enforced by (tests)

`jazz::node::tests::exclusive_transactions::receiver_tracks_partial_exclusive_payload_coverage_per_view`

## Implementation

`node/views.rs::NodeState::view_update_for_query_binding_with_peer_payload_inventory_and_plan`; `node/views.rs::NodeState::retain_policy_atomic_rows`; `node/ingest.rs::NodeState::ingest_transaction_fragment_without_current_indexes`
