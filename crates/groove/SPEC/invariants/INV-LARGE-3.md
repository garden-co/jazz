# INV-LARGE-3

- Status: now
- Coverage: ✓

## Invariant

Blocked, yielded, cancelled, stale, or failed large-value work MUST publish no partial node, arrangement, index, or terminal state.

## Enforced by (tests)

`large_value_query::chunk_failure_is_reported_without_publishing_a_partial_result`; `large_value_query::graph_streaming_checksum_failure_publishes_nothing_and_can_retry`; `large_value_query::subscription_materializes_large_insert_and_update_deltas_atomically`; `async_hydration_session::cancelling_cold_hydration_releases_a_later_shared_subscription`; `async_hydration_session::hydration_failure_ends_only_affected_terminal_and_releases_later_work`

## Implementation

`groove/src/ivm/runtime/evaluation_session.rs`; `groove/src/ivm/runtime/evaluator.rs`
