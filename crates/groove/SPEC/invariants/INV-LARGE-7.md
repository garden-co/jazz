# INV-LARGE-7

- Status: now
- Coverage: ✓

## Invariant

Cache ownership, evaluation leases, and returned-result ownership MUST have separate lifetimes and accounting; durable IVM state MUST NOT accidentally pin input chunks.

## Enforced by (tests)

`groove::chunks::tests::byte_budget_evicts_verified_ownership_without_invalidating_live_bytes`; `large_value_query::{sequential_cursor_reads_post_edit_logical_value_in_atomic_bounded_windows,graph_streaming_checksum_yields_and_publishes_one_complete_row}`

## Implementation

`groove/src/chunks.rs::{VerifiedChunkCache,LeasedBytes}`; `groove/src/ivm/runtime/evaluation_session.rs`
