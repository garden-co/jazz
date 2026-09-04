# INV-LARGE-6

- Status: now
- Coverage: ✓

## Invariant

Sequential large-value processing MUST bound resident input memory independently of total value size.

## Enforced by (tests)

`large_value_query::{sequential_cursor_reads_post_edit_logical_value_in_atomic_bounded_windows,cached_streaming_checksum_obeys_cooperative_work_budget}`; `groove::large_values::tests::streaming_prepare_matches_one_shot_across_random_input_windows`

## Implementation

`groove/src/large_values.rs::{StreamingBuilder,LargeValueCursor,StreamingChecksum}`
