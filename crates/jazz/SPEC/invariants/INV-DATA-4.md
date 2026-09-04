# INV-DATA-4

- Status: now
- Coverage: ✓

## Invariant

`TxTime` and `GlobalTime` MUST encode physical milliseconds in the high 46 bits and a logical counter in the low 18 bits. Their unsigned packed order is canonical. Their allocators MUST advance physical time on logical exhaustion and return a typed overflow only after the final packed position.

## Enforced by (tests)

`jazz::time::tests::tx_time_packs_physical_millis_and_logical_counter`; `jazz::time::tests::packed_hlc_golden_boundaries_are_big_endian_ordered_u64s`; `jazz::time::tests::global_time_has_the_same_packed_boundary_and_overflow_contract`; `jazz::time::tests::logical_exhaustion_advances_physical_time_without_a_clock_panic`; `jazz::time::tests::packed_hlc_reports_typed_overflow_only_at_its_final_position`; `jazz::time::tests::high_same_millisecond_burst_remains_strictly_monotone`

## Implementation

`time.rs::{TxTime,GlobalTime}::{physical_ms,counter,tick}`
