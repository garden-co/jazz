# INV-QUERY-24

- Status: now
- Coverage: ✓

## Invariant

`TopByOp` MUST apply bag semantics to window occupancy: a record with positive multiplicity `m` occupies `m` consecutive ordinals of the partition's ordered stream, the retained window is the ordinal range `[offset, offset + limit)` (all ordinals `>= offset` when unbounded), and records with non-positive multiplicity are absent.

## Enforced by (tests)

`groove::db::tests::top_by_counts_duplicate_multiplicity_toward_window_occupancy`; `groove::db::tests::top_by_finite_zero_stays_empty`; `groove::ivm::runtime::tests::top_by_distinguishes_finite_max_from_unbounded_limit`

## Implementation

`groove/src/ivm/runtime/mod.rs::top_by_window_from_records`
