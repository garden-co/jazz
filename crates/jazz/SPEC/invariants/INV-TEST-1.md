# INV-TEST-1

- Status: now
- Coverage: ✓

## Invariant

`m3_seeded_run_is_deterministic_for_fixed_seed` proves bit-for-bit replay; `lens_parallel_materialization_oracle_matches_engine_reads_seeded` is the schema/lens seeded oracle gate.

## Enforced by (tests)

jazz::node::tests::sync::m3_seeded_run_is_deterministic_for_fixed_seed; jazz::node::tests::catalogue_lenses::lens_parallel_materialization_oracle_matches_engine_reads_seeded

## Implementation
