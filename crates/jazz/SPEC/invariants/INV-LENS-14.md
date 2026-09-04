# INV-LENS-14

- Status: now
- Coverage: ✓

## Invariant

For every non-rejected natural lens delta sequence, translating then applying MUST equal applying then translating for all known schema materializations.

## Enforced by (tests)

`jazz::node::tests::catalogue_lenses::lens_parallel_materialization_oracle_matches_engine_reads_seeded`

## Implementation

`jazz/src/oracle.rs::ParallelMaterializationOracle::apply_accepted_write`
