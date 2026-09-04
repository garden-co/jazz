# INV-BVIEW-20

- Status: now
- Coverage: ✓

## Invariant

The first frozen physical current secondary-index layout is V1: `by_physical_user_v1_<PhysicalColumnId>` is keyed by `(BranchKey, UserIndexKey...)`. Fresh registration and population MUST use V1 directly; no alternate current-index descriptor, decoder, bootstrap, or predecessor-layout backfill path exists.

## Enforced by (tests)

`jazz::node::tests::harness::branch_coordinates_use_one_canonical_prefix_in_memory_and_after_rocks_reopen`; `jazz::node::tests::harness::physical_index_backfills_existing_rows_and_read_cost_ignores_schema_variant_count`

## Implementation

`node/physical/{catalogue,descriptors,projections}.rs`; `node/state/lifecycle.rs`
