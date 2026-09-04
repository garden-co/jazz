# INV-HIST-15

- Status: now
- Coverage: ✓

## Invariant

Merge strategy behavior MUST be deterministic and grouping-insensitive over the parent/head set; write-time canonicalization remains validation and rejects loudly.

## Enforced by (tests)

`jazz::node::tests::counter_merge::counter_merge_seeded_concurrent_increments_converge_to_exact_sum`

## Implementation

`jazz/src/node/ingest.rs::merge_cells_for_heads`; `jazz/src/node/ingest.rs::counter_merge_value`
