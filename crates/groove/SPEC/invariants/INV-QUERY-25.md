# INV-QUERY-25

- Status: now
- Coverage: ✓

## Invariant

A record straddling a window boundary MUST contribute exactly its in-window copies, as one output record whose weight is the in-window copy count.

## Enforced by (tests)

`groove::db::tests::top_by_offset_splits_duplicate_copies_across_boundary`

## Implementation

`groove/src/ivm/runtime/mod.rs::top_by_window_from_records`
