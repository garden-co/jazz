# INV-STORAGE-26

- Status: now
- Coverage: ✓

## Invariant

Record-store persistence is row-only: each logical stored record has its canonical row key/value entry, and no storage maintenance may replace a run of rows with a second logical representation.

## Enforced by (tests)

`groove::db::tests::history_rows_remain_plain_across_hydration_post_write_and_reopen`

## Implementation

`groove/src/storage/mod.rs::RecordStore`
