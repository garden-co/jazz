# Windowed Absence Index

Date: 2026-07-23
Branch: `track/windowed-absence-index`

## Summary

Implemented a per-column-family window range index for windowed `RecordStore`s.
The index is stored under reserved meta key `\xffGWIN2-RANGE-INDEX` and contains
ordered `(window storage key, max_key)` entries. Existing window and record entry
formats are unchanged.

## Change Classification

- CLEARLY-GOOD: `is_window_meta_key` now hides the range-index meta key from
  logical record scans, matching marker/cursor behavior.
- CLEARLY-GOOD: consolidation updates the range index in the same
  `OwnedWriteOperation` batch that deletes plain rows and writes window entries.
  If the index is absent, consolidation first reconstructs it from existing
  window entries, then applies the staged window set/delete operations before
  appending the index write to the same atomic batch.
- CLEARLY-GOOD: `get_raw_run_aware` uses the range index to restrict absent-key
  probing to windows whose `[window_key, max_key]` could cover the key. Plain
  tail records are not touched after the exact point lookup misses.
- CLEARLY-GOOD: legacy stores migrate lazily. If the marker exists but the range
  index key is absent, the first lookup uses the legacy backwards walk for exact
  result compatibility, then writes a freshly rebuilt range index. Later lookups
  use the indexed path.
- SPECULATIVE: the range/prefix run-aware paths still use the existing full
  logical scan/decode path. They were not changed because the observed blocker is
  point absent lookup, and changing range/prefix semantics would require a wider
  design pass.

## Consistency Notes

The only code path found that creates encoded window values is
`append_window_operations`, called from the consolidation methods before one
`write_many`. This change hooks that path.

The storage layer still exposes raw `RecordStore::delete`/`set` operations that
can target any physical key, including a window start key, but the current window
design already treats consolidated windows as immutable maintenance output. I did
not add approximate index repair to generic raw writes; if a future feature
intentionally mutates window entries outside consolidation, it needs to update or
rebuild `\xffGWIN2-RANGE-INDEX` in the same atomic batch.

## Receipts

Focused regression:

```text
cargo test -p groove windowed_absent_get_uses_range_index_instead_of_plain_tail_walk -j 2 -- --nocapture
test storage::tests::windowed_absent_get_uses_range_index_instead_of_plain_tail_walk ... ok
```

The regression builds three consolidated windows plus a 5k plain tail, checks an
absent-key `get_raw` through `StorageReadMetrics`, and explicitly compares
indexed vs legacy results for present-in-window, present-as-plain, absent inside
a window span, and absent after the plain tail.

Scaling receipt from the same run:

| Plain tail records | Legacy absent lookup history-row reads | Indexed absent lookup history-row reads |
| -----------------: | -------------------------------------: | --------------------------------------: |
|              1,000 |                                  1,003 |                                       3 |
|              5,000 |                                  5,003 |                                       3 |
|             20,000 |                                 20,003 |                                       3 |

Tooling-friction: a prebuilt or cached RocksDB test artifact would have saved
about 18 minutes on the first focused `groove` test compile.

## Part 2: run-aware range scans

Implemented bounded run-aware range collection for windowed `RecordStore`
`range`, `prefix`, `scan_range`, and the new `range_reverse` helper. Indexed
stores now scan plain physical records only inside the requested bounds and use
`\xffGWIN2-RANGE-INDEX` to load only windows whose `[window_key, max_key]`
intersects those bounds.

### Change Classification

- CLEARLY-GOOD: `range(start, end)` no longer full-scans the history column
  family when a window range index is present. It range-scans plain rows in
  `[start, end)` and point-reads only candidate windows with
  `window_key < end && max_key >= start`.
- CLEARLY-GOOD: `prefix(prefix)` uses the minimal finite prefix upper bound
  when one exists, so narrow exact-key/prefix probes avoid unrelated plain tail
  records. Prefixes without a finite upper bound retain prefix-scan semantics.
- CLEARLY-GOOD: merge ordering is explicit. Bounded plain rows and candidate
  windows are sorted by physical key and inserted into a `BTreeMap`, matching
  the legacy full-scan overwrite behavior before returning logical key order.
  `range_reverse` reverses that final logical ordering.
- CLEARLY-GOOD: legacy stores without `\xffGWIN2-RANGE-INDEX` still take the
  previous full-scan path for result compatibility, then lazily write the index
  for later calls.
- SPECULATIVE: reverse range is now exposed as a `RecordStore` helper because
  this checkout did not have a pre-existing public `range_reverse` method on
  `RecordStore`; existing reverse prefix behavior continues to reverse
  `prefix()` output.

### Receipts

Focused regression:

```text
cargo test -p groove windowed_run_aware_ranges_use_bounded_plain_and_candidate_window_reads -j 2 -- --nocapture
test storage::tests::windowed_run_aware_ranges_use_bounded_plain_and_candidate_window_reads ... ok
```

The regression builds three consolidated windows plus a 5k plain tail. It
asserts bounded `range`, `prefix`, and `range_reverse` reads through
`StorageReadMetrics`, and compares indexed vs no-index legacy paths for
plain-only, window-only, mixed, and empty ranges in both forward and reverse
order.

Scaling receipt from the same run:

| Plain tail records | Legacy narrow range_reverse history-row reads | Indexed narrow range_reverse history-row reads |
| -----------------: | --------------------------------------------: | ---------------------------------------------: |
|              1,000 |                                         1,005 |                                              5 |
|              5,000 |                                         5,005 |                                              5 |
|             20,000 |                                        20,005 |                                              5 |

Tooling-friction: a single in-repo helper for constructing legacy/no-index
windowed stores would have saved a bit of bespoke regression setup.
