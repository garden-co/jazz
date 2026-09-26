# IndexedDB resident range seeks, 2026-09-26

## Work removed

Both forward and reverse range traversal checked every key in each resident
leaf and every child separator in each internal node. A narrow range that
returns one row, or no rows, paid that linear search cost on every call.

The change binary-seeks the intersecting child interval and matching leaf
interval. A leaf wholly within the bounds skips the boundary searches. The
page format, transaction/history validation, corruption checks, missing-page
batching, ownership fences and returned value copies are unchanged.

Why this merits a trial: browser startup profiling sampled this traversal
under transaction-index prefix reads during view-bundle validation. This is a
frequently repeated operation; skipping validation is not part of the change.
No application startup speedup has yet been established for this patch.

## Matched native receipt

20,000 persisted rows with 32-byte keys and 48-byte values; keys are spaced to
include empty gaps. The receipt uses the public `IdbTree` and `PageStore` API,
an in-memory backing store with no invented latency, and the native `perf`
profile. Each selective phase makes 20,000 distributed requests. The broad
phase makes 16 complete 20,000-row scans. Reads and output consumption are
timed; fixture construction, reopen and cache population are separate phases.

Order: control, candidate, candidate, control, repeated twice. Four samples
per arm, with no concurrent compiler builds or instrumented app captures.
This is native CPU work under Rust's system allocator and perf opt-level 2,
not an IndexedDB round-trip or release-WASM startup measurement. No timings
from other PRs are added to these ratios.

At the default 16 KiB page size, medians in milliseconds:

| Phase          | Control | Candidate | Ratio |
| -------------- | ------: | --------: | ----: |
| hit            |  42.906 |    15.715 | 2.73× |
| miss           |  38.564 |    13.238 | 2.91× |
| reverse_hit    |  85.644 |    16.338 | 5.24× |
| bounded_prefix |  39.781 |    26.043 | 1.53× |
| broad          |  28.824 |    26.447 | 1.09× |

The strongest repeatable effect is on selective ranges. The small broad-scan
delta is not the reason to ship the change.

At 1 KiB pages (a nondefault shape with fewer entries per leaf):

| Phase          | Control | Candidate | Ratio |
| -------------- | ------: | --------: | ----: |
| hit            |  20.466 |    17.718 | 1.16× |
| miss           |  17.404 |    13.668 | 1.27× |
| reverse_hit    |  26.834 |    18.575 | 1.44× |
| bounded_prefix |  34.319 |    32.261 | 1.06× |
| broad          |  34.038 |    32.301 | 1.05× |

All warm phases issue zero store reads. Both arms return the same result
counts/signatures and cold page/call counts in all eight observations.
The first trial binary-searched even fully covered leaves and slowed broad
1 KiB scans; that implementation was not retained. The report below pins the
final child-slice iteration and full-leaf fast path only.

## Setup and cold-read attribution

Medians in milliseconds, control → candidate:

| Page size   |    Seed + flush |        Reopen | 64 cold selective reads | Populate remaining cache |
| ----------- | --------------: | ------------: | ----------------------: | -----------------------: |
| 1024 bytes  | 27.267 → 27.012 | 0.002 → 0.002 |           0.498 → 0.450 |            7.934 → 8.264 |
| 16384 bytes | 20.096 → 19.752 | 0.001 → 0.002 |           1.558 → 1.283 |            5.151 → 5.102 |

## Complete measured phase samples

Each cell lists forward hit / missing range / reverse hit / bounded seven-row
prefix / full scan, in milliseconds. All observations are retained.

| Order | Arm       | 1 KiB                                      | 16 KiB                                     |
| ----: | --------- | ------------------------------------------ | ------------------------------------------ |
|     1 | control   | 19.134 / 17.605 / 26.890 / 34.409 / 34.059 | 42.433 / 39.747 / 87.892 / 39.987 / 29.370 |
|     2 | candidate | 17.501 / 14.727 / 18.577 / 33.729 / 32.304 | 17.805 / 14.167 / 16.201 / 23.908 / 26.087 |
|     3 | candidate | 18.925 / 14.603 / 18.573 / 32.355 / 31.822 | 15.640 / 14.367 / 16.764 / 26.648 / 26.194 |
|     4 | control   | 20.908 / 17.489 / 29.218 / 37.666 / 41.474 | 44.488 / 36.779 / 83.939 / 37.735 / 28.957 |
|     5 | control   | 20.046 / 17.318 / 26.777 / 31.113 / 33.055 | 42.290 / 37.380 / 82.578 / 39.574 / 26.362 |
|     6 | candidate | 17.936 / 12.732 / 17.551 / 31.706 / 32.298 | 15.789 / 11.037 / 16.475 / 25.437 / 26.699 |
|     7 | candidate | 16.766 / 11.802 / 18.980 / 32.168 / 33.562 | 13.917 / 12.308 / 14.789 / 27.222 / 27.106 |
|     8 | control   | 20.887 / 16.681 / 24.205 / 34.229 / 34.018 | 43.379 / 40.064 / 87.350 / 41.025 / 28.691 |

## Correctness and scope

- New public storage tests compare exact key/value output with an ordered-map
  oracle at every stored key and adjacent gap, using cold multi-level trees,
  forward/reverse reads, limits 0/1/7/unbounded, equal/reversed bounds,
  variable-length keys, overflow values, staged deletes/updates and reopen.
- All 61 `idb-tree` tests pass, including existing corrupt-page, ownership,
  abandoned-commit and cold-frontier batching cases. Existing tests are unchanged.
- Browser-target `cargo check -p idb-tree --target wasm32-unknown-unknown` passes.
- Scoped Clippy for the library, new benchmark and new test passes with
  `-D warnings`. All-target Clippy still fails on an existing `options.clone()`
  in a library test (`clone_on_copy`); that line is present in the parent and
  was left intact.
- Broader Jazz canaries/oracle and hosted landing gates are tracked in the PR.
  This focused local receipt is not CI-equivalent. The optional private
  sensitive-data guard is not available in this checkout.

Examples: a range `[key(40), key(41))` visits only intersecting child pages and
returns exactly key 40; an absent prefix returns no rows after logarithmic
search within the resident pages. Equal or reversed bounds still return no
rows. A cold bounded scan still stops at its first missing page and does not
hydrate beyond the result limit. An unbounded cold scan still collects the
entire missing frontier. Encountered invalid/shared pages still fail.

## Preflight and provenance

Read the rejected-experiment ledger and searched preserved branches plus open
and closed PR descriptions. [#3431](https://github.com/garden-co/jazz/pull/3431)
already fixes cold-page round trips and repeated cloning; its behavior is
preserved. [#1001](https://github.com/garden-co/jazz/pull/1001) and older OPFS
commits optimize a different backend. No rejected equivalent idb-tree range
seek was found. No durable or wire encoding changes are introduced here.

Build both arms with:

```sh
CARGO_INCREMENTAL=0 cargo bench -p idb-tree --profile perf --bench selective_ranges --no-run
```

Run the preserved executables in ABBAABBA order after compilation is complete.
Both use the exact same benchmark source and parent
`3466279eb6badae76c2836366a88bc1f82e1eba0`; only the range implementation differs.
Rust 1.93.1, aarch64-apple-darwin. The regression test is not linked into the
benchmark binary.

- Control binary SHA256: `501cfaa71f6335f0498d2f9f3c1506d18f457db5c0e41fd55ed0a9626092a6d3`.
- Candidate binary SHA256: `4a35fa4ef478b6298125264d3df7596375c0c922de6d13c7598dd272e41ea70b`.
- Fixture source SHA256: `9431180bbcaa60bf391c2f572d1abf55b1f2f1292b5f199dd1464c8bb14662e4`.

Exact source copies, patches, binaries and raw output are retained locally in
`target/local-perf-handoff/idb-range-final-trial/`; the earlier trial receipts
remain in sibling directories.

Tooling-friction: the small native idb-tree leaf builds in seconds; maintaining
this selective-range receipt avoids an unnecessary WASM/package rebuild per trial.
