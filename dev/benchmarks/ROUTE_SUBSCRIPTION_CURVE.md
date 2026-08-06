# Current-core route/subscription scaling receipt

Date: 2026-08-05

Status: quiet developer-machine result, not a performance promise.

## Question

After the refresh batching and selective-hydration work, what still grows when
one current-core query shape is retained across many distinct application
bindings: shared graph state, write refresh, or binding-private maintained
state?

The retained harness is
`crates/jazz/benches/route_subscription_curve.rs`. It uses public Jazz schema,
query, subscription, and write APIs plus existing test-only sizing diagnostics.
Every initial result and later mutation is checked exactly.

## Workload

- `MemoryStorage`, local reads, immediate local updates;
- one public `documents` table with `team`, `updated_at`, and `title`;
- 2,000 documents across 1,001 teams;
- team 0 has 1,000 documents and every other team has one;
- one parameterized `team = $team` query, ordered by `updated_at DESC`, limited
  to 100 rows;
- 1 / 10 / 100 / 1,000 retained bindings of the same validated shape;
- one matching above-boundary insert, one unrelated-team insert, and one
  matching below-boundary insert;
- a fresh process per route count so RSS and allocator retention remain
  attributable.

The measurements ran from `d4ebc2a74` with only the benchmark and receipt
changes present. The one-minute host load was 0.82 before the release matrix.

Example command:

```sh
JAZZ_ROUTE_CURVE_ROUTES=100 \
  cargo bench --profile perf -p jazz --features testing \
  --bench route_subscription_curve --quiet
```

The smoke ledger retains the 10-route correctness lane.

## Results

| Routes | Total attach | Median subscribe | Matching write | Unrelated write | Below-boundary write | Private maintained/control estimate | Process RSS after attach |
| -----: | -----------: | ---------------: | -------------: | --------------: | -------------------: | ----------------------------------: | -----------------------: |
|      1 |     38.28 ms |         38.24 ms |        0.93 ms |         0.13 ms |              0.71 ms |                            5.30 MiB |                49.46 MiB |
|     10 |    151.16 ms |         12.58 ms |        1.22 ms |         0.23 ms |              0.80 ms |                           52.12 MiB |               141.98 MiB |
|    100 |      1.292 s |         12.54 ms |        3.41 ms |         1.37 ms |              1.78 ms |                          520.36 MiB |                 1.04 GiB |
|  1,000 |     14.285 s |         14.23 ms |       25.17 ms |        20.45 ms |             19.94 ms |                            5.08 GiB |                10.16 GiB |

All initial resets, the matching add/evict delta, and both quiet-write oracles
were exact at every scale.

## Attribution

The shared Groove structures are not the dominant slope:

| Routes | Graph nodes | Arrangements | Arrangement rows | Arrangement encoded bytes |
| -----: | ----------: | -----------: | ---------------: | ------------------------: |
|      1 |          81 |           15 |           19,001 |               3,331,365 B |
|     10 |          99 |           15 |           19,019 |               3,334,020 B |
|    100 |         279 |           15 |           19,199 |               3,360,750 B |
|  1,000 |       2,079 |           15 |           20,999 |               3,629,850 B |

The private maintained view is the slope. Every binding retains witnesses for
the complete 2,000-row source even when its result contains one row:

| Routes | Result rows | Version identities | Replacement entries | Version-index bytes | Replacement-index bytes |
| -----: | ----------: | -----------------: | ------------------: | ------------------: | ----------------------: |
|      1 |         100 |              2,000 |               2,000 |         3,304,380 B |             2,143,028 B |
|     10 |         109 |             20,000 |              20,000 |        33,043,800 B |            21,430,280 B |
|    100 |         199 |            200,000 |             200,000 |       330,438,000 B |           214,302,800 B |
|  1,000 |       1,099 |          2,000,000 |           2,000,000 |     3,304,380,000 B |         2,143,028,000 B |

Source inspection agrees with the receipt. Maintained version and replacement
witness terminals are built from each complete resolved source without routing
fields. Each bound `LocalMaintainedViewSubscription` then owns independent
`WeightedVersionIndex` and `ReplacementIndex` instances so it can materialize
row versions and replacement winners when result membership changes.

## Conclusion

The old repeated-flush write pathology is no longer the dominant route-scale
cost. Current writes remain tens of milliseconds at 1,000 routes, including
quiet writes. The remaining clear problem is full-source witness retention and
attach work repeated once per binding.

This receipt does **not** propose routing or sharing witness state. Witnesses
support version materialization, transaction lookup, replacement-winner
selection, deletions, and rows entering a finite window later. Narrowing them
requires an explicit correctness design and adversarial transition coverage.
The next investigation should compare two designs:

1. share route-independent version/replacement witness indexes across bindings
   while retaining binding-private result membership; or
2. route witness facts to bindings and prove that every row entering a result
   has the required content, deletion, and replacement witnesses in the same
   transition cycle.

Tooling friction: fresh worktrees need enough disk for a Rust target; old
recoverable build outputs had filled the filesystem before this run.
