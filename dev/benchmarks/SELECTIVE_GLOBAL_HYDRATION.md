# Selective Global hydration receipt

`crates/jazz/benches/selective_global_hydration.rs` measures the persisted
Global query path that the permission/fan-out investigation identified as
potentially non-selective.

## Question

When a prepared query selects a fixed team and a fixed-size page, does initial
hydration work remain bounded by that selection, or grow with the total number
of current rows in the table?

## Method

Each ladder rung creates a fresh RocksDB fixture with:

- one `documents` table whose `team` column is declared indexed;
- exactly 100 rows in the selected team;
- an increasing number of rows belonging to another team;
- a prepared `team = $team AND active = true` query ordered by
  `updated_at DESC, id DESC` and limited to 50 rows;
- settled Global rows, a clean close, and a fresh RocksDB/Jazz reopen before
  measurement.

The selected IDs and their SHA-256 digest must be identical at every rung. The
receipt records logical reads and ranges for Global-current rows and indexes,
alongside query wall time. Logical reads are the primary mechanism evidence;
wall time is directional because the same-process seed leaves operating-system
cache state uncontrolled.

The default ladder is 10,000 / 100,000 / 1,000,000 total rows. Override it for a
bounded smoke run:

```sh
JAZZ_SELECTIVE_HYDRATION_RECEIPT=1 \
JAZZ_SELECTIVE_HYDRATION_ROWS=1000,10000 \
  cargo bench -p jazz --features testing \
  --bench selective_global_hydration --quiet
```

The final `scale_summary` line reports the table-size ratio and both query-path
and end-to-end Global-current row/index read ratios. Reopen, preparation, and
query execution have separate counters. The query-path summary combines
preparation and execution; the end-to-end summary also includes reopen so work
cannot disappear by moving between phases. This benchmark does not gate the
current behavior: its first purpose is to establish a durable before/after
boundary for selective hydration work.

## Baseline

A single default-ladder run on the local development box produced:

| Total rows | Selected rows | Result rows | Open row reads | Prepare row reads | Query row reads | Query index reads | Query time |
| ---------: | ------------: | ----------: | -------------: | ----------------: | --------------: | ----------------: | ---------: |
|      1,000 |           100 |          50 |          1,000 |                 0 |           1,100 |               100 |   2.443 ms |
|     10,000 |           100 |          50 |         10,000 |                 0 |          10,100 |               100 |  15.793 ms |
|    100,000 |           100 |          50 |        100,000 |                 0 |         100,100 |               100 | 174.877 ms |

All three rungs returned the same ordered-ID digest,
`1ab7cbe99449f81b16f1375ce2f8ff751f03940a6f1287b6d561c0146ec9be3d`.
The declared index supplies the same 100 candidates at every size, while each
rung reads exactly the total table rows plus those 100 candidates from
Global-current row storage. This is evidence that initial hydration currently
combines the selective index walk with table-wide row work. Timings are
illustrative rather than a performance claim; the deterministic logical reads
are the retained baseline. Reopen separately reads every Global-current row;
reporting that phase prevents a future query optimization from hiding
equivalent work in startup.

## Indexed one-shot result

Inlining the concrete one-shot binding before lowering prevents execution from
replacing the index-selected program with the generic cached parameterized
shape. On the same default ladder, query-path reads are fixed at 100 index rows
plus 100 selected current rows for every table size. The 100,000-row query fell
from 174.877 ms in the baseline run to 0.744 ms in the optimized run, while the
ordered-ID digest remained unchanged. Reopen work is unaffected and remains
reported separately.

The benchmark now enforces the structural result: query current-row reads must
not exceed the fixed candidate count, and the candidate count must be supplied
by the declared index. Wall time remains informational rather than gated.

## Acceptance rule for an optimization

An optimization is admissible only if:

1. every rung returns the same exact ordered IDs and digest as this baseline;
2. Global-current row reads are bounded by the selected candidates rather than
   total table rows;
3. the declared Global-current index records non-zero use when it supplies the
   candidate set; and
4. no full-table work is moved into an unreported preparation or reopen phase.

Tooling friction: a reusable settled fixture would avoid reseeding each scale
rung while preserving the fixed-selection read boundary.
