# groove — Specification · Appendix B. Benchmarks

*Non-normative (guidance).* How groove is measured, and how to read the numbers.
Retained timings are developer-laptop, directional data — never read a p50 here
as a promise.

## B.1 The harnesses

Benchmarking separates system behavior from local overhead. Scenario benchmarks
exercise stateful workloads in which each commit changes the database and the
next commit observes that changed state; microbenchmarks isolate small local
costs and are useful only as supporting signals.

The benchmark suite uses two custom `harness = false` Cargo bench binaries.
**`scenario`** is the stateful workload harness, selected by `GROOVE_SCENARIO`
(`social_feed`, `acl`, `oneshot`) and `GROOVE_ENGINE`. **`micro`** is a
stateless microbenchmark (`GROOVE_MICRO_ITERS`) over `record_encode_decode`,
`query_planning`, and `subscribe_unsubscribe`, emitting nanosecond quantiles.
Microbenchmarks measure local overheads only — they have no oracle and must not
outweigh scenario runs.

## B.2 The validity contract

Benchmark numbers are admissible only after correctness has been checked. A
scenario run is **valid only if the harness-maintained materialized cache equals
its oracle after the run** — `expected_feed_rows` for social feed,
`expected_acl_rows` for ACL, and a fresh re-query for SQLite baselines. This is
the benchmark-side enforcement of the ch. 7 oracle property: a fast run that
disagrees with recompute is a failed run, not a result.

## B.3 Durability comparability

Benchmark comparisons record their durability assumptions explicitly. groove
scenario runs use `Durability::WalNoSync` (WAL on, per-write sync off) and
report `"wal_no_sync"`; SQLite baselines use `journal_mode=WAL` +
`synchronous=NORMAL` and report `"wal_normal"`. The labels travel in the JSON so
comparisons stay honest about the durability setting.

## B.4 Scenarios and engines

The scenario set is deliberately small: it covers a join-heavy subscription
workload, a recursive authorization workload, and one-shot query overhead.

- **social_feed** — per-user feed over `follows ⋈ posts`. Engines:
  `groove` (one literal graph per subscriber), `groove_prepared` (one builder
  prepared shape), `groove_prepared_sql` (the SQL prepare/bind headline path),
  vs `sqlite_naive` / `sqlite_indexed` baselines.
- **acl** — recursive group-reachability ⋈ grants. Engines:
  `groove` (one recursive graph per principal), `groove_prepared` (one shared
  parameterized recursive graph), vs SQLite baselines `sqlite_naive` /
  `sqlite_indexed` (a recursive-CTE baseline; per Known staleness below, these
  two ACL names run the same re-query-every-principal path).
- **oneshot** — latency for `groove_query`, `groove_subscribe`, and
  `groove_scan` (raw storage `scan_prefix`).

## B.5 Counters and how to read them

The report combines noisy elapsed-time data with deterministic shape data. The
JSON report carries timings (microsecond p50/p95/p99/max; `commit_us` wraps the
whole op, split into `storage_us` vs `tick_us` to separate base-write from
IVM-propagation cost) and **deterministic structural counters** —
`notifications`, `notification_records`, `engine_records_processed`,
`graph_nodes`, `arrangements`, `arrangement_rows`/`bytes`,
`logical_nodes_requested`, `deduped_graph_nodes`, `dedupe_ratio`,
`result_cache_rows`. The discipline: **deterministic counters are hard regression
signals** (a change is a bug or an intended behavior change), while **timings are
directional** — compare by ratio or repeated medians. (`engine_records_processed`
counts output deltas processed, not storage rows scanned.)

## B.6 Retained-baseline workflow

`bench_run.py` writes JSONL enriched with git SHA, dirty state, host, rustc, and
the active `GROOVE_*` knobs, under `benchmarks/results/` (not `target/`).
`bench_summary.py` medians retained rows; `bench_compare.py` equality-checks
deterministic counters and ratios timings. SQLite baselines are usually frozen
and refreshed only when fixtures, SQL/indexes, durability, or knobs change — this
keeps laptop cost down while preserving a comparison target.

> **Known staleness:**
>
> - The ACL `sqlite_naive` vs `sqlite_indexed` names currently run the same
>   re-query-every-principal path.
> - The first retained baseline uses three repetitions at `10 100 1000`; with
>   density fixed at 50 follows per subscriber, the literal per-subscription
>   snapshot path does not produce a 10k result promptly on a laptop. That
>   missing right edge is intentional for that retained baseline and is itself
>   part of the parameterized-subscription motivation.

---

## In flight & operational detail (non-normative)

*B.1–B.6 above are the durable methodology. The following is the operational
detail — commands, knobs, the headline/ACL driver workflow, and retained
results — from the former groove README benchmarks section. Where it conflicts
with B.1–B.6, B.1–B.6 win.*

The scenario harness is custom because the interesting workloads are stateful:
each commit changes the database and the next commit builds on it.

```sh
GROOVE_SCENARIO=social_feed GROOVE_ENGINE=groove cargo bench -p groove --bench scenario --quiet
```

Each scenario run emits one JSON object. Groove uses `Durability::WalNoSync`
(WAL enabled, no per-commit fsync); SQLite baselines use matching
`PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;`. The report separates
`storage_us` from `tick_us`, includes notification counts and bytes-ish row
counts, and fails the run if the materialized subscription state differs from
the harness oracle.

Scenarios:

- `GROOVE_SCENARIO=social_feed`: per-user feed subscriptions over
  `follows ⋈ posts`. Engines: `groove`, `groove_prepared`, `groove_prepared_sql`,
  `sqlite_naive`, `sqlite_indexed`.
- `GROOVE_SCENARIO=acl`: recursive group reachability joined to grants.
  Engines: `groove`, `groove_prepared`, `sqlite_naive`, `sqlite_indexed`. Set
  `GROOVE_ACL_SERIES=insert|delete` to separate incremental-friendly inserts
  from the recursive-recompute retraction cliff.
- `GROOVE_SCENARIO=oneshot`: one-shot query overhead. Engines:
  `groove_query`, `groove_subscribe`, `groove_scan`. Here `commit_us` is the
  per-query latency, not a write commit.

Common knobs: `GROOVE_SEED`, `GROOVE_SUBSCRIPTIONS`, `GROOVE_COMMITS`, plus
scenario-specific sizes such as `GROOVE_USERS`, `GROOVE_INITIAL_FOLLOWS`,
`GROOVE_PRINCIPALS`, `GROOVE_GROUPS`, `GROOVE_RESOURCES`, `GROOVE_ROWS`,
`GROOVE_AUTHORS`, and `GROOVE_QUERIES`. For social feed,
`GROOVE_INITIAL_FOLLOWS` defaults to `50 * GROOVE_SUBSCRIPTIONS`, so the
headline curve keeps per-subscriber feed density roughly stable as the
subscription count changes.

The headline curve driver sweeps social-feed subscription counts. Its default
reruns only `groove_prepared_sql`, the public SQL prepare/bind surface, and
compares it against the retained SQLite baseline.

```sh
GROOVE_BENCH_REPETITIONS=3 \
  scripts/bench_headline.sh \
  > benchmarks/results/headline-worktree.jsonl

scripts/bench_merge_baseline.py \
  benchmarks/results/headline-v1.jsonl \
  benchmarks/results/headline-worktree.jsonl \
  > target/headline-with-frozen-sqlite.jsonl

scripts/bench_summary.py target/headline-with-frozen-sqlite.jsonl
```

Override the sweep with `GROOVE_BENCH_SUBSCRIPTIONS="10 100"` or
`GROOVE_BENCH_ENGINES="groove_prepared"`. Set `GROOVE_BENCH_REPETITIONS=3` to
keep repeated runs for noisy laptops.

Rerun SQLite only when fixture generation, SQLite SQL/indexes, durability, or
workload knobs change:

```sh
GROOVE_BENCH_REPETITIONS=3 scripts/bench_headline.sh --with-sqlite \
  > benchmarks/results/headline-vNext.jsonl
```

ACL uses the same frozen-baseline workflow:

```sh
GROOVE_BENCH_REPETITIONS=3 \
  scripts/bench_acl.sh \
  > benchmarks/results/acl-worktree.jsonl

scripts/bench_merge_baseline.py \
  benchmarks/results/acl-v1.jsonl \
  benchmarks/results/acl-worktree.jsonl \
  > target/acl-with-frozen-sqlite.jsonl
```

For retained baselines, write under `benchmarks/results/` rather than
`target/`. The runner enriches every JSON line with git SHA, dirty state,
timestamp, host, platform, rustc version, run id, and active `GROOVE_*`
settings:

```sh
mkdir -p benchmarks/results
GROOVE_BENCH_RUN_ID=baseline-v0 \
  scripts/bench_headline.sh \
  > benchmarks/results/$(date -u +%Y%m%d)-$(git rev-parse --short HEAD)-$(hostname -s)-headline.jsonl
scripts/bench_summary.py benchmarks/results/*.jsonl
scripts/bench_compare.py old.jsonl new.jsonl
```

Treat deterministic counters (`notifications`, graph/arrangement counts,
cache rows, processed records) as hard regression signals. Treat timings as
directional on developer laptops and compare ratios or repeated medians.

Retained baseline files distinguish the literal per-subscription implementation,
explicit parameterized prepared shapes for social feed and ACL, and the SQL
prepare/bind headline social-feed engine. Files that include both
`groove_prepared_sql` and `groove_prepared` keep `groove_prepared` as a
counter-equality cross-check. Developer-laptop social-feed medians:

| subscriptions | groove_prepared_sql p50 / p95 | sqlite_indexed p50 / p95 | delivered rows | groove graph nodes / arrangements |
| ---: | ---: | ---: | ---: | ---: |
| 10 | 28us / 45us | 90us / 506us | 749 | 9 / 4 |
| 100 | 78us / 256us | 793us / 4.8ms | 6,035 | 9 / 4 |
| 1,000 | 516us / 2.1ms | 13.5ms / 49.8ms | 52,144 | 9 / 4 |
| 10,000 | 5.8ms / 25.4ms | 312ms / 735ms | 509,258 | 9 / 4 |

The retained ACL prepared-shape baseline keeps one prepared recursive graph for
all subscribed principals:
insert-series p50 is 65us for `groove_prepared` vs 10.2ms for `sqlite_indexed`;
delete-series p50 is 11.8ms vs 9.3ms, which is the remaining recursive
recompute cliff in measured form.
