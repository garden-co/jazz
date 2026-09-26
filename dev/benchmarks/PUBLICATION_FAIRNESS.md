# Completed query delivery versus total receiver work

`publication_fairness` measures whether independent completed query results
wait behind other work in an owner poll. It opens every point subscription
before pumping either database. The fixture uses public Db/query APIs,
synthetic rows, memory storage, and SYSTEM admission. Both modes serve a fresh,
non-durable foreground receiver:

- `JAZZ_FAIR_MODE=global-core` (default): a history-complete owner serves Global
  reads with immediate own-write overlays. This preserves the original diagnostic.
- `JAZZ_FAIR_MODE=local-relay`: accepted rows are seeded, the owner storage is
  reopened as a host-admitted, scope-isolated client relay, and the foreground
  opens Local reads. This exercises the ordinary browser read topology. It has
  no upstream server during measurement and uses memory storage, so it measures
  resident relay work rather than browser or IndexedDB startup latency.

The read modes use different source graphs. A Global-overlay finding must not
be attributed to a browser's default Local read without checking that path.

It reports mode, setup, owner reopen, prepare, subscribe, timed owner polls,
timed foreground turns, result extraction, first result, all results, and
individual completed ViewUpdate queue delays. Owner reopen is part of setup;
setup is excluded from the endpoint. Owner/foreground
measurements are elapsed time around active polls, not operating-system CPU
counters. The host allows receiver progress at each owner suspension boundary.
No browser task latency, IndexedDB delay, authorization cost, or parallel-core
speedup is modeled. The `compilations` field counts entry into query lowering;
it does not count physical template cache misses.

Every result must have the exact expected ID, cardinality, and all 18 field
values. The signature compares all returned values across arms. These checks
validate workload identity; the fixture has no timing threshold.

```sh
JAZZ_FAIR_ROWS=600 JAZZ_FAIR_QUERIES=1,12,60 \
  cargo bench -p jazz --profile perf --no-default-features \
  --features testing,transport-compression-zstd --bench publication_fairness
```

Set `JAZZ_FAIR_MODE=local-relay` on the same command to measure Local reads.
The [Local relay receipt](receipts/publication-fairness-local-relay.json) records
both modes on the same runtime and fixture, with three runs per mode. These
are topology baselines, not before/after optimization gains. The exact IDs and
all returned field values agree across modes.

`JAZZ_FAIR_QUERIES=600` amplifies the fanout for diagnosis. Run timing without
other builds or tests. Sampling/debug runs must be kept outside timing receipts.

## Results and rejected changes

The [receipt](receipts/publication-fairness.json) retains all 72 timed case
observations and source/binary provenance. Each comparison used ABBAABBA with
four process runs per arm. The measured runtime baseline was `7f7d1b2e4`; it is
an integration checkout, not a claim of improvement against this PR's parent.
No tested runtime change is included here.

| 600 rows, 60 point subscriptions       |    Control | Four-publication yields |
| -------------------------------------- | ---------: | ----------------------: |
| First complete point result            | 174.504 ms |              120.920 ms |
| All results                            | 174.505 ms |              202.175 ms |
| Foreground work                        |  29.668 ms |               55.883 ms |
| Largest frame hold, median across runs |  46.892 ms |               10.754 ms |

Yielding exposed results sooner but raised total completion time 15.9%.
It is excluded. No app speedup follows from the first-result number.

A second trial ran the existing static TableSource filter before choosing
which graph nodes to activate. It preserved normal handling for errors,
unbounded/index/binding sources, and source batches larger than 16 records.
It maintained global storage frontiers and retained unaffected node state.
The public-API point test passed updates, an unrelated insertion, deletion,
reinsertion, a later different-point update, and a fresh read. With 10 versus
100 simple point subscriptions, the selected graph work stayed constant.

The Jazz benchmark still regressed under the same four-publication schedule:

| Point subscriptions |  Yield only | Yield plus source filter |
| ------------------- | ----------: | -----------------------: |
| 12                  |   42.605 ms |                42.965 ms |
| 60                  |  206.715 ms |               226.702 ms |
| 600                 | 7203.054 ms |              8765.270 ms |

Without the yields, source filtering was essentially flat for 60 and 600
queries (177.600 to 178.124 ms; 2856.112 to 2865.143 ms). It is excluded too.
The source sets had fewer activated nodes, but other inputs retained most of
the evaluation's required ancestors: a 60-query receive batch changed 1694
activated nodes to 1190 while required graph nodes only fell from 2017 to 1905.
The early projection/filter work did not earn its cost.

In an amplified native stack sample, 3161 of 4236 main-thread samples were in
`apply_view_updates_in_batch` through `ingest_reset_view_bundle_refs_in_bulk`
and Groove `apply_batch`; the dominant descendant was the IVM tick. Public
subscription refresh accounted for 292 samples. These are instrumented work
attribution counts, not browser timings or performance comparisons.

The runtime patches, test, full per-frame arrays, native stack samples and
additional diagnostic output are preserved with the issue follow-up in
[#3569](https://github.com/garden-co/jazz/issues/3569). The code here adds the
reproducer only. No storage/wire representation or public behavior changes.

A [source-identity follow-up](https://github.com/garden-co/jazz/issues/3569#issuecomment-5847776582)
identified the broad Global current-row predecessor fence in the immediate
pending overlay. The three history/transaction/change sources individually
reached only 3–5 nodes; the predecessor source reached 1,143 operators and
1,886 required states at 60 point subscriptions. Point-bounding both the
pending arm and predecessor fence reduced 600-query receiver work but did not
improve the common smaller cases. Combining it with the earlier selector and
yield also regressed total completion. Both trials were removed. This diagnosis
belongs to `global-core`; it does not establish the cause of default Local
browser startup cost.

Tooling friction: match the read mode and relay topology before extrapolating
source traces; retain per-batch activation counts beside first/all-result latency.
