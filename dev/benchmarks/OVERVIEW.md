# Benchmarks: current map and performance roadmap

An orientation for anyone picking up performance work. Originally compiled
2026-07-28 by reading the benchmark sources rather than trusting their names;
updated 2026-07-31 after the incremental-delivery, persisted-read, cold-settle,
policy/fan-out, and relation-hydration investigations.

## The short version

We have enough benchmarks to identify and verify important engine wins. The
remaining problem is not a total absence of receipts: it is that retention and
comparison are still split across several workflows, and the highest-value
remaining specification properties do not yet have controlled scale sweeps.

Do not block useful performance work on a repository-wide receipt rewrite.
Every new high-value lane should emit an attributable, correctness-checked
receipt and state where that receipt is retained.

| harness                        | output and retention                                                                                                                                                |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dev/benchmarks/smoke.sh`      | JSONL receipts plus committed previous/delta history in `SMOKE_LEDGER.md`                                                                                           |
| realistic native/browser       | per-scenario JSON; selected CI results are retained in `realistic/history/bench_history.json`, while local exploratory runs remain local unless explicitly imported |
| Groove scenario/micro          | JSON on stdout; `receipt-adapters.sh` can retain results and append smoke-shaped summaries                                                                          |
| Criterion                      | native reports under `target/criterion`; benchmark-specific JSON receipts may survive separately, but Criterion output itself is machine-local                      |
| storage, OPFS, and WASM probes | harness-native output; retain deliberately when a result is used to justify a decision                                                                              |

Timing medians are directional unless the harness controls the relevant cache,
fixture, and host conditions. Prefer deterministic structural counters for hard
gates. A fast result that fails its oracle or result digest is not a benchmark
result.

## What each benchmark family measures

### Core `jazz` benches and smoke receipts

| bench                       | measures                                                                          | sensitive to                                              |
| --------------------------- | --------------------------------------------------------------------------------- | --------------------------------------------------------- |
| `cold_subscription`         | global update and local current-row materialization over increasing history depth | history depth, pending local state, current-row hydration |
| `validation`                | multi-client exclusive ingest against an independent simplified model             | clients, rows, contention, OCC, and predicate sets        |
| `sync`                      | mixed commits over UI → worker → edge → core with periodic view updates           | commit mix, topology, view cadence, known-state reuse     |
| `large_value_checkpointing` | small text edits followed by checkpointed versus full replay                      | text history, checkpoint interval, body size              |
| `merge_back_cost`           | branch creation, mergeable writes, merge-back, and current-row count              | branch write count; not a complete offline-sync scenario  |
| `relation_include_delivery` | one-row relation/include delivery over a 1k–20k accumulated-view ladder           | incremental delivery work versus retained view size       |

The smoke gate retains these receipts and their deltas. The
`relation_include_delivery` ladder is the quantitative `INV-INC-1` gate added by
#1166; it holds the change fixed and enforces a `1.025x` allocation/byte ratio.
#1192 corrects its reporting to use independent per-metric medians without
retuning that threshold.

### `jazz-sim` scenarios

S1–S7 and S9 cover SaaS, canvas, permissions, order processing, durable
streams, text traces, migrations, and durable execution. They are retained by
the smoke workflow.

**S8 still does not exist.** The specification reserves it for
branch/merge/offline edits. `merge_back_cost` and the declarative R8 fixture
cover pieces of that story, not the complete lifecycle.

### Groove

`scenario` covers social-feed joins, recursive ACLs, and one-shot reads;
`micro` isolates record encoding, planning, and subscribe/unsubscribe costs.
Groove emits correctness-checked JSON, and `dev/benchmarks/receipt-adapters.sh`
provides the retained-baseline path described in `crates/groove/SPEC/B_benchmarks.md`.

### Realistic `jazz-tools` and browser lanes

The older Criterion cases are predominantly `MemoryStorage`; their “batch”
insert/update cases are serial insert-and-wait calls, not atomic transactions.
Do not use them as evidence about persisted reads or transaction batching.

`realistic_phase1` includes the persisted RocksDB R3 lane. The stack beginning
at #1174 adds reproducible phase receipts, controlled warm/best-effort-evicted
cache modes, startup attribution, and exact result validation. #1175, #1176,
#1178, #1199, and #1203 then use those receipts to remove redundant or
unbounded startup work. #1202 is a separate design arm: representative-row
layout validation is faster, but its durable-format contract still needs team
alignment.

Browser realistic scenarios cover writes, reads, fan-out, permissions, and
history. CI-scale runs are shape/correctness checks rather than representative
latency numbers. #1224 adds B7 for public relation-result hydration coverage.

### Storage, OPFS, and WASM

- Native storage benchmarks compare raw SQLite, RocksDB, and redb KV behavior.
- `opfs-btree/hot_paths` uses an in-memory file; the WASM worker harness is the
  actual OPFS path.
- Jazz WASM probes primarily measure runtime characteristics such as
  arithmetic, indirect calls, `RefCell`, and allocation—not full Jazz data
  paths.

## What has changed since the original inventory

### Closed or substantially addressed

1. **`INV-INC-1` receipt and gate:** #1166 added the scale curve and retained
   smoke receipt. #1192 is the reporting correction still to land.
2. **Persisted realistic read attribution:** #1174–#1203 establish and pull the
   R3 startup thread. The ordinary arm reduces clean startup from the original
   roughly 3.2-second baseline to roughly 0.75 seconds on the benchmark fixture;
   the separate #1202 design arm can reduce it further if its format contract is
   accepted.
3. **Cold-settle attribution:** #1167 and #1187 identify receiver-side bulk view
   ingest as the dominant phase and separate it from transport and maintained
   settle.
4. **First-read attribution and hydration:** #1207 attributes first read inside
   Groove execution. #1219 and #1221–#1225 add measured join/hydration
   optimizations and a public browser relation benchmark.
5. **Policy/fan-out exploration:** draft #1170 supplies realistic permission,
   route, persisted-hydration, and subscription-count evidence plus
   implementation plans. Its stable findings still need extraction into
   focused retained lanes.
6. **PERF-4 known-state payload dedup:** `known_state_scaling` holds a persisted
   whole-table rehydrate fixed while sweeping exact receiver coverage. The
   retained receipt proves that emitted bodies and variable exchange bytes fall
   with coverage while result membership remains identical. Full coverage cuts
   the measured variable exchange by about 67%, including declaration cost.
7. **PERF-5 maintained versus rehydrate:** `maintained_rehydrate_scaling` holds
   one source-row change fixed while increasing source and retained-view size.
   Exact result equality is hard-gated; maintained output and storage work stay
   flat while full-rehydrate bytes and reads grow linearly. The lane also
   exposes a separate O(view) metrics-footprint refresh on the maintained path.

### Important negative results retained

- #1190 removes one Groove flush per subscription refresh and is a major
  high-route write win, but does not improve the adopter-shaped cold-settle
  workload; that workload performs few refresh cycles and is dominated by bulk
  ingest volume.
- One-shot reads intentionally reuse subscription machinery. Do not treat
  subscribe/wait/unsubscribe setup as an accidental one-shot-only defect.
- R3 startup gains vary with fixture state. Quote the complete clean/unclean and
  warm/evicted receipt, not a single best-case number.

## Remaining gaps, ranked

Ranked by their ability to change an engineering decision:

1. **Policy and selective-hydration cost receipts.** Extract the stable lanes
   from #1170: authorized write-to-reader visibility, route/subscription scale,
   policy churn/reconnect, and selective Global hydration.
2. **S4 fixed-delta/varying-view gate.** S4 already separates settlement and
   propagation; add a deterministic structural bound proving propagation stays
   proportional to the affected delta.
3. **S8 branch/merge/offline lifecycle.** Cover accumulated offline edits,
   reconnect, merge-back, conflicts, and payload reuse end to end.
4. **S5–S7 promised dimensions.** Add remote resume and evicted-prefix coverage
   for S5, full-history memory for S6, and native-versus-lens plus migration-wave
   costs for S7.

## Source of acceptance targets

| property                                   | current evidence                                                                 | next enforcement                                                                               |
| ------------------------------------------ | -------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `INV-INC-1` bounded incremental delivery   | retained 1k–20k ladder and `1.025x` gate                                         | land #1192 and extend only when a new maintained mechanism needs its own shape-specific canary |
| PERF-4 known-state payload dedup           | retained exact-coverage sweep with bytes, bundles, reads, and correctness digest | profile the coverage-invariant serving work only if a user-facing cost justifies it            |
| PERF-5 maintained converges to rehydrate   | exact-result, cost, bytes, reads, and retained-state scale receipt               | optimize the O(view) metrics-footprint refresh if its measured latency warrants it             |
| PERF-7/8 current reads are O(current rows) | R3 persisted receipts, current-row and checkpoint benches                        | retained filtered/indexed-read slope where selection is held fixed                             |
| S4 post-acceptance propagation is O(delta) | separate settlement/propagation phases                                           | fixed-delta/varying-view structural gate                                                       |

Targets should come from specification properties and measured deterministic
spread, not from an arbitrary percentage around today’s laptop timing.

## Recommended order of work

1. Land the benchmark-foundation corrections and the existing R3 and
   first-read/hydration stacks in dependency order. Keep #1202 separate until
   its durable-format decision has team agreement.
2. Extract focused policy/selective-hydration receipts from #1170 rather than
   merging one omnibus benchmark investigation.
3. Add the S4 structural gate.
4. Build S8, then fill the remaining S5–S7 dimensions.

Add retention alongside each lane. A broad receipt-unification project is no
longer a prerequisite for useful performance work.
