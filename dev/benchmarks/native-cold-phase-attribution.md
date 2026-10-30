# Native cold-load phase attribution

The opt-in `cold-settle-attribution` feature now produces `settle_phase_timing`
alongside the existing operator cardinality and transport sizing counters.
The final measured tree was `fb95c86bd20c0f1529aaba4e3270d97cd199d618`, clean.

Same optimized 39-subscription member fixture: 27,518 expected output rows,
preseeded Core, initially empty relay and client, RocksDB WalNoSync, semantic
in-process transport. All expected rows were present. Seeding and final
one-shot diagnostic queries are outside these phase measurements.

Readiness was **30.037s**, including setup; settling was **29.550s**. The previous
uninstrumented runs were 29.485s and 29.720s to readiness. These observations put
instrumentation in the same timing range, but do not isolate its exact overhead.
The preceding expanded attribution run was 29.894s to readiness.

## Exclusive phase times

These rows are additive. Nested phase time has been removed from its parent.

| Phase                                                   |   Core |  Relay | Client |  Total |
| ------------------------------------------------------- | -----: | -----: | -----: | -----: |
| Query operators (including collection/index projection) | 2.522s | 2.710s | 1.444s | 6.676s |
| Decode query outputs into maintained state              | 2.132s | 1.195s | 1.115s | 4.442s |
| Build supporting-row updates                            | 2.044s | 1.428s | 0.000s | 3.472s |
| Receive and ingest, including parent completion         | 0.000s | 3.437s | 2.050s | 5.487s |
| Apply storage changes, excluding nested operators       | 0.000s | 2.051s | 1.193s | 3.244s |
| Persist storage changes                                 | 0.000s | 1.206s | 0.695s | 1.901s |
| Query setup, excluding nested execution/output decoding | 0.525s | 1.064s | 0.000s | 1.589s |
| Deliver application query outputs                       | 0.000s | 0.000s | 0.419s | 0.419s |
| Benchmark transport measurement                         | 0.408s | 0.242s | 0.003s | 0.654s |
| Unclassified / executor wait / timing overhead          | 0.570s | 0.837s | 0.253s | 1.660s |

Node totals: **Core 8.200s, relay 14.170s, client 7.173s**. Their sum differs
from the outer settling clock by about 7ms of benchmark-loop work. Each node's
exclusive phases sum exactly to its inclusive tick time; the collector asserts
this identity when emitting the report. About 94.4% of tick time is assigned to
named phases beyond the explicit remainder.

`storage_persist` measures the persistence call, not all possible storage costs.
Storage reads can occur under query execution or setup; synchronous I/O inside
any instrumented call is included there. These timers do not prove that all
storage work or all device waiting totals 1.9s.

## What the boundaries mean

- `query_setup`: opening the seeded maintained subscription, excluding its
  nested query execution and output decoding.
- `ivm_update` / `ivm_hydrate`: query graph maintenance/evaluation. Collection
  and index-projection child spans have their own exclusive times, grouped with
  query operators in the table.
- `decode_query_outputs`: `apply_multisink_deltas`, turning typed query output
  batches into the maintained subscription's application/source/witness state.
- `publish_supporting_rows`: constructing the supporting-row update from
  maintained result members for downstream delivery.
- `receive_updates`: applying a received group of view updates, excluding the
  separately timed bulk ingest and its children.
- `ingest`: bulk supporting-version ingestion, excluding parent completion,
  storage apply, persistence and any other nested measured phases.
- `storage_apply`: resident storage/write preparation, delta processing and
  related bookkeeping, excluding separately timed query operators.
- `deliver_query_outputs`: applying terminal changes to the application
  subscription snapshot.
- `benchmark_transport`: the in-process transport's message accounting and
  encoding/compression probes. It is harness work, not simulated network latency.

Parent completion alone is approximately **0.353s** across relay and client.
Its prominent allocation-stack rank did not imply a correspondingly dominant
elapsed-time cost. Query-output decoding (~4.44s) and supporting-row publication
(~3.47s) are larger representation boundaries worth examining closely.

Projection counters recorded **520,896 Core**, **977,810 relay**, and **394,002
client** input visits: **1,892,708** total. These are operator input visits,
not unique rows or network transfers. They demonstrate repeated processing
across operators/queries/nodes, without proving those visits are all avoidable.

## Accounting and limitations

A minimal subscriber accepts only named `cold.phase.*` spans. Async instrumented
functions enter their spans while polling and during scoped cleanup. They release
spans on Pending, so suspended work cannot capture another node's execution.
Nested elapsed time is subtracted from parent exclusive time. Inclusive time is
also emitted for containment analysis and must not be summed.

The driver brackets each sequential node tick with a role span. The collector
is thread-local and specifically intended for this native single-thread driver;
it does not aggregate arbitrary production worker threads. Times are elapsed
wall time within spans, not OS thread CPU clocks: preemption and synchronous
blocking remain included. Root remainder includes executor waiting, unmeasured
code and instrumentation overhead. Span `entries` count polling/cleanup entries,
not logical operation calls.

The phase snapshot is captured immediately at readiness, before final diagnostic
queries. `outside_node_ticks` reports instrumented setup work separately; it is
not a complete timer for all setup. Normal builds contain no phase annotations
or collector unless the feature is enabled.

## Validation and reproduction

Two deterministic accounting tests pass: nested partition/role isolation and a
Pending future resumed under another role. Mutating child-time subtraction and
role attribution makes both tests fail; the mutations were restored and the
tests passed again. The full fixture passed with exact row counts and runtime
partition assertions. Feature-enabled and default-feature Clippy checks passed.
No independent review or full canonical CI result is claimed.

Build with:

```sh
cargo build -p jazz-example-permissioned-resources-benchmark --bin permissioned-resources-profile --profile perf --features cold-settle-attribution
```

Run the resulting executable from the repository root with
`JAZZ_CUSTOMER_IDENTITY=member JAZZ_CUSTOMER_PHASES=cold JAZZ_CUSTOMER_SCALE=1.0
JAZZ_CUSTOMER_MAX_TICKS=200000`. Keep unrelated builds/tests out of the timed run.
Read `settle_phase_timing.roles` for nested times and the existing
`cold_settle_attribution.operator_cardinality` for operator input/output counts.
Local raw evidence uses the `phase-attribution-final` prefix under
`jazz-debug-evidence/permissioned-profile/`.

## Poll-site and operator phases (2026-09-30, #3396)

Queued IVM evaluations (subscription hydrations and incremental slices) are
hand-polled state machines, not `async fn`s. Whoever drains the queue (a bind,
a write's IVM tick, a later owner turn) runs them, so before this change their
time landed in that caller's phase: on the fixture above, Core's `query_setup`
held 8.5 s of exclusive time that was really hydration. That is the same
mechanism behind the impossible flamegraph parents in #3396.

New phases, all behind the same feature:

- `pending_hydration` / `pending_incremental`: one poll of a queued evaluation,
  excluding the operator kernels below.
- `op_source`, `op_arrange`, `op_join`, `op_map`, `op_aggregate`, `op_other`:
  synchronous kernel time per operator family (`compute_batch`, the row loop of
  fused filter/map pipelines, streaming checksums, and the async index-source
  read), wherever it is polled. Memo lookups and the pipeline's fallbacks to
  single-node evaluation stay with the caller's phase.
- `op_recursive`: recursive fixpoint iteration outside the kernels (seed and
  step scheduling, frontier unions, snapshot and witness bookkeeping).
- `ivm_tick`: a storage write's resident IVM tick, excluding kernels and queued
  evaluations. `table_deltas`: computing table deltas for that write.
- `query_compile`, `query_subscribe`, `query_bind`, `query_prepare` and
  `live_attach` split subscription opening into its steps.

Receipt at `a6794316` on a 4-core x86 box (`perf` profile, member identity,
scale 1.0): 35.07 s wall / 30.67 s settle instrumented, against 34.38 s /
29.93 s with the previous phase set on the same tree. Exclusive seconds:

| Phase               |  Core | Relay | Client |
| ------------------- | ----: | ----: | -----: |
| `pending_hydration` | 2.619 | 0.012 |  0.044 |
| `op_join`           | 2.315 | 0.068 |  0.266 |
| `op_arrange`        | 1.442 | 0.093 |  0.863 |
| `op_aggregate`      | 0.933 | 0.061 |  0.524 |
| `op_map`            | 0.634 | 0.000 |  0.424 |
| `op_other`          | 0.466 | 0.046 |  0.187 |
| `op_source`         | 0.273 | 0.056 |  0.057 |
| `ivm_tick`          | 0.000 | 0.762 |  2.354 |
| `table_deltas`      | 0.000 | 0.328 |  0.366 |
| `query_bind`        | 0.372 | 0.000 |  0.000 |

The largest single phase per node fell from 63% to 19% of Core tick time and
from 56% to 25% on the client. The relay's unphased remainder (1.58 s, 20%) is
unchanged and still needs a boundary. `pending_hydration` and `ivm_tick`
exclusive time is scheduler, memo and install work around the kernels, not
kernel time. In this receipt it also included recursive fixpoint glue and
streaming checksums; those now have their own phases (#3798). On the same
fixture `op_recursive` is 0.05 s on Core, so the overstatement was small.
