# jazz — Specification · Appendix B. Benchmarks

*Non-normative (guidance).* The vision-level, end-to-end benchmark suite that
measures product claims (complementing per-milestone micro-benchmarks).
`INV-BENCH-*` ids are auditability anchors, not conformance law. Retained timings
are directional; only anchored headline artifacts become claims.

## B.1 The scenarios

In dependency order: **S0** micro primitives (HLC, domination, deletion
resolution, ingest/commit-unit/read-set costs) · **S1** relevance-scaled
query-driven partial sync · **S2** realtime canvas (mergeable, tier `none`) ·
**S3** recursive permission-filtered sync · **S4** serializable order processing
(exclusive, TPC-C-derived) · **S5** durable streams · **S6** collaborative text ·
**S7** migration lenses · **S8** branching · **S9** durable execution. Every
*implemented* harness runs against the current feature set; **S8 has no harness yet** (`[needs: scenario
harness]`).

## B.2 Methodology

- **Seeded, simulation-first.** A scenario has a deterministic correctness run
  and (where applicable) a threaded timing run; a scenario that can only run on
  one driver is incomplete (`INV-BENCH-1`).
- **Declared topology/profile.** Each run declares its topology and per-link
  latency model (`local`/`regional`/`edge`) and durability; compared systems and
  retained runs use identical declared profiles (`INV-BENCH-2`).
- **Anchored.** Headline metrics report against at least one floor (echo latency,
  bytes), ceiling (naive refetch), or reference (SQLite) anchor (`INV-BENCH-3`).
- **Counters gate, timings inform.** Deterministic counters (rows, versions,
  bytes synced, shipped-vs-referenced complete payloads, merges, aborts,
  shape/binding counts) are hard regression signals; timings are directional
  ratios.
- **JSONL + retention.** Every retained line carries `scenario`, `driver`,
  `seed`, `profile`, `git_sha`, `git_dirty`, `hostname`, and knobs, under
  `benchmarks/results/jazz` (`INV-BENCH-4`).

## B.3 Correctness oracles

A deterministic scenario run asserts oracle equality (or explicit security
counters) before reporting success — a fast-but-wrong run fails (`INV-BENCH-5`).
Examples that double as load-bearing scenario invariants: S3 delivers **exactly
zero** forbidden rows/deltas to an unauthorized client (`INV-BENCH-6`); S4's
accepted jazz schedule, replayed against SQLite, produces identical final state
(`INV-BENCH-7`); S5 tailers observe **prefix-monotone** content and converge
exactly (`INV-BENCH-8`); S9 injected races equal double-advance rejects with zero
double-advances.

## B.4 Honesty rules (the meta-learnings)

- **Measure the generality tax, don't hide it.** S5/S6 model append streams and
  text as *full-value rewrites* (`content: bytes`), not hand-modeled app logs or
  CRDT structures — that's the point, and it motivates `[needs: column-delta]`.
- **Pair storage ratios with read latency.** S2 and S6 forbid reporting a
  compressed-history byte ratio alone, because O(history) replay can win bytes and
  lose the product claim — the time-travel/history-depth read latency must ship
  alongside.
- **Label by durability envelope.** S6 CRDT comparisons distinguish in-memory
  (CPU/latency floor) from durable (apples-to-apples storage/cold-load) baselines.
- **Scale discipline is anti-gaming.** S4 grows aggregate throughput by adding
  warehouses and data, not request rate against fixed hot rows.
- **Server cost tracks relevance.** S1/S5 share the thesis that server cost
  scales with affected subscribers/streams and relevant rows, not total
  workspace size.

## B.5 Systems tier and reporting

The systems tier is distinct: docker-composed competitors over real localhost
transports, wall-clock only, spot-check correctness, same logical workloads, with
**envelope labels** (competitors may lack full history, offline merge, or matched
durability — the labels keep comparisons honest). Reporting renders only
per-scenario headline artifacts into claims, always showing floor / jazz /
ceiling-or-reference and deterministic-counter equality.

## Open questions

- 🔶 **Db-surface migration status** is mixed: some scenarios (S1/S2/S3/S5/S6)
  have Db-surface smoke/summary paths while others remain peer-layer; state it
  per-scenario rather than as a blanket "blocked."
- 🔶 **S7 is smoke-sized**; define the retained-run fields and matrix it must
  emit to graduate from an interactive harness to a reporting benchmark.
- 🔶 **Dirty-tree retention.** `git_dirty` is recorded but a dirty run is still
  appendable when retention is on — decide whether tooling should refuse to quote
  dirty-tree results or rely on author discipline.

---

## In flight & operational detail (non-normative)

*B.1–B.6 above are the durable benchmark methodology. The following is the full
operational detail — per-scenario fixtures, axes, metrics, gates, systems-tier
mapping, and retained-run rules — from the former `BENCHMARKS.md`. It settles
upward as scenarios stabilize.*


This is the vision-level benchmark suite: it measures jazz's four product
claims end to end, one scenario per claim. It complements (does not replace)
the per-milestone benchmarks that gate individual features.

| # | scenario | product claim under test | headline artifact |
|---|---|---|---|
| 1 | SaaS app | query-driven partial sync scales with *relevance*, not data | cold-load bytes vs. naive baseline; core cost vs. subscriber count |
| 2 | Canvas | realtime mergeable collaboration converges fast and cheap | cross-participant p95 at tier `none` vs. link-latency floor; history bytes vs. zstd-JSON event log *paired with* time-travel read latency |
| 3 | Permissions | recursive RLS over partial replicas is correct and fast | revocation-to-disappearance p95 at scale |
| 4 | Order processing | jazz is competitive as a classical serializable database | max sustained scale factor; exclusive-counters vs. mergeable-counters variant |
| 5 | Durable stream | append-heavy streams with tail/resume are trivial on jazz | history+metadata bytes per appended token vs. log-file floor; append→tail p99 |
| 6 | Collaborative text | jazz competes with CRDT libraries on real editing traces | storage ratio vs. durable CRDTs *paired with* cold-load and time-travel latency |

A micro tier (scenario 0) covers the primitive costs underneath all of them.

## Overview: phase-level coverage audit

Evidence checked 2026-06-14: `jazz-sim/benches/*.rs`,
`jazz-sim/src/{lib,fixture}.rs`, `jazz/benches/*.rs`,
`groove/benches/*.rs`, `jazz/src/{query,node/*,db}.rs`, and appendix C.
This table is phase-level by design: a scenario may have useful runnable
coverage while still being unable to support every headline claim honestly.
S8 is intentionally excluded from this audit.

**Topology update (2026-06-15).** All implemented scenarios (S1–S7, S9) now route
the full `client↔edge↔core` path through the `Db` facade with full client `Db`s
(tracks E6a/E6b), emitting `edge_mergeable_acceptance` and
`edge_permission_scope_hydration` phases. The per-row notes below predate that
conversion and describe *phase* coverage, not topology.

**Latest default-size run (2026-06-15, git `6ff5f2e`).** Canary (smoke) then
default sizes. Clean at default: groove (`micro`, `scenario`), jazz (`sync`,
`validation`, `cold_subscription`, `large_value_checkpointing`), and jazz-sim S1,
S2, S3, S5, S7, S9. **Two scenarios fail at default** (both pass smoke; both
touched by the edge conversion): **S4** — the settlement vs propagation-inclusive
throughput runs no longer accept an identical schedule under edge routing
(assertion in `s4_order_processing.rs`); **S6** — `MissingTransaction` panic in
`read_doc` after the edge-routed ~2000-edit replay. Both have tracked fixes.

**Interactive fast/canary helper.** `scripts/bench_jazz_sim_fast.sh --all-fast`
runs the implemented jazz-sim scenarios that fit `JAZZ_BENCH_PROFILE=fast`,
including the smoke-sized S7 migrations harness. For an encoded sync canary, use
`scripts/bench_jazz_sim_fast.sh --encoded-wire-canary`: it runs short S2 canvas
and S1 reconnect paths with `wire_frames` transport (`JAZZ_S2_TRANSPORT_CODEC`
and `JAZZ_S1_RECONNECT_TRANSPORT_CODEC`). The S1 bench also emits
The old `abi_direct_surface` phase has been removed with the command/event ABI
runtime. Binding performance canaries should be rebuilt around direct
WASM/NAPI-style object wrappers plus row-record decoding. For narrow S1
canary runs, `JAZZ_BENCH_PHASES=high_fan_out_hydration` runs only that S1 phase;
the fast helper accepts the same selection as
`scripts/bench_jazz_sim_fast.sh --phase <name> s1_saas`.
The S1 high-fan-out hydration canary also reports maintained subscription view
incremental, full-recompute, and full-diff counters. Production maintained
subscription view full-recompute counters are expected to remain zero; nonzero
values indicate test/oracle forcing, an explicit migration harness, or a
regression that should fail the canary instead of being treated as a semantic
fallback.
S5's process-local resume
canary reports `phase: "process_local_resume"` as cursor catch-up compared
against fresh/full snapshot bytes (`full_rehydrate_bytes`, `resume_bytes`,
ratio, and status); tiny fixtures may not make process-local resume smaller
than a fresh/full snapshot. S5 also accepts narrow phase selection through the
same helper: `scripts/bench_jazz_sim_fast.sh --phase db_surface_live
s5_durable_stream` or `--phase process_local_resume s5_durable_stream` runs only
that canary path; `JAZZ_BENCH_PHASES=default` keeps the main stream/log/sqlite
summary path.

**Scaling signals — mostly bench artifacts, not an engine cliff (investigated
2026-06-15).** Initial default-run numbers *looked* superlinear (S9 aggregate
92→13 transitions/s with "history metadata" 9 KB→121 KB/step; S6 text replay
33 edits/s at 2000 commits) but investigation showed these are largely measurement
and harness problems, not engine O(n²):
- **S9** — `history_metadata_bytes_per_step` is a *misleading metric*: it divides
  the whole RocksDB directory by committed transitions, but the run pre-seeds
  `instances` rows proportional to ladder size before the timed loop, so the ratio
  inflates with scale. Settle/commit throughput stays roughly flat (568→530/s);
  the *aggregate* collapse is the bench running full dashboard + tailer correctness
  scans after every transition and cold-resume rehydrating all instances/steps/
  events. Fix (bench): split fixture bytes from transition-history bytes and
  compute per-step from bytes added after seeding; report aggregate-with-assertions
  separately; treat cold resume as its own workload.
- **S6** — a real but bench-side issue: the DB-surface replay writes the whole text
  blob through `Db::update` every edit instead of `Db::edit_text`/`TextEdit`
  (forcing materialize + diff of a growing value), and opens nodes with checkpoint
  interval `1` (a full document checkpoint per tx). Fix (bench): replay via
  `edit_text`, use bounded checkpoint retention + the production interval.

No fundamental engine ingest superlinearity is demonstrated by these runs. By
contrast the **cold-load read path is validated flat**: `cold_subscription`
current-row reads are ~122 µs regardless of history depth (1k/5k/10k), scaling
only with the ahead-of-global count. Other setup gaps: S2 reports
`bytes_floor: 0` (entropy-floor anchor uncomputed for canvas). Hotspot note: S3
cold/block-tree load is dominated by
`NodeState::expand_query_closure`, `PeerState::rehydrate_current_rows`,
`OrderedKvStorage::prefix`, and global-layer-winner lookups (samply, 2026-06-15).

Gap kinds: **FEATURE** means a required capability is not implemented, not
available through the needed surface, or not incrementally maintained where the
phase requires live updates; **HARNESS** means no benchmark code for the phase;
**ORACLE** means missing or incomplete correctness/counter gating under
`INV-PERF-2`; **BASELINE** means a required external floor/reference is not
wired.

| scenario | phase | runnable honestly today? | gap kind |
|---|---|---|---|
| S0 micro | HLC time/receive/compare and `TxTimeSortKey` | Partial | HARNESS: `jazz-sim/benches/micro.rs` emits primitive JSONL via `emit_hist`, but no isolated HLC or `TxTimeSortKey` primitive was found; `jazz/benches/sync.rs` only exercises them inside four-tier sync. |
| S0 micro | domination over N heads | Yes | `jazz-sim/benches/micro.rs` has a domination primitive over head counts; useful older primitives also exist in `groove/benches/micro.rs`. |
| S0 micro | deletion-register resolution | Yes | `jazz-sim/benches/micro.rs` has a deletion-resolution primitive over mixed events. |
| S0 micro | version ingest rate | Yes | `jazz-sim/benches/micro.rs` emits version-ingest rate and bytes; `jazz/benches/sync.rs` is broader sync-path evidence. |
| S0 micro | commit-unit encode/decode, 1/10/100 rows | Yes | `jazz-sim/benches/micro.rs` has commit-unit size rows-per-unit cases and `commit_unit_bytes`. |
| S0 micro | read-set capture overhead | Yes | `jazz-sim/benches/micro.rs` emits read-set capture samples; `jazz/benches/validation.rs` exercises the exclusive validation path. |
| S0 micro | validation by row vs predicate read-set entry | Yes | `jazz-sim/benches/micro.rs` splits row and predicate validation; `jazz/benches/validation.rs` has broader exclusive validation histograms and a hand-rolled `BaselineModel`. |
| S1 SaaS | cold load | Yes | `jazz-sim/benches/s1_saas.rs` emits cold/warm summary with `cold_bytes`, `cold_bytes_floor`, `naive_refetch_ceiling_bytes`, result rows, closure rows, and oracle equality checks. |
| S1 SaaS | warm local read | Yes | `jazz-sim/benches/s1_saas.rs` emits `warm_local_*` and `warm_settled_*`; current query paths are validated against the S1 oracle. |
| S1 SaaS | reconnect | Partial | FEATURE: `jazz-sim/benches/s1_saas.rs` emits `phase: reconnect` and bytes/floor, but the spec's delta-resubscribe variant remains `[needs: payload-inventory]`; the current path is full rehydrate. |
| S1 SaaS | subscriber sweep | Partial | HARNESS: `jazz-sim/benches/s1_saas.rs` emits `phase: subscriber_sweep`, notification bytes, bundles/refs, adds/removes, now uses full client `Db`s through an edge (track E6b); the full 10/100/1k/10k client retained sweep matrix is still pending. |
| S1 SaaS | distinct-shape sweep | No | HARNESS: no distinct-shape phase emission was found in `jazz-sim/benches/s1_saas.rs`; groove has predecessor shape-sharing signals in `groove/benches/scenario.rs`, not the S1 fixture. |
| S1 SaaS | query churn | No | HARNESS: the search-as-you-type binding/shape churn phase is specified but no emitted phase or harness code was found. |
| S1 SaaS | high-fan-out hydration | Yes | `jazz-sim/benches/s1_saas.rs` emits `phase: high_fan_out_hydration`, bytes/floor, mid-hydration fate counters, maintained subscription view incremental/full-recompute/full-diff counters, and a harness-deterministic legacy-named `membership_history_scan_fallbacks` zero check; the latter is not claimed as a live serving counter. |
| S1 SaaS | ordered/page queries | Partial | FEATURE: query support exists in `jazz/src/query.rs` and node evaluation (`jazz/src/node/query_eval.rs`), but no S1 ordered-page phase is emitted and live IVM maintenance of order/limit has not been demonstrated by the phase. |
| S1 SaaS | aggregate milestone page | Partial | FEATURE: aggregate query capability must be proven incrementally maintained for live subscriptions before the phase can be honest; no aggregate S1 phase is emitted. |
| S2 canvas | live collaboration | Yes | `jazz-sim/benches/s2_canvas.rs` emits `phase: live` for deterministic/threaded runs, coalescing, latency histograms via `hdrhistogram`, bytes/floor, merge counters, core tick, and spy/non-invite correctness. |
| S2 canvas | loading: current state only | Yes | `jazz-sim/benches/s2_canvas.rs` has `phase: db_surface_live` and current-row checks; `jazz/benches/cold_subscription.rs` is a focused current-row cold-load receipt. |
| S2 canvas | loading: current + full history | Partial | HARNESS: the history-complete node path is used for historical reads, but no separate current+full-history load metric was found. |
| S2 canvas | historical state at 25/50/75% | Yes | `jazz-sim/benches/s2_canvas.rs` emits `phase: historical_load` with cut percent/global seq and prefix-replay correctness. |
| S2 canvas | history-depth current-row reads | Partial | HARNESS: `jazz/benches/cold_subscription.rs` covers history-depth current-row subscription, but the S2 JSONL phase does not yet pair those depth-latency lines with the storage-ratio artifact. |
| S2 canvas | failure injection | Yes | `jazz-sim/benches/s2_canvas.rs` emits `phase: failure`, recovery-to-convergence, final rows, spy rows, and convergence checks. |
| S2 canvas | history storage ratio vs zstd JSON event log | No | BASELINE: the spec-required zstd JSON event-log anchors are not emitted by `s2_canvas.rs`; only wire bytes/floor and history rows are present. |
| S2 canvas | edge topology (client↔edge↔core) | Yes | `s2_canvas.rs` routes Db client → edge Node → core Node (track E6b) and emits `edge_mergeable_acceptance` + `edge_permission_scope_hydration`. |
| S3 permissions | cold load by persona | Yes | `jazz-sim/benches/s3_permissions.rs` emits `phase: cold` for simple/admin personas with bytes/floor, including Db-surface variants. |
| S3 permissions | grant latency | Yes | `jazz-sim/benches/s3_permissions.rs` emits `phase: grant` with grant latency and oracle visibility checks. |
| S3 permissions | revocation | Yes | `jazz-sim/benches/s3_permissions.rs` emits `phase: revocation` across configured revoke sizes with visibility convergence and recompute counters. |
| S3 permissions | forbidden writes | Yes | `jazz-sim/benches/s3_permissions.rs` emits `phase: forbidden_writes` and gates forbidden deliveries at zero, matching `INV-BENCH-6` / `INV-PERF-2`. |
| S3 permissions | reconnect | No | HARNESS: no reconnect phase emission was found in `s3_permissions.rs`; S1 has reconnect machinery but S3 permission-filtered catch-up remains unwired. |
| S3 permissions | edge profile and permission-subscription hydration | Yes | `s3_permissions.rs` routes client↔edge↔core (track E6a) with narrow `(policy_shape, writer_claim)` scope hydration + dedup; emits edge acceptance + scope-hydration phases. |
| S3 permissions | block-tree fixture variant | Yes | `jazz-sim/benches/s3_permissions.rs` emits `phase: block_tree_variant`, `joint_cold_hydration_headline`, and `headline_progress` with bytes/floor and visibility checks. |
| S4 order processing | scale-out | Yes | `jazz-sim/benches/s4_order_processing.rs` emits scale/SLO phases, p50/p95 settlement, warehouse/throughput fields, and same-schedule SQLite replay assertions. |
| S4 order processing | contention | Yes | `jazz-sim/benches/s4_order_processing.rs` emits contention modes, abort/retry information, and hot-payment counter notes. |
| S4 order processing | mergeable-counter strategy variant | Yes | `jazz-sim/benches/s4_order_processing.rs` emits the counter-strategy side-by-side variant. |
| S4 order processing | settlement vs propagation split | Partial | HARNESS: appendix C requires reporting settlement throughput and propagation-inclusive throughput separately; `s4_order_processing.rs` has settlement-centric fields, while the propagation-inclusive split remains incomplete for retained claims. |
| S4 order processing | SQLite same-schedule reference | Yes | `s4_order_processing.rs` uses `rusqlite`, `run_sqlite_reference`, and `assert_sqlite_replay_matches`; this is a real SQLite reference, unlike `jazz/benches/validation.rs`'s `BaselineModel`. |
| S5 durable stream | append workload, batching, run length, stream-count axis | Yes | `jazz-sim/benches/s5_durable_stream.rs` emits the main stream summary with token batching, stream count, appends/sec, history bytes, sync bytes, and storage bytes/token. |
| S5 durable stream | live tailers | Yes | `s5_durable_stream.rs` measures append-to-tail latency and synced bytes per token per tailer. |
| S5 durable stream | resumers | Partial | FEATURE: the bench measures process-local cursor catch-up bytes/time against fresh/full snapshot bytes and emits `phase: "process_local_resume"` with `full_rehydrate_bytes`, `resume_bytes`, `resume_ratio`, and `resume_status`; process-local resume may not be smaller in tiny fixtures, and the portable delta-resubscribe variant remains `[needs: payload-inventory]`. |
| S5 durable stream | Db-surface remote tail/resume | Partial | HARNESS: `s5_durable_stream.rs` emits `phase: db_surface_live`, but it is a live Db-surface summary, not the full remote tail/resume/resumer matrix. |
| S5 durable stream | log-file floor | Yes | `s5_durable_stream.rs` has `run_log_floor` and emits log floor bytes/token and elapsed time. |
| S5 durable stream | SQLite WAL baseline | Yes | `s5_durable_stream.rs` uses `rusqlite` in `run_sqlite_baseline` and emits SQLite bytes/token and elapsed time. |
| S5 durable stream | prefix-monotone oracle | Yes | `s5_durable_stream.rs` asserts tailer/resumer content convergence and prefix monotonicity for the generated stream, matching `INV-BENCH-8`. |
| S5 durable stream | column-delta efficiency target | Partial | FEATURE: the scenario intentionally rewrites full bytes values; no structural column-delta path was found in `jazz/src/node/content_store.rs` or the sync benches, so bytes/token are baseline numbers only. |
| S5 durable stream | evicted-prefix resumer | No | FEATURE: the `[needs: eviction]` phase is specified but not implemented. |
| S6 text | trace replay | Yes | `jazz-sim/benches/s6_text_traces.rs` emits `phase: trace_replay` plus `db_surface_trace_replay`, throughput, echo latency, history bytes/edit, zstd anchors, and final/prefix correctness. |
| S6 text | live observation | Yes | `s6_text_traces.rs` emits `phase: live_observation` with observer p95, synced bytes, and link floor. |
| S6 text | cold load: current only | Yes | `s6_text_traces.rs` emits `phase: cold_load` current-only latency and current bytes. |
| S6 text | cold load: full history | Partial | FEATURE: `s6_text_traces.rs` marks full-history load as gated until a history subscription load path exists. |
| S6 text | point-in-time reads | Yes | `s6_text_traces.rs` emits `phase: point_in_time_read` for cut percentages with direct-prefix replay correctness. |
| S6 text | storage | Yes | `s6_text_traces.rs` reports history/metadata bytes per edit and zstd final-doc / JSON-op-log anchors. |
| S6 text | CRDT adversary comparisons | Partial | BASELINE: `s6_text_traces.rs` has in-process adversary comparison fields and Automerge/diamond-style paths, but these are local/library baselines, not the full pinned eg-walker/dmonad external-result matrix for every trace. |
| S6 text | memory | Partial | ORACLE: the bench reports `peak_memory_proxy_bytes` as peak document characters/bytes, not process peak RSS; useful but not the spec's memory metric. |
| S6 text | concurrent merge | No | FEATURE: current text semantics remain whole-value HLC-LWW; the concurrent merge phase is gated by `[needs: text-merge]` despite `jazz/src/node/text_oplog.rs` existing. |
| S6 text | column-delta efficiency target | Partial | FEATURE: full text values are rewritten per edit; no structural column-delta maintenance was found, so storage/wire ratios are baseline numbers only. |
| S7 migrations | mixed-version steady state | Partial | HARNESS: `jazz-sim/benches/s7_migrations.rs` is a smoke-style executable over a schema chain and `MigrationLens` with JSONL phase output, but still has no retained reporting matrix. |
| S7 migrations | lens-tax measurement | No | HARNESS: no native vs 1-hop/3-hop latency, write translation, or sync-byte overhead output was found. |
| S7 migrations | rollout wave | No | HARNESS: no mid-stream population migration phase or latency/recompute metrics were found. |
| S7 migrations | late offline client | Partial | ORACLE: `s7_migrations.rs` exercises an offline v1 client reconnecting into the chain and emits smoke-sized JSONL phase fields, but the retained counter contract is still incomplete. |
| S9 durable execution | instance-count ladder | Yes | `jazz-sim/benches/s9_durable_execution.rs` emits `phase: scale_ladder`, instances, steps, SLO fields, transition/sec, dashboard/tail/resume latencies, and store bytes. |
| S9 durable execution | injected race rejection | Yes | `s9_durable_execution.rs` emits attempts, rejects, injected races, double-advance rejects, double advances, and gates no-double-advance correctness. |
| S9 durable execution | dashboard/per-instance tailers | Yes | `s9_durable_execution.rs` measures dashboard p95, tail p95, sync bytes, and asserts dashboard/tailer oracle state. |
| S9 durable execution | crash/resume | Partial | HARNESS: the bench measures cold resume/reattach and bytes; explicit crash injection/restart of workers is not a separate phase. |
| S9 durable execution | SQLite transition baseline | Yes | `s9_durable_execution.rs` uses `rusqlite`, `run_sqlite_reference`, and same-schedule replay checks. |
| S9 durable execution | S5 log-file floor | Yes | `s9_durable_execution.rs` has `run_log_floor` and emits log floor bytes/step and elapsed time. |
| S9 durable execution | payload-inventory resume | Partial | FEATURE: resume is full rehydrate; delta-resubscribe remains `[needs: payload-inventory]`. |

Landed capabilities were retired from the gate list; git history is the record.

| gate | status | evidence |
|---|---|---|
| `[needs: payload-inventory]` | FUTURE | Reconnect/resume delta-resubscribe from peer payload inventory is still not the measured path in S1/S5/S9; steady-state complete-tx payload dedup (`PeerState.shipped_complete_tx_payloads`, appendix C) is a different mechanism. |
| `[needs: column-delta]` | FUTURE | S5/S6 intentionally rewrite full `bytes`/text column values; no structural column-delta encoding path was found in `jazz/src/node/content_store.rs` or the scenario benches. |
| `[needs: text-merge]` | FUTURE | `jazz/src/node/text_oplog.rs` exists, but current S6 scenario semantics are still whole-value HLC-LWW; no rich-text three-way merge strategy is wired into the phase. |
| `[needs: eviction]` | FUTURE | S5's evicted-prefix resumer phase is specified but no eviction/resume benchmark path was found. |

### Existing infrastructure inventory

- **Harness pattern.** `jazz-sim/src/lib.rs` provides deterministic and
  threaded drivers, `PeerProfile`, scenario metadata, and `emit_json_line`.
  Scenario benches are hand-rolled `cargo bench --bench ...` binaries, not
  Criterion; `jazz-sim/Cargo.toml`, `jazz/Cargo.toml`, and `groove/Cargo.toml`
  depend on `hdrhistogram` but not Criterion.
- **Latency histograms.** `hdrhistogram` is used in `jazz-sim/benches/micro.rs`,
  `s2_canvas.rs`, `s3_permissions.rs`, `s4_order_processing.rs`,
  `s9_durable_execution.rs`, `jazz/benches/sync.rs`, `jazz/benches/validation.rs`,
  and `groove/benches/{micro,scenario}.rs`.
- **Output and retention.** `jazz-sim/src/lib.rs::metadata_fields` emits
  `scenario`, `driver`, `seed`, `profile`, `git_sha`, `git_dirty`, and
  `hostname`; `emit_json_line` appends to `benchmarks/results/jazz/<scenario>.jsonl`
  when `JAZZ_BENCH_RETAIN=1`. Legacy `scripts/bench_run.py` enriches groove
  scenario JSONL with `git_sha`, `git_dirty`, and host metadata.
- **Bytes/token accounting.** Several benches have local helpers:
  `view_update_bytes` / `bytes_floor` in S1/S2/S3/S6/S9, stream bytes/token in
  `s5_durable_stream.rs`, text bytes/edit in `s6_text_traces.rs`, and storage
  tree walkers in S5/S6/S9. This is useful but not shared infrastructure.
- **Memory accounting.** No reusable peak-RSS/process-memory utility was found.
  S6 emits `peak_memory_proxy_bytes`, but it is derived from document length or
  library-visible characters, not process peak RSS.
- **Baselines.** Real `rusqlite` references exist in
  `jazz-sim/benches/s4_order_processing.rs`, `s5_durable_stream.rs`, and
  `s9_durable_execution.rs`; `groove/benches/scenario.rs` also has SQLite modes
  for predecessor social-feed/ACL/oneshot workloads. `jazz/benches/validation.rs`
  is not a SQLite reference: its `BaselineModel` is a hand-rolled in-memory
  decision model for exclusive validation.
- **External anchors.** S5 has an fsync-disciplined log floor and SQLite WAL
  baseline; S6 has zstd anchors and local CRDT-library comparison scaffolding;
  S2 lacks the required zstd JSON event-log storage anchor.
- **Missing cross-cutting enablers.** Peak-RSS utility; a shared real SQLite
  reference runner where scenarios need same-schedule replay; shared
  bytes/token and bytes/floor accounting; shared oracle/counter wiring that
  makes `INV-PERF-2` gates uniform across phase benches; and a retained-run
  validator that refuses or labels dirty-tree claim artifacts consistently.

### Prioritized benchmark gaps, excluding S8

0. **HARNESS (topmost): convert every scenario to the `client↔edge↔core`
   topology through the `Db` facade.** This is now the only supported topology
   (§"Topology") and supersedes the piecemeal "add an edge variant to S2/S3"
   items: every scenario drives through the `Db` facade with full client `Node`s
   (no `PeerState` stand-ins, fixing S1's subscriber-sweep gap), routes all sync
   through an edge node that terminates the client identity and hydrates narrow
   permission scopes from core, and reports the two-leg profile latencies. New
   measured phases this unlocks: edge mergeable-acceptance latency near the user,
   and edge permission-scope hydration cost (narrow `(policy_shape, writer_claim)`
   scope vs. the rejected whole-table scope — the B2 win). Folds in the Db-API
   migration of the still-peer-layer scenarios (S4/S7/S9). Depends on the edge
   topology being wired into the sim driver (edge-role node + edge↔core link).
1. **INFRA / ORACLE:** factor shared oracle/counter gating and retained JSONL
   validation so every phase has explicit `INV-PERF-2` pass/fail counters
   instead of bespoke fields.
2. **INFRA:** add a process peak-RSS utility and replace S6's
   `peak_memory_proxy_bytes` with real memory metrics while keeping the proxy as
   a debug field if useful.
3. **INFRA:** share bytes/floor and bytes-per-token/edit accounting across
   S1/S2/S3/S5/S6/S9; today every bench reimplements its own approximation.
4. **HARNESS / BASELINE:** finish S2's history artifact: zstd JSON event-log
   anchors plus paired history-depth read latency in the same retained output.
5. **HARNESS:** add the missing S1 phases: distinct-shape sweep, query churn,
   ordered/page, and aggregate milestone page.
6. **FEATURE:** prove and benchmark live incremental maintenance for S1
   order/limit and aggregate phases before reporting them as honest live-sync
   claims.
7. **FEATURE:** implement payload-inventory delta resubscribe for reconnect/resume
   phases, then re-measure S1 reconnect, S5 resumers, and S9 worker reattach.
8. **HARNESS:** finish S4's appendix-C split: settlement throughput and
   propagation-inclusive throughput as separate retained lines.
9. **HARNESS:** add the S3 reconnect (permission-filtered catch-up) phase. (The
   S3 edge-profile permission-hydration phase is folded into item 0.)
10. **HARNESS:** expand S5 Db-surface coverage from live smoke to the full
    remote tail/resume/resumer matrix.
11. **FEATURE:** implement column-delta maintenance, then re-run S5/S6 storage
    and wire bytes/token or bytes/edit as claims rather than baseline pain
    measurements.
12. **BASELINE:** complete S6's pinned CRDT/literature matrix across the trace
    catalog, not only local comparison scaffolding.
13. **FEATURE:** implement rich-text three-way merge before enabling S6
    concurrent merge.
14. **HARNESS:** graduate S7 from smoke to JSONL phases for mixed-version
    steady state, lens tax, rollout wave, and late-offline reconnect.
15. **HARNESS:** make S9 crash/restart a distinct failure phase, separate from
    cold reattach.

## Shared setup and rules

These rules exist because benchmarks without them produced misleading numbers
in previous experiments (groove's benchmark history is the case law).

**One workload definition, two drivers.** Every scenario is defined as a
seeded workload over simulation-first nodes and uses deterministic simulation
testing (DST) for reproducible correctness runs. *Deterministic* mode executes
all correctness checks (oracle comparisons are exact, repeatable, and
zero-noise); *threaded* mode produces timing numbers.
A scenario implementation that can only run on one driver is incomplete.

**Declared network model.** Settlement latency is dominated by link latency,
so every run declares its topology and per-link latency distributions, and
holds them identical across compared systems and retained runs. **The topology
is always `client↔edge↔core`** (§"Topology" below) — every profile is a
two-leg path. Standard profiles (overridable, always reported):

- `local`: client↔edge 1ms, edge↔core 1ms (CI / development profile)
- `regional`: client↔edge 5ms, edge↔core 30ms (edge near the user, core regional)
- `edge`: client↔edge 20ms, edge↔core 80ms (far-core deployment — the edge pitch)

Durability config is part of the profile: core uses WAL-no-fsync-per-commit
(the groove `WalNoSync` tier) unless a scenario says otherwise; reference
systems get the equivalent setting.

**Floors, ceilings, and baselines.** Every metric is reported against an
anchor so it is interpretable across hardware:

- *latency floor*: a raw message echo through the same simulated links —
  what is physically possible;
  (Measured note, 2026-06-11: the threaded driver adds ~1ms sleep-scheduling
  overhead over the configured profile on macOS — echo p50 3.0ms vs a 2.0ms
  configured RTT. The echo scenario measures this per run; when canvas-class
  sub-ms floors start to matter, either spin-wait the final stretch before
  delivery deadlines or report echo overhead as a third anchor line.)
- *bytes floor*: the entropy of the actual change stream (sum of encoded
  payload bytes that genuinely changed) — what a perfect protocol would ship;
- *naive ceiling* (per scenario): broadcast-everything (canvas),
  refetch-on-change (SaaS);
- *reference implementation* where stated (SQLite for scenario 4).

**Metrics regime** (inherited from groove): deterministic counters — rows,
versions, bytes synced, versions shipped vs. referenced, merges created,
aborts, prepared-shape/binding counts — are **hard regression gates**; timings are
directional and compared as **ratios** against retained baselines. Output is
JSONL enriched with git SHA, dirty flag, host, seed, profile, and `GROOVE_*`/
`JAZZ_*` knobs; retained runs live under `benchmarks/results/`; nothing is
quoted from a dirty tree.

**Correctness is part of every run.** Each scenario ends with its oracle
checks (brute-force model comparison) in deterministic mode; a benchmark that
gets fast by being wrong must fail, not report.

**Feature gates.** Each phase is tagged by the feature set it needs: `[base]`
runs on the currently-shipped engine, while `[needs: …]` phases depend on a
not-yet-built feature — `[needs: column-delta]` (structural sharing / delta
encoding of large column values across row versions) and `[needs: text-merge]` (a
rich-text column merge strategy doing three-way merges against the version DAG's
common ancestor). The suite lands incrementally; a `[needs: …]` phase is
specified now and activated when its feature ships, and the gates double as the
demand signal for prioritizing those features.

**Topology.** Every scenario runs **only** the full `client↔edge↔core` path —
the real deployment topology, with the edge always in the path terminating the
client identity, deciding mergeable fate near the user, and hydrating its narrow
permission scopes from core (ch. 9). There is no `client↔core` benchmark: a
two-node measurement would not exercise the edge acceptance latency, the
permission-scope hydration cost, or the edge↔core durability leg that the system
actually pays in production, so it would not honestly prove the system. The
**client** drives through the `Db` facade — its real application API — while the
**edge** and **core** are `Node`s (the `Db` facade is client-side only; ch. 13).
Clients are full `Db` instances, not peer-layer stand-ins, so the measured path is
the one applications run.

---

## 0. Micro tier

*Motivation: every scenario cost decomposes into a handful of primitives;
when a scenario regresses, this tier localizes it.*

Hand-rolled harness (groove `micro.rs` style), reporting ns percentiles and
bytes:

- HLC time, receive-max, compare (incl. `TxTimeSortKey`)
- domination over a row with N heads (N = 1, 2, 8, 64)
- deletion-register resolution with mixed delete/restore events
- version ingest rate (versions/sec into history tables, single node)
- commit-unit encode/decode (1 row, 10 rows, 100 rows)
- read-set capture overhead per statement inside an open exclusive tx
- validation cost per read-set entry (row entries; predicate entries via
  prepared shapes)

---

## 1. SaaS app (Linear-style task tracking)

*Motivation: the core partial-sync claim — a client syncs what its queries
need, not the workspace; the server's cost grows with affected subscribers,
not with subscriber count. This is groove's headline curve, lifted to the
full protocol.*

### Schema

```ts
orgs:                   { name: string }
users:                  { userID: uuid, name: string }
teams:                  { name: string, org: ref(orgs) }
userTeamMemberships:    { user: ref(users), team: ref(teams) }
tags:                   { name: string, color: string }
projects:               { title: string, org: ref(orgs) }
projectTeamMemberships: { project: ref(projects), team: ref(teams) }
milestones:             { title: string }
milestoneDependencies:  { dependsOn: ref(milestones), dependent: ref(milestones) }
cycles:                 { team: ref(teams), start: timestamp, end: timestamp }
issues: {
  title: string, body: string,
  state: enum[draft|planned|inProgress|done|canceled],
  priority: int,                       // added: needed by ordered queries
  assignee: ref(users) | null,        // added: needed by the headline query
  milestone: ref(milestones) | null,
  project: ref(projects) | null,
  cycle: ref(cycles) | null,
}
issueTags: { issue: ref(issues), tag: ref(tags) }
```

Note: "updated time" is *derived* — it is the `made_at` of a row's current
version; no column needed.

### Fixture (seeded, Zipf-skewed activity)

20 orgs × 10 teams × 50 users; 5,000 issues/org; 30 tags; 200 projects; 100
milestones; issue↔tag and membership edges at realistic densities; ongoing
write stream of issue edits/creations at a configurable rate, authorship
Zipf-distributed.

### Client states and axes

- **cold**: empty store, subscribe, measure to first complete page;
- **warm**: fully synced, measure local read;
- **reconnecting**: synced, then offline for W ∈ {1min, 1h, 1day} of
  workspace activity, then reconnect — measure catch-up time and bytes.
  `[base]` measures full rehydrate; `[needs: payload-inventory]` re-measures
  with delta resubscribe.
- **subscriber sweep**: 10 / 100 / 1k / 10k concurrently subscribed clients
  (same prepared shape, different bindings) — measure per-commit core tick
  cost and notification fanout. Must stay flat-ish per the binding-set
  aggregation design (register the shape once per peer, ship binding deltas); this is the protocol-level version of groove's headline curve.
- **distinct-shape sweep** `[base]`: the orthogonal axis — 10 / 100 / 1k
  *different* shapes live on one core (vary filters/joins structurally).
  Stresses graph hash-consing and arrangement sharing directly, where the
  same-shape sweep stresses binding aggregation. Competing systems degrade
  on exactly one of these two axes; jazz must stay flat-ish on both.
- **query churn (search-as-you-type)** `[base]`: a client turns bindings over
  rapidly — keystroke-style binding add/remove at 5–20/sec against a
  prefix-filter shape, plus a shape-churn variant (each keystroke a new
  shape). Measure subscribe-to-first-result latency (local-store hit vs
  streamed), cost of abandoned subscriptions (registration/teardown work
  per never-read binding), and core load under churn. Content-addressed
  idempotent registration is the mechanism under test; this is the
  workload the query-driven competitors demo, and nothing measures it
  today.
- **high-fan-out hydration** `[base]`: cold client subscribes to many
  related shapes at once over a schema where few parent rows fan out
  into very large child sets (closure fan-out axis 1:10 / 1:100 /
  1:1000; e.g. tens of parents with tens of thousands of children
  total), while transaction fates keep arriving mid-hydration. Measure
  time-to-complete hydration, and assert via deterministic counters
  that per-transaction row-membership resolution stays index-bounded
  (seeks, never history rescans) under the burst. This axis exists
  because the burst-of-rows-plus-fates-during-hydration pattern is
  where naive clients degrade to repeated broad scans.

### Queries

1. `[base]` issues where `assignee = $user` and `state != done` and
   `cycle = active` (unordered set; ordered-page phase adds
   order-by-updated + first page only — capability landed, phase pending)
2. `[base]` issues in `$project` filtered by tag and state
   (ordered-page phase adds priority/updated ordering and pagination)
3. aggregate milestone page (phase pending): milestones + dependent milestones +
   open-issue counts

### Metrics

cold-load time and **bytes vs. bytes-floor and refetch ceiling** · warm-read
latency · reconnect catch-up time/bytes vs. offline window · per-commit core
tick time vs. subscriber count · notification bytes per commit · local store
size after full sync · counters: versions shipped vs. referenced (peer dedup
ratio), prepared-shape count, binding count.

### Correctness (oracle, deterministic mode)

local pages == core-evaluated results at the requested settled tier ·
clients receive no rows/deltas outside their subscriptions · reconnecting
client converges to identical state as a never-disconnected client.

---

## 2. Realtime canvas

*Motivation: mergeable transactions under super-realtime concurrent writes —
convergence, merge behavior, history growth, and the cost of passive
observers. Also the suite's failure-injection home.*

### Schema

```ts
canvases:      { name: string }
canvasInvites: { canvas: ref(canvases), userID: uuid }   // drives RLS: invited users only
shapes:        { canvas: ref(canvases), type: enum[circle|rectangle],
                 text: string, x: float, y: float }
```

One canvas, 200 shapes. RLS: a user reads/writes a canvas iff invited
(makes the scenario exercise permission-filtered realtime sync, not raw
broadcast).

### Phase 1 — live collaboration `[base]`

- 5 active participants, each moving shapes at **120 mergeable commits/sec**
  (x/y cell writes; text excluded); shape choice Zipf-skewed so concurrent
  same-shape moves genuinely occur;
- 10 passive participants subscribed to the canvas;
- durations: 2s (small) and 30s (large);
- **coalescing axis**: run with per-client batching off and with 16ms
  coalescing — 600 raw commits/sec vs. ~300 batched at core is a
  product-relevant tradeoff, reported side by side.

Metrics: input-to-receipt latency p50/p95/p99 at every *other* participant,
**at settled tier `none`**, vs. the peer-latency floor · **merge versions
created per second** (and merges-of-merges) — the design predicts merges are
the exception; this is the scenario built to check that prediction · bytes
per participant vs. bytes floor · active and passive participant CPU · core
tick time · **history storage ratio**: on-disk size of the full run history
vs. a reference **zstd-compressed JSON event log** of the equivalent events
(one JSON object per version — `row_uuid`, tx ref, `made_at`, `parents`,
changed cells or deletion event — newline-delimited, sorted keys, written by
the harness from the oracle's event stream, compressed at **zstd level 3 and
level 19, both reported** — the pragmatic and the near-entropy anchor). This
ratio is
the history-compaction headline: today's uncompacted multiple is the
baseline that future compaction must beat. **Rule: the ratio is only ever
reported paired with the phase-2 time-travel/history-depth read latencies** —
a compressed blob with O(history) replay reads would win the ratio and lose
the product; the pair is the claim ("compact history *and* fast time-travel
lookup"), so the pair is the artifact.

### Phase 2 — loading `[base except where tagged]`

A fresh invited user loads: current state only · current + full history ·
historical state at 25/50/75% of phase-1 duration.
Additionally, **history-depth read cost**: current-row read latency as
versions-per-row grows (10 / 1k / 18k versions on hot shapes — phase 1
produces these naturally).

### Phase 3 — failure injection `[base]`

During a phase-1 run: disconnect/reconnect 2 participants mid-run; crash and
restart one participant (recover from local store); kill the core and
restart it (recover from durable storage). Measure recovery time; assert
convergence after quiescence.

### Correctness

all participants converge to identical canvas state at quiescence ·
current-only load == full-history load's derived current state ·
historical loads == replay of the accepted-history
prefix · non-invited spy client receives nothing.

---

## 3. Offline-first app with complex permissions

*Motivation: recursive RLS (team nesting × access edges) over partial
replicas — correctness of permission-filtered sync and, above all, the **cost
of permission evaluation in the common path**. That cost is measured two ways,
which are the primary signals: (1) **cold load** — evaluating a persona's whole
visible set at hydration; and (2) **new-write-to-reader** latency/throughput —
evaluating each incoming write against each connected reader's policy as it
propagates under realistic concurrent load (the same actor topology as S2, plus
RLS). Permission **changes** (grant/revocation) are deliberately **secondary**:
the design assumes they are rare relative to reads and writes, so the
revocation path is retained as a recursive-retraction correctness-and-cost
check (the known recompute cliff), not as the headline. Both fixtures below —
the org fixture and the block-tree variant — are equally important.*

### Schema

```ts
orgs:                { name: string }
teams:               { name: string, isAdmin: boolean,
                       isUserTeam: uuid | null, org: ref(orgs) }
teamTeamMemberships: { member: ref(teams), parent: ref(teams), onlyAdmins: boolean }
resources:           { name: string, enumLikeField: enum[alpha|beta|gamma|delta],
                       intField: int, floatField: float, smallOrGiantJsonField: json }
resourceAccess:      { resource: ref(resources), team: ref(teams),
                       adminsOnly: boolean, permission: enum[read|write|delete] }
```

### Fixture

20 orgs · 100 teams/org, team→team nesting up to 5 deep · 40,000
resources/org · 10 resourceAccess edges per resource. Permission rule:
recursive path via resourceAccess × teamTeamMembership; `isAdmin`/
`onlyAdmins`/`adminsOnly` filters as in the schema. Two personas: *simple
user* (~2,000 visible resources) and *admin user* (all 40,000, via explicit
resourceAccess evidence).

**Block-tree fixture variant `[base]`** — the document-workspace shape:
`pages → blocks` as a deep recursive tree (blocks nest 8–12 levels;
10k pages × ~200 blocks), permissions **inherited down the tree with
per-subtree overrides** (a share/lock edge at any block scopes its
subtree), plus a claim-parameterized visibility dimension (the same
content visible or not depending on a session claim — context-dependent
permissions). Write stream at agent rates (bursty block
inserts/updates concentrated in a few hot pages). Same phases and
metrics as the org fixture, with the same priority order: the headline is
**permission-aware cold load and steady-state new-write-to-reader cost under
granular, context-dependent policies** — per-commit/per-reader core cost must
track affected subtrees and connected readers, not workspace size. Grant/revoke
at an inner block reflowing exactly its subtree's visibility is retained as the
(secondary) incremental-retraction correctness check. This variant exists
because caches and granular permissions are traditionally at war; the
policy-composed-graph design claims they aren't.

### Phases

1. **Cold load** `[base]` — *primary*: full org-visible data for simple vs.
   admin user. Measures permission evaluation over a persona's whole visible
   set at hydration.
2. **New-write-to-reader** `[base]` — *primary*: under realistic concurrent
   load (permitted writers committing mergeable at tier `none`; many connected
   readers, **each an independent subscriber**, not a shared broadcast),
   measure per-write-to-reader latency (reader-observe − `tx.made_at`, with the
   reader's policy evaluated on the delivery path) and sustained throughput
   (writes/sec accepted, updates delivered/sec) as reader count scales — i.e.
   **how many readers the system can serve under live permission filtering**.
   Same concurrent actor topology as S2, plus RLS.
3. **Grant latency** `[base]` — *secondary*: warm client; add a resource / a
   resourceAccess edge / a teamTeamMembership that makes resources visible —
   p50/p95/p99 from commit to appearance at the client, at settled tiers `none`
   and `global`.
4. **Revocation** `[base]` — *secondary (permission changes are assumed rare)*:
   remove a teamTeamMembership or resourceAccess edge that makes 1 / 100 / 2,000
   resources invisible to a warm client. Measure commit-to-disappearance p50/p95
   at the client, and core (later edge) CPU during the recursive recompute.
   Retained as the recursive-retraction correctness-and-cost check (the known
   recompute cliff) and the standing baseline for incremental retraction work —
   not as a headline number, since changes are rare relative to reads/writes.
5. **Forbidden writes** `[base]`: a write to a resource the client cannot see
   is committed elsewhere. This is a *non-event* at the client — measured as
   a deterministic counter, not a latency: a harness-side spy asserts **zero
   forbidden rows/deltas delivered within K ticks** (any nonzero count is a
   security failure and fails the run outright).
6. **Reconnect** `[base]`: as scenario 1, with permission-filtered catch-up.
7. Repeat the grant/revocation/forbidden phases with the edge profile;
   additionally measure permission-subscription hydration (first mergeable write
   that forces the edge to acquire a permission subscription) vs. already-hydrated
   acceptance.

### Metrics

cold-load time/bytes per persona vs. bytes floor · permission-evaluation
time at core (later edge) · **permission-view sizes** (rows required to
evaluate policies — the cost driver of the edge-authority design) · grant
and revocation latencies as above · local store size per persona · counters:
forbidden-delivery (must be 0), recursive recompute count, recompute row
volume.

### Correctness

simple user holds exactly the oracle-computed visible set, at every
quiescent point · admin sees all 40,000 via explicit evidence · grant and
revocation converge to oracle visibility at the requested settled tier ·
edge fate decisions equal core decisions once permission
subscriptions are settled.

---

## 4. Globally consistent order processing (TPC-C derived)

*Motivation: jazz used deliberately against its grain — no partial
replication, no offline, all writes `exclusive`, clients wait for `global`
settlement. Tests whether commit-time validation (two-point predicate
evaluation over prepared shapes) makes jazz competitive as a classical
serializable database — and surfaces the OCC hot-row problem on purpose, so
the mergeable-counters answer to it can be measured rather than asserted.*

### Schema

As in the prior experiment (warehouses, districts, customers, items, stock,
orders, orderLines, payments — TPC-C shapes with jazz types). Unchanged
except terminology: "local store size", not "SQLite size".

### Scale discipline (anti-gaming, kept verbatim in spirit)

- scale factor = number of warehouses; each warehouse owns a fixed logical
  data volume (10 districts, 3,000 customers/district, one stock row per
  item, 100,000 shared items);
- per-warehouse transaction rate is **capped**, so higher aggregate
  throughput requires more warehouses and more data — never more requests
  against the same hot rows;
- primary scale-out result: the largest scale factor sustaining the target
  mix within SLO. SLO is declared in the run config and reported with the
  result; default: **p95 submission→global-settlement ≤ 10× core-link RTT**
  under the declared profile;
- report data volume, warehouse count, and per-warehouse throughput
  alongside aggregate throughput.

### Transaction shapes `[base]`

- **new order**: read customer/district/item/stock rows; increment
  `district.nextOrderNumber`; decrement stock; insert order + lines.
- **payment**: update customer balance/counters; update district and
  warehouse revenue counters; insert payment row.
- **delivery**: read the district's undelivered-orders *predicate* into the
  read-set and select the oldest **client-side** (no ORDER BY needed — the
  predicate read makes the choice phantom-safe under validation); mark order
  and lines delivered; update customer balance.
- **stock level**: read recent orders in a district; count referenced stock
  rows below threshold.

### Runs

1. **Scale-out** `[base]`: grow scale factor at bounded per-warehouse rate.
2. **Contention** `[base]`: small fixed scale factor with deliberately hot
   districts/items; raises read-set conflicts and hot-row validation
   pressure; report abort/retry curves at low/medium/high contention.
3. **Counter-strategy variant**: identical mix, but
   `yearToDate*`/`paymentCount` counters modeled as **mergeable counter
   columns** instead of exclusive writes. The all-exclusive variant
   serializes every payment in a warehouse through the warehouse row
   (first-committer-wins on a hot row — the classic OCC wall, present by
   design); this variant is jazz's intended answer. The side-by-side is the
   scenario's second headline.

### Metrics

committed exclusive tx/sec · max sustained scale factor within SLO ·
per-warehouse throughput · p50/p95/p99 submission→global settlement ·
abort/retry rate by contention level · read-set capture and validation time
(row vs. predicate entries split) · row deltas per committed tx · core CPU in
validation vs. materialization · store sizes after a fixed committed count ·
**ratio vs. the reference SQLite implementation** running the same logical
mix under the same durability setting.

### Correctness

no duplicate `orderNumber` per district · stock quantities/counters reconcile
with committed order lines · payment/revenue totals reconcile · delivery
state consistent between orders and lines · rejected transactions leave no
visible effect at any tier · **same-schedule replay**: jazz's accepted
transaction schedule, replayed against the reference SQLite implementation,
produces identical final state.

---

## 5. Durable stream (LLM-agent append log)

*Motivation: agents appending tokens to durable streams, with listeners
tailing live or resuming from their last known point, is a workload entire
startups are built around. The jazz thesis under test is **brainless
dumping**: the application writes the full stream state into one column on
every append — no userland event/chunk modeling, no bespoke append protocol —
and jazz is responsible for making that efficient. The competition is
therefore not other modelings on jazz; it is what a competent engineer would
build bespoke. This scenario is the suite's pressure test for the two
storage problems that thesis implies: **redundancy between row versions of a
large, mostly-shared column value**, and **the per-version metadata overhead
itself** (at 100 tokens/s, transaction/version metadata can dwarf the
payload). Today's numbers are the baseline `[needs: column-delta]` must
beat.*

### Schema

```ts
streams:    { name: string }
streamDocs: { stream: ref(streams), content: bytes }   // full state, rewritten per append
```

One row per stream. Every append commits a new version of the entire
`content` value — each version logically duplicates the whole prefix. That
is the point: jazz must converge (via `[needs: column-delta]` structural
sharing, on disk and on the wire) toward what the bespoke systems below
achieve with explicit append logs, while the app code stays a one-line
column write.

### Workload `[base]`

- one writer per stream appending at **100 tokens/s** (~4 bytes/token),
  batching axis: 1 / 10 / 100 tokens per commit;
- run lengths 1min / 10min (≈ 6k–60k tokens per stream);
- **L live tailers** per stream (subscribed, tier `none`) and **R resumers**
  joining at seeded points with an existing payload inventory — `[base]` resumes
  by rehydration, `[needs: payload-inventory]` re-measures with delta resubscribe;
- **stream-count axis**: 1 / 100 / 10k concurrent streams (agents at scale) —
  per-commit core cost must track *affected* streams; shape registration
  with binding-set aggregation is the mechanism under test, as in scenario
  1's subscriber sweep.

### Adversarial comparisons

- **floor**: an fsync-disciplined append-only log file with length-prefixed,
  zstd-framed records, tail via in-process notification — the minimal
  purpose-built durable stream (what stream startups' core loop is). This
  measures jazz's *generality tax* directly.
- **pragmatic baseline**: a SQLite WAL table (one row per event, `(stream,
  seq)` pk) with a notify/poll tailer.
- **storage anchors**: zstd (levels 3 and 19) of the concatenated payloads —
  the bytes floor for any encoding.
- external systems (Kafka/Redpanda/NATS JetStream) are deliberately *not* in
  the suite: operationally heavy, different durability envelopes; revisit
  only if a marketing claim ever needs them.

### Metrics

append→tail-delivery p50/p99 at tier `none` vs. link floor · sustained
appends/sec per stream and aggregate vs. stream count · resume time/bytes
vs. gap size · **history + metadata bytes per appended token** (the
headline: payload bytes are ~4/token; everything above that is jazz
overhead — duplicated prefixes, transaction rows, version metadata,
intervals; report the multiple vs. the log-file floor) · **synced bytes per
token per tailer** (the wire-side twin: today each append ships the full
value; `[needs: column-delta]` must collapse both) · prefix-sharing ratio
(stored bytes / logical unique bytes; re-measured when column-delta lands) ·
core CPU per append · local store size at tailers vs. resumers.

### Correctness

every tailer observes a strictly prefix-monotone sequence of content
versions and converges to the final content, byte-exact · resumer state ==
full-replay state, byte-exact · a late resumer over an evicted prefix
(`[needs: eviction]`) re-fetches correctly from upstream.

---

## 6. Collaborative text editing (real editing trace)

*Motivation: large documents in a text column, edited as linear runs at
random positions — the same value-versioning pressure as scenario 5 but with
mid-value edits instead of appends. This is the arena where jazz competes
directly with CRDT libraries, on their own canonical benchmark.*

### The trace

The [automerge-perf editing trace](https://github.com/automerge/automerge-perf):
Martin Kleppmann's keystroke-by-keystroke recording of writing the LaTeX
source of *“A Conflict-Free Replicated JSON Datatype”* (Kleppmann &
Beresford) — **182,315 single-character insertions and 77,463 deletions
(259,778 edit operations)** producing a ~100KB final document; CC-BY-4.0;
the standard benchmark for Automerge, Yjs, diamond-types, Loro, et al.
### Trace catalog (eg-walker superset)

We adopt the [eg-walker evaluation set](https://arxiv.org/abs/2409.14252)
verbatim as the literature-comparable core — their published per-trace
results (stored size, load time, replay/merge time, memory) become free
side-by-side baselines — organized by their taxonomy and extended with
jazz-specific traces in each group:

| label | trace | character | gate |
|---|---|---|---|
| S1 | *automerge-paper* (Kleppmann LaTeX; 2 authors taking turns) | sequential, keystroke | `[base]` |
| S2 | *seph-blog1* (8,800-word blog post, 1 author) | sequential, keystroke | `[base]` |
| S3 | *egwalker* (the eg-walker paper's own source, 2 authors) | sequential, keystroke | `[base]` |
| W1 | Wikipedia revision history of one large page (pinned page + revision range + content hash; CC-BY-SA) | sequential, multi-author, **full-state revisions with reverts** — the brainless-dump model in the wild | `[base]` |
| C1 | *friendsforever* (2 users realtime, 1s simulated latency, ~26k edits) | concurrent, fine-grained | ingest/storage `[base]`; merged-content assertions `[needs: text-merge]` |
| C2 | *clownschool* (2 users realtime, 0.5s latency, 5,380 txns, timestamps) | concurrent, fine-grained | as C1 |
| A1 | `src/node.cc` from Node.js (git-derived) | async divergence, **human merge resolutions recorded** | DAG replay with recorded merges `[base]`; jazz-generated merges `[needs: text-merge]` |
| A2 | `Makefile` from git.git (git-derived) | as A1 | as A1 |
| A3 | repo-corpus variant: every file of a busy repo over a commit range | async, thousands of text rows merged concurrently | as A1 |
| X1 | synthetic contention generator (seeded; K = 2–32 authors, Zipf edit positions, per-author offline windows from seconds to hours) | **contention as a dial** — the axis no recorded trace provides; offline divergence is jazz's home turf | ingest `[base]`; merge `[needs: text-merge]` |

Methodology adopted with the traces:

- **mirror their per-trace metrics** — stored size, cold-load time,
  replay/merge time, steady-state and peak memory — so every cell is
  directly comparable with the eg-walker paper's tables;
- run both **natural size** and their **~500k-inserted-characters
  normalization** (repetition) — the latter for cross-trace comparability,
  the former for realism;
- **sourcing is pinned**: S and C traces from
  [automerge-perf](https://github.com/automerge/automerge-perf) and
  [josephg/editing-traces](https://github.com/josephg/editing-traces); A1/A2
  from the paper's artifacts, or regenerated by their described method
  (minimal edit operations per commit diff, pinned repo + commit range,
  content-hashed into fixtures).

The [dmonad/crdt-benchmarks](https://github.com/dmonad/crdt-benchmarks)
suite remains in use for cross-library comparability where shapes overlap.

**Semantic honesty up front**: jazz today merges text columns by whole-value
HLC-LWW — concurrent edits to one document *replace*, they do not interleave.
Character-level concurrent merging is `[needs: text-merge]`: a rich-text
column merge strategy doing **three-way merges**, with the common ancestor
supplied by the version DAG (`parents` gives it directly — this is the
ancestor-aware strategy the README defers). The Kleppmann trace is
single-author and sequential, so phases 1–4 are a fair fight on storage,
throughput, and latency without merge semantics; the concurrent phase is
specified now and activated with the feature.

### Modeling — brainless dump only

One row per document, the full `text` column rewritten on every edit — the
same thesis as scenario 5: no userland op-log modeling, no CRDT structures
in app code; jazz owns the efficiency problem (`[needs: column-delta]`) and,
later, the merge problem (`[needs: text-merge]`). The adversaries below are
the systems where humans did that work by hand.

### Phases

1. **Trace replay** `[base]`: single writer replays all 259,778 edits as
   mergeable commits (batching axis: 1 / 32 / 256 edits per commit).
   Metrics: ingest throughput (edits/s), local-echo latency, peak memory.
2. **Live observation** `[base]`: writer replays at a realistic 10 edits/s
   sample while a second client tails — edit→observer p95 at tier `none`.
3. **Cold load** `[base]`: fresh client loads the finished document — current
   state only vs. with full history; historical
   states at 25/50/75% of the trace. The canvas pairing rule applies: any
   storage claim is reported *with* these latencies.
4. **Storage** `[base]`: total store size and **history+metadata bytes per
   edit** after full replay, against the anchors below.
5. **Concurrent merge** `[needs: text-merge]`: replay the concurrent traces
   below; the converged document must match the three-way strategy's
   documented semantics (compared against CRDT-library output on the same
   trace as a *semantic*, not byte, comparison — the strategies legitimately
   differ).

### Adversarial comparisons

Two explicitly-labeled tiers, because most CRDT libraries are in-memory and
jazz is durable — the label *is* the fairness mechanism:

- **in-memory CRDT floor** (non-durable): diamond-types, Yjs, Loro replaying
  the same trace — CPU/latency/memory floor;
- **durable CRDT baseline**: Automerge with its persisted save format /
  Yjs with a persistence backend — the apples-to-apples storage and
  cold-load comparison (their save-file sizes on this trace are published
  and reproducible);
- **storage anchors**: zstd (3 and 19) of the final document, and of the
  JSON edit-op log — the latter is the same anchor pattern as canvas.

### Metrics

trace replay throughput vs. in-memory floor · storage ratio vs. durable CRDT
baseline and zstd anchors · synced bytes per edit per observer ·
history+metadata bytes per edit ·
cold-load time (current vs. full history) vs. durable CRDT load · memory ·
point-in-time read latency (paired with storage, per
the canvas rule).

### Correctness

final document byte-equals the reference string produced by directly
applying the trace · every prefix replay equals the corresponding reference
prefix · `[needs: text-merge]` concurrent convergence per the strategy's
spec, identical on every node.

---

## 7. Parallel schema evolution (migration lenses)

*Motivation: the no-stop-the-world migration claim — clients on different
schema versions read and write concurrently through bidirectional
translation lenses, nothing on disk is rewritten, and offline clients
that predate a schema change still sync in. The cost under test is the
**lens tax**: translated reads and writes vs native ones.*

### Setup

The scenario-1 SaaS schema as v1; three published evolutions: v2 adds a
column with default (naturally mappable), v3 renames a column + drops one
with a backwards default (naturally mappable), v4 applies a value
transform (non-trivial lens). Lens chain v1↔v2↔v3↔v4; clients pinned per
version.

### Phases

1. **Mixed-version steady state**: clients on v1 and v4 concurrently
   write the S1 edit stream; every client subscribes to the same logical
   queries in its own schema version. Correctness: all clients converge
   to lens-consistent states (the round-trip invariant: translated deltas
   applied to translated state == translation of the natively-derived
   state), and each client sees every other version's writes.
2. **Lens-tax measurement**: identical query mix run native (single
   version) vs through 1-hop and 3-hop lens chains. Metrics: read
   latency overhead per hop, write translation overhead, sync bytes
   overhead (translated vs native payloads).
3. **Rollout wave**: population migrates version-by-version mid-stream
   (no quiesce); measure disruption (latency spikes, recompute volume)
   during the wave. Correctness: zero stop-the-world, zero lost writes.
4. **Late offline client**: a client offline since v1 reconnects into a
   v4 world; its queued v1 writes must land and translate. This is the
   scenario's headline correctness case.

### Anchors

The native single-version run is the floor; the naive alternative
(stop-the-world rewrite) is described, not implemented.

## 8. Branching `[needs: scenario harness]`

*Motivation: branches as a first-class database feature — isolated
parallel lines of work over a shared base (sandboxing, drafts, staging,
agent experimentation) with snapshot-base semantics, cheap creation, and
storage shared with the base.*

### Workload (agent-sandbox shape)

A large base database (S1 fixture scale). N concurrent short-lived
branches (N = 1 / 10 / 100): each branch is created off current main,
receives a burst of writes (mixed inserts/updates over base rows),
serves reads (queries spanning branch-local + base-visible state), then
is either merged back or discarded. Meanwhile main keeps receiving its
own write stream.

### Metrics

branch creation cost (target: O(1) snapshot capture, independent of base
size) · branch read overhead vs main reads (the overlay tax) · storage
per branch vs naive copy (sharing ratio; only branch-local writes may
cost) · merge-back cost vs branch size · discard cost · subscription
behavior across branches (a subscriber on main must see nothing from
unmerged branches — counter, must be 0; branch subscribers see
base+overlay consistently).

### Correctness

branch reads == base-snapshot-plus-overlay oracle · main is bit-identical
whether or not discarded branches ever existed · merge-back equals the
equivalent direct-on-main write sequence under the same merge strategies
· two branches never observe each other.

## 9. Durable execution backend

*Motivation: workflow/durable-execution engines need exactly the pair
jazz claims to unify — append-heavy per-instance step logs with live
tailing, and serializable per-instance state transitions — at high
concurrent-instance counts with natural partitioning. The incumbent
architecture is a hand-tuned partitioned KV store: fast and scalable,
but the application code carries the consistency burden. The claim
under test: jazz matches the scalability while the application writes
ordinary rows and transactions. Mostly reuses S4 (exclusive validation,
scale-out ladder) and S5 (append streams, tailers) machinery under a
workflow-shaped fixture.*

### Schema

```ts
workflows:  { name: string, definition: json }
instances:  { workflow: ref(workflows), state: enum[pending|running|
              sleeping|completed|failed], currentStep: int, wakeAt:
              timestamp | null }
steps:      { instance: ref(instances), seq: int, kind: string,
              input: json, output: json | null,
              status: enum[started|completed|failed] }
events:     { instance: ref(instances), seq: int, payload: bytes }
```

### Workload `[base]`

- N concurrent workflow instances (ladder: 100 / 1k / 10k), each
  advancing through K steps; step execution simulated, persistence
  real;
- state transitions (`instances.state`, `currentStep`) are **exclusive
  transactions** (an instance must never double-advance — two workers
  racing the same instance is the core correctness hazard, validated by
  read-set conflict);
- step/event appends are mergeable writes (the S5 append shape);
- per-instance rate capped, so throughput scales only with instance
  count (the S4 anti-gaming rule, re-skinned);
- observers: a dashboard-shaped subscription (all running instances of
  one workflow) and per-instance tailers;
- resume: workers crash and re-attach, recovering instance state +
  step history from a cold node — `[base]` full rehydrate,
  `[needs: payload-inventory]` delta resubscribe.

### Metrics

step transitions/sec aggregate and per instance · max concurrent
instances within SLO (p95 transition settle ≤ 10× link RTT) ·
double-advance attempts rejected (counter; must equal injected races) ·
worker re-attach time vs instance history depth · dashboard
subscription cost vs instance count · history+metadata bytes per step
vs the S5 log-file floor · store size after a fixed step count.

### Correctness

every instance's step sequence is gap-free and monotone · no instance
ever double-advances (oracle replays the accepted schedule) · resumed
worker state == continuous-worker state, byte-exact · dashboard view
matches oracle at quiescence.

### Anchors

S5's fsync-disciplined log file (floor for the append side) · SQLite
WAL transactional baseline running the same logical schedule (floor for
the transition side) · the partitioned-KV incumbent is described, not
implemented (operationally heavy; revisit per the systems-tier rule).

## Systems tier (whole-system competitors)

The scenarios above compare against floors, anchors, and same-process
references because that is what can be held to identical durability and
driven deterministically. The systems jazz actually competes with —
**Zero (Rocicorp), InstantDB, Convex, ElectricSQL** (all open source) —
are client+server+Postgres services with JS clients, so they get their own
tier under `benchmarks/systems/` with deliberately different rules:

- docker-composed services, real transports on localhost, wall-clock only;
- no deterministic driver, no oracle integration — correctness spot-checks
  only (converged-state equality at quiescence);
- **same logical workloads and fixtures** as the scenarios they mirror,
  one TS driver per system, emitting the same JSONL schema;
- every emitted line carries an **envelope label** (durability, consistency
  guarantee, history kept or not) — these systems do not pay for the same
  product: jazz runs the identical workload while keeping full edit
  history, offline merge semantics, and its declared durability. The
  asymmetry is reported, never normalized away.

Scenario mapping (the claim each cell tests):

| cell | systems | shared claim / expected differentiator |
|---|---|---|
| S1 partial sync | **Zero** (closest cousin: query-driven sync + IVM), InstantDB, Electric shapes, Convex subscriptions | cold-load bytes/time, subscriber + distinct-shape sweeps, query churn |
| S2 realtime | all four | input-to-receipt latency; their convergence is server-ordered LWW without history — envelope label carries the asymmetry |
| S3 permissions | InstantDB perms, Electric shape where-clauses, Zero query auth | **revocation-to-disappearance vs their resubscribe/invalidation storm** — likely the suite's most differentiating chart |
| S4 serializable | **Convex only** (the others do not claim serializable transactions) | OCC vs OCC at the same guarantee — a fairer "same product" reference than local SQLite |
| S5/S6 | none | not their product; CRDT libraries remain the right adversaries |
| S9 durable execution | stateful-compute platforms (actor/durable-object model) | ephemeral compute attaching to shared state: cold-attach latency, cross-instance coordination via exclusive txs vs single-threaded state islands, fan-out without pre-chunking the app. v1 is jazz-side metrics + architecture contrast (the incumbent platforms need their own cloud to run honestly) |

Additional workload this tier motivates (also useful standalone):
**optimistic-write depth on reconnect** — N offline local writes against
concurrent server-side edits; Zero rebases pending mutations, jazz merges;
measure reconciliation time and intermediate-state quality on reconnect.

Sequencing: after the suite's own completeness items (TRACKS.md backlog);
first cell is **S1 vs Zero**. Integration cost is one TS driver + one
service per system — each cell ships independently.

## Reporting

Each scenario emits one JSONL stream per run (metadata-enriched, retained
under `benchmarks/results/` when baseline-worthy). A summary tool renders
the per-scenario headline artifacts; comparisons always show: floor,
jazz, ceiling/reference, and the deterministic-counter equality column. The
headline artifacts — and only those — are what end up in README-level claims.
