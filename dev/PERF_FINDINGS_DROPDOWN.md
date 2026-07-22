# Dropdown Child Subscription Perf Findings

## Fixture

`crates/jazz-sim/benches/customer_cold_start.rs` now includes the target shape:

- dominant bare child subscription: `res_l_child_3`
- parent resources: 65 `res_l` rows
- child rows: 43,000 total
- visible child rows for member identity: 23,831, inherited through 36 visible parents
- parent access edges: 3,000 neutral `res_l_access_edges` rows
- subscriptions remain bare `Query::from(table)` over the 39-table fixture

The fixture is anonymized/name-blind (`res_l`, `res_l_child_3`, neutral generated values) and deterministic.

## Native Measurements

Command:

`JAZZ_REHYDRATE_TRACE=1 JAZZ_CUSTOMER_PHASES=cold,warm cargo bench -p jazz-sim --bench customer_cold_start -j 2`

Allocator command:

`JAZZ_CUSTOMER_PHASES=warm cargo bench -p jazz-sim --features bench-alloc-metrics --bench customer_cold_start -j 2`

### Per-Phase Comparison

| Phase                                                       |                                                                        Native | Native us/visible row |          Browser target |
| ----------------------------------------------------------- | ----------------------------------------------------------------------------: | --------------------: | ----------------------: |
| Server reset / maintained update compute for dominant child |         365-375 ms for 23,867 adds (`drain_ms` 194-198 + `bundle_ms` 171-177) |          15-16 us/row |             ~195 us/row |
| Cold local rehydrate/materialize of full child table        |                1,019 ms for 43,065 raw adds (`open_ms` 663 + `bundle_ms` 356) |         24 us/raw row |                     n/a |
| Warm reopen maintained subscription open                    |                  4,464 ms for 23,867 adds (`open_ms` 4,293 + `bundle_ms` 171) |            187 us/row |             ~260 us/row |
| Delta apply/delivery for dominant child                     |                                                    365-375 ms for 23,867 adds |          15-16 us/row |     TS apply ~85 us/row |
| Wire encode/decode/compression, whole warm workload         | zstd encode 9.5 ms, decode 3.7 ms; raw payload ~20.2 MB, zstd payload ~506 KB | <1 us/row compression | wasm ingest ~180 us/row |

Whole-workload receipts:

- cold: 27,518 rows, wall 26.4s, settle 21.9s, materialize one-shot 4.0s
- warm: 27,518 rows, wall 21.1s, settle 15.8s, materialize one-shot 4.2s
- warm allocator feature: 60,921,636 allocs / 41.9 GB allocated, or 2,214 allocs and 1.52 MB allocated per materialized row

## Storage Reads

Dominant child trace lines:

- cold reset-like rehydrate: `positioned_members=43000`, `open_reads=172195`, `open_ranges=18`, `bundle_reads=46065`, `bundle_ranges=1`
- cold delta/update to member-visible rows: `adds=23867`, `drain_reads=0`, `bundle_reads=26896`, `bundle_ranges=1`
- warm reopen: `positioned_members=23831`, `open_reads=190846`, `open_ranges=60`, `bundle_reads=26896`, `bundle_ranges=1`

Evidence: warm reopen uses a small number of ranges plus roughly 8 point reads per visible row on the open path. The bundle path is range-batched (`bundle_ranges=1`) but still reads one-ish bundle row per delivered row.

## Suspects

1. Warm reopen missing effective batching in subscription-open membership/materialization path: SUPPORTED.
   Evidence: `res_l_child_3` warm reopen spends 4.293s in `open_ms` before bundle construction, with 190,846 reads / 60 ranges for 23,831 rows. This closely matches the browser warm reopen symptom and is the only native phase near the browser per-row constant.

2. Per-row policy join/authorization instead of set-level parent visibility semi-join: LIKELY for warm reopen, less so for server update.
   Evidence: visible children inherit through only 36 parent rows, but warm open still performs ~190k reads over 23.8k children. Server update has `drain_reads=0` and 15-16 us/row, so the maintained delta path is not paying the same policy cost.

3. Reset chunk encode/decode or transport compression as the dominant server/native cost: RULED OUT natively.
   Evidence: whole-workload zstd encode/decode is ~13 ms combined on ~20 MB raw payload. That is far below browser wasm ingest (~4.3s). Native transport cost is not the core bottleneck.

4. One transport tick processes the whole reset synchronously: PLAUSIBLE browser-side, not proven in native core.
   Evidence: native receipt shows one bulk client ingest commit for 27,518 bundles and a ~20 MB raw payload. Native compression/decode is cheap, but browser has one 4.3s transport tick, so wasm/JS scheduling and synchronous ingest remain likely outside this core-native bench.

5. Allocation pressure: SUPPORTED as a broad cost multiplier, not isolated to a site.
   Evidence: allocator run reports ~60.9M allocations and ~41.9GB allocated for warm reopen. I did not run stack-sampled allocation sites because the aggregate run already made the warm path materially slower and stack sampling would heavily perturb the 4s open path.

## Ranked Fix Candidates

1. CLEARLY-GOOD: make warm reopen child-table subscription open use a parent-visibility set and batched/range materialization instead of per-child authorization/read work.
   Ceiling: warm reopen dominant child from ~4.46s toward the cold/update bundle path floor (~0.36-1.0s), saving roughly 3-4s.

2. CLEARLY-GOOD: cache or precompute inherited parent visibility for `inherits(parent_id)` subscriptions during local-store reopen.
   Ceiling: remove most of the ~8 point reads per visible child in warm open; likely multi-second on this shape.

3. CLEARLY-GOOD: split/reuse bundle materialization so one-row-per-bundle reads do not repeat across adjacent subscriptions after the dominant child open.
   Ceiling: smaller than #1, but trace shows later small subscriptions sometimes pay `bundle_reads=467xx`, implying cross-subscription bundle/materialization spillover.

4. SPECULATIVE: chunk client ingest/application work at the transport boundary.
   Ceiling: browser-only responsiveness and tick latency; native compression is already cheap, but one huge reset payload/tick is consistent with the browser 4.3s wasm ingest symptom.

5. SPECULATIVE: reduce per-row allocations in maintained-subscription reopen and bundle building.
   Ceiling: unknown without allocation site sampling; aggregate pressure is high enough to justify a follow-up targeted allocator-site run after the read-path fix narrows the hotspot.

## Negative Results

- The existing pre-change bench did not model the requested dominant shape exactly; it had a 19,894-row child table, not ~43k total/~24k visible over ~65 parents.
- Native server-side reset/update compute is not 195 us/row; it is ~15-24 us/row on this fixture.
- Native wire compression/decode is not the 180 us/row ingest problem.
- The native warm reopen symptom is real and close to browser scale: 187 us/row native versus ~260 us/row browser.

## Tooling Friction

A public bench-only `Db::take_storage_read_metrics_for_test`/`reset_storage_read_metrics_for_test` would have avoided relying on trace text for per-subscription storage-read counts.

## Phase 2 results

Phase 2 stopped before engine changes under the requested STOP rule.

I traced the warm-reopen maintained-subscription open path:

- `PeerConnection::rehydrate_query_maintained_subscription_view` calls `NodeState::open_seeded_maintained_subscription_view`.
- `open_seeded_maintained_subscription_view` compiles the query as `CurrentQueryProgramOutput::MaintainedView`, subscribes the lowered Groove program, receives the initial snapshot, and applies it into `MaintainedSubscriptionView`.
- The inherited child policy lowers in `normalize_inherited_parent_policy` as an inner join from the child source to a visible parent source.
- The existing range batching lever found in-tree is `preload_tx_versions_for_materialization`, which batches version lookups for materialization/bundle construction by contiguous transaction-time spans. That applies after result members are known; it does not remove the open-time maintained-view snapshot cost.

The tempting implementation for fix candidates 1+2 is to compute the visible parent set once at reopen, then feed the child source from `parent_id` index scans over that set. That would improve the initial open snapshot, but it is not byte-equivalent as a maintained subscription: future permission grants/revokes could make children under newly visible or newly hidden parents enter/leave the result, while a source statically restricted to the parent set visible at open time would not observe the same deltas.

Therefore this approach would change delivered subscription semantics unless the maintained graph still dynamically tracks parent visibility. Preserving semantics appears to require a join-planning/execution optimization: keep the child source dynamic and maintained, but make open-time materialization of an inherited-parent join drive from the small parent-visibility relation and use child `parent_id` range/index scans. That likely belongs either in query lowering as a new dynamic semi-join/access-path representation, or in the underlying join/open strategy that can choose the small side without making it a static source filter.

Phase 2 classification:

- CLEARLY-GOOD: optimize dynamic inherited-parent joins so open-time materialization is driven by the visible parent relation while preserving the maintained child source for future deltas. Estimated ceiling remains roughly 3-4s on the dropdown fixture.
- SPECULATIVE/REJECTED FOR THIS PATCH: precompute visible parents once and replace the child source with static `parent_id` scans. It is fast-looking but not semantically safe for later access-edge changes.
- CLEARLY-GOOD: keep using the existing version-materialization batching (`preload_tx_versions_for_materialization`) for bundle construction; it already explains why `bundle_ranges=1` while open remains expensive.

No after benchmark was run because no engine change was applied. The relevant before measurement remains:

- warm reopen `res_l_child_3`: `open_ms=4293`, `bundle_ms=171`, `open_reads=190846`, `open_ranges=60`, `adds=23867`, approximately `187 us/row` for open+bundle.

## Phase 3 results

Command:

`JAZZ_REHYDRATE_TRACE=1 JAZZ_CUSTOMER_PHASES=warm cargo bench -p jazz-sim --bench customer_cold_start -j 2`

The storage metric correction matters: Groove records every row visited by a range scan as a read (`record_range_row`), so `open_reads` is not evidence of point gets by itself. I temporarily instrumented the warm-open stages and added per-destination open-read buckets to the rehydrate trace.

### Stage split for `res_l_child_3`

| Scenario                        | open_ms | compile_ms | subscribe_ms | snapshot_recv_ms | snapshot_apply_ms |            Rows | Open us/visible row |
| ------------------------------- | ------: | ---------: | -----------: | ---------------: | ----------------: | --------------: | ------------------: |
| Warm-prime/cold-like local open |     684 |          0 |          354 |                0 |               329 | 43,065 raw adds |     15.9 us/raw row |
| Warm reopen from disk           |   4,444 |          1 |        4,240 |                0 |               202 |     23,867 adds |        186.2 us/add |

The four temporary stage timers sum to the traced `open_ms` within rounding. Warm reopen is dominated by `subscribe_lowered_program`, not Jazz query compilation, snapshot receive, or `MaintainedSubscriptionView::apply_multisink_deltas`.

### Open-read destination buckets

| Scenario                        | Total reads | Total ranges | `global_current_rows` reads | `global_current_rows` ranges | `register_global_current_rows` reads | `register_global_current_rows` ranges | Other buckets |
| ------------------------------- | ----------: | -----------: | --------------------------: | ---------------------------: | -----------------------------------: | ------------------------------------: | ------------- |
| Warm-prime/cold-like local open |     172,195 |           18 |                     172,195 |                            7 |                                    0 |                                    11 | all zero      |
| Warm reopen from disk           |     190,846 |           60 |                     190,846 |                           28 |                                    0 |                                    32 | all zero      |

Point-vs-range row split is not cheaply available without changing Groove's metric shape: `reads` includes range-visited rows. The destination buckets are still decisive here: the open path is visiting persisted `global_current_rows`; it is not reading history, changes, transactions, indexes, or other destinations.

### Updated diagnosis

Warm reopen does overscan: 190,846 `global_current_rows` visits for 23,831 positioned visible child rows, roughly 8.0 visited rows per visible row. But overscan alone does not explain the regression. The warm-prime local open visits a similar order of rows, 172,195, with `subscribe_ms=354` (~2.1 us/visited row), while warm reopen spends `subscribe_ms=4240` (~22.2 us/visited row). The dominant cost is therefore a high per-row hydration/arrangement constant inside the maintained subscription open path over persisted `global_current_rows`.

The earlier Phase 2 static-parent-set shortcut remains rejected: freezing the parent visibility set at open time would not be byte-equivalent for future permission-edge changes. The equivalent fix needs to preserve the dynamic inherited-parent join while improving its persisted-state open/materialization strategy.

### Updated ranked fix candidates

1. CLEARLY-GOOD: optimize `subscribe_lowered_program` / the maintained-view open strategy for persisted `global_current_rows` on inherited-parent joins. Keep the child source and parent visibility dynamic, but make initial open materialization avoid rebuilding expensive per-row state at the current warm-reopen constant. Estimated ceiling: 3.5-4.0s on this fixture.

2. CLEARLY-GOOD: add a dynamic inherited-parent semi-join open strategy that drives initial materialization from the small visible-parent side and uses child `parent_id` range/index scans, without turning the parent set into a static filter. Estimated ceiling: 3-4s if it removes both the per-row hydration constant and part of the overscan.

3. CLEARLY-GOOD: reduce repeated `global_current_rows` visits in the inherited-child open plan. The trace shows range overscan, not point gets: 190,846 visits for 23,831 visible rows. Estimated ceiling: 0.5-1.0s if it only reduces visits; higher only if paired with the maintained-open per-row constant fix.

4. SPECULATIVE: target allocation reductions inside the persisted maintained-open path. Phase 1 aggregate allocation pressure was high, but Phase 3 shows `snapshot_apply_ms` is only 202ms and does not identify allocation sites inside the 4,240ms subscribe stage. I skipped `bench-alloc-sites`: the feature exists, but stack backtrace sampling over this allocation volume could not be assumed to stay under the requested ~2x perturbation limit.

5. SPECULATIVE: transport tick chunking remains a browser/WASM responsiveness candidate. Native warm reopen is already dominated before reset bundling/transport, and native receive is 0ms in this path.

Temporary stage timers were reverted after capture. The per-destination `JAZZ_REHYDRATE_TRACE` bucket extension is clean and generally useful, so I left only that trace extension in the working tree.

## Phase 4 results

Temporary Groove instrumentation traced the Jazz-to-Groove open path:

`NodeState::subscribe_lowered_program` -> `Database::subscribe` for the unparameterized/bare child subscription -> `IvmRuntime::subscribe` -> `hydration_snapshots_for_subscription` -> non-aggregate `hydration_snapshot` -> `snapshot_table_deltas` -> `TickEvaluator::update_node` -> source/filter/project/join operators in `crates/groove/src/ivm/runtime/mod.rs` and `crates/groove/src/ivm/runtime/join.rs`.

Warm reopen target line from the final run:

- `res_l_child_3`: `open_ms=4270`, `open_reads=190846`, `open_ranges=60`, `positioned_members=23831`, `adds=23867`

### Dominant Groove Split

The most relevant Groove hydration line for the warm-reopen target output had:

| Stage                                                  | Calls | Input rows |                                      Output rows |           Time |
| ------------------------------------------------------ | ----: | ---------: | -----------------------------------------------: | -------------: |
| `snapshot_table_deltas_total`                          |     1 |          0 |                                           26,943 |        12.3 ms |
| `jazz_res_l_child_3_global_current` scan               |     1 |          0 |                                           23,831 |   10.9-11.1 ms |
| `table_source`                                         |     1 |     23,831 |                                           23,831 |         0.4 ms |
| `filter`                                               |     1 |     23,831 |                                           23,831 |     2.0-2.4 ms |
| `join_keyed_deltas`                                    |    14 |    200,418 |                                          200,418 |       25-26 ms |
| `arrangement_build_index` / `arrangement_apply_update` |    19 |    274,947 |                                          214,739 |          28 ms |
| `map_project` exclusive                                |    16 |    340,298 |                                          340,298 |     100-112 ms |
| `anti_join_apply`                                      |     1 |     23,831 |                                           23,831 |           9 ms |
| `join_apply` exclusive                                 |     5 |    128,925 |                                           78,157 | 3,501-3,556 ms |
| `join_probe_emit`                                      |    15 |    183,321 | 2,399,165 matched/emitted pre-consolidation rows | 1,951-2,067 ms |
| `join_consolidate_output`                              |     5 |     78,157 |                                           78,157 | 1,443-1,501 ms |

The inclusive operator timings double-count child work, so the exclusive counters are the useful ones. The storage snapshot and decode/copy path is not the hotspot: the dominant child rows scan in ~11ms and all snapshot table reads for this output take ~12ms. Key encoding and arrangement rebuild are also not the hotspot, at ~25-28ms each.

The hot path is `JoinState::apply` in `crates/groove/src/ivm/runtime/join.rs`, specifically `append_join_deltas` plus `create_join_record_into`, followed by `consolidate_deltas`. The pathological part is not persisted row decode; it is join fanout during hydration: only 23,831 visible child rows are delivered, but the join probe emits about 2.4M intermediate rows before consolidation.

### Overscan Answer

The `open_reads=190846` count is range-row visits across multiple hydration snapshots/operators, not point gets. The dominant target output itself scans:

- `res_l_child_3_global_current`: 23,831 rows
- `res_l_access_edges_global_current`: 3,000 rows
- `res_l_global_current`: 36 rows
- small group/group-entry sources: 76 rows
- total snapshot rows for the output: 26,943

The broader open issues several related hydration snapshots for sibling/internal outputs, some scanning the child table at 43,000 rows, so the same persisted `global_current_rows` are visited by multiple operators/snapshots. But the 4.3s target cost is not explained by scan volume: the decisive multiplier is join fanout inside policy/access hydration, where the join emits ~2.4M intermediate rows and then consolidates them back down.

### Updated Fix Recommendation

CLEARLY-GOOD: add a semijoin/existence-style hydration path for authorization/policy joins that does not materialize full joined records for multiplicity that will be discarded by consolidation.

Mechanism: for inherited-parent visibility during maintained open, represent the access check as an existence/semi-join over the small visible-parent/access relation, or teach `JoinState::apply`/lowering to use a payload-preserving semijoin when only left-row visibility is needed. The optimized path should probe parent/access existence and emit each child row at most once, preserving dynamic maintained semantics for later permission changes. Do not freeze the parent set at open time.

Estimated ceiling: up to ~3.5s on this fixture. The current warm-reopen target spends ~3.5s inside `join_apply`, with ~2.0s in `append_join_deltas`/record construction and ~1.5s in `consolidate_deltas`; storage snapshot, keying, and arrangement rebuild together are under ~100ms.

All Groove instrumentation used for Phase 4 was temporary and reverted after capture.

## Phase 5 results

Phase 5 stopped before implementation because the requested one-site change is not currently supported by the query-engine lowering path.

Requested change: wrap the inherited-parent policy `parent_current` node in `RowSetExpr::Distinct { input: parent_current, keys: vec![NormalizedValueRef::RowId(RowIdRef::Source(parent_source.clone()))] }` and use that node as the right side of the inherited-parent authorization join in `crates/jazz/src/node/query_eval.rs::normalize_inherited_parent_policy`.

Compatibility check:

| Check                                                                        | Result                                                                                                                                                                                                                                                 |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Can the Distinct key be expressed as the requested parent row id?            | Yes. `RowSetExpr::Distinct` accepts `Vec<NormalizedValueRef>`, and `NormalizedValueRef::RowId(RowIdRef::Source(parent_source.clone()))` is the same value used by the join predicate.                                                                  |
| Does the inherited-parent join reference parent fields downstream?           | No evidence of extra parent payload dependence in this lowering site. The join predicate compares `child.parent_column` to `parent_source` row id, and the result identity remains the child/current row.                                              |
| Does one-shot compile through the same normalized plan?                      | Yes. `compile_current_query_program_for_one_shot_read` builds the same `current_query_program_request` before applying one-shot access paths, so adding this node would affect maintained and one-shot together.                                       |
| Does query-engine lowering support `RowSetExpr::Distinct` in this placement? | No. The join right side goes through `analyze_relation_input_node` -> `analyze_linear_subplan` -> `analyze_current_node`; `RowSetExpr::Distinct { .. }` currently returns `UnsupportedReason::Operator("distinct row-set nodes are not lowered yet")`. |

Per the stop rule, I did not add the node and did not broaden the change into Distinct lowering support. That would no longer be the requested minimal one-lowering-site change.

Before-bench receipt captured before the stop:

| Scenario           |                                         Rows | Target open_ms |      Target open us/row | Whole-workload wall_ms | Notes                                                                                                                                                                                                                                                   |
| ------------------ | -------------------------------------------: | -------------: | ----------------------: | ---------------------: | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Cold before        | 43,000 positioned / 23,831 visible delivered |            614 | 14.3 per positioned row |                 26,889 | `res_l_child_3`, `open_reads=172195`, `open_ranges=18`, `bundle_ms=317`; whole workload `settle_ms=21894`.                                                                                                                                              |
| Warm reopen before |                               23,831 visible |          4,270 |                   179.2 |                 20,517 | From the immediately preceding same-tree Phase 4 target run after instrumentation was reverted; Phase 5 warm receipt confirmed whole-workload `wall_ms=20517`, `settle_ms=15512`, but the raw target trace line was truncated from the captured output. |

`join_apply` was not traceable in the Phase 5 bench because the temporary Groove instrumentation from Phase 4 was reverted. Phase 4 attributed the warm target open to ~3.5s in `JoinState::apply`.

Change classification: CLEARLY-GOOD in principle, but BLOCKED as a one-site implementation until `RowSetExpr::Distinct` is supported by the relevant maintained/one-shot lowering path.

Gates run in Phase 5:

| Gate                                                                                                                          | Exit code |
| ----------------------------------------------------------------------------------------------------------------------------- | --------: |
| `JAZZ_REHYDRATE_TRACE=1 JAZZ_CUSTOMER_PHASES=cold,warm cargo bench -p jazz-sim --bench customer_cold_start -j 2` before bench |         0 |
| `cargo check -p jazz -j 2` stop-state sanity check                                                                            |         0 |

Tooling-friction: a trace sink that preserves the full `res_l_child_3` line separately from Cargo bench output would have avoided relying on the prior same-tree target trace when terminal output truncated.

## Phase 6 results

Implemented a Jazz-side prototype of inherited-parent policy semi-join lowering:

- Added `JoinMode::Semi` to the normalized query-engine `JoinMode`.
- Changed `normalize_inherited_parent_policy` to emit `mode: Semi` only for inherited parent authorization joins.
- Added `LinearStep::Join` lowering for `Semi` using `GraphBuilder::semi_join`.
- Confirmed one-shot reads compile through `compile_current_query_program_for_one_shot_read` into the same lowered Groove program path; there is no separate current-value join interpreter for this path.
- Added a focused inherited-policy regression in `crates/jazz/src/node/tests/policies_rls.rs`.

Important semantic blocker found by the full Jazz suite: the current Semi route-field plumbing is not acceptable. In order to keep prepared-shape route fields present after a left-only semi-join, I projected right-side claim route fields as literals. That makes cached prepared graphs identity-sensitive without the cache key knowing it. `cargo test -p jazz -j 2` then failed `db::tests::inherited_child_policy_allows_two_and_three_level_chains_per_identity`: the second identity saw both its own inherited child and the first identity's inherited child. This is a real semantic regression, so this prototype needs another design pass before it can be considered shippable.

The right fix likely needs Semi to preserve bindable route fields without baking identity claims into the graph, or to mark this lowered shape as identity-sensitive for prepared-plan caching. The latter is SPECULATIVE and has cache-hit/perf implications; the former is CLEARLY-GOOD if it can be represented with existing Groove graph primitives.

Before/after receipts:

| Scenario                         |                                Before |       After prototype | Notes                                                                                      |
| -------------------------------- | ------------------------------------: | --------------------: | ------------------------------------------------------------------------------------------ |
| Cold `res_l_child_3` open_ms     |                                   614 |                   678 | target trace from tee log                                                                  |
| Cold whole-workload wall_ms      |                                26,889 |                13,461 | after receipt: `settle_ms=12296`, `dominant_child_materialized_ms=12765`                   |
| Warm `res_l_child_3` open_ms     |                                 4,270 |                   404 | after warm trace: `reset=false`, `known_state=Fast`, `open_reads=172195`, `open_ranges=18` |
| Warm `res_l_child_3` open us/row |                                 179.2 |                 16.95 | 23,831 visible child rows                                                                  |
| Warm whole-workload wall_ms      |                                20,517 |                 7,229 | after receipt: `settle_ms=5508`, `dominant_child_materialized_ms=5960`                     |
| `join_apply`                     | ~3.5s before, Phase 4 instrumentation | not traced in Phase 6 | Groove instrumentation was not reintroduced                                                |

Gate results:

| Gate                                                                                                                                            | Exit code |
| ----------------------------------------------------------------------------------------------------------------------------------------------- | --------: |
| `cargo check -p jazz -j 2`                                                                                                                      |         0 |
| focused `cargo test -p jazz inherited_parent_policy_semijoin_preserves_visibility_across_duplicate_derivations -j 2`                            |         0 |
| `cargo fmt -p jazz --check`                                                                                                                     |         0 |
| `cargo test -p jazz --test warm_reopen_differential reopen_from_rebuild_and_persisted_placeholder_are_incrementally_equivalent -j 2 -- --exact` |         0 |
| `cargo test -p jazz --test shared_coverage_differential forced_shared_coverage_group_matches_per_subscription_observations -j 2 -- --exact`     |         0 |
| `cargo check -p jazz-sim --benches -j 2`                                                                                                        |         0 |
| `cargo check -p jazz -p jazz-sim --lib --tests --examples -j 2`                                                                                 |         0 |
| after bench `JAZZ_REHYDRATE_TRACE=1 JAZZ_CUSTOMER_PHASES=cold,warm cargo bench -p jazz-sim --bench customer_cold_start -j 2`                    |         0 |
| `cargo test -p jazz -j 2`                                                                                                                       |       101 |

Failed Jazz tests:

- `db::tests::inherited_child_policy_allows_two_and_three_level_chains_per_identity`: semantic leak across identities from cached route-literal Semi graph.
- `node::tests::harness::inherited_parent_policy_semijoin_preserves_visibility_across_duplicate_derivations`: the focused test's delta assertion is unstable under full-suite execution and should be rewritten against a lower-level maintained-view differential harness before being treated as an acceptance canary.

Not run after the semantic failure: `cargo test -p groove -j 2`, `JAZZ_SEED_COUNT=300 cargo test -p jazz m3_maintained_one_shot_differential_oracle -j 2`, `cargo test -p jazz --test incremental_delivery_canary -j 2`, `cargo test -p jazz-tools --features test -j 2`, and `cargo test -p jazz-server -j 2`.

Tooling-friction: a small maintained-view delta harness that applies returned updates as a real client would would have avoided using `PeerState::query_update` as an ambiguous protocol-delta oracle.

## Phase 7 results (final design, all gates green)

The Phase 6 blocker is resolved. Final design (`crates/jazz/SPEC/14_lowering_to_groove.md` §14.7):
`JoinMode::Semi` marks inherited-parent policy joins; the lowering projects the
parent-policy subtree to (join keys + route fields), collapses derivations with
`arg_max_by` grouped on those fields, then plain-inner-joins the reduced side.
Claims stay runtime-bound (multisink identity routing preserved); the parent
set stays dynamic (maintained deltas across permission changes preserved);
`last_join_right` carries the reduced field set so downstream Project steps
resolve prefixed fields correctly (this was the `row_uuid` failure).

Corrections to Phase 6 artifacts: the codex prototype's route-literal
projection is gone; its new policy test wrongly asserted no retraction on
last-edge revoke (asserted maintained/one-shot divergence) — now asserts the
retraction and the re-grant add.

| Metric                              |                            Before |                                   After |
| ----------------------------------- | --------------------------------: | --------------------------------------: |
| Warm reopen `res_l_child_3` open_ms | 4,270 (190,846 reads / 60 ranges) | 405 (172,195 range-visited / 18 ranges) |
| Warm reopen us/visible row          |                             179.2 |                                    17.0 |
| Warm whole-workload wall_ms         |                            20,517 |                                   7,155 |
| Cold whole-workload wall_ms         |                            26,889 |                                  12,427 |

Gates (all exit 0): jazz suite, groove suite, JAZZ_SEED_COUNT=300 oracle,
3 incremental-delivery canaries, warm-reopen differential (exact),
shared-coverage differential (exact), jazz-tools --features test, jazz-server,
cargo check jazz-sim --benches, cargo check jazz+jazz-sim lib/tests/examples,
cargo fmt jazz+groove. crates/groove source untouched.

## Phase 8 results (client wasm attribution)

Final real-app receipt directory: `/Users/anselm/app-harness-v2/browser-profile-20260721T093651Z`.
This was captured from the served production the customer app app after temporarily swapping in a profiling `jazz-wasm` build from this worktree. The served `jazz_wasm_bg.wasm` and `jazz_wasm.js` were restored afterward; md5s matched the pre-swap backups:

| Artifact            | Restored md5                       |
| ------------------- | ---------------------------------- |
| `jazz_wasm_bg.wasm` | `3673f5012c18891661b2799cfb8dad96` |
| `jazz_wasm.js`      | `79d3b1f69a9770e01777ecc36bc8cf1a` |

Instrumentation note: the first pass emitted per-node operator spans and made the harness fail with `RangeError: Invalid string length`. The final pass emitted cumulative Groove operator buckets with exclusive operator timing, plus named maintained-open stage spans.

Warm dominant subscription: `dropdown_entry`, 24,081 delivered rows.

| Stage                                                 |       ms | us/row | Share of 5,437.9ms worker `createExecutedSubscription` |
| ----------------------------------------------------- | -------: | -----: | -----------------------------------------------------: |
| compile                                               |      0.4 |   0.02 |                                                   0.0% |
| Groove subscribe/hydrate                              |    517.6 |   21.5 |                                                   9.5% |
| snapshot recv                                         |      0.0 |    0.0 |                                                   0.0% |
| wasm maintained-view apply                            |    383.6 |   15.9 |                                                   7.1% |
| wasm-core named open total                            |    901.6 |   37.4 |                                                  16.6% |
| TS/browser `subscription_apply_chunk` for 24,045 rows |  2,085.8 |   86.7 |                                                  38.4% |
| Remaining worker request time outside these spans     | ~2,450.5 | ~101.8 |                                                  45.1% |

Warm `dropdown_entry` Groove subscribe/hydrate exclusive operator buckets:

| Operator          | Calls |    ms | rows_in | rows_out | us/row out |
| ----------------- | ----: | ----: | ------: | -------: | ---------: |
| `map_project`     |    25 | 263.7 | 336,810 |  336,810 |        0.8 |
| `join_apply`      |     1 | 122.3 |  48,090 |   24,045 |        5.1 |
| `semi_join_apply` |     3 |  27.4 |  72,243 |   24,117 |        1.1 |
| `unwrap_nullable` |     1 |  23.5 |  24,045 |   24,045 |        1.0 |
| `anti_join_apply` |     2 |  20.6 |  48,090 |   48,090 |        0.4 |
| `table_source`    |     4 |   0.5 |       0 |   24,081 |       ~0.0 |

Warm `dropdown_entry` raw snapshot table hydration is not the owner: 10 `snapshot_table_deltas` calls, 96,288 rows emitted, 51.0ms total. OPFS read time for the whole warm run was only 27.3ms. The warm retained bottleneck is therefore not browser IO; it is mostly client-side materialization outside Groove, with a smaller Groove join/project hydrate component.

Policy operators after the Phase 7 derivation-collapse fix are present but no longer dominant: `join_apply` + `semi_join_apply` + `anti_join_apply` total 170.3ms of the 5,437.9ms dominant worker request (3.1%) and 170.3ms of the 901.6ms wasm-core open (18.9%). No `arg_max_by` bucket appeared in the `dropdown_entry` warm open; the lowered semi-join reduction does not show up as a warm client hot bucket in this capture.

Cold big-tick exclusive operator split, all tables:

| Operator          |   Calls |    ms | rows_in | rows_out | us/row out |
| ----------------- | ------: | ----: | ------: | -------: | ---------: |
| `table_source`    | 620,886 | 705.5 |       0 |   78,473 |        9.0 |
| `map_project`     | 488,592 | 366.0 | 369,835 |  369,835 |        1.0 |
| `persist`         | 879,564 | 341.6 | 105,958 |  105,958 |        3.2 |
| `index_by`        | 879,564 | 166.3 | 105,904 |  105,958 |        1.6 |
| `join_apply`      |  16,356 | 159.9 |  52,226 |   26,113 |        6.1 |
| `semi_join_apply` |  67,860 |  84.5 |  83,914 |   26,982 |        3.1 |
| `anti_join_apply` |  32,712 |  60.1 |  52,226 |   52,226 |        1.2 |
| `unwrap_nullable` |  51,156 |  36.2 |  27,541 |   27,527 |        1.3 |

Cold whole-run timing: wall 22,117ms, Jazz path 15,250ms, wire ingest ticks 7,432.1ms, subscription apply chunks 6,924.0ms. The largest tick was 4,337.5ms; the exclusive Groove buckets above explain only about 1.9s of operator body time across all ticks, so the remaining cold tick time is outside the measured operator apply bodies: runtime traversal/memo/arrangement state management, record movement/clone/drop, and persistence/index write plumbing around the operators. OPFS write time was only 30.0ms, so this is CPU/object-work in wasm, not storage latency.

The 24,045-row apply chunk is TS-side dominated. In cold, the 24,045-row `subscription_apply_chunk` took 2,010.3ms. In warm, the same chunk took 2,085.8ms. The wasm-core maintained-view apply inside `open_seeded_maintained_subscription_view` for `dropdown_entry` was 383.6ms, so the chunk wrapper/adapter/materialization work outside that core apply accounts for roughly 1.7s of the dominant row apply in warm.

Ranked conclusion:

1. CLEARLY-GOOD: optimize TS/browser subscription materialization for large same-table chunks. Measured share: ~2.09s of warm 5.44s dominant create call, and 2.01s of cold apply for the 24,045-row chunk. Owning span/function surface: `subscription_apply_chunk` around `createExecutedSubscription` result delivery, outside `open_seeded_maintained_subscription_view`'s wasm-core apply. Estimated ceiling: ~1.5-2.0s warm and cold for this workload if per-row JS object/adaptor work is reduced or chunked.
2. CLEARLY-GOOD: reduce wasm cold tick runtime overhead around table-source hydration and persist/index maintenance. Measured share: cold `server_pump_tick` total 7.43s, with exclusive operator bodies led by `table_source` 705.5ms, `persist` 341.6ms, `map_project` 366.0ms, `index_by` 166.3ms; the unmeasured remainder is runtime traversal/memo/state-management around those bodies. Estimated ceiling: several seconds cold, especially the 4.34s largest tick.
3. SPECULATIVE: further policy-join tuning client-side. Measured share after Phase 7 is small: warm `dropdown_entry` policy-ish join buckets are 170.3ms of a 5.44s worker call; cold join/semi/anti buckets total 304.5ms across all ticks. Ceiling is likely sub-500ms unless a hidden non-operator policy cost is found.
4. SPECULATIVE: raw OPFS/read batching for warm reopen. Measured warm OPFS read time is 27.3ms and `snapshot_table_deltas` for `dropdown_entry` is 51.0ms total, so storage latency is ruled out for the browser warm path. Ceiling is low for this profile.

Gate/build/capture results:

| Step                                                               |                                                           Exit code |
| ------------------------------------------------------------------ | ------------------------------------------------------------------: |
| `cargo check -p jazz -j 2` after initial instrumentation           |                                                                   0 |
| `cargo check -p groove -j 2` after initial instrumentation         |                                                                   0 |
| first `wasm-pack build --target web --profiling`                   |                                                                   0 |
| first capture with per-node spans                                  |                             1 (`RangeError: Invalid string length`) |
| reduced cumulative-span `wasm-pack build --target web --profiling` |                                                                   0 |
| cumulative-span capture                                            | 0 (`/Users/anselm/app-harness-v2/browser-profile-20260721T093200Z`) |
| exclusive-timing `cargo check -p jazz -j 2`                        |                                                                   0 |
| exclusive-timing `wasm-pack build --target web --profiling`        |                                                                   0 |
| final exclusive-timing capture                                     | 0 (`/Users/anselm/app-harness-v2/browser-profile-20260721T093651Z`) |
| artifact restore md5 verification                                  |                                                                   0 |
| restored-stack `scripts/demo-stack.sh --skip-build --prod`         |                                                                   0 |
| restored-source `cargo check -p jazz -j 2`                         |                                                                   0 |

Tooling-friction: a worker-span side channel that writes bounded binary/NDJSON receipts directly, instead of serializing all console messages into one JSON result, would have allowed finer per-operator spans without perturbing or overflowing the harness.

## Consolidation spin

Mechanism: `jazz::db::Db::tick_stats` calls `Node::post_tick_consolidate_history_windows`, which calls `groove::db::Database::consolidate_history_windows` with `POST_TICK_HISTORY_WINDOW_BUDGET = 4`. That walks every schema table/direct store whose physical name is a windowed history table and calls `RecordStore::consolidate_full_windows_bounded`.

Convergence contract: full-window maintenance uses `consolidate_windows_bounded_inner(..., consolidate_tail = false)`, so it scans `OrderedKvStorage::range(column_family, window_consolidation_cursor(), WINDOW_MARKER_KEY)`. Plain records are accumulated into runs of `TARGET_RECORDS_PER_WINDOW`; each appended window rewrites the plain records into one codec window, deletes the original plain keys, and writes `WINDOW_MARKER_KEY` if this is the first window. For cursor-mode history maintenance, it writes `WINDOW_CURSOR_KEY` only when `consolidated.records > 0`, using the last window key or last plain key observed before the budget stopped. A later call resumes from that cursor. Encountering already encoded windows flushes the current plain run and then moves the in-memory cursor candidate to that window key; however, no durable cursor is written when no new records were consolidated.

Rescheduling owner: the production `jazz-tools` websocket path calls `ServerShellHandle::tick_take`, which calls `InMemoryServerShell::tick`; if outbound frames are produced, `drain_ws_outbound` calls `notify_activity`, causing another activity-driven tick. The plain server shell owner thread itself blocks on its job queue; there is no autonomous idle tick interval in `crates/jazz-tools/src/server/core_server_shell.rs`.

Verdict from this lane: strict headless repro was negative on the copied store. I copied `/Users/anselm/app-harness-v2/store-snapshots/customer-app-2026-07-21T114247764Z` to `/tmp/jazz-consolidation-spin.gdnQHW/store`, built `/Users/anselm/jazz_core-perf-dropdown/target/release/jazz-tools` with `--features cli`, and started PID `18513` on port `6299` with app id `019dcd19-a699-7191-b0bc-b8ce08eb7cd6`. With zero clients and no websocket/session activity, sampled CPU stayed at `0.0%` for 60s at elapsed times `01:45`, `01:55`, `02:05`, `02:15`, `02:25`, and `02:35`. Because no idle tick ran, temporary env-gated consolidation logging did not produce cursor evidence for loop-vs-backlog in this strict headless mode.

Failure hypotheses after source read:

1. CLEARLY-GOOD: stop rescheduling purely because a tick succeeded. If an activity loop calls `notify_activity` after every non-empty outbound drain, consolidation work can ride that loop at a 100% duty cycle while bounded maintenance still has backlog.
2. CLEARLY-GOOD: add progress telemetry/assertions around cursor-mode consolidation: log/table-counter start cursor, end cursor, visited rows, consolidated windows/records, and `advanced`; treat `records == 0 && visited > 0 && cursor unchanged` as a no-progress condition with backoff.
3. CLEARLY-GOOD: persist skip-when-converged bookkeeping per history store/table, or avoid scanning a store again until a write dirties it. Today `consolidate_history_windows` starts from the first windowed history table on every call and relies on each `RecordStore` cursor scan to discover no work.
4. SPECULATIVE: increase the per-tick consolidation budget. If the observed spin is legitimate backlog, the current budget of 4 windows per tick makes a large imported history drain through many tiny range scans and write batches.
5. SPECULATIVE: fix cursor advancement if instrumentation on an activity-driven repro shows the same table/cursor repeating with `records > 0` or `visited > 0`. Source inspection did not prove that bug in the strict idle run.

## Consolidation spin (armed repro + fix)

Armed repro result in this worktree: the exact persistent 100% post-disconnect burn did not reproduce with the public `jazz-tools` websocket route, but the cursor bug that can make a periodic tick driver rescan completed windows was reproduced from source and pinned with a Groove regression. Scratch PIDs used: `29161`, `41169`, and `46064` on port `6299`; all used copied stores under `/tmp/jazz-armed-*`, never the original snapshot.

Mechanism found: cursor-mode consolidation used an inclusive lower bound: `range(cf, window_consolidation_cursor(), WINDOW_MARKER_KEY)`. The code persisted the last seen key itself, including encoded window keys, so a later tick could start by seeing the same encoded window again. The budget-full path also updated the in-memory cursor after the budget check, so budgeted calls could persist a stale cursor. In a driver that keeps calling `InMemoryServerShell::tick` (for example a periodic websocket/loopback driver), this turns convergence into repeated no-progress scans of already encoded windows.

Fix: persist an exclusive lower-bound cursor by storing `last_seen_key + 0x00`, and update the in-memory cursor before budget-break checks. This keeps completed prefixes from being revisited without changing the window codec or the produced consolidated window bytes.

Evidence:

| Run                                        | CPU trace                               | Result                                                                                    |
| ------------------------------------------ | --------------------------------------- | ----------------------------------------------------------------------------------------- |
| Pre-fix public armed scratch, PID `29161`  | `12.1%` immediate, then `0.0%` by 5s    | Public route quiesced; store compacted from 60M to 15M                                    |
| Fixed armed scratch, PID `41169`           | `13.2%`, `3.3%`, `0.0%`, `0.0%`, `0.0%` | Quiesced by 5s after arming                                                               |
| Fixed second arming, PID `41169`           | `7.9%`, `2.1%`, `0.0%`, `0.0%`          | Did not restart sustained burn                                                            |
| Fixed write-after-convergence, PID `41169` | `45.0%`, `1.9%`, `0.0%`, `0.0%`         | Three public `spell_check` writes synced and quiesced                                     |
| Fixed diagnostic run, PID `46064`          | `0.0%`, `2.8%`, `0.0%`, `0.0%`, `0.0%`  | Six diagnostic tick reports, all final no-progress: `windows=0 records=0`; driver stopped |

Regression: `full_window_consolidation_cursor_advances_past_encoded_windows` covers budgeted cursor-mode consolidation. It asserts that after several bounded passes, the durable cursor sits past all already encoded windows and a no-work call does not mutate or restart scanning.

Gate results:

| Gate                                                                                                                                    | Result |
| --------------------------------------------------------------------------------------------------------------------------------------- | ------ |
| `cargo fmt --check -p groove -p jazz`                                                                                                   | pass   |
| `cargo test -p groove -j 2`                                                                                                             | pass   |
| `cargo test -p jazz -j 2`                                                                                                               | pass   |
| `JAZZ_SEED_COUNT=100 cargo test -p jazz m3_maintained_one_shot_differential_oracle -j 2`                                                | pass   |
| `cargo test -p jazz --test incremental_delivery_canary maintained_relation_include_single_row_changes_are_scale_independent -- --exact` | pass   |
| `cargo test -p jazz --test incremental_delivery_canary reset_batch_post_reset_single_row_changes_are_scale_independent -- --exact`      | pass   |
| `cargo test -p jazz --test incremental_delivery_canary mergeable_transaction_write_cost_is_scale_independent -- --exact`                | pass   |
| `cargo check -p jazz-sim --benches -j 2`                                                                                                | pass   |

## Consolidation treadmill (partial-tail convergence)

Mechanism: the live production burn was a two-part treadmill. The store has about 26k imported history records spread across about 200 windowed history tables, so most stores are permanent partial tails below `TARGET_RECORDS_PER_WINDOW = 256`. `consolidate_full_windows_bounded` correctly refuses to encode those tails, and the cursor correctly cannot advance past them. Without per-store convergence state, each maintenance pass rescanned each table tail and produced `windows=0 records=0`.

The persistent-client repro also exposed a separate scheduling self-rearm: `ServerShellHandle::tick_take` unconditionally called `notify_activity` after every successful tick. A websocket listener woken by activity would call `tick_take`, which would notify activity again even with no inbound frames and no outbound frames. With the partial-tail scan bug, that made one persistent backend websocket enough to drive `InMemoryServerShell::tick` at a 100% duty cycle forever.

Fix: `groove::db::Database` now keeps an in-memory set of converged windowed history stores. A store is marked converged when bounded full-window consolidation returns zero windows. Later history maintenance skips that store, and `consolidate_history_windows` returns immediately once all windowed history stores are converged. Successful writes to a windowed history table clear only that store's convergence mark. The websocket route no longer re-broadcasts activity after merely sending outbound frames, and `ServerShellHandle::tick_take` no longer re-arms activity after a no-input tick.

Evidence:

| Run                                                | Trace                                                                                                                     | Result                                                                      |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| Pre-fix persistent-client scratch, PID `67566`     | `98.0%`, `98.4%`, `98.2%`, `99.3%`, `97.4%`, `98.1%`, `99.5%`; CPU time `0:21.02` to `3:16.56` over 180s                  | Reproduced sustained burn with one idle persistent websocket                |
| First convergence-only attempt, PID `78807`        | `71.1%`, `46.5%`, `117.4%`, `114.1%`, `110.8%`, `118.1%`, `116.1%`                                                        | Convergence state alone removed scans but exposed websocket tick self-rearm |
| Temporary diagnostic run, PID `79777`              | First call: `windows=0 records=0 scanned=215 skipped=0 marked=215 converged=215`; then repeated `early_out all_converged` | Store convergence worked; remaining CPU was tick scheduling                 |
| Final fixed persistent-client scratch, PID `80380` | `0.0%` at every sample over 180s; CPU time flat at `0:01.45`                                                              | Persistent idle websocket stayed connected without burn                     |
| Final fixed write-past-256 check, PID `80380`      | After 260 `spell_check` writes: `0.0%`, `0.0%`, `0.0%`, `0.0%`, `0.0%`; CPU time `0:04.09` to `0:04.10` over 60s          | Writes dirtied history, work ran, and server reconverged                    |

Regression: `partial_history_tail_is_marked_converged_until_dirtied` uses the public `Database` batch API over a real `jazz_docs_history` table and a scan-counting memory storage wrapper. It asserts that a below-window partial tail is scanned once, skipped on the next maintenance call, dirtied by a later write, consolidated after reaching the test window target, and then skipped again after reconvergence.

Gate results:

| Gate                                                                                                                                    | Result |
| --------------------------------------------------------------------------------------------------------------------------------------- | ------ |
| `cargo test -p groove -j 2`                                                                                                             | pass   |
| `cargo test -p jazz -j 2`                                                                                                               | pass   |
| `JAZZ_SEED_COUNT=100 cargo test -p jazz m3_maintained_one_shot_differential_oracle -j 2`                                                | pass   |
| `cargo test -p jazz --test incremental_delivery_canary maintained_relation_include_single_row_changes_are_scale_independent -- --exact` | pass   |
| `cargo test -p jazz --test incremental_delivery_canary reset_batch_post_reset_single_row_changes_are_scale_independent -- --exact`      | pass   |
| `cargo test -p jazz --test incremental_delivery_canary mergeable_transaction_write_cost_is_scale_independent -- --exact`                | pass   |
| `cargo fmt --check -p groove -p jazz`                                                                                                   | pass   |
| `cargo check -p jazz-sim --benches -j 2`                                                                                                | pass   |
