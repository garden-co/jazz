# Scopes

```
 ┌──────────────────────────────────────────────────────────┐
 │ Phase A: Foundations (no behavior change)               │
 │ - PhysicalPlan IR, AccessPath, OpCapabilities           │
 │ - PlannerCatalog trait                                   │
 │ - Trivial lowering reproduces today's plan exactly      │
 └────────────────────────┬─────────────────────────────────┘
                          │
                          ▼
 ┌──────────────────────────────────────────────────────────┐
 │ Phase B: Storage extensions (no behavior change)        │
 │ - Ordered index scan, cardinality, distinct count       │
 │ - All storage backends                                   │
 │ - Planner does not yet consume them                     │
 └────────────────────────┬─────────────────────────────────┘
                          │
                          ▼
 ┌──────────────────────────────────────────────────────────┐
 │ Phase C: Cost & single-table DP                         │
 │ - Cost model, cardinality propagation, DP               │
 │ - New ops: OrderedIndexScan, TopKHeap, KWayMerge,       │
 │   StreamingStop                                          │
 │ - Streaming pull protocol                                │
 │ - Original top-k pitch optimizations land here          │
 └────────────────────────┬─────────────────────────────────┘
                          │
                          ▼
 ┌──────────────────────────────────────────────────────────┐
 │ Phase D: Multi-table DP                                 │
 │ - Join enumeration over table subsets                   │
 │ - New ops: IndexNestedLoop                               │
 │ - Order-preserving NLJ enables top-k across joins       │
 └────────────────────────┬─────────────────────────────────┘
                          │
                          ▼
 ┌──────────────────────────────────────────────────────────┐
 │ Phase E: Cleanup                                        │
 │ - Remove ExecutionQueryPlan, QueryEnvelope              │
 │ - Remove unwrap_query_envelope                          │
 │ - Delete legacy lowering path                           │
 └──────────────────────────────────────────────────────────┘

 Existing correctness tests must keep passing as each phase lands.
 Plan shape and cost-regression tests are added within each phase.
```

## Phase A: Foundations (no behavior change)

**No dependencies — start here.**

The goal is to introduce the planner module and IR without changing any execution behavior. Lowering is gated: the planner produces the same graph node DAG that today's lowering produces. All existing tests pass byte-identically.

- [ ] Create `crates/jazz-tools/src/query_manager/planner/` with the module layout from `2_pitch.md`.
- [ ] Define `PhysicalPlan`, `PhysicalOp` (initial variants matching today's operators only), `AccessPath`, `OpCapabilities`.
- [ ] Define `PlannerCatalog` trait with index introspection (no stats methods yet — those land in Phase B).
- [ ] Implement `PlannerCatalog` backed by the existing schema/index registry.
- [ ] Implement `plan(RelExpr, mode, demanded_order) -> PhysicalPlan` as a deterministic lowering: for each `RelExpr` node, produce the corresponding `PhysicalOp` with a single fixed algorithm choice. No enumeration, no cost.
- [ ] Implement Phase 3 lowering: `PhysicalPlan -> ExecutionGraph` reusing existing graph nodes.
- [ ] Wire the planner behind a feature flag or runtime toggle so the legacy path remains the default.
- [ ] Snapshot test: for a curated set of queries (≥ 20 covering single-table, multi-table, union, recursive, multi-branch), the graph produced by the planner-backed path is structurally equivalent to the legacy path. Use an existing graph-shape assertion or build one if absent.
- [ ] All `crates/jazz-tools/tests/*` continue to pass with the planner enabled.
- [ ] Implement `Display` for `PhysicalPlan` (basic tree rendering, no cost annotations yet).
- [ ] Plan shape snapshot tests for the same curated query set (≥ 20).
- [ ] `OpCapabilities` declared on every initial operator. Capability-composition unit test: chain validation rejects mixed-mode chains.
- [ ] No new graph node types introduced in this phase.

**Exit criterion**: planner toggle enabled in CI; full test suite passes with both legacy and planner-backed lowering producing identical execution.

## Phase B: Storage extensions (no behavior change)

**Depends on: nothing planner-side. Can run in parallel with Phase A.**

Extend the `Storage` trait with the methods the planner will consume in Phase C. Do not yet plumb them into the planner.

- [ ] Add `index_scan_ordered(table, column, branch, start, end, direction, take, skip, skip_past) -> Vec<(Value, ObjectId)>` to the `Storage` trait.
- [ ] Add `index_count(table, column, branch, value) -> usize`.
- [ ] Add `index_count_all(table, column, branch) -> usize`.
- [ ] Add `index_distinct_count(table, column, branch) -> usize`.
- [ ] Add `index_range_count(table, column, branch, range) -> usize`.
- [ ] Implement all methods on `MemoryStorage`. Verify O(1) cost on `index_count` (hash lookup + set size); maintain a running counter for `index_count_all` if needed.
- [ ] Implement on the SQLite-backed storage.
- [ ] Implement on the RocksDB-backed storage (or whichever durable native KV is current).
- [ ] Implement on the `opfs-btree` WASM storage.
- [ ] Verify `encode_value` lexicographic ordering matches value ordering across all backends; add property-based tests if absent.
- [ ] IEEE 754 edge case tests: ±0.0 and NaN under `index_scan_ordered`.
- [ ] Cursor (`skip_past`) semantics tests: resume after a deleted cursor row produces the next-greater (or next-lesser, by direction) entry; ties on the order column resolved by `ObjectId`.

**Exit criterion**: all storage backends expose the new trait methods, with parity tests confirming identical results across backends for a curated workload.

## Phase C: Cost & single-table DP

**Depends on: Phase A (planner module exists), Phase B (storage stats available).**

Activate the cost-based DP for single-table plans. The original top-k pitch's optimizations land here as natural consequences of the plan space.

- [ ] Implement cardinality propagation per `2_pitch.md` cost model section.
- [ ] Implement cost function: `α * input_card + β * output_card` per operator, with PolicyFilter's per-row constant.
- [ ] Tune α and β constants from microbenchmarks for each operator.
- [ ] Implement the System-R DP for single-table plans: enumerate access paths, memoize by `(table_set, output_order, mode)`, keep cheapest.
- [ ] Implement Phase 1 normalization rules: predicate pushdown, sort elimination, sargability classification.
- [ ] Add `OrderedIndexScan` graph node under `query_manager/graph_nodes/`. Maintainability: re-pull from index on dependent change.
- [ ] Add `TopKHeap` graph node. Maintenance: bounded sorted buffer; insert/evict on delta.
- [ ] Add `KWayMerge` graph node. Composes per-branch ordered streams into one ordered stream.
- [ ] Add `StreamingStop` semantics on `LimitOffsetNode` (or new `StreamingLimitOffsetNode`).
- [ ] Define and document the streaming pull protocol: how a node signals "no more rows", backpressure, async-buffering semantics.
- [ ] Implement `can_stream` on `MaterializeNode`, `FilterNode`, `PolicyFilterNode`, `LimitOffsetNode`. Each operator gains a pull-mode entry point alongside its existing batch path.
- [ ] Multi-column ORDER BY: prefix-scan + group-sort algorithm on the first ORDER BY column when it has an index.
- [ ] Cursor pagination: thread `skip_past` from access path through to the storage call. Expose at the subscription API edge (separate task; the planner only needs to carry the cursor through the plan).
- [ ] Plan shape snapshots for: basic top-k with index, basic top-k without index (full sort), filtered top-k (both order-driven and filter-driven choices), multi-column ORDER BY (high- and low-cardinality first column), multi-branch top-k, cursor resume.
- [ ] Cost-regression tests: assert plan family (e.g., "uses ordered-index path") for each canonical workload.
- [ ] Execution correctness tests: results identical to today for all the above query shapes, including incremental subscription deltas.
- [ ] Benchmark suite: top-k with k ∈ {1, 10, 100} on tables of size {1k, 10k, 100k, 1M}. Assert O(k log n) or better on the ordered-index path.
- [ ] Pagination benchmarks: page 1 vs page 50 latency. Assert O(k) per page on the cursor path.
- [ ] Selectivity threshold: empirically determine the order-driven vs filter-driven crossover; encode in cost constants. Add a runtime fallback that abandons order-driven after N × limit unsuccessful pulls.

**Exit criterion**: the original `ordered-index-topk-query-path` pitch is fully delivered, as cost-driven plan choices in this architecture, with no special-case code paths.

## Phase D: Multi-table DP

**Depends on: Phase C (single-table DP works end-to-end).**

Extend the DP to multi-table plans with join algorithm choice. Order-preserving nested-loop joins enable top-k across joins.

- [ ] Extend the DP to enumerate join orders bottom-up over table subsets (left-deep only).
- [ ] Add `IndexNestedLoop` graph node. Driver chosen to satisfy demanded order; probe via existing index lookup. Maintenance: re-probe affected rows on either side's delta.
- [ ] Cost function for joins: `(|L| * |R|) / max(distinct_L, distinct_R)` for cardinality; per-algorithm cost constants.
- [ ] Plan shape snapshots: two-table join with order on each side, LEFT JOIN with ordering on each side, three-table join.
- [ ] Cost-regression tests: assert join algorithm choice is correct for canonical workloads.
- [ ] Multi-branch + JOIN composition test: per-branch k-way merge feeds into NLJ driver.
- [ ] Execution correctness tests: results identical to today for all join shapes, including multi-branch and incremental subscription deltas.
- [ ] Join cost benchmarks: HashJoin vs IndexNestedLoop crossover on selective vs unselective joins.

**Exit criterion**: ordered top-k across joins works as a cost-driven plan choice. Plan space matches the design from `2_pitch.md` for v1.

## Phase E: Cleanup

**Depends on: Phases A through D landed and stable in production for a release cycle.**

Remove the legacy lowering path. The planner is now the only lowering.

- [ ] Delete `ExecutionQueryPlan` and `QueryEnvelope`.
- [ ] Delete `unwrap_query_envelope`.
- [ ] Delete the legacy lowering path in `graph/mod.rs`.
- [ ] Remove the planner toggle / feature flag.
- [ ] Update all tests that depended on legacy types.
- [ ] Remove the byte-identical-to-legacy snapshot tests from Phase A — they are no longer meaningful.
- [ ] Document the planner module in the in-repo architecture docs (`specs/`).

**Exit criterion**: no references to legacy lowering remain. Planner is the sole path. CI green.

## Out of scope (deferred to follow-up projects)

- Bushy join trees (v1.5).
- Sort-merge join (v1.5).
- Histograms or approximate stats (no-go in v1; revisit only if exact counts prove insufficient for real workloads).
- Replan on stats drift (v2).
- User-facing EXPLAIN API (no-go in v1).
- Aggregation, GROUP BY, window functions (separate project).
- Composite indexes (separate project).
- Subquery flattening / decorrelation (separate project).
- Cascades-style top-down optimizer (revisit only if join planning grows past ~10 tables in practice).
