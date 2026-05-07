# Pitch: Cost-based Query Planner

## Problem

Every query has exactly one execution shape today. Lowering from `RelExpr` to the graph node DAG is mechanical — no choice, no alternatives, no cost. Every optimization we want to add (top-k fusion, ordered-index sort elimination, order-driven vs filter-driven scan choice, predicate pushdown into joins, join algorithm choice) needs a place to make a decision, and there is no such place. See `1_problem.md` for the full audit.

This pitch lays down that foundation: a planner with a real plan space, an access path abstraction, a cost model, and a System-R bottom-up DP. The optimizations described in `todo/projects/ordered-index-topk-query-path/2_pitch.md` then fall out of this architecture as concrete rules and access paths, not as special cases.

## Solution

### Architecture

Three phases between `RelExpr` (logical, exists today) and the graph node DAG (executable, exists today):

```
RelExpr (logical, exists today, annotated in place with sargability flags)
    │
    │ Phase 1 — NORMALIZE (logical → logical, deterministic, fixed-point)
    │   - predicate pushdown
    │   - sort elimination (Sort(Sort(x)) -> Sort(x))
    │   - sargability classification on predicates
    ▼
LogicalPlan (canonical RelExpr — same type, no parallel IR)
    │
    │ Phase 2 — ENUMERATE & COST (System-R bottom-up DP)
    │   for each subset of tables in dependency order:
    │     for each access path on the subset:
    │       for each (output_order, mode):
    │         keep cheapest plan
    ▼
PhysicalPlan (best plan — new IR, lives in planner module)
    │
    │ Phase 3 — LOWER (mechanical translation)
    │   PhysicalOp tree → existing graph_nodes/* DAG
    │   (plus new node types where the plan space introduces them)
    ▼
ExecutionGraph (existing infrastructure, unchanged contract)
```

Phase 1 is deterministic rewrites. Phase 2 is the cost-based DP. Phase 3 is mechanical. Each phase has clear inputs and outputs and is independently testable.

There is no parallel logical IR — `RelExpr` already exists and is canonical-shaped. Sargability is a derived annotation added in place. The "logical plan" concept lives in the contract of what Phase 1 produces, not in a new type.

`PhysicalPlan` is a new IR. It absorbs `ExecutionQueryPlan` and `QueryEnvelope` — `ORDER BY`, `OFFSET`, `LIMIT` become physical operators, not envelope fields. `ExecutionQueryPlan` is removed in Phase E (see `3_scopes.md`).

### Access paths

The atom of the plan space. Each scan in the logical plan produces zero or more access paths during enumeration:

```rust
pub struct AccessPath {
    pub source: AccessPathSource,
    pub predicate: Option<SargablePredicate>,  // folded into the scan
    pub residual_predicate: Option<Predicate>, // applied as PostFilter above
    pub order_provided: Option<SortKeys>,      // what order this path delivers
    pub branch_strategy: BranchStrategy,
    pub cardinality: Cardinality,              // exact count from index_count
}

pub enum AccessPathSource {
    FullScan { table: TableId },
    IndexScan { table: TableId, column: ColumnId, predicate_kind: IndexPredicateKind },
    OrderedIndexScan { table: TableId, column: ColumnId, direction: SortDirection },
}

pub enum IndexPredicateKind {
    Equality,
    Range,
    PrefixIn,  // for IN (a, b, c) — scan multiple ranges
}

pub enum BranchStrategy {
    Single,
    KWayMerge,  // preserves order, fan-in N -> 1
    Union,      // unordered fan-in
}
```

For each table reference, the planner enumerates one access path per (index, direction, sargable predicate) combination plus the full scan as a baseline. Each path declares what order it provides; the DP uses that as a memo key dimension.

### Operators and capabilities

`PhysicalOp` is a tree node. Each variant carries the data needed to lower it; each declares its capabilities:

```rust
pub struct OpCapabilities {
    pub can_run_one_shot: bool,
    pub can_maintain_under_deltas: bool,
    pub can_stream: bool,        // pull-mode input/output
    pub can_batch: bool,
    pub provides_order: Option<SortKeys>,
    pub preserves_order: bool,
}
```

The DP only forms chains where capabilities line up: a streaming chain requires every operator in it to have `can_stream`; a subscription plan requires every operator to have `can_maintain_under_deltas`.

Initial operator catalog:

| Op            | Algorithms                                                             |
| ------------- | ---------------------------------------------------------------------- |
| `Scan`        | `FullScan`, `IndexScan(eq)`, `IndexRangeScan`, `OrderedIndexScan(dir)` |
| `Filter`      | `PostFilter`, `IndexPredicate` (folded into scan)                      |
| `Join`        | `HashJoin`, `IndexNestedLoop(driver, probe)`, `SortMergeJoin` (v1.5)   |
| `Sort`        | `FullSort`, `TopKHeap(k)`, `Eliminated` (when child provides order)    |
| `Limit`       | `Limit`, `LimitOffset`, `StreamingStop(k)`                             |
| `Branches`    | `Single`, `KWayMerge(order)`, `Union`                                  |
| `Policy`      | `PolicyFilter` (black-box constant cost)                               |
| `Materialize` | `Batch`, `Pull`                                                        |
| `Project`     | `Project`                                                              |
| `Union`       | `Union`                                                                |

Each operator implementation lives in `planner/ops/<op>.rs`, with a constructor, capability declaration, cost function, cardinality propagation rule, and lowering shim to existing graph nodes. New graph nodes (`OrderedIndexScan`, `TopKHeap`, `KWayMerge`, `IndexNestedLoop`) are added under `query_manager/graph_nodes/`.

### Cost model

Single `f64` per plan. No vectors, no separate IO/CPU dimensions.

Stats inputs (from `Storage` trait extensions):

```rust
fn index_count(&self, table, column, branch, value) -> usize;
fn index_count_all(&self, table, column, branch) -> usize;
fn index_distinct_count(&self, table, column, branch) -> usize;
fn index_range_count(&self, table, column, branch, range) -> usize;
```

Cardinality propagation:

```
cardinality(scan)        = exact (from index_count / range_count / index_count_all)
cardinality(filter)      = input * estimated_selectivity   // 0.1 default for non-sargable
cardinality(equi-join)   = (|L| * |R|) / max(distinct_L, distinct_R)
cardinality(limit)       = min(input, k)
cardinality(top-k)       = min(input, k)
cardinality(union)       = sum(inputs)         // upper bound
cardinality(branch_merge)= sum(inputs)
cardinality(project)     = input
cardinality(policy)      = input * 1.0         // unknown, treat as no-op for cardinality
```

Cost per operator: `α * input_card + β * output_card`, with α and β tuned per operator from microbenchmarks. PolicyFilter has a flat per-row constant added for the policy-evaluation cost.

This is a hard line. We do not add histograms, sampling, or any approximate stats. Jazz auto-indexes every column, so exact counts are always available; that is sufficient.

### System-R bottom-up DP

Memo key: `(table_set: BitSet, output_order: Option<SortKeys>, mode: PlanMode)`.

`PlanMode = OneShot | Subscription`. `mode` is set by the caller and stays constant through the DP — it filters the candidate operator set, not the plan space dimension.

Enumeration:

```
for each table T in dependency order:
    for each AccessPath P on T:
        for each (output_order, mode) implied by P:
            insert (best plan) into memo[{T}, output_order, mode]

for each pair of subsets (L, R) with L ∪ R covers more tables and L ∩ R = ∅:
    for each (l_plan, r_plan) in memo[L] × memo[R]:
        for each JoinAlgorithm J that can join L and R:
            output_order = J.output_order(l_plan, r_plan)
            insert (best plan) into memo[L ∪ R, output_order, mode]

at the end:
    pick the cheapest plan in memo[all_tables, demanded_order, mode]
```

Tiebreak on equal cost is stable: lexical order on operator names then access path ids. This is for test determinism.

Left-deep only in v1. Bushy is permitted by the framework but disabled by enumeration order. Adding bushy is a small change to the subset enumeration in v1.5.

The DP is restricted to single-table plans in Phase C of the rollout, then extended to multi-table in Phase D.

### Phase 1 normalization rules (v1)

Two rules ship in v1, applied to fixed-point:

1. **Predicate pushdown**: push `Filter(Filter(x, p1), p2)` to `Filter(x, p1 AND p2)`; push filters through `Project` if no expressions reference projected-away columns; push filters through `Join` to whichever side(s) the predicate references; push filters into the `Scan` predicate slot when sargable on an existing index.

2. **Sort elimination**: `Sort(Sort(x, k1), k2)` → `Sort(x, k2)`. `Sort(x, k)` where `x` already provides `k` (proven by structural traversal) → `x`.

Other rewrites (projection pruning, constant folding, union/join flattening, predicate simplification) land when concrete cases motivate them. Adding a rule is a local change — the rule pipeline is open to extension.

Sargability classification runs as part of Phase 1: each predicate is annotated with which indexes it can be folded into and as what kind (`Equality`, `Range`, `PrefixIn`). The DP reads these annotations when enumerating access paths.

### Catalog and stats interface

```rust
pub trait PlannerCatalog {
    fn indexes(&self, table: TableId) -> &[IndexDescriptor];
    fn estimate(&self, table: TableId, predicate: &SargablePredicate) -> Cardinality;
    fn distinct_count(&self, table: TableId, column: ColumnId) -> usize;
    fn total_count(&self, table: TableId) -> usize;
}
```

Backed by the `Storage` trait but separate as a concept. Subscriptions snapshot stats at plan time; data drift is a non-issue because we do not replan (per-subscription planning, no cache).

Mocking the catalog in planner tests becomes trivial: a `TestCatalog` implementation lets us drive plan-shape tests without spinning up storage.

### Plan introspection

`PhysicalPlan` implements `Display` to render a tree:

```
TopKHeap(k=10)
└─ HashJoin(posts.author_id = users.id)
   ├─ OrderedIndexScan(posts, created_at, DESC)
   │  └─ branches: KWayMerge(2)
   └─ IndexLookup(users, id)
```

Internal only — not exposed through the TS API. Used for snapshot tests and for human debugging when planner decisions need to be inspected.

A second `Display`-like rendering includes cost annotations for cost-regression debugging; this is a separate impl behind a debug flag, not the default.

### Phase 3: lowering

Mechanical translation from `PhysicalPlan` to the graph node DAG. Each `PhysicalOp` knows how to construct its corresponding graph node(s). Most existing graph nodes are reused; new graph nodes are added for new operator algorithms (`OrderedIndexScan`, `TopKHeap`, `KWayMerge`, `IndexNestedLoop`).

`unwrap_query_envelope` is removed. `OrderBy`, `Offset`, `Limit` are no longer envelope fields — they become physical operators chosen by the planner.

### Maintenance under deltas

Subscriptions require `can_maintain_under_deltas` on every operator in the chosen plan. The DP filters candidate operators by this capability when `mode = Subscription`. Most existing graph nodes are already maintainable; new ones must declare and implement maintenance behavior at the time they are added (e.g., `TopKHeap` maintains a bounded sorted buffer; `OrderedIndexScan` re-pulls from the index on dependent change).

Operators that are useful for one-shot reads but not maintainable can still ship — they are simply absent from the subscription candidate set. This is how `mode` earns its place in the memo key.

The streaming pull protocol described in the original top-k pitch is realized through the `can_stream` capability: each operator in a streaming chain implements both batch and pull modes, and the planner assembles streaming chains only when every link supports it. The protocol itself (how a node signals "no more rows", backpressure semantics) is defined alongside the first streaming-capable operators in Phase C.

### Top-k as emergent behavior

The optimizations from `todo/projects/ordered-index-topk-query-path/2_pitch.md` are not new code paths; they are the natural output of this planner:

- **Sort elimination via ordered index**: the DP enumerates `OrderedIndexScan` as an access path; if it provides the order requested by the parent `Sort`, the `Sort` is replaced by `Sort::Eliminated`. No special-case rule.
- **Top-k fusion**: `Limit(Sort(x))` enumerates a `TopKHeap(k)` alternative; cost picks it when k is small relative to input. Falls back to `FullSort + Limit` otherwise.
- **Streaming top-k on ordered index**: when `OrderedIndexScan` provides the demanded order and the chain above it supports `can_stream`, the planner forms `LimitOffset (with StreamingStop) → Filter → PolicyFilter → Materialize(Pull) → OrderedIndexScan`. The streaming pull protocol carries the stop signal upstream.
- **Order-driven vs filter-driven**: both alternatives appear as separate plans in the memo (different access paths produce different `output_order` and different cardinality estimates). The cost function picks based on exact cardinality.
- **Multi-branch top-k**: `KWayMerge` is an operator the DP can place above per-branch `OrderedIndexScan` plans; it preserves order and produces the merged stream.
- **Cursor pagination**: `OrderedIndexScan` carries an optional `skip_past` cursor as part of its access path; the lowering passes it to the storage call. The cursor is exposed at the subscription API edge (out of scope for the planner spec, in scope for the storage extension in Phase B).
- **Order-preserving NLJ for top-k across joins**: the DP enumerates `IndexNestedLoop` with the driver chosen to satisfy the demanded order; the cost function picks it when it beats `HashJoin + Sort`. Falls back to hash join when ordering is not requested or no driver can satisfy it.
- **Multi-column ORDER BY (prefix-scan + group-sort)**: a dedicated operator algorithm above `OrderedIndexScan` that buffers ties on the prefix column and sorts within them. Eligible only when the first ORDER BY column has an index.

Each appears as one access path variant or one operator algorithm registered into the same plan space.

### Module placement

```
crates/jazz-tools/src/query_manager/planner/
    mod.rs                — entry point: plan(RelExpr, mode, demanded_order) -> PhysicalPlan
    logical.rs            — sargability annotations on RelExpr
    physical.rs           — PhysicalPlan, PhysicalOp, AccessPath, OpCapabilities
    normalize.rs          — Phase 1 rules
    enumerate.rs          — Phase 2 access path generation per table/scan
    dp.rs                 — System-R bottom-up DP
    cost.rs               — cost model + cardinality propagation
    catalog.rs            — PlannerCatalog trait + storage-backed impl
    explain.rs            — Display impls
    lower.rs              — Phase 3: PhysicalPlan -> graph node DAG
    ops/
        scan.rs
        filter.rs
        join.rs
        sort.rs
        limit.rs
        branches.rs
        policy.rs
        materialize.rs
        project.rs
        union.rs
    tests/
        normalize_tests.rs
        enumerate_tests.rs
        dp_tests.rs
        plan_shape_snapshots.rs
        cost_regression.rs
```

New graph nodes (`OrderedIndexScan`, `TopKHeap`, `KWayMerge`, `IndexNestedLoop`) live in the existing `query_manager/graph_nodes/` directory. The planner does not own graph node implementations.

## Rabbit holes

1. **Stats freshness vs subscription longevity**: subscription plans are long-lived; stats are snapshotted at plan time. If a table grows 100× after the subscription is created, the chosen plan may become wildly suboptimal. v1 ignores this. v2 could re-plan on cost divergence; v1 must not preclude that but must not deliver it. The boundary is: stats access goes through the catalog interface, which can be extended later to support snapshot-vs-live without changing the planner.

2. **Selectivity threshold tuning**: the 0.1 default for non-sargable filter selectivity will be wrong in obvious cases (e.g., a WHERE on a column with two distinct values). The right answer is per-column distinct count, which we already have. Use it when available; fall back to 0.1 otherwise. The risk is a sprawl of small selectivity heuristics; budget for one or two layers of refinement, not a dozen.

3. **PolicyFilter cost as constant**: treating policy as a fixed per-row cost is wrong for INHERITS chains that hit warm caches differently from cold ones. v1 accepts this. The risk is that order-driven plans look better than they are when policy evaluation is expensive. The runtime fallback (abandon order-driven after N × limit unsuccessful pulls) is the safety net. If the safety net trips frequently in practice, the cost model needs revisiting.

4. **DP plan space explosion**: System-R memoization keeps best-per-property; the property dimensions are `(table_set, output_order, mode)`. If `output_order` has many distinct values across plans (e.g., every column of every table), the memo grows. In practice the demanded orders are few (one per query). Cap the memo per cell and prune aggressively if needed; revisit if real workloads OOM the planner.

5. **Capability matrix correctness**: `OpCapabilities` is a fragile interface — getting it wrong on one operator silently produces invalid plans. Test exhaustively at the unit level for each operator. Consider a dynamic check during lowering: assert that the chosen plan's capability composition matches what was claimed at enumeration time.

6. **Streaming protocol semantics**: the pull-mode contract between operators (how to signal "no more rows", how backpressure works, what happens when an operator must buffer for an async dependency) needs a single canonical definition. v1 defines this when the first streaming-capable operators land; do not let it accrete one operator at a time.

7. **Sargability and predicate normalization**: pushing `WHERE a = 1 AND b > 5` requires recognizing both atoms. CNF/DNF normalization is a deep rabbit hole. v1 handles AND of literals only; OR/NOT and complex boolean trees stay as `PostFilter`. Document this clearly.

8. **Lowering churn during phased rollout**: while Phase A and B are landing, lowering must produce today's plan exactly to keep tests passing. This means the planner is gated and inert. Guard against drift: a snapshot test asserting "for queries X, Y, Z, the lowered graph is byte-identical to the legacy lowering" until the legacy path is deleted.

9. **Catalog/Storage layering**: the catalog interface is the planner's view of indexes and stats; the storage trait is the executor's view of data access. They share concepts. Avoid the catalog becoming a thin pass-through that doubles every storage call; prefer a clean per-table snapshot at plan-time.

10. **Test snapshot churn**: plan shape snapshots are sensitive to rename / reorder changes. Mitigate with stable display formatting, alphabetic tiebreak rules, and a small custom snapshot reviewer that highlights semantic changes (operator algorithm change, access path change) over cosmetic ones.

## No-gos

1. **Histograms, sampled stats, or approximate distinct counts**. Hard line. Exact `index_count` + `index_distinct_count` is the ceiling.

2. **Index hints or query-plan control from user code**. The planner decides automatically. No `/*+ use_index(...) */` style hints.

3. **Per-query replan based on stats drift**. v1 plans once at subscription creation. Replan logic is a v2 concern.

4. **A second logical IR**. `RelExpr` is the logical plan. Sargability annotations live on `RelExpr` directly.

5. **Aggregation, GROUP BY, window functions**. Out of scope for this project. The architecture must not preclude them but does not deliver them.

6. **Composite indexes**. Multi-column ORDER BY uses prefix-scan + group-sort on a single-column index. No new index types.

7. **A Cascades-style top-down memoizing optimizer**. System-R bottom-up DP is sufficient and simpler. Revisit only if join planning grows past ~10 tables in practice.

8. **A user-facing `EXPLAIN` API**. Plan introspection ships as a Rust `Display` impl, internal only.

9. **Bushy join trees in v1**. Left-deep only. Bushy is permitted by the framework but disabled by enumeration; lifted in v1.5 if needed.

## Testing strategy

Three layers of tests:

1. **Unit tests** per planner module: normalization rule unit tests, access path enumeration unit tests, cost function unit tests, capability composition unit tests. All hermetic — no storage, just `TestCatalog`.

2. **Plan shape snapshots**: for a curated set of canonical query shapes, snapshot the rendered `PhysicalPlan` tree. Failures are reviewed in PRs. Categories:
   - Single-table top-k with and without index
   - Single-table filtered top-k (order-driven vs filter-driven)
   - Multi-column ORDER BY
   - Multi-branch single-table
   - Two-table join, order on each side
   - LEFT JOIN with ordering on each side
   - Selective filter that should pick filter-driven
   - Unselective filter that should pick order-driven
   - No suitable index — falls back to full sort

3. **Execution correctness tests**: existing integration tests at the `QueryManager` / `RuntimeCore` level continue to assert correct results. As planner phases land, these tests verify that the new lowering produces results identical to today, regardless of which plan is chosen.

A fourth implicit layer is the cost-regression suite: for each canonical workload, assert the plan family (e.g., "uses ordered-index path", "no Sort node", "join algorithm is index NLJ"). This is shape-only, not full-tree, and survives unrelated tree-shape changes.

Benchmarks are out of scope for the planner spec but in scope for the rollout — see `3_scopes.md` for which phases gain benchmark coverage.
