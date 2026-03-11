# Ordered Index Top-K Query Path — Rewrite Spec (MVP)

This spec describes the optimization work on the `codex/index-first-topk-exact-match` branch in a form that can be reimplemented cleanly.

It is intentionally not a line-by-line restatement of the current branch. It extracts the behavior and invariants worth keeping, calls out where the current branch broadened scope beyond `main`, and records one structural issue that should not survive the rewrite.

Related baseline docs:

- [Query Manager](../../status-quo/query_manager.md)
- [Storage](../../status-quo/storage.md)
- [Top-N Subscription Updates](topn_subscription_updates.md)

## Problem On `main`

On `main`, paginated ordered queries are compiled as a generic pipeline:

`IndexScan -> Materialize -> Filter -> Sort -> LimitOffset -> Output`

That has two avoidable costs for `ORDER BY ... LIMIT/OFFSET` shapes:

1. The engine reads every matching row ID for the filter shape.
2. It materializes and sorts the full result set before applying pagination.

For a top-k query, this is the wrong asymptotic shape. The engine should prefer reading an ordered prefix from storage and stop once it has enough rows to satisfy the visible window.

## Scope Of The Branch

Compared with `main`, the branch adds three connected ideas:

1. An ordered index scan API in storage.
2. A planner path that can satisfy top-k queries directly from ordered scans.
3. An index-first execution path for exact-match join chains and correlated lookups.

The branch implementation mostly realizes this through `IndexedQueryNode`, with supporting storage changes and provenance/sync-scope fixes.

## Rewrite Goal

Implement a single "ordered index top-k" path that:

1. Avoids full-result sorting for eligible queries.
2. Uses ordered storage scans as the driver.
3. Stops after reading only the ordered prefix needed for `offset + limit`.
4. Preserves subscription correctness, including sync scope and output ordering.
5. Supports exact-match probe joins only where the shape is explicit and easy to reason about.

## Supported Query Shapes

### Single-table top-k

Eligible when all of the following hold:

- Single base table.
- No `include_deleted`.
- No recursive relation.
- No unsupported projection that changes ordering semantics before pagination.
- `LIMIT` or `OFFSET` is present.
- `ORDER BY` is empty or uses a supported key.
- Filter can be split into:
  - driver bounds on the ordered key
  - optional exact-match narrowing probes on other indexed columns
  - optional residual predicate on materialized tuples

### Join-assisted top-k

Supported only for linear inner-equijoin chains where each join edge is an exact-match probe.

The execution model is:

1. Pick one driver scan in ordered form.
2. For each driver row, probe the adjacent join edge by index lookup.
3. Expand left/right along the linear chain.
4. Apply residual tuple filtering.
5. Maintain global order and stop after enough visible tuples are collected.

### Explicit non-goals for MVP

- General join reordering.
- Hash joins or merge joins.
- Non-linear join graphs.
- Outer joins.
- Multi-column ordered scans from storage.
- Array subqueries inside the join-optimized path.
- Full incremental maintenance of the top-k window without rescanning.

## Planner Contract

The planner should lower eligible queries into one source node that owns:

- ordered driver scan selection
- pagination windowing
- exact-match index probes
- optional per-row policy checks
- residual tuple filtering

Single-table ordered pagination is the zero-join specialization of this same node. The rewrite should not introduce a second planner node just for the single-table case.

The planner must still preserve the downstream row-shaping stages that are semantically separate, such as:

- array subqueries after the outer window is known
- output projection when it changes row shape
- output flattening for join tuples

## Driver Selection

Choose the driver from the leading sort key.

Rules:

- If `ORDER BY` is empty, use row identity (`_id`) ascending.
- If `ORDER BY` is present, the leading term defines:
  - driver table/scope
  - scan direction
  - ordered key bounds
- Additional sort terms are evaluated in memory only within driver-key ties.
- Stable tie-breaker is always row ID ascending.

## Filter Partitioning

Partition predicates into three buckets.

### 1. Driver bounds

Use range/equality predicates on the driver key to constrain the ordered scan.

Examples:

- `score >= 10`
- `score BETWEEN 10 AND 20`
- `id < X`

### 2. Exact-match narrowing probes

For predicates on the driver scope that are exact matches on indexed columns other than the driver key, intersect candidate driver IDs before materialization.

Example:

- `WHERE name = 'Alice' ORDER BY score DESC LIMIT 5`

This should probe `name = 'Alice'` by index and use that ID set to filter ordered `score` scan candidates.

### 3. Residual predicate

Anything not fully discharged by the ordered scan or by exact-match probes stays as a tuple predicate.

This includes:

- non-driver predicates
- non-equality predicates on non-driver columns
- multi-table predicates
- expressions that require materialized row content

## Storage Contract

Storage must provide an ordered index scan primitive with:

- table
- column
- branch
- start bound
- end bound
- direction
- optional `take`
- optional `resume_after`

The primitive must return ordered cursors, not just row IDs.

Each cursor carries:

- decoded ordered value
- row ID

This is required for resumable scans when callers filter rows after scanning.

## Ordered Scan Semantics

The ordered scan must preserve:

1. Primary ordering by encoded index value.
2. Secondary ordering by row ID ascending.
3. Deterministic behavior in both ascending and descending directions.
4. Correct handling of duplicate ordered values.
5. Resume semantics defined in output order, not physical scan order.

For descending scans over duplicate values, the storage layer may need internal grouping so that emitted order remains:

- value descending
- row ID ascending

## Execution Contract

The source node should do the following:

1. Build one ordered driver stream per branch and per disjunct when needed.
2. Pull candidates in ordered batches.
3. Group candidates by the leading sort key so all ties are considered together.
4. Materialize only the rows needed to evaluate policies, probes, joins, and residual predicates.
5. Emit tuples in final query order.
6. Stop once the visible ordered prefix is known.

`desired_prefix_len = offset + limit` when `limit` exists.

If there is no `limit`, the prefix is the full ordered input because downstream replay/sync scope still needs all rows after the offset point.

## Policy Semantics

If row-level select policies apply:

- policies are evaluated before the tuple becomes visible
- policy checks may require extra row loads or inherited lookups
- the engine may need to scan more than `offset + limit` raw index entries to collect enough visible rows

The rewrite can implement this conservatively. Correctness matters more than minimizing rescans.

## Sync Scope And Provenance

This optimization is not only about visible rows.

Subscriptions must retain the full contributing ordered prefix needed to replay the same paginated result downstream.

For paginated queries:

- visible rows are not enough
- sync scope must include every row that affected the window boundary
- for `OFFSET n LIMIT m`, that means the first `n + m` ordered visible tuples

Tuple provenance must survive:

- source-node pagination
- materialization/rematerialization
- projection/select-element rewrites
- output flattening

This is required so sync propagation can answer "which rows must the downstream have to reproduce this result?".

## Clean Rewrite Guidance

The new implementation should keep the ideas but simplify the shape:

1. Have exactly one top-k source path for eligible ordered queries, including the single-table ordered-pagination case.
2. Make planner eligibility explicit and easy to inspect.
3. Keep storage ordering/resume logic isolated behind a small API.
4. Keep tuple provenance handling centralized.
5. Separate "driver scan orchestration" from "row shaping" so joins, projection, and sync scope stay understandable.

## Known Issue In The POC Branch

The branch adds both `OrderedPaginationNode` and `IndexedQueryNode`, but the planner dispatch currently calls `compile_indexed_query_plan()` before `ordered_pagination_plan_for_execution_plan()`.

Because `compile_indexed_query_plan()` accepts any single-table query with `LIMIT` or `OFFSET`, the dedicated ordered-pagination path is effectively unreachable for the exact query family it was introduced for.

For the rewrite, take the simpler route:

- delete the separate ordered-pagination node
- fold single-table ordered pagination into the single top-k path as the no-join case
- keep one planner entry for eligible ordered queries and configure it for single-table or join-assisted execution based on query shape

Do not keep two overlapping planner entries where one shadows the other.

## Acceptance Tests

The rewrite should cover at least these cases:

- single-table `ORDER BY key LIMIT n`
- single-table `ORDER BY key OFFSET n LIMIT m`
- offset-only queries
- default ordering by row ID when `ORDER BY` is omitted
- duplicate ordered values with deterministic row-ID tie-breaking
- descending scans with duplicate ordered values
- exact-match narrowing probe on a non-driver column
- exact-match linear join chain driven by a joined-table sort key
- residual filter after ordered scan
- policy-filtered paginated query where hidden rows force extra scan work
- sync-scope reporting includes the full ordered prefix
- projection/select-element output still preserves ordered output
- storage resume works in both ascending and descending directions

## Minimal Mental Model

The clean version should be explainable in one sentence:

"Read the rows in the order the query wants, probe anything that can be answered exactly by index, materialize only what survives, and stop as soon as the window boundary is known."
