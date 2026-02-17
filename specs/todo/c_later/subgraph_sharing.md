# Subgraph Sharing — TODO

Optimization opportunities for array subqueries.

> Status quo: [specs/status-quo/subgraph_sharing.md](../status-quo/subgraph_sharing.md)

## Shared Hash Index (Recommended First Optimization)

**Priority: Medium (when performance matters)**

For simple foreign-key correlations, build a single hash index instead of per-instance graphs:

```
inner_rows: HashMap<CorrelationValue, Vec<Row>>
```

- O(1) lookup per outer row vs current O(settle_cost) per row
- Single index shared across all correlations
- Inner table changes rebuild index once (not N times)
- Matches what JoinNode already does
- Keep current approach as fallback for complex subqueries (filters, joins, nested arrays)

> `crates/groove/src/query_manager/graph_nodes/array_subquery.rs:305-351` (current reevaluate_all)

## Memoized Subgraph Results

**Priority: Low**

Cache settled results keyed by correlation value. Invalidate affected entries on inner table change. Reduces redundant evaluation but adds cache invalidation complexity.

## Batched Evaluation

**Priority: Low**

Collect all unique correlation values, build single graph with `IN (v1, v2, ...)` condition, group results. Single graph settlement, better cache locality, but complex result grouping.

## Incremental Index Maintenance

**Priority: Low**

For FK correlations, maintain per-correlation-value row sets incrementally. When a post with `author_id = X` is inserted, add directly to `author_X_posts` set. O(1) update cost, no full re-evaluation.

## Metrics to Track

When optimizing, consider tracking:

- Number of subgraph instances per query
- Time in `reevaluate_all()` vs `process_with_context()`
- Cache hit rate if memoization is added
- Inner table change frequency vs outer result size
