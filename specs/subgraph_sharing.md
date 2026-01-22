# Subgraph Sharing: Learnings from Array Subquery Implementation

This document captures learnings from implementing correlated subqueries (array expressions)
using the "dynamic graph instances" approach, to inform future optimization decisions.

## Current Approach: Recompile Per Binding

For each unique outer row, we:
1. Extract the correlation value from the outer tuple
2. Create a fresh `SubgraphInstance` by calling `SubgraphTemplate::instantiate()`
3. This compiles a new `QueryGraph` with the correlation value as a filter condition
4. Settle the graph to get results
5. Store the array result

### Why This Approach?

The recompile-per-binding approach was chosen to:
1. Explore subgraph patterns without premature optimization
2. Keep the implementation simple and correct
3. Collect data about real-world usage patterns

## Observations

### Memory Overhead Per Instance

Each `SubgraphInstance` contains:
- A full `QueryGraph` with nodes, edges, dirty tracking
- Node state for IndexScan, Materialize, Filter, Sort, etc.
- Current tuple sets in each node

For a query like `users.with_array("posts", ...)`:
- 1000 users = 1000 compiled graphs
- Each graph has ~5-8 nodes depending on query complexity
- Memory is proportional to: `num_outer_rows * nodes_per_subgraph`

**Observation:** Memory usage could be significant for large outer result sets.

### Update Cost Distribution

When inner data changes (e.g., a new post is inserted):
- Currently: ALL instances are re-evaluated via `reevaluate_all()`
- Each re-evaluation settles the full subgraph
- If 1000 users exist but only 1 user's posts changed, we still re-settle all 1000 graphs

**Observation:** This is O(outer_rows * inner_change_frequency), which could be expensive.

### Common Patterns Observed

1. **Foreign key correlation**: Most array subqueries correlate on foreign key relationships
   - `posts.author_id = users.id`
   - `comments.post_id = posts.id`

2. **Shared base scan**: All subgraph instances scan the same inner table index
   - They differ only in the filter value

3. **Identical structure**: All instances have identical graph structure
   - Same nodes, edges, processing order
   - Only the filter condition changes

## Potential Optimizations

### 1. Shared Hash Index (Like JoinNode)

Instead of per-instance graphs, build a single hash index on the correlation column:

```
inner_rows: HashMap<CorrelationValue, Vec<Row>>
```

When outer row arrives:
- Probe the hash index with correlation value
- Return matching rows as array

**Pros:**
- O(1) lookup per outer row
- Single index structure shared across all correlations
- Updates to inner table only rebuild the hash index once

**Cons:**
- Less flexible than full subgraph (can't do complex filters/joins inside)
- Memory for full hash index might exceed per-instance approach for sparse correlations

### 2. Memoized Subgraph Results

Cache settled subgraph results keyed by correlation value:

```
cache: HashMap<CorrelationValue, Vec<Row>>
```

On inner table change:
- Invalidate affected cache entries
- Lazily re-evaluate on next access

**Pros:**
- Reduces redundant evaluation
- Works with complex subgraphs

**Cons:**
- Cache invalidation complexity
- Memory for cached results

### 3. Batched Evaluation

Instead of one graph per outer row, batch outer rows and evaluate together:

1. Collect all unique correlation values
2. Build single graph with `IN (v1, v2, ...)` condition
3. Group results by correlation value

**Pros:**
- Single graph settlement
- Better cache locality

**Cons:**
- More complex result grouping
- May not work well with LIMIT inside subquery

### 4. Incremental Index Maintenance

For foreign key correlations, maintain per-correlation-value row sets incrementally:

When post is inserted with `author_id = X`:
- Add to `author_X_posts` set directly
- No need to re-scan

**Pros:**
- O(1) update cost
- No full re-evaluation

**Cons:**
- Only works for simple correlations
- Requires index maintenance bookkeeping

## Recommendation for Future Work

Based on these observations, the most impactful optimization would be:

**Shared Hash Index approach** for simple foreign-key correlations:
- Handles the common case efficiently
- Matches what JoinNode already does
- Could be added as optimization path when subquery is "simple" (single table, correlation-only filter)

Keep current approach as fallback for complex subqueries with filters, joins, or nested arrays inside.

## Metrics to Track

For production use, consider tracking:
- Number of subgraph instances per query
- Time spent in `reevaluate_all()` vs `process_with_context()`
- Cache hit rate if memoization is added
- Inner table change frequency vs outer result size

These metrics would inform which optimization to prioritize.
